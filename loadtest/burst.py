import argparse
import asyncio
import collections
import math
import os
import subprocess
import time
from pathlib import Path

import httpx

POLL_INTERVAL_SECONDS = 0.5
RETRY_FIRST_DELAY_SECONDS = 0.25
RETRY_MAX_DELAY_SECONDS = 2
retried_503s = 0  # how many 503s were retried, printed in the summary
PAYLOAD = {"model": "mock", "prompt": "hello there", "max_tokens": 32}
REPO_ROOT = Path(__file__).resolve().parent.parent
# --crash: a rescued job waits up to CLAIM_IDLE (75 s) + RECLAIM_EVERY (2 s)
# + one job (31 s) before it is done (CLAUDE.md timeout budget).
CRASH_MIN_TIMEOUT_SECONDS = 110


def percentile(values: list[float], p: float) -> float:
    # Nearest-rank: the smallest value with at least p% of values at or below it.
    ordered = sorted(values)
    rank = math.ceil(p / 100 * len(ordered))
    return ordered[max(rank, 1) - 1]


async def request_retrying_503(http: httpx.AsyncClient, method: str, url: str, deadline: float, **kwargs):
    # 503 = the gateway is up but Redis isn't (e.g. during a Redis restart).
    # Retry with backoff instead of crashing; give up at the job's deadline.
    global retried_503s
    delay = RETRY_FIRST_DELAY_SECONDS
    while True:
        resp = await http.request(method, url, **kwargs)
        if resp.status_code != 503 or time.perf_counter() + delay > deadline:
            return resp
        retried_503s += 1
        await asyncio.sleep(delay)
        delay = min(delay * 2, RETRY_MAX_DELAY_SECONDS)


async def one_job(http: httpx.AsyncClient, timeout: float) -> dict:
    sent_at = time.perf_counter()
    deadline = sent_at + timeout
    resp = await request_retrying_503(http, "POST", "/v1/generate", deadline, json=PAYLOAD)
    if resp.status_code != 202:
        # The gateway refused it, so there is no job to wait for.
        return {"outcome": "failed", "job_id": None, "reason": f"POST returned HTTP {resp.status_code}"}
    job_id = resp.json()["job_id"]

    while time.perf_counter() - sent_at < timeout:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        resp = await request_retrying_503(http, "GET", f"/v1/result/{job_id}", deadline)
        if resp.status_code == 503:
            break  # still 503 at the deadline
        # 404 after a Redis restart means the job's keys are gone: keep polling,
        # and it ends up LOST at the timeout.
        body = resp.json()
        status = body.get("status")
        if status in ("done", "failed") and "result" not in body:
            continue  # the gateway can briefly report a status before its result; poll again
        if status == "done":
            return {
                "outcome": "done",
                "job_id": job_id,
                # Measured by the client, so it includes up to 0.5 s of polling delay.
                "e2e_s": time.perf_counter() - sent_at,
                "queue_wait_s": body["result"]["queue_wait_s"],
                # > 1 means the job was delivered more than once (e.g. rescued after a crash).
                "attempts": body["result"].get("attempts", 1),
            }
        if status == "failed":
            return {"outcome": "failed", "job_id": job_id, "reason": body["result"].get("error", "unknown")}
    return {"outcome": "lost", "job_id": job_id}


def docker(*args: str, env: dict | None = None) -> str:
    out = subprocess.run(
        ["docker", *args], cwd=REPO_ROOT, env={**os.environ, **(env or {})},
        capture_output=True, text=True, check=True,
    )
    return out.stdout.strip()


async def start_crash_worker() -> str:
    # The M1 crash test, automated: no other workers, then one that kills
    # itself (SIGKILL) right after its first pick.
    await asyncio.to_thread(docker, "compose", "stop", "worker")
    container = await asyncio.to_thread(
        docker, "compose", "run", "-d", "--no-deps", "-e", "CRASH_AFTER_POP=1", "worker"
    )
    # Wait until it is reading, so it is the one that receives the burst.
    for _ in range(60):
        logs = await asyncio.to_thread(docker, "logs", container)
        if "starting" in logs:
            break
        await asyncio.sleep(0.5)
    return container


async def replace_after_crash(container: str, started_at: float) -> float:
    # Wait for the crash worker to die, then start a healthy one.
    while await asyncio.to_thread(docker, "inspect", "-f", "{{.State.Status}}", container) != "exited":
        await asyncio.sleep(0.2)
    crashed_s = time.perf_counter() - started_at
    await asyncio.to_thread(docker, "compose", "up", "-d", "worker", env={"CRASH_AFTER_POP": "0"})
    return crashed_s


async def duplicates_counter(http: httpx.AsyncClient) -> int | None:
    try:
        return (await http.get("/v1/metrics")).json()["duplicates"]
    except (httpx.HTTPError, KeyError, ValueError):
        return None


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", type=int, default=50, help="requests to fire at once")
    parser.add_argument("--timeout", type=float, default=60, help="seconds before a job counts as LOST")
    parser.add_argument("--url", default="http://localhost:8000", help="gateway base URL")
    parser.add_argument("--crash", action="store_true",
                        help="crash test: a CRASH_AFTER_POP worker takes the burst, then a healthy one replaces it")
    args = parser.parse_args()
    if args.crash and args.timeout < CRASH_MIN_TIMEOUT_SECONDS:
        parser.error(f"--crash needs --timeout >= {CRASH_MIN_TIMEOUT_SECONDS} (claim idle + reclaim + one job)")

    async with httpx.AsyncClient(base_url=args.url) as http:
        dup_before = await duplicates_counter(http)
        if args.crash:
            container = await start_crash_worker()
        started_at = time.perf_counter()
        jobs = asyncio.gather(*(one_job(http, args.timeout) for _ in range(args.n)))
        if args.crash:
            results, crashed_s = await asyncio.gather(jobs, replace_after_crash(container, started_at))
            await asyncio.to_thread(docker, "rm", container)
        else:
            results = await jobs
        dup_after = await duplicates_counter(http)

    done = [r for r in results if r["outcome"] == "done"]
    failed = [r for r in results if r["outcome"] == "failed"]
    lost = [r for r in results if r["outcome"] == "lost"]

    duplicates = "n/a" if dup_before is None or dup_after is None else dup_after - dup_before
    print(f"sent {args.n}  done {len(done)}  failed {len(failed)}  LOST {len(lost)}  "
          f"duplicates {duplicates}  (503s retried: {retried_503s})")
    for reason, count in collections.Counter(r["reason"] for r in failed).most_common():
        print(f"  failed x{count}: {reason}")
    if args.crash:
        rescued = sum(1 for r in done if r["attempts"] > 1)
        print(f"crash worker died at {crashed_s:.1f}s; healthy worker started; rescued (attempts > 1): {rescued}")
    if done:
        e2e = [r["e2e_s"] for r in done]
        waits = [r["queue_wait_s"] for r in done]
        print(f"end-to-end  p50 {percentile(e2e, 50):.2f}s  p95 {percentile(e2e, 95):.2f}s  max {max(e2e):.2f}s")
        print(f"queue wait  p50 {percentile(waits, 50):.2f}s  p95 {percentile(waits, 95):.2f}s  mean {sum(waits) / len(waits):.2f}s")
    if lost:
        print("lost job ids:", " ".join(r["job_id"] for r in lost[:10]))


if __name__ == "__main__":
    asyncio.run(main())
