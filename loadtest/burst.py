import argparse
import asyncio
import math
import time

import httpx

POLL_INTERVAL_SECONDS = 0.5
PAYLOAD = {"model": "mock", "prompt": "hello there", "max_tokens": 32}


def percentile(values: list[float], p: float) -> float:
    # Nearest-rank: the smallest value with at least p% of values at or below it.
    ordered = sorted(values)
    rank = math.ceil(p / 100 * len(ordered))
    return ordered[max(rank, 1) - 1]


async def one_job(http: httpx.AsyncClient, timeout: float) -> dict:
    sent_at = time.perf_counter()
    resp = await http.post("/v1/generate", json=PAYLOAD)
    if resp.status_code != 202:
        # The gateway refused it, so there is no job to wait for.
        return {"outcome": "failed", "job_id": None}
    job_id = resp.json()["job_id"]

    while time.perf_counter() - sent_at < timeout:
        await asyncio.sleep(POLL_INTERVAL_SECONDS)
        body = (await http.get(f"/v1/result/{job_id}")).json()
        status = body.get("status")
        if status == "done":
            return {
                "outcome": "done",
                "job_id": job_id,
                # Measured by the client, so it includes up to 0.5 s of polling delay.
                "e2e_s": time.perf_counter() - sent_at,
                "queue_wait_s": body["result"]["queue_wait_s"],
            }
        if status == "failed":
            return {"outcome": "failed", "job_id": job_id}
    return {"outcome": "lost", "job_id": job_id}


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-n", type=int, default=50, help="requests to fire at once")
    parser.add_argument("--timeout", type=float, default=60, help="seconds before a job counts as LOST")
    parser.add_argument("--url", default="http://localhost:8000", help="gateway base URL")
    args = parser.parse_args()

    async with httpx.AsyncClient(base_url=args.url) as http:
        results = await asyncio.gather(*(one_job(http, args.timeout) for _ in range(args.n)))

    done = [r for r in results if r["outcome"] == "done"]
    failed = [r for r in results if r["outcome"] == "failed"]
    lost = [r for r in results if r["outcome"] == "lost"]

    print(f"sent {args.n}  done {len(done)}  failed {len(failed)}  LOST {len(lost)}")
    if done:
        e2e = [r["e2e_s"] for r in done]
        waits = [r["queue_wait_s"] for r in done]
        print(f"end-to-end  p50 {percentile(e2e, 50):.2f}s  p95 {percentile(e2e, 95):.2f}s  max {max(e2e):.2f}s")
        print(f"queue wait  p50 {percentile(waits, 50):.2f}s  p95 {percentile(waits, 95):.2f}s  mean {sum(waits) / len(waits):.2f}s")
    if lost:
        print("lost job ids:", " ".join(r["job_id"] for r in lost[:10]))


if __name__ == "__main__":
    asyncio.run(main())
