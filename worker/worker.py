import asyncio
import json
import logging
import os
import signal
import time

import httpx
import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
MODEL_URL = os.getenv("MODEL_URL", "http://localhost:8001")
CONCURRENCY = int(os.getenv("CONCURRENCY", "4"))
CRASH_AFTER_POP = os.getenv("CRASH_AFTER_POP") == "1"
WORKER_NAME = os.getenv("WORKER_NAME", f"worker-{os.getpid()}")

QUEUE_KEY = "inference_queue"
BRPOP_TIMEOUT_SECONDS = 5
STATUS_TTL_SECONDS = 3600  # same as the gateway
RESULT_TTL_SECONDS = 300
MODEL_TIMEOUT_SECONDS = 60  # httpx's default is 5 s, shorter than a long generation

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("worker")


def crash():
    # SIGKILL can't be caught, so no cleanup runs, like an OOM kill.
    # Windows has no SIGKILL; os.kill with SIGTERM there calls TerminateProcess,
    # which is just as abrupt.
    os.kill(os.getpid(), getattr(signal, "SIGKILL", signal.SIGTERM))


async def consume(name: str, r: redis.Redis, http: httpx.AsyncClient):
    while True:
        try:
            popped = await r.brpop(QUEUE_KEY, timeout=BRPOP_TIMEOUT_SECONDS)
        except redis.TimeoutError:
            # The default 5 s socket timeout fires at the same moment as the
            # 5 s BRPOP block, so an empty queue often shows up as TimeoutError.
            continue
        if popped is None:
            continue

        picked_at = time.time()
        _, raw = popped
        job = json.loads(raw)
        job_id = job["job_id"]
        log.info("%s picked %s", name, job_id)

        if CRASH_AFTER_POP:
            log.info("%s CRASH_AFTER_POP set, killing process", name)
            crash()

        await r.set(f"status:{job_id}", "running", ex=STATUS_TTL_SECONDS)

        started = time.perf_counter()
        try:
            resp = await http.post(
                f"{MODEL_URL}/v1/completions",
                json={
                    "model": job["model"],
                    "prompt": job["prompt"],
                    "max_tokens": job["max_tokens"],
                    "stream": False,
                },
            )
            resp.raise_for_status()
        except httpx.HTTPError as exc:
            await r.set(f"status:{job_id}", "failed", ex=STATUS_TTL_SECONDS)
            log.info("%s failed %s: %r", name, job_id, exc)
            continue
        inference_s = time.perf_counter() - started

        body = resp.json()
        done_at = time.time()
        result = {
            "response": body["choices"][0]["text"],
            "completion_tokens": body["usage"]["completion_tokens"],
            "queue_wait_s": round(picked_at - job["enqueued_at"], 3),
            "inference_s": round(inference_s, 3),
            "total_s": round(done_at - job["enqueued_at"], 3),
            "worker": name,
        }
        await r.set(f"result:{job_id}", json.dumps(result), ex=RESULT_TTL_SECONDS)
        log.info("%s completed %s in %.2fs", name, job_id, result["total_s"])


async def main():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    async with httpx.AsyncClient(timeout=MODEL_TIMEOUT_SECONDS) as http:
        log.info("%s starting %d consumers", WORKER_NAME, CONCURRENCY)
        await asyncio.gather(
            *(consume(f"{WORKER_NAME}-{i}", r, http) for i in range(CONCURRENCY))
        )


if __name__ == "__main__":
    asyncio.run(main())
