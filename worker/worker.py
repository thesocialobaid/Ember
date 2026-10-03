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
# Consumer name: unique per container in Docker/Kubernetes. Redis tracks
# which consumer holds each pending job by this name.
WORKER_NAME = os.getenv("HOSTNAME", f"worker-{os.getpid()}")

STREAM_KEY = "jobs"
GROUP = "workers"
BLOCK_MS = 5000
# Must be longer than BLOCK_MS (M1 lesson): otherwise an empty queue and a
# hung Redis both look like TimeoutError.
SOCKET_TIMEOUT_SECONDS = 10
STATUS_TTL_SECONDS = 3600  # same as the gateway
RESULT_TTL_SECONDS = 300
# Derivations are in the CLAUDE.md timeout budget.
# > longest job: 0.3 s + 0.03 s/token x 1024 tokens = 31.02 s
MODEL_TIMEOUT_S = float(os.getenv("MODEL_TIMEOUT_S", "45"))
# > longest hold: 10 s (SET running) + 45 s (model) + 10 s (final MULTI) = 65 s
CLAIM_IDLE_MS = int(os.getenv("CLAIM_IDLE_MS", "75000"))
RECLAIM_EVERY_S = float(os.getenv("RECLAIM_EVERY_S", "2"))
RECLAIM_COUNT = 10
# Deliveries allowed before a job is dead-lettered. Retry delay = CLAIM_IDLE_MS.
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
DEAD_KEY = "jobs:dead"
DUPLICATES_KEY = "metrics:duplicates"

# Status may only move forward: queued -> running -> done/failed.
# A script, not GET then SET: Redis runs it atomically, so no other client can
# write between the check and the write. Returns 1 if written, 0 if refused.
SET_STATUS_LUA = """
local rank = {queued = 0, running = 1, done = 2, failed = 2}
local current = redis.call('GET', KEYS[1])
local current_rank = -1
if current then current_rank = rank[current] or -1 end
if rank[ARGV[1]] <= current_rank then return 0 end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
return 1
"""
set_status = None  # registered in main()

# Graceful shutdown: SIGTERM sets this; loops stop taking work and return once
# their current job is acked.
stopping = asyncio.Event()
in_flight = 0  # jobs currently being processed, for the "draining N jobs" log


def on_sigterm():
    stopping.set()
    log.info("draining %d jobs", in_flight)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("worker")


def crash():
    # SIGKILL can't be caught, so no cleanup runs, like an OOM kill.
    # Windows has no SIGKILL; os.kill with SIGTERM there calls TerminateProcess,
    # which is just as abrupt.
    os.kill(os.getpid(), getattr(signal, "SIGKILL", signal.SIGTERM))


async def consume(name: str, r: redis.Redis, http: httpx.AsyncClient):
    global in_flight
    # A read already blocking when SIGTERM arrives may still return a job
    # (within BLOCK_MS). It is processed: dropping it would leave it pending
    # until reclaim.
    while not stopping.is_set():
        try:
            # ">": only entries never delivered to any consumer in the group.
            read = await r.xreadgroup(
                GROUP, WORKER_NAME, {STREAM_KEY: ">"}, count=1, block=BLOCK_MS
            )
        except redis.TimeoutError:
            # Socket timeout > block, so this means Redis is hung, not empty.
            log.info("%s redis did not answer within %ss", name, SOCKET_TIMEOUT_SECONDS)
            continue
        if not read:
            continue  # BLOCK expired with no new job

        _, entries = read[0]
        entry_id, job = entries[0]
        in_flight += 1
        try:
            await process(name, r, http, entry_id, job, attempts=1)  # first delivery
        finally:
            in_flight -= 1


async def dead_letter(r: redis.Redis, entry_id: str, job: dict, reason: str, attempts: int):
    # One MULTI: the job moves to jobs:dead and leaves jobs together, so it is
    # never in both or in neither.
    job_id = job["job_id"]
    result = {"error": reason, "attempts": attempts}
    async with r.pipeline(transaction=True) as pipe:
        pipe.xadd(DEAD_KEY, {**job, "reason": reason, "attempts": attempts})
        await set_status(keys=[f"status:{job_id}"], args=["failed", STATUS_TTL_SECONDS], client=pipe)
        # NX: if another worker already wrote a result, keep it.
        pipe.set(f"result:{job_id}", json.dumps(result), ex=RESULT_TTL_SECONDS, nx=True)
        pipe.xack(STREAM_KEY, GROUP, entry_id)
        pipe.xdel(STREAM_KEY, entry_id)
        acked = (await pipe.execute())[3]
    log.warning("dead-lettered %s after %d attempts: %s", job_id, attempts, reason)
    if acked == 0:
        await count_duplicate(r, job_id)


async def count_duplicate(r: redis.Redis, job_id: str):
    # XACK returned 0: the entry was no longer pending, so another worker
    # already finished this job. Harmless thanks to NX + set_status, but counted.
    log.warning("duplicate completion of %s", job_id)
    await r.incr(DUPLICATES_KEY)


async def process(
    name: str, r: redis.Redis, http: httpx.AsyncClient, entry_id: str, job: dict, attempts: int
):
    # Shared by new jobs (consume) and reclaimed ones (reclaim).
    picked_at = time.time()
    job_id = job["job_id"]
    log.info("%s picked %s (%s)", name, job_id, entry_id)

    if CRASH_AFTER_POP:
        log.info("%s CRASH_AFTER_POP set, killing process", name)
        crash()

    try:
        moved = await set_status(keys=[f"status:{job_id}"], args=["running", STATUS_TTL_SECONDS])
        if not moved:
            log.info("%s status of %s left as is (already running or finished)", name, job_id)

        started = time.perf_counter()
        # Stream fields come back as strings, so numbers are converted here.
        resp = await http.post(
            f"{MODEL_URL}/v1/completions",
            json={
                "model": job["model"],
                "prompt": job["prompt"],
                "max_tokens": int(job["max_tokens"]),
                "stream": False,
            },
        )
        resp.raise_for_status()
        inference_s = time.perf_counter() - started

        body = resp.json()
        enqueued_at = float(job["enqueued_at"])
        done_at = time.time()
        result = {
            "response": body["choices"][0]["text"],
            "completion_tokens": body["usage"]["completion_tokens"],
            "queue_wait_s": round(picked_at - enqueued_at, 3),
            "inference_s": round(inference_s, 3),
            "total_s": round(done_at - enqueued_at, 3),
            "worker": name,
            "attempts": attempts,
        }
        # Ack only after the result is written; XDEL after XACK so the
        # stream holds only unfinished jobs.
        async with r.pipeline(transaction=True) as pipe:
            # NX: the first finisher's result wins; a late duplicate can't overwrite it.
            pipe.set(f"result:{job_id}", json.dumps(result), ex=RESULT_TTL_SECONDS, nx=True)
            await set_status(keys=[f"status:{job_id}"], args=["done", STATUS_TTL_SECONDS], client=pipe)
            pipe.xack(STREAM_KEY, GROUP, entry_id)
            pipe.xdel(STREAM_KEY, entry_id)
            acked = (await pipe.execute())[2]
        if acked == 0:
            await count_duplicate(r, job_id)
    except httpx.HTTPStatusError as exc:
        reason = f"HTTP {exc.response.status_code}: {exc.response.text}"
        if exc.response.is_client_error:
            # 4xx: the request itself is wrong. Same input, same answer, so
            # retrying can't help. Dead-letter now.
            await safe_dead_letter(r, entry_id, job, reason, attempts)
            return
        # 5xx: the model may be overloaded or restarting. Retryable.
        log.warning("%s attempt %d of %s failed (%s), left pending", name, attempts, job_id, reason)
        return
    except Exception:
        # Timeouts, connection errors, anything unexpected: possibly transient.
        # No ack: reclaim() retries after CLAIM_IDLE_MS, up to MAX_ATTEMPTS.
        log.exception("%s attempt %d of %s failed, left pending", name, attempts, job_id)
        return
    log.info("%s completed %s in %.2fs (attempt %d)", name, job_id, result["total_s"], attempts)


async def safe_dead_letter(r: redis.Redis, entry_id: str, job: dict, reason: str, attempts: int):
    # If Redis fails here, the job stays pending and is dead-lettered on a
    # later reclaim, instead of the exception killing the worker loop.
    try:
        await dead_letter(r, entry_id, job, reason, attempts)
    except Exception:
        log.exception("dead-lettering %s failed, left pending", job["job_id"])


async def handle_claimed(name: str, r: redis.Redis, http: httpx.AsyncClient, entry_id: str, job: dict):
    try:
        # Redis increments times_delivered on every delivery (XREADGROUP and
        # XAUTOCLAIM), even if the worker then dies, so it counts crashes too.
        info = await r.xpending_range(STREAM_KEY, GROUP, entry_id, entry_id, 1)
    except Exception:
        log.exception("%s could not read delivery count of %s", name, entry_id)
        return
    if not info:
        return  # its original owner acked it after we claimed it
    deliveries = info[0]["times_delivered"]
    if deliveries > MAX_ATTEMPTS:
        # This delivery is the one we refuse to run, so attempts = deliveries - 1.
        reason = f"gave up after {deliveries - 1} attempts (MAX_ATTEMPTS={MAX_ATTEMPTS})"
        await safe_dead_letter(r, entry_id, job, reason, deliveries - 1)
        return
    await process(name, r, http, entry_id, job, attempts=deliveries)


async def reclaim(r: redis.Redis, http: httpx.AsyncClient):
    global in_flight
    name = f"{WORKER_NAME}-reclaim"
    while True:
        # Sleep RECLAIM_EVERY_S, but wake at once on SIGTERM: a draining
        # worker must not claim new jobs.
        try:
            await asyncio.wait_for(stopping.wait(), RECLAIM_EVERY_S)
            return
        except asyncio.TimeoutError:
            pass
        try:
            # Take over entries pending longer than CLAIM_IDLE_MS, from any
            # consumer (usually a dead one). Claiming resets their idle time.
            _, claimed, deleted = await r.xautoclaim(
                STREAM_KEY, GROUP, WORKER_NAME, CLAIM_IDLE_MS, "0-0", count=RECLAIM_COUNT
            )
        except redis.TimeoutError:
            log.info("%s redis did not answer within %ss", name, SOCKET_TIMEOUT_SECONDS)
            continue

        # Entries XDELed while still pending: the job data is gone, so there
        # is nothing to run. Ack to drop them from the pending list.
        if deleted:
            log.warning("%s acking %d deleted pending entries: %s", name, len(deleted), deleted)
            await r.xack(STREAM_KEY, GROUP, *deleted)

        # Concurrently, not one by one: each claim resets idle to 0, so a job
        # waiting behind others could pass CLAIM_IDLE_MS and be stolen again.
        in_flight += len(claimed)
        try:
            await asyncio.gather(
                *(handle_claimed(name, r, http, entry_id, job) for entry_id, job in claimed)
            )
        finally:
            in_flight -= len(claimed)


async def ensure_group(r: redis.Redis):
    # Same as the gateway: whoever starts first creates the group.
    try:
        await r.xgroup_create(STREAM_KEY, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            raise


async def main():
    global set_status
    r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=SOCKET_TIMEOUT_SECONDS)
    set_status = r.register_script(SET_STATUS_LUA)
    await ensure_group(r)
    try:
        asyncio.get_running_loop().add_signal_handler(signal.SIGTERM, on_sigterm)
    except NotImplementedError:
        pass  # Windows event loops don't support signal handlers; Docker/Linux do
    async with httpx.AsyncClient(timeout=MODEL_TIMEOUT_S) as http:
        log.info("%s starting %d consumers + reclaim", WORKER_NAME, CONCURRENCY)
        await asyncio.gather(
            *(consume(f"{WORKER_NAME}-{i}", r, http) for i in range(CONCURRENCY)),
            reclaim(r, http),
        )
    log.info("drained, exiting")


if __name__ == "__main__":
    asyncio.run(main())
