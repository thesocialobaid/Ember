import json
import os
import secrets
import time
from contextlib import asynccontextmanager

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_KEY = "jobs"
GROUP = "workers"
STATUS_TTL_SECONDS = 3600

r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=1, socket_connect_timeout=1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # "0": the group sees every entry already in the stream, so none are skipped.
    # MKSTREAM: create the stream if it doesn't exist yet.
    try:
        await r.xgroup_create(STREAM_KEY, GROUP, id="0", mkstream=True)
    except redis.ResponseError as e:
        if "BUSYGROUP" not in str(e):  # BUSYGROUP: the worker already created it
            raise
    yield


app = FastAPI(lifespan=lifespan)


@app.exception_handler(redis.ConnectionError)
@app.exception_handler(redis.TimeoutError)
async def redis_unavailable(request, exc):
    return JSONResponse(status_code=503, content={"detail": "redis unavailable"})


class GenerateRequest(BaseModel):
    # pattern r"\S": must contain at least one non-whitespace character
    prompt: str = Field(min_length=1, max_length=8000, pattern=r"\S")
    max_tokens: int = Field(default=32, ge=1, le=1024)
    model: str = Field(pattern=r"\S")


@app.post("/v1/generate", status_code=202)
async def generate(req: GenerateRequest):
    job_id = secrets.token_hex(6)  # 6 random bytes -> 12 hex chars
    job = {
        "job_id": job_id,
        "prompt": req.prompt,
        "max_tokens": req.max_tokens,
        "model": req.model,
        "enqueued_at": time.time(),
    }
    # MULTI/EXEC: Redis applies both commands or neither.
    # No MAXLEN: trimming by count could delete jobs that were never acked.
    async with r.pipeline(transaction=True) as pipe:
        await (
            pipe.set(f"status:{job_id}", "queued", ex=STATUS_TTL_SECONDS)
            .xadd(STREAM_KEY, job)
            .execute()
        )
    return {"job_id": job_id}


@app.get("/v1/result/{job_id}")
async def result(job_id: str):
    # One MGET, not two GETs: the worker writes result and status in one MULTI,
    # and a single command sees both from the same moment. Two GETs could
    # straddle that MULTI and report "done" with no result.
    raw, status = await r.mget(f"result:{job_id}", f"status:{job_id}")
    if raw is not None:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="stored result is not valid JSON")
        # A result exists for both "done" and "failed" (dead-lettered) jobs;
        # it carries "attempts" either way.
        return {"job_id": job_id, "status": status or "done", "result": parsed}
    if status is not None:
        return {"job_id": job_id, "status": status}
    raise HTTPException(status_code=404, detail="job not found")


@app.get("/v1/queue")
async def queue():
    # length counts every entry ever added (acked ones stay until trimmed);
    # pending counts entries delivered to a worker but not yet acked.
    length = await r.xlen(STREAM_KEY)
    pending = await r.xpending(STREAM_KEY, GROUP)
    return {"stream": STREAM_KEY, "length": length, "pending": pending["pending"]}


@app.get("/v1/metrics")
async def metrics():
    # Incremented by workers when XACK returns 0 (another worker already finished the job).
    return {"duplicates": int(await r.get("metrics:duplicates") or 0)}


@app.get("/health")
async def health():
    await r.ping()  # if Redis is down, redis_unavailable() returns 503
    return {"status": "ok"}
