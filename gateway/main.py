import json
import os
import secrets
import time

import redis.asyncio as redis
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
QUEUE_KEY = "inference_queue"
STATUS_TTL_SECONDS = 3600

r = redis.from_url(REDIS_URL, decode_responses=True, socket_timeout=1, socket_connect_timeout=1)
app = FastAPI()


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
    # MULTI/EXEC: Redis applies both commands or neither
    async with r.pipeline(transaction=True) as pipe:
        await (
            pipe.set(f"status:{job_id}", "queued", ex=STATUS_TTL_SECONDS)
            .lpush(QUEUE_KEY, json.dumps(job))
            .execute()
        )
    return {"job_id": job_id}


@app.get("/v1/result/{job_id}")
async def result(job_id: str):
    raw = await r.get(f"result:{job_id}")
    if raw is not None:
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            raise HTTPException(status_code=500, detail="stored result is not valid JSON")
        return {"job_id": job_id, "status": "done", "result": parsed}
    status = await r.get(f"status:{job_id}")
    if status is not None:
        return {"job_id": job_id, "status": status}
    raise HTTPException(status_code=404, detail="job not found")


@app.get("/v1/queue")
async def queue():
    return {"queue": QUEUE_KEY, "length": await r.llen(QUEUE_KEY)}


@app.get("/health")
async def health():
    await r.ping()  # if Redis is down, redis_unavailable() returns 503
    return {"status": "ok"}
