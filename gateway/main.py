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

r = redis.from_url(REDIS_URL, decode_responses=True)
app = FastAPI()


class GenerateRequest(BaseModel):
    prompt: str = Field(min_length=1, max_length=8000)
    max_tokens: int = Field(default=32, ge=1, le=1024)
    model: str


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
    await r.set(f"status:{job_id}", "queued", ex=STATUS_TTL_SECONDS)
    await r.lpush(QUEUE_KEY, json.dumps(job))
    return {"job_id": job_id}


@app.get("/v1/result/{job_id}")
async def result(job_id: str):
    raw = await r.get(f"result:{job_id}")
    if raw is not None:
        return {"job_id": job_id, "status": "done", "result": json.loads(raw)}
    status = await r.get(f"status:{job_id}")
    if status is not None:
        return {"job_id": job_id, "status": status}
    raise HTTPException(status_code=404, detail="job not found")


@app.get("/v1/queue")
async def queue():
    return {"queue": QUEUE_KEY, "length": await r.llen(QUEUE_KEY)}


@app.get("/health")
async def health():
    try:
        await r.ping()
    except redis.ConnectionError:
        return JSONResponse(status_code=503, content={"status": "redis unavailable"})
    return {"status": "ok"}
