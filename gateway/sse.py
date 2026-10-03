import json
import os
import time

import redis.asyncio as redis
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import StreamingResponse

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
# Short, so the loop wakes up regularly to check status, keepalive and disconnects.
SSE_BLOCK_MS = int(os.getenv("SSE_BLOCK_MS", "2000"))
# > block (M1 lesson): with the gateway's 1 s client, every idle XREAD would
# raise TimeoutError. Here a timeout can only mean Redis hung.
SSE_SOCKET_TIMEOUT_S = float(os.getenv("SSE_SOCKET_TIMEOUT_S", "5"))
# Comment line after this much silence, so proxies don't close an idle connection.
SSE_KEEPALIVE_S = float(os.getenv("SSE_KEEPALIVE_S", "15"))
# Longest one connection lives; the client resumes from the cursor in the timeout event.
SSE_MAX_STREAM_S = float(os.getenv("SSE_MAX_STREAM_S", "300"))
if SSE_SOCKET_TIMEOUT_S * 1000 <= SSE_BLOCK_MS:
    raise ValueError("SSE_SOCKET_TIMEOUT_S must be longer than SSE_BLOCK_MS")

# Dedicated client for the blocking XREADs; the gateway's main client keeps its
# 1 s timeout so ordinary requests still fail fast. Each open stream holds one
# pooled connection while it blocks.
stream_r = redis.from_url(
    REDIS_URL, decode_responses=True, socket_timeout=SSE_SOCKET_TIMEOUT_S, socket_connect_timeout=1
)

router = APIRouter()


def sse(event: str, data: dict, id: str | None = None) -> str:
    # One message: optional id, event name, one JSON data line, blank line to end it.
    head = f"id: {id}\n" if id else ""
    return f"{head}event: {event}\ndata: {json.dumps(data)}\n\n"


async def events(job_id: str, cursor: str, request: Request):
    key = f"tokens:{job_id}"
    started = last_sent = time.monotonic()
    last_status = None
    while True:
        if await request.is_disconnected():
            return
        if time.monotonic() - started >= SSE_MAX_STREAM_S:
            yield sse("timeout", {"resume_from": cursor})
            return

        # Entries strictly after cursor. A missing key just blocks until it appears.
        read = await stream_r.xread({key: cursor}, count=100, block=SSE_BLOCK_MS)
        if read:
            for entry_id, fields in read[0][1]:
                cursor = entry_id
                yield sse(fields["type"], fields, id=entry_id)
                if fields["type"] == "done":
                    return
            last_sent = time.monotonic()
            continue

        # Idle. One MULTI, so status, result and the stream are seen from the
        # same moment (the worker writes them in one MULTI too).
        async with stream_r.pipeline(transaction=True) as pipe:
            (status, raw), stream_exists = await pipe.mget(
                f"status:{job_id}", f"result:{job_id}"
            ).exists(key).execute()
        result = json.loads(raw) if raw else {}
        if status == "failed":
            yield sse("error", {"reason": result.get("error", "unknown")})
            return
        if status == "done" and not stream_exists:
            # Token stream expired (or was deleted): send the stored result instead.
            yield sse("done", {"fallback": True, "result": result})
            return
        if status in ("queued", "running") and status != last_status:
            yield sse("status", {"status": status})
            last_status = status
            last_sent = time.monotonic()
        elif time.monotonic() - last_sent >= SSE_KEEPALIVE_S:
            # A line starting with ":" is a comment: clients ignore it.
            yield ": keepalive\n\n"
            last_sent = time.monotonic()


@router.get("/v1/stream/{job_id}")
async def stream(job_id: str, request: Request, last_event_id: str | None = Header(default=None)):
    # Checked before the response starts: once the 200 and headers are sent,
    # a 404 is no longer possible.
    if not await stream_r.exists(f"status:{job_id}", f"tokens:{job_id}", f"result:{job_id}"):
        raise HTTPException(status_code=404, detail="job not found")
    # "0-0": from the beginning. Otherwise the client's last ID, which is a
    # stream entry ID, so XREAD resumes right after it.
    cursor = last_event_id or "0-0"
    return StreamingResponse(
        events(job_id, cursor, request),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # tells nginx not to buffer the response
            "Connection": "keep-alive",
        },
    )
