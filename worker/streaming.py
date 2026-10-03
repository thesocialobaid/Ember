import json
import os
import signal
import time

import httpx
import redis.asyncio as redis

# The stream lives this long after its last entry. Same as RESULT_TTL_SECONDS:
# a client that can still fetch the result can still replay the tokens.
TOKEN_STREAM_TTL_S = 300
# Failure injection (M3 stage 5): SIGKILL this process after writing N tokens.
CRASH_MID_STREAM = int(os.getenv("CRASH_MID_STREAM", "0"))


async def stream_completion(
    r: redis.Redis, http: httpx.AsyncClient, model_url: str, job: dict, attempt: int
) -> dict:
    """Stream one completion into tokens:{job_id}. The caller writes the done entry."""
    key = f"tokens:{job['job_id']}"
    if attempt > 1:
        # Old entries stay: a connected client has already seen their IDs, and
        # deleting them can't take back what it showed. reset tells it to clear.
        await r.xadd(key, {"type": "reset", "attempt": attempt})

    tokens = []
    first_token_at = None
    async with http.stream(
        "POST",
        f"{model_url}/v1/completions",
        json={
            "model": job["model"],
            "prompt": job["prompt"],
            "max_tokens": int(job["max_tokens"]),
            "stream": True,
        },
    ) as resp:
        if resp.is_error:
            await resp.aread()  # so the caller can read exc.response.text
        resp.raise_for_status()
        async for line in resp.aiter_lines():
            # SSE from the model: "data: {...}" lines, blank lines between them.
            if not line.startswith("data: "):
                continue
            payload = line.removeprefix("data: ")
            if payload == "[DONE]":
                break
            text = json.loads(payload)["choices"][0]["text"]
            if first_token_at is None:
                first_token_at = time.time()
            # Not a transaction, just one round trip. EXPIRE on every token
            # means the stream expires 300 s after its last token, even if
            # this worker dies before reaching the end.
            async with r.pipeline(transaction=False) as pipe:
                await pipe.xadd(key, {"type": "token", "attempt": attempt, "text": text}).expire(
                    key, TOKEN_STREAM_TTL_S
                ).execute()
            tokens.append(text)
            if CRASH_MID_STREAM and len(tokens) == CRASH_MID_STREAM:
                # Same as worker.crash(): no cleanup runs, like an OOM kill.
                os.kill(os.getpid(), getattr(signal, "SIGKILL", signal.SIGTERM))

    return {
        "text": "".join(tokens),
        "completion_tokens": len(tokens),
        # Wall clock on both sides (gateway wrote enqueued_at): this is what
        # the user waits for, including queue wait.
        "ttft_s": round(first_token_at - float(job["enqueued_at"]), 3) if first_token_at else None,
    }
