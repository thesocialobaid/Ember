import asyncio
import json
import os
import time
import uuid

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

PREFILL_SECONDS = float(os.getenv("PREFILL_SECONDS", "2"))
SECONDS_PER_TOKEN = float(os.getenv("SECONDS_PER_TOKEN", "0.02"))

FAKE_WORDS = ["the", "quick", "brown", "fox", "jumps", "over", "a", "lazy", "dog"]

app = FastAPI()


class CompletionRequest(BaseModel):
    model: str
    prompt: str
    max_tokens: int = 16
    stream: bool = False


def fake_token(i: int) -> str:
    return FAKE_WORDS[i % len(FAKE_WORDS)] + " "


async def generate_tokens(max_tokens: int):
    await asyncio.sleep(PREFILL_SECONDS)
    for i in range(max_tokens):
        # First token is produced by prefill; each later one costs a decode step.
        if i > 0:
            await asyncio.sleep(SECONDS_PER_TOKEN)
        yield fake_token(i)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/v1/completions")
async def completions(req: CompletionRequest):
    # Failure injection: a marker in the prompt forces an error.
    if "POISON" in req.prompt:
        raise HTTPException(status_code=500, detail="injected server error (POISON)")
    if "BADREQUEST" in req.prompt:
        raise HTTPException(status_code=400, detail="injected bad request (BADREQUEST)")

    completion_id = f"cmpl-{uuid.uuid4().hex}"
    created = int(time.time())
    prompt_tokens = len(req.prompt.split())

    if req.stream:
        async def event_stream():
            i = 0
            async for token in generate_tokens(req.max_tokens):
                i += 1
                chunk = {
                    "id": completion_id,
                    "object": "text_completion",
                    "created": created,
                    "model": req.model,
                    "choices": [{
                        "index": 0,
                        "text": token,
                        "logprobs": None,
                        "finish_reason": "length" if i == req.max_tokens else None,
                        "stop_reason": None,
                    }],
                }
                yield f"data: {json.dumps(chunk)}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    text = "".join([t async for t in generate_tokens(req.max_tokens)])
    return {
        "id": completion_id,
        "object": "text_completion",
        "created": created,
        "model": req.model,
        "choices": [{
            "index": 0,
            "text": text,
            "logprobs": None,
            "finish_reason": "length",
            "stop_reason": None,
        }],
        "usage": {
            "prompt_tokens": prompt_tokens,
            "total_tokens": prompt_tokens + req.max_tokens,
            "completion_tokens": req.max_tokens,
        },
    }
