"""Hand-written SSE client for GET /v1/stream/{job_id}.

Checks resume: drops the connection after N tokens, reconnects with
Last-Event-ID, and verifies the reassembled text has no gaps and no repeats.
Run: .venv\\Scripts\\python -u loadtest\\sse_check.py --max-tokens 100 --drop-after 8
"""
import argparse
import asyncio
import json
import time

import httpx


async def read_events(resp: httpx.Response):
    # SSE parsing: "field: value" lines; an empty line ends one message;
    # lines starting with ":" are comments (keepalives).
    event = {}
    async for line in resp.aiter_lines():
        if line == "":
            if event:
                yield event
            event = {}
        elif line.startswith(":"):
            continue
        else:
            name, _, value = line.partition(":")
            value = value.removeprefix(" ")
            if name == "data" and "data" in event:
                event["data"] += "\n" + value  # several data lines are joined with newlines
            else:
                event[name] = value


def id_key(entry_id: str) -> tuple[int, int]:
    # Redis stream IDs are "<ms>-<seq>"; compare them as numbers, not strings.
    ms, seq = entry_id.split("-")
    return int(ms), int(seq)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:8080", help="gateway base URL")
    parser.add_argument("--job-id", help="stream an existing job instead of submitting one")
    parser.add_argument("--max-tokens", type=int, default=32)
    parser.add_argument("--drop-after", type=int, default=0, help="disconnect once after N token events")
    args = parser.parse_args()

    async with httpx.AsyncClient(base_url=args.url, timeout=httpx.Timeout(10, read=60)) as http:
        started = time.perf_counter()
        job_id = args.job_id
        if job_id is None:
            resp = await http.post(
                "/v1/generate", json={"model": "mock", "prompt": "hello", "max_tokens": args.max_tokens}
            )
            job_id = resp.json()["job_id"]
        print(f"job {job_id}")

        text = ""
        last_id = None
        connections = token_events = tokens_since_reset = repeats = 0
        arrivals = []  # seconds since start, one per token event
        dropped = finished = fallback = False
        done_tokens = None  # completion_tokens from the done event

        while not finished:
            connections += 1
            headers = {"Last-Event-ID": last_id} if last_id else {}
            print(f"connection {connections}: Last-Event-ID={last_id}")
            async with http.stream("GET", f"/v1/stream/{job_id}", headers=headers) as resp:
                if resp.status_code != 200:
                    await resp.aread()
                    print(f"HTTP {resp.status_code}: {resp.text}")
                    return
                async for ev in read_events(resp):
                    kind = ev.get("event", "message")
                    data = json.loads(ev.get("data", "null"))
                    if "id" in ev:
                        # Every event with an id must come strictly after the last one.
                        if last_id and id_key(ev["id"]) <= id_key(last_id):
                            repeats += 1
                        last_id = ev["id"]

                    if kind == "token":
                        arrivals.append(time.perf_counter() - started)
                        text += data["text"]
                        token_events += 1
                        tokens_since_reset += 1
                    elif kind == "reset":
                        print(f"  reset: attempt {data['attempt']} starts over, "
                              f"discarding {tokens_since_reset} tokens ({len(text)} chars)")
                        text = ""
                        tokens_since_reset = 0
                    elif kind == "status":
                        print(f"  status: {data['status']}")
                    elif kind == "done":
                        if data.get("fallback"):
                            print("  done (fallback): token stream gone, using the stored result")
                            text = data["result"]["response"]
                            fallback = True
                            done_tokens = data["result"]["completion_tokens"]
                        else:
                            print(f"  done: attempt {data['attempt']}, {data['completion_tokens']} tokens, "
                                  f"server ttft {data['ttft_s']} s")
                            done_tokens = int(data["completion_tokens"])
                        finished = True
                    elif kind == "error":
                        print(f"  error: {data['reason']}")
                        finished = True
                    elif kind == "timeout":
                        print(f"  timeout: resume from {data['resume_from']}")
                        last_id = data["resume_from"]

                    if args.drop_after and not dropped and token_events == args.drop_after:
                        print(f"  dropping connection after {token_events} token events (last id {last_id})")
                        dropped = True
                        break
            if not finished and not dropped:
                print("  server closed the stream without done; reconnecting")

        result = (await http.get(f"/v1/result/{job_id}")).json().get("result", {})

    print(f"\n{connections} connections, {token_events} token events, "
          f"{tokens_since_reset} tokens in the final attempt")
    if arrivals:
        print(f"client ttft {arrivals[0]:.3f} s, last token {arrivals[-1]:.3f} s")
        print("token arrival times (s since start):")
        for i in range(0, len(arrivals), 10):
            print(f"  {i + 1:>4}: " + " ".join(f"{t:.3f}" for t in arrivals[i:i + 10]))
    # A fallback replaces the text with the stored result, so there is nothing to count.
    gaps_ok = fallback or tokens_since_reset == done_tokens
    match = text == result.get("response")
    print(f"repeats (ids not increasing): {repeats}")
    print(f"no gaps (tokens since last reset == done count {done_tokens}): {gaps_ok}")
    print(f"text == /v1/result response: {match}  ({len(text)} chars)")
    print("PASS" if repeats == 0 and gaps_ok and match else "FAIL")


if __name__ == "__main__":
    asyncio.run(main())
