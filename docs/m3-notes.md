# Ember M3: my notes

## Goal

The client should see tokens as they are generated, and a dropped connection should resume
with no gap and no repeated token. The worker never talks to the client: it writes every token
into a per-job Redis stream `tokens:{job_id}`, and the gateway turns that stream into
Server-Sent Events at `GET /v1/stream/{job_id}`. The SSE `id:` is the Redis entry ID, so the
standard `Last-Event-ID` header is also the resume cursor.

Entries in `tokens:{job_id}`: `type=reset attempt=N`, `type=token attempt=N text=...`, and
`type=done attempt=N completion_tokens=... ttft_s=...`, which is always last.

## Results (2026-10-04)

Setup: Docker Compose on my laptop, mock model (PREFILL 2 s + 0.02 s/token), Redis 7 with AOF,
2 workers with 4 consumer loops each. Full tables are in the experiment log (M3-1 to M3-5).

| Test | Result |
|---|---|
| First text visible, 32 tokens, 5 runs each | polling about 3.19 s; SSE first token about 2.09 s, last token about 2.74 s |
| Resume: 100 tokens, drop after 8 | 2 connections, 100 tokens, 0 repeats, no gaps, text == result |
| Worker SIGKILLed after 20 tokens | stream: 20 tokens (attempt 1), reset, 50 tokens (attempt 2), done; client cleared on reset, text == result |
| Late joiner after `DEL tokens:<id>` | one `done` with `fallback: true` and the stored result |
| Unknown job ID | HTTP 404 before the stream opens |
| 60 simultaneous streams of 10 tokens | 60/60 correct; gateway Redis connections 1 → 120, still 120 after 30 s |

- With SSE, even the **last** token arrived before polling showed anything. Polling waits for
  the whole answer and adds up to 0.5 s of poll delay on top.
- The client sees a token 50 to 75 ms after the worker records it (XADD, XREAD wakes up,
  HTTP chunk).
- Across the reconnect in the resume test, tokens 8 and 9 arrived 23 ms apart, about the same
  as any two tokens (about 21 ms).
- In the crash test the user saw nothing for about 8.4 s between the two attempts:
  `CLAIM_IDLE_MS` (8 s for this test) + up to one reclaim tick + prefill.

## How I built M3, step by step

1. **SSE by hand.** A throwaway `/demo` endpoint that sent five ticks. Each message is
   `id:`, `event:`, `data:` lines and then an empty line. Sending `Last-Event-ID: 3` got all
   five ticks again, because `/demo` stored nothing and ignored the header. Deleted after.
2. **The worker writes tokens.** `worker/streaming.py` calls the mock with `stream: true` and
   parses its `data:` lines until `[DONE]`. Each token is one XADD, pipelined with an EXPIRE of
   300 s, so the stream expires 300 s after its last token even if the worker dies. On attempt
   > 1 it writes a `reset` first. The attempt number is Redis' delivery count (1 for a `>` read,
   XPENDING's `times_delivered` for a reclaimed job). The final MULTI is now: SET result,
   status `done`, XADD `done`, EXPIRE, XACK, XDEL. The result gained `ttft_s`.
3. **The gateway endpoint.** `gateway/sse.py`: 404 before the stream opens if none of
   `status:`, `tokens:`, `result:` exist; then a loop of `XREAD BLOCK 2000 COUNT 100` from the
   cursor. While idle, one MULTI reads status, result and whether the stream exists, and sends
   `status`, `error`, fallback `done` or a `: keepalive` comment. It ends with a `timeout` event
   after 300 s. Its Redis client has a 5 s socket timeout, longer than the 2 s block.
4. **Resume.** `loadtest/sse_check.py` parses SSE itself, drops after N tokens, reconnects with
   `Last-Event-ID`, and checks IDs only increase, the token count matches `done`, and the text
   matches `/v1/result`.
5. **Where M2 and M3 collide.** `CRASH_MID_STREAM=N` SIGKILLs the worker after N tokens. Then
   the late joiner, the 404, and 60 streams at once.

Surprises along the way:
- **Two pools, never shrinking.** 60 streams left the gateway holding 120 Redis connections:
  about 59 last ran `xread` (the SSE client, one per open stream) and about 59 last ran `exec`,
  most likely the main client from 60 concurrent POSTs. That split is inferred from the last
  command, not proven. redis-py keeps idle pooled connections open, so the count stays at its
  peak.
- **`ttft_s` lies after a retry.** In the crash test it says 10.857 s, but I saw a first token
  at 2.12 s. It measures the attempt that succeeded, not what the user experienced.
- **Status only when idle.** The crash-test client got `status: running` only after the crash.
  Before that every XREAD returned tokens, so the loop never checked status. A queued job's
  first `status` also comes after one 2 s block, not on connect.
- **My first 60-stream run wasn't simultaneous.** Starting 60 Python processes on Windows
  staggered them, so only 39 streams were open at once. I reran it from one asyncio process.
- **Shell escaping bit me twice.** Writing `\n` through a heredoc and Python turned it into a
  real line break inside an f-string. Exact edits avoid that.

Still open:
- With streaming, `MODEL_TIMEOUT_S` limits the gap between chunks, not the whole call, so the
  65 s "longest hold" row in the timeout budget no longer strictly holds. The SSE timeouts
  aren't in the budget table yet.
- The 404 check uses the 5 s SSE client, so a hung Redis gives a 503 after 5 s, not 1 s.
- A Redis error, or a malformed `Last-Event-ID`, after the 200 has been sent just ends the
  connection.
- If a slow-but-alive worker's job is stolen, both attempts would write tokens into the same
  stream, interleaved.

## Self-check (say these out loud before opening the answers)

**1. Why does the empty line at the end of an SSE message matter?**

<details><summary>Answer</summary>

It is the only thing that ends a message. `data:` can repeat inside one message (the lines are
joined with newlines), so a single line break can't mean "done". The client buffers fields
until it sees the empty line and only then fires the event. Forget it and nothing is ever
delivered.
</details>

**2. What does the server have to do with `Last-Event-ID` to resume properly?**

<details><summary>Answer</summary>

Read it, map it to a position in the sequence of events, and send only what comes after. That
only works if the events are stored, in order, after they were sent. `/demo` stored nothing,
so it sent everything again. In Ember the ID is a stream entry ID, so the mapping step
disappears: the ID already is the position.
</details>

**3. Why must the `done` entry be in the same MULTI as the result?**

<details><summary>Answer</summary>

A client that sees `done` will fetch `/v1/result`. If `done` could land before the result, that
fetch could find nothing. In one MULTI, either both exist or neither does. It's the same lesson
as the M2 `/v1/result` race, which was fixed with one MGET.
</details>

**4. Why a reset marker instead of deleting the old tokens?**

<details><summary>Answer</summary>

A connected client has already shown the old tokens and holds their IDs. Deleting them can't
take back what it showed, and a client that only reads new entries would never notice they
were gone: it would just append attempt 2 to attempt 1. The reset is an explicit "start over"
that arrives in order, at the exact point in the stream where the retry began. Keeping the old
entries also keeps IDs strictly increasing, and leaves a history I could read with XRANGE in
the crash test.
</details>

**5. Why do status events have no id?**

<details><summary>Answer</summary>

The client sends back the last `id:` it saw as `Last-Event-ID`, and the gateway passes it
straight to XREAD. A status event isn't a stream entry, so it has no entry ID to give. Any id it
carried would become the resume cursor: a made-up one could make XREAD skip tokens or fail.
Without an id, a status event doesn't move the cursor. Status is also current state, not
history: after a reconnect the gateway re-reads it, so it never needs to be replayed.
</details>

**6. Why does passing the last ID to XREAD give resume with no extra bookkeeping?**

<details><summary>Answer</summary>

Redis assigns every entry a unique ID that only ever increases, and `XREAD ... STREAMS key <id>`
returns exactly the entries after that ID. So the client's last ID is the cursor. The client
holds it and sends it back; the server stores nothing per connection, no offsets, no sessions.
Any gateway replica can serve the resume. Tokens produced while the client was away are already
in the stream, so the first XREAD after a reconnect returns them at once (resume test: 23 ms).
</details>

**7. If the client ignored reset events, what would the user see?**

<details><summary>Answer</summary>

Both attempts glued together: in the crash test, 20 tokens from attempt 1 followed by all 50
from attempt 2, so 70 tokens, with the beginning of the answer repeated. The text wouldn't
match `/v1/result`. The mock is deterministic, so it looks like a repeat; a real model with
sampling would give a different continuation, so the user would see an answer that changes
direction halfway through.
</details>

**8. Why does the SSE endpoint need its own Redis client?**

<details><summary>Answer</summary>

It's the M1 lesson. The gateway's main client has a 1 s socket timeout so ordinary requests
fail fast as a 503. An `XREAD BLOCK 2000` on that client would raise TimeoutError on every idle
read, and an empty stream would look like a hung Redis. The SSE client's 5 s timeout is longer
than the 2 s block, so a timeout there can only mean Redis really hung. The cost: each open
stream holds one of its connections (M3-5).
</details>
