# CLAUDE.md

Ember is a **learning project**: SLO-aware, scale-to-zero LLM inference on Kubernetes
(see README.md for architecture and roadmap). The author must be able to explain every
line they commit. Optimize for understanding, not speed.

## How to work in this repo

- **Explain before code.** Before writing any code, explain in plain words:
  1. what the piece does,
  2. why it exists,
  3. one alternative you considered and why you rejected it.
  Keep the explanation under 10 lines.
- **One concept per step.** Do one thing, then stop. Don't bundle unrelated changes.
- **Don't create files that weren't asked for.** No speculative scaffolding, helpers,
  tests, configs, or docs unless requested.
- **Don't fix bugs you weren't asked to fix.** Some bugs are intentional so they can be
  measured (e.g. at-most-once delivery before M2). If you notice one, you may mention it
  in one line; do not change it.
- **docs/design-doc.md is the author's own reasoning.** Never write reasoning, decisions,
  or rationale into it. Only critique what the author has written, when asked.

## Stack

- Python 3.12 (venv in `.venv`; create with `py -3.12 -m venv .venv` — the default
  `python` on this machine is a different version)
- FastAPI (async)
- redis-py, asyncio API (`redis.asyncio`)
- httpx (async client)

## Milestone M2: reliable delivery

Replace the Redis list queue with Redis Streams and consumer groups so no job is lost
when a worker dies.

- Keep M1's rules: explain before coding, one concept per step, no unrequested fixes.
- M1 lesson: any blocking Redis call needs a socket timeout longer than its block time.
- Success criterion: the M1 crash test (kill a worker holding jobs) ends with LOST = 0.
- Every timeout must be explained in terms of the others (see the budget below).

## Timeout budget

Each line states what a timeout must be longer (or shorter) than, and why.

| Timeout | Value | Constraint |
|---|---|---|
| XREADGROUP block | 5 s | Short, so the loop wakes up regularly. |
| Worker Redis socket timeout | 10 s | > block (5 s), so an empty queue never looks like a hung Redis. |
| Gateway Redis socket timeout | 1 s | Gateway makes no blocking calls; a hung Redis must fail fast as a 503. |
| Longest legitimate model call | 31.02 s | max_tokens cap 1024. Using 0.3 s + 0.03 s/token: 0.3 + 0.03 × 1024 = 31.02 s. (Mock as coded: PREFILL 2 s + 0.02 s × 1023 = 22.46 s; the larger bound is used.) |
| MODEL_TIMEOUT_S (httpx) | 45 s | > 31.02 s longest call, ~14 s slack. Assumes the read phase dominates (httpx applies it per phase, not as a total). |
| Longest hold (delivery → ack) | 65 s | SET running (≤ 10 s socket) + model (≤ 45 s) + final MULTI (≤ 10 s). |
| CLAIM_IDLE_MS | 75 000 ms | > 65 s longest hold, so a slow-but-alive job is never stolen. |
| RECLAIM_EVERY_S | 2 s | Adds at most 2 s to recovery. |
| Worst-case crash recovery | 77 s | CLAIM_IDLE (75 s) + RECLAIM_EVERY (2 s). |
| burst.py --timeout (crash test) | ≥ 110 s | > recovery (77 s) + longest job (31 s); the 60 s default would count reclaimed jobs as LOST. |
| stop_grace_period (worker) | 90 s | > slowest drain: XAUTOCLAIM (10 s) + XPENDING (10 s) + longest hold (65 s) = 85 s. Kubernetes' default terminationGracePeriodSeconds (30 s) is too short; M4 must set it. |
| Status TTL | 1 h | Far longer than any job's lifetime. |
| Result TTL | 300 s | Client must poll within 5 min of completion. |
