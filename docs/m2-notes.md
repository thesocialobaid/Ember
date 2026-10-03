# Ember M2: my notes

## Crash test results (2026-10-03)

Setup: Docker Compose on my laptop, mock model, Redis 7. Redis Streams with consumer group
`workers`, ack after the result is written, XAUTOCLAIM reclaim loop. Overrides for this test
(32-token jobs only): `CLAIM_IDLE_MS=10000`, `MODEL_TIMEOUT_S=5`, burst `--timeout 60`.

Procedure: start a worker with `CRASH_AFTER_POP=1`, fire a burst of 50 jobs, start a healthy
worker about 4.5 s after the crash.

| Metric | M1 (BRPOP) | M2 (Streams + XAUTOCLAIM) |
|---|---|---|
| Jobs sent | 50 | 50 |
| Done | — | 50 |
| LOST | 4 destroyed by the crash | **0** |
| Jobs held by the crashed worker | 4 | 1 |
| Crash → rescued job reclaimed | never | 10.5 s |
| Crash → rescued job done | never | 13.2 s |

- Rescued job `5672e8b73109`: `queue_wait_s` 10.51, `inference_s` 2.65, `total_s` 13.16,
  `worker` = `c3a4728f621f-reclaim` (the healthy worker's reclaim loop).
- Burst: end-to-end p50 21.30 s, p95 37.05 s, max 39.89 s; queue wait p50 17.88 s,
  p95 33.88 s, mean 19.39 s.
- After the run, `XINFO CONSUMERS` still lists the dead consumer `5c7daf976df4`, with 0 pending.

## Retry and dead-letter results (2026-10-03)

Setup: same Compose stack, one worker with `CLAIM_IDLE_MS=10000`, `MODEL_TIMEOUT_S=5`,
`MAX_ATTEMPTS=3`. The mock model returns HTTP 500 for prompts containing `POISON` and
HTTP 400 for `BADREQUEST`. Three jobs sent at once, each polled every 0.25 s until it
reached `done` or `failed`.

| Job | Final status | Time to final status | Attempts | Path |
|---|---|---|---|---|
| `please POISON me` | failed | 30.8 s | 3 | 500 at 0 s, retried at +10.5 s and +20.5 s, dead-lettered at +30.6 s |
| `a BADREQUEST here` | failed | 0.3 s | 1 | 400, dead-lettered immediately, no retry |
| `hello` (control) | done | 2.8 s | 1 | normal path |

- Predicted: POISON about 30 s, BADREQUEST about 1 s. Both matched.
- Retries were about 10 s apart, so the retry delay is the claim idle.
- `XRANGE jobs:dead - +` holds both failed jobs with their original fields (`job_id`, `prompt`,
  `max_tokens`, `model`, `enqueued_at`) plus `reason` and `attempts`.
  - POISON reason: `gave up after 3 attempts (MAX_ATTEMPTS=3)`
  - BADREQUEST reason: `HTTP 400: {"detail":"injected bad request (BADREQUEST)"}`
- After the run, `jobs` had `pending: 0`. Nothing was stuck.
- The POISON job was dead-lettered on its 4th delivery without running, so `attempts` = 3.
- The POISON dead-letter reason does not include the 500 error text, because the attempt
  count comes only from Redis and the worker keeps no per-job error state.

## How I built M2, step by step

Each step was one concept, explained before coding, then checked with a measurement.

1. **Gateway on Streams.** `XADD jobs` in the same `MULTI` as the status; consumer group
   `workers` created at startup with start ID `"0"` and `MKSTREAM`. Quiz: with `"$"`, jobs
   added before the group exists are silently skipped forever.
2. **Worker on Streams.** `XREADGROUP ... ">"`, then result + status + `XACK` + `XDEL` in one
   `MULTI`. Socket timeout (10 s) > block (5 s). A crashed worker's job stayed pending forever
   (M2-1 in the experiment log): not deleted, but still lost to the client.
3. **Reclaim loop.** `XAUTOCLAIM` every 2 s. Derived the timeouts from `max_tokens ≤ 1024`
   and wrote them into the CLAUDE.md timeout budget. Found the mock's real timing
   (2 s + 0.02 s/token) differs from the 0.3 s + 0.03 s/token I assumed; used the larger bound.
4. **Crash test.** LOST = 0 (M2-2). Only 1 job stranded, not 4.
5. **Retries and dead-letter.** Delivery count from `XPENDING`, `MAX_ATTEMPTS=3`, 4xx goes
   straight to `jobs:dead`, everything else retries through reclaim (M2-3).
6. **Duplicates.** Monotonic status via Lua, `SET NX` results, `XACK` return value counted in
   `metrics:duplicates`. Broke it on purpose with `CLAIM_IDLE_MS=800`: 50 duplicates, 0 wrong
   results (M2-4).
7. **Graceful shutdown.** SIGTERM drains in-flight jobs; `stop_grace_period: 90s`. Stop vs
   kill (M2-5).
8. **Durable Redis.** AOF with `appendfsync everysec` on a named volume; `burst.py` retries
   503s with backoff. Redis hard kill (M2-7): without AOF, 34 of 50 LOST and the group was
   gone; with AOF, LOST = 0.
9. **Load test reporting.** `burst.py` reports failure reasons and duplicates, and `--crash`
   automates the crash test.

Surprises along the way:
- The mock model's timing wasn't what I assumed; always read the code before deriving.
- The log line "not moved back to running" was misleading: the script also refuses
  `running → running`, so most refusals were no-ops, not real backward moves.
- `/v1/result` read result and status in two GETs, so it could briefly say `done` with no
  result. Fixed with one `MGET`, which sees both keys from the same moment.
- Workers crash when Redis drops the connection (`ConnectionError` isn't caught). In M2-7 both
  had to be restarted by hand. Still open.
- 503s after Redis was recreated under a running gateway: most likely stale pooled connections,
  each failing once. Inferred from timing.
- My first stop-vs-kill check counted 1 pending job for the stopped worker. It was really 0:
  `redis-cli` prints a blank line for an empty list, and `wc -l` counted it.

## Self-check (say these out loud before opening the answers)

**1. Why did BRPOP lose jobs while XREADGROUP doesn't? (Use the words owner and pending list.)**

<details><summary>Answer</summary>

`BRPOP` removes the job from Redis at the moment of pickup. From then on, the only copy is in
the worker's memory, and Redis has no record of who has it. `XREADGROUP` delivers the entry
but keeps it, and records it in the group's **pending list** with an **owner** (the consumer
name) and a delivery time. It stays there until the owner sends `XACK`. If the owner dies, the
entry is still in the pending list, its idle time grows, and `XAUTOCLAIM` transfers it to a
new owner.
</details>

**2. Ack vs delete: why do we need both XACK and XDEL, and why not MAXLEN?**

<details><summary>Answer</summary>

`XACK` removes the entry from the pending list ("done, stop tracking it") but leaves it in the
stream. `XDEL` removes it from the stream, so the stream holds only unfinished work and
`XLEN` = waiting + in-flight. Neither does the other's job, and XDEL first would leave the
pending list pointing at a deleted entry. `MAXLEN` trims the oldest entries by count, whether
acked or not, so a big burst could delete jobs nobody has run yet: silent loss again.
</details>

**3. Walk through the timeout budget from max_tokens ≤ 1024 to CLAIM_IDLE_MS. What breaks if you get the order wrong?**

<details><summary>Answer</summary>

`max_tokens ≤ 1024` → longest model call 0.3 + 0.03 × 1024 = 31.02 s → `MODEL_TIMEOUT_S` 45 s
→ longest hold = SET running (10 s socket) + model (45 s) + final MULTI (10 s) = 65 s →
`CLAIM_IDLE_MS` 75 s → worst crash recovery 75 + 2 = 77 s → crash-test `--timeout` ≥ 110 s.
Separately: socket timeout 10 s > block 5 s; grace period 90 s > slowest drain 85 s.

Wrong order:
- model timeout < longest job → healthy long jobs time out and get retried, then dead-lettered.
- claim idle < longest hold → slow jobs are stolen while running → duplicates (M2-4: 50 of 50).
- socket timeout ≤ block → an empty queue looks like a hung Redis (the M1 race).
- grace period < drain time → SIGKILL in the middle of draining → jobs wait for reclaim.
- load-test timeout < recovery → rescued jobs are counted as LOST.
</details>

**4. Why can't a system tell a slow worker from a dead one, and how did you make the consequence harmless?**

<details><summary>Answer</summary>

The only signal is silence: both a dead worker and a slow one stop acking, and from Redis's
side a network partition looks the same. All `XAUTOCLAIM` can see is idle time. So sometimes a
live "zombie" and a reclaimer both run the job. Harmless: status can only move forward (Lua
script, atomic), the first result wins (`SET NX`), and a late finisher sees `XACK` return 0 and
just counts a duplicate. Rare: `CLAIM_IDLE` is sized above the longest possible job.
</details>

**5. Which errors do you retry, which go straight to dead-letter, and why?**

<details><summary>Answer</summary>

Retry: timeouts, connection errors, 5xx, and anything unexpected. They may be transient
(model overloaded or restarting). The job is left pending, retried after `CLAIM_IDLE` (built-in
backoff, which avoids hammering a struggling GPU), and capped by Redis's delivery count at
`MAX_ATTEMPTS`. Straight to dead-letter: 4xx, because the request itself is wrong and the same
input gives the same answer. Measured: POISON (500) failed after 30.8 s and 3 attempts;
BADREQUEST (400) failed in 0.3 s and 1 attempt.
</details>

**6. Why is graceful shutdown really an autoscaling feature?**

<details><summary>Answer</summary>

An autoscaler removes capacity by sending SIGTERM, waiting `terminationGracePeriodSeconds`,
then SIGKILL. With scale-to-zero this happens after every quiet period, not rarely. Without
draining, every scale-down is a crash: in-flight jobs wait for the full claim idle and may run
twice. With draining, a scale-down costs nothing (M2-5: p95 27.5 s vs 81.8 s). Kubernetes'
default grace (30 s) is shorter than our slowest drain (85 s), so M4 must set it.
</details>
