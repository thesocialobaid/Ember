# Ember experiment log

Every run, including the ones that failed. Each entry: setup, results, observation, why.

Common setup unless stated: Docker Compose on my laptop, mock model (PREFILL 2 s +
0.02 s/token, so a 32-token job takes about 2.62 s), Redis 7, worker with 4 consumer loops,
`loadtest/burst.py` with 50 jobs of 32 tokens.

---

## M2-1: stream without reclaim, worker crash (2026-10-03)

**Setup.** Gateway on Redis Streams (`XADD jobs`), worker on `XREADGROUP` with ack after the
result is written. No `XAUTOCLAIM` yet. Worker started with `CRASH_AFTER_POP=1`, 6 jobs sent
one at a time, then a healthy worker started.

| | length | pending | waiting |
|---|---|---|---|
| Right after the crash | 7 | 1 | 5 |
| After the healthy worker started | 2 | 1 | 0 |

(Both lengths include one leftover entry from earlier testing.)

**Observation.** The job the crashed worker held (`20dbea34fbef`) was not deleted, but it
stayed pending forever: status `queued`, idle time growing. The 5 waiting jobs all ran.

**Why.** The new worker reads with `">"`, which only delivers entries never delivered to anyone.
The crashed job is still owned by the dead consumer in the pending list, so nothing will run it
until something reclaims it. It is not deleted (unlike BRPOP), but to the client it is still lost.

---

## M2-2: crash test with XAUTOCLAIM (2026-10-03)

**Setup.** Reclaim loop added (`XAUTOCLAIM` every 2 s). Overrides for this test:
`CLAIM_IDLE_MS=10000`, `MODEL_TIMEOUT_S=5`, burst `--timeout 60`. Crash worker
(`CRASH_AFTER_POP=1`) takes the burst; a healthy worker starts about 4.5 s after the crash.

| Metric | M1 (BRPOP) | M2 (Streams + XAUTOCLAIM) |
|---|---|---|
| Sent | 50 | 50 |
| Done | — | 50 |
| LOST | 4 destroyed by the crash | **0** |
| Jobs held by the crashed worker | 4 | 1 |
| Crash → rescued job reclaimed | never | 10.5 s |
| Crash → rescued job done | never | 13.2 s |
| End-to-end p50 / p95 / max | — | 21.30 / 37.05 / 39.89 s |
| Queue wait p50 / p95 / mean | — | 17.88 / 33.88 / 19.39 s |

Rescued job `5672e8b73109`: `queue_wait_s` 10.51, `inference_s` 2.65, `total_s` 13.16,
`worker` = `c3a4728f621f-reclaim`.

**Observation.** LOST = 0, the M2 success criterion. Only 1 job was stranded, not the 4
predicted. The rescue took claim idle (10 s) + up to one reclaim interval (2 s) + one job.
After the run, `XINFO CONSUMERS` still listed the dead consumer, with 0 pending.

**Why.** Each `XADD` wakes one blocked consumer. The crashed worker's first loop got job 1 and
killed the process within microseconds, before the gateway had added job 2. How many jobs a
crash strands depends on job arrival spacing versus how fast the worker dies; it is 4 only if
all 4 loops are busy at the moment of death.

---

## M2-3: retries and dead-letter (2026-10-03)

**Setup.** One worker, `CLAIM_IDLE_MS=10000`, `MODEL_TIMEOUT_S=5`, `MAX_ATTEMPTS=3`. Mock model
returns 500 for prompts containing `POISON`, 400 for `BADREQUEST`. Three jobs sent at once,
polled every 0.25 s.

| Job | Final status | Time to final status | Attempts | Path |
|---|---|---|---|---|
| `please POISON me` | failed | 30.8 s | 3 | 500 at 0 s, retried at +10.5 s and +20.5 s, dead-lettered at +30.6 s |
| `a BADREQUEST here` | failed | 0.3 s | 1 | 400, dead-lettered immediately |
| `hello` (control) | done | 2.8 s | 1 | normal path |

**Observation.** Both predictions matched (POISON about 30 s, BADREQUEST about 1 s). Retries
were about 10 s apart. `jobs:dead` holds both jobs with their original fields plus `reason` and
`attempts`; `jobs` ended with 0 pending. The POISON reason does not contain the 500 text.

**Why.** A retry only happens when reclaim picks the job up again, so the retry delay equals
the claim idle: built-in backoff. The 4th delivery is the one refused, so `attempts` = 3.
A 400 is deterministic, so retrying cannot help. The POISON reason has no error text because
the attempt count comes only from Redis and the worker keeps no per-job error state.

---

## M2-4: duplicates when CLAIM_IDLE is too short (2026-10-03)

**Setup.** Monotonic status (Lua script), `SET NX` results, `XACK` return value counted in
`metrics:duplicates`. 2 workers, `MODEL_TIMEOUT_S=5`, 50 jobs, a checker polling every 0.2 s
to catch a status moving backwards or a result changing.

| | CLAIM_IDLE_MS=800 | CLAIM_IDLE_MS=75000 |
|---|---|---|
| Times a job ran | 100 (50 + 50 reclaimed copies) | 50 |
| Duplicates (`/v1/metrics`) | **50** | **0** |
| Correct results (done, 32 tokens) | 50/50 | 50/50 |
| Status went backwards / result changed | 0 | 0 |
| Dead-lettered | 0 | 0 |
| `attempts` in the kept result | 1 for all 50 | 1 for all 50 |

**Observation.** At 800 ms every job ran twice, yet every result was correct and no status went
backwards. At the derived 75 s, no duplicates. The first 800 ms run crashed my checker: one
poll returned `status: done` without a `result`.

**Why.** A 2.62 s job crosses an 800 ms claim idle while still running, so reclaim starts a
second copy. The original always finished first and won; the copy's `SET NX` and `done` write
were refused and its `XACK` returned 0, which is the counted duplicate. The checker crash is
an old gateway race: `/v1/result` reads `result:` and `status:` in two separate GETs, so a job
finishing between them looks done with no result. Fixed afterwards: `/v1/result` now reads
both keys with one `MGET`.

---

## M2-5: graceful shutdown vs kill (2026-10-03)

**Setup.** Worker drains on SIGTERM (stops reading, finishes and acks in-flight jobs, exits).
`stop_grace_period: 90s`. 2 workers, default `CLAIM_IDLE_MS` (75 s), burst `--timeout 120`.
One worker stopped about 4 s into the burst.

| | `docker stop` (SIGTERM) | `docker kill` (SIGKILL) |
|---|---|---|
| Time to exit | 1.5 s, exit code 0 | 0.6 s, exit code 137 |
| Worker's pending jobs before → after exit | 4 → **0** | **4** stranded |
| Log | `draining 4 jobs` → `drained, exiting` | nothing |
| LOST | 0 | 0 |
| End-to-end p95 / max | 27.52 / 30.12 s | **81.75 / 81.76 s** |

**Observation.** Polite shutdown leaves nothing for reclaim; a kill costs the stranded jobs the
full claim idle (about +54 s on p95). LOST was 0 both times.

**Why.** On SIGTERM the worker finishes and acks what it holds, so the pending list is empty
when it exits. SIGKILL can't be caught, so its jobs stay owned by a dead consumer until
`XAUTOCLAIM` sees them idle for 75 s.

---

## M2-6: automated crash test, default timeouts (2026-10-03)

**Setup.** `python loadtest/burst.py -n 50 --crash --timeout 120 --url http://localhost:8080`.
Service workers stopped; a one-off `CRASH_AFTER_POP=1` worker takes the burst; once it exits,
`docker compose up -d worker` starts a healthy one. Default `CLAIM_IDLE_MS` (75 s),
`MODEL_TIMEOUT_S` (45 s). Redis with AOF on.

| Metric | Value |
|---|---|
| Sent / done / failed / LOST | 50 / 50 / 0 / **0** |
| Duplicates | 0 |
| Crash worker died at | 0.7 s |
| Rescued (attempts > 1) | 4 |
| End-to-end p50 / p95 / max | 21.84 / 79.98 / 80.01 s |
| Queue wait p50 / p95 / mean | 18.57 / 76.47 / 21.21 s |
| 503s retried by burst.py | 14 |
| XPENDING after the run | 0 |

**Observation.** LOST = 0 with the production timeouts. This time all 4 consumer loops were
holding a job when the worker died, so 4 were rescued, unlike M2-2 (1). The rescued jobs set the
p95: about 80 s, roughly claim idle (75 s) plus one job. The 14 retried 503s: see the note below.

**Why.** The burst sends 50 jobs concurrently, so all 4 blocked loops received a job before
the first one's `SIGKILL` landed. Each rescued job waits for its idle time to pass 75 s before
`XAUTOCLAIM` can take it, so a crash costs those jobs about 75 s of latency but no loss.

**503 note (investigated later the same day).** The 503s were not caused by `--crash`: a rerun
had 0, and so did a plain burst. Redis had been recreated at 18:11 (switch to the named volume)
while the gateway kept running since 17:54. Both runs with 503s (29, then 14) came right after
that; every run since had 0. Most likely explanation: the gateway's redis-py connection pool
still held connections to the old Redis container, each failed once (503) and was replaced.
Inferred from timing, not proven.

---

## M2-7: Redis hard kill, without and with AOF (2026-10-03)

**Setup.** For each run: `docker compose down -v` (fresh volume), `REDIS_AOF=no|yes`,
`docker compose up -d --build --scale worker=2`, then a 50-job burst (`--timeout 120`).
5 s into the burst: `docker compose kill -s SIGKILL redis`, `docker compose start redis`,
then `docker compose up -d --scale worker=2 worker` to restart the workers. AOF run uses
`appendfsync everysec`. Default `CLAIM_IDLE_MS` (75 s).

| | Without AOF | With AOF (`everysec`) |
|---|---|---|
| Before kill: XLEN / pending / status keys | 42 / 8 / 50 | 42 / 8 / 50 |
| After Redis restart: XLEN | **0** | **34** |
| After Redis restart: consumer group | gone (`ERR no such key`) | `workers` intact |
| After Redis restart: status keys | **0** | **50** |
| Workers when Redis came back | both exited (1), `ConnectionError` | both exited (1), `ConnectionError` |
| Sent / done / failed / LOST | 50 / 16 / 0 / **34** | 50 / 50 / 0 / **0** |
| 503s retried by burst.py | 86 | 86 |
| End-to-end p50 / p95 / max | 3.45 / 5.99 / 5.99 s (done jobs only) | 20.65 / 84.61 / 84.61 s |
| Final XLEN / XPENDING | 0 / 0 | 0 / 0 |

**Observation.** Without AOF, Redis came back empty: the stream, the group and every status key
were gone, and 34 jobs (8 in flight plus 26 waiting) were lost; only the 16 that finished
before the kill survived. With AOF, everything came back and LOST = 0. In both runs the workers
crashed on the Redis `ConnectionError` and had to be restarted by hand. In the AOF run, p95 was
about 85 s: the jobs the dead workers held waited for the 75 s claim idle.

**Why.** Without AOF, Redis only has its default RDB snapshots (save rules of 60 s or more),
and none had been taken in the few seconds before the kill. With AOF every write is appended to
a file and replayed on start. A SIGKILL of the Redis process doesn't lose the last second
either: the bytes Redis already wrote sit in the OS page cache, so `everysec` only matters if the
whole machine dies. The workers crash because `consume()` and `reclaim()` only catch
`redis.TimeoutError`, not `ConnectionError`. Not fixed yet.

**Follow-up checks on the same stack.** The first burst after the AOF run failed with
`httpx.RemoteProtocolError: Server disconnected without sending a response`, while the gateway
container never restarted. Two more bursts right after were clean (0 503s, LOST 0). Not
reproduced; cause unknown.
