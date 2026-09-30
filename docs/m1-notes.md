# Ember M1: my notes

## Problems
GPUs are expensive, and most of the time they sit idle. The obvious fix is to turn the GPU off when nobody is using it. But that creates a new problem: a switched-off GPU can't answer right away. Booting a node and loading a model takes minutes. If a client waits on an open HTTP connection that long, timeouts kill the connection, and if the gateway crashes, the request is lost.

Building M1 showed me two more problems I hadn't expected. First, in a 50-request burst, about 85% of each request's time was spent waiting in the queue; the model itself only took about 2.9 s. Second, the system can lose jobs without any error at all. The job's status still says `queued`, and only the client, after timing out, notices anything went wrong.

## Goals and non-goals
M1's goal was the full request path running on my laptop with no GPU: gateway, Redis queue, worker, mock model, stored result, and a client that polls for it. I also built a load test to measure it, because without numbers I can't tell whether a later change helps.

I deliberately left out several things. Delivery is at-most-once, so jobs can be lost. I did that on purpose, so I can measure the loss before fixing it in M2. There's also no autoscaling, no streaming responses, no real model or GPU, no authentication and no support for multiple tenants. Those belong to later milestones. I still need to decide on Ember's SLO as an actual number.

## Async vs sync
In a synchronous design, the gateway would call the model and hold the connection open until the answer came back. That fails for a scale-to-zero system, because the wait can be minutes. So the gateway replies `202 Accepted` with a `job_id` within milliseconds, and the client polls `/v1/result/{job_id}` until the result is ready.

This isn't free. The client has to write polling logic, results arrive up to one poll interval late (0.5 s in my load test), and results expire: a result lasts 300 s and a status lasts 1 hour. In exchange, a request survives a cold GPU, a slow model and a gateway restart, because the job lives in Redis instead of in an open connection.

## Always-on gateway
The gateway is the one part that never scales to zero. It has to be running to receive the request that wakes the GPU. That's affordable because it's small, CPU-only and stateless, so it costs cents an hour compared with a GPU.

Because it's always on, its failure handling matters. I learned that a dependency being **down** and being **hung** are different failures. A stopped Redis fails instantly, but a frozen Redis just doesn't answer. At first a hung Redis made `/health` take 5 to 7 s and then crash with a 500. Now every Redis failure returns a 503, and a 1 s timeout makes it fail in about 1 s. A 500 says "I'm broken"; a 503 says "something I depend on is unavailable," and load balancers and Kubernetes treat the two differently. I also made saving the status and queuing the job a single atomic operation (`MULTI`), so a crash between the two can't leave a job marked "queued" that was never queued. The open question is that the gateway is now a single point of failure.

## Delivery guarantee
The worker takes jobs with `BRPOP`, which removes a job from Redis the moment the worker picks it up. From then on, the only copy is in the worker's memory. If the worker dies before writing the result, the job is gone. This is at-most-once delivery: every job runs once or never.

I measured it. A worker has 4 consumer loops, and Redis can hand a job to all 4 at once, so one crash lost **4 jobs**, not 1. In Docker, all 8 jobs in a run counted as LOST: 4 had been destroyed in the crash, and 4 were still waiting in the queue because no worker was running. The load test can't tell those two apart. I also found a second way to lose a job: a race between the BRPOP block (5 s) and the Redis client's default socket timeout (also 5 s).

Two things tried to hide the crashes from me. In Docker, the worker was PID 1 in its container, and the Linux kernel ignores a SIGKILL that PID 1 sends itself, so the crash simulation silently did nothing until I added `init: true`. And a restart policy would have brought the worker back so fast that I wouldn't notice. That's why the worker has none.

M2 has to fix this: a job shouldn't be deleted until the work is done. That probably means Redis Streams with acknowledgements, which gives at-least-once delivery, so I'll also need a way to make running a job twice harmless.

## Scaling signal
The gateway reports the queue length at `/v1/queue`, and that looked like the natural signal for waking a GPU. But queue length drops as soon as a worker picks up a job, even though the work isn't finished. The queue can read 0 while every worker is busy. The number that matches what users feel is **queue wait**. With 4 consumers and a burst of 50, the median wait was about 16 s and the p95 was about 30 s.

I also learned why p95 matters more than the average. The average hid that 1 user in 20 waited over 30 s. Before M4, I have to decide which signal wakes the GPU, which one sends it back to zero, the thresholds for each, and how long to wait before scaling down.

## Worker concurrency
The worker runs 4 async loops in one thread. Async isn't parallelism: the loops take turns while each one waits on the network. That works because the worker spends almost all its time waiting. Jobs beyond the fourth waited for a free loop. In the burst, 50 jobs took about 13 rounds of about 2.9 s each, so about 38 s, which matched the 40 s I measured.

Concurrency is a trade-off. More loops mean more throughput, but also more jobs lost in a single crash. My numbers also come with a caveat: the mock model only sleeps, so it can serve any number of requests at once. A real GPU is limited by memory (the KV cache) and by how requests are batched, so what limits concurrency there is something I still have to learn.
