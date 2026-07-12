# idemkit

**Make any operation safe to retry. No double charges, no duplicate emails, no repeated side effects.**

When a client retries a request, a broker redelivers a message, or an AI agent repeats a step, idemkit runs your code once and gives the first result back to the duplicates. Same idea in three places: HTTP requests, queue messages, and function calls. Python today, tested on real Redis and PostgreSQL.

## What it gives you

- **One atomic claim.** Concurrent duplicates resolve to exactly one run and the rest replay its result — no check-then-set gap where two requests both see "not done" and both execute.
- **Crash recovery, not a stuck key.** A worker that dies mid-run doesn't wedge the key: its claim expires on a storage-clock lease, the next attempt reclaims it, and a fencing token rejects the dead worker's late write.
- **Waits and replays instead of erroring.** A duplicate that arrives while the first is still running waits for that result, rather than getting an immediate conflict.
- **Rejects a reused key with a different body** instead of silently replaying the wrong response.
- **One core, three surfaces, pluggable backends** — HTTP, queues, function calls, on in-memory / Redis / Postgres / MongoDB / DynamoDB, sync or async.
- **Effectively-once, stated honestly.** At-least-once delivery plus idempotent execution — with the failure modes written down, not hidden.

**→ [Start with the Python guide.](./python/README.md)** Runnable examples: [`python/examples/`](./python/examples/). Design and a per-behavior comparison with Stripe / AWS Powertools / durable-execution engines: [spec](./spec/idemkit-unified-spec.md).

Pre-release: not on PyPI yet, install from source. Apache-2.0, see [`LICENSE`](./LICENSE). Bug reports, spec feedback, and PRs welcome.
