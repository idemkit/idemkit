# idemkit

[![PyPI](https://img.shields.io/pypi/v/idemkit.svg)](https://pypi.org/project/idemkit/)
[![CI](https://github.com/idemkit/idemkit/actions/workflows/ci.yml/badge.svg)](https://github.com/idemkit/idemkit/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)
[![Downloads](https://img.shields.io/pypi/dm/idemkit.svg)](https://pypi.org/project/idemkit/)

**Make any operation safe to retry. No double charges, no duplicate emails, no repeated side effects.**

When a client retries a request, a broker redelivers a message, or an AI agent repeats a step, idemkit runs your code once and gives the first result back to the duplicates. Same idea in three places: HTTP requests, queue messages, and function calls.

The reasoning behind it is written up in [Why an idempotency key isn't an idempotency guarantee](https://www.infoworld.com/article/4191741/why-an-idempotency-key-isnt-an-idempotency-guarantee.html). Short version: the key alone does not stop a double charge, the design around it does. idemkit is that design, packaged as a library.

## What it gives you

- **One atomic claim.** Concurrent duplicates resolve to exactly one run and the rest replay its result — no check-then-set gap where two requests both see "not done" and both execute.
- **Crash recovery, not a stuck key.** A worker that dies mid-run doesn't wedge the key: its claim expires on a storage-clock lease, the next attempt reclaims it, and a fencing token rejects the dead worker's late write.
- **Waits and replays instead of erroring.** A duplicate that arrives while the first is still running waits for that result, rather than getting an immediate conflict.
- **Rejects a reused key with a different body** instead of silently replaying the wrong response.
- **One core, three surfaces, pluggable backends** — HTTP, queues, function calls, on in-memory / Redis / Postgres / MongoDB / DynamoDB, sync or async.
- **Effectively-once, stated honestly.** At-least-once delivery plus idempotent execution — with the failure modes written down, not hidden.

**→ [Start with the Python guide.](./python/README.md)** Runnable examples: [`python/examples/`](./python/examples/). The design rationale and the full behavioral contract are in the [spec](./spec/idemkit-unified-spec.md).

Pre-release. Apache-2.0, see [`LICENSE`](./LICENSE). Bug reports, spec feedback, and PRs welcome.
