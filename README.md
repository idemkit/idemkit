# idemkit

**Make any operation safe to retry. No double charges, no duplicate emails, no repeated side effects.**

When a client retries a request, a broker redelivers a message, or an AI agent repeats a step, idemkit runs your code once and gives the first result back to the duplicates. Same idea in three places: HTTP requests, queue messages, and function calls. Python today, tested on real Redis and PostgreSQL.

**→ [Start with the Python guide.](./python/README.md)** Runnable examples: [`python/examples/`](./python/examples/). Design and a per-behavior comparison with Stripe / AWS Powertools / durable-execution engines: [spec](./spec/idemkit-unified-spec.md).

Pre-release: not on PyPI yet, install from source. Apache-2.0, see [`LICENSE`](./LICENSE). Bug reports, spec feedback, and PRs welcome.
