# idemkit

**Make any operation safe to retry. No double charges, no duplicate emails, no repeated side effects.**

When a client retries a request, a broker redelivers a message, or an AI agent repeats a step, idemkit runs your code once and gives the first result back to the duplicates. Same idea in three places: HTTP requests, queue messages, and plain function calls.

Python today, tested on real Redis and PostgreSQL. **[Start with the Python guide.](./python/README.md)**

## Try it in 30 seconds

A shaky client sends the same charge 5 times. Without idemkit the customer pays 5 times. With it, once.

```bash
git clone https://github.com/idemkit/idemkit && cd idemkit
pip install -e "python/[asgi]" httpx
python python/examples/http/double_charge.py
```

```text
WITHOUT idemkit:  5 retries  ->  5 charges   (billed 5x)
WITH idemkit:     5 retries  ->  1 charge    (the other 4 replayed)
```

## What it handles

Pick the row that matches your problem, then read that section of the [Python guide](./python/README.md).

| Your duplicate is | idemkit keys on | You add |
|---|---|---|
| a client retrying a `POST` | the `Idempotency-Key` header | one middleware line |
| a broker redelivering a message | the message id | a consumer wrapper |
| an agent or job repeating a call | the function's arguments | one decorator |

## Why idemkit

Most libraries do the easy part (store a key) and skip the hard part (what happens under load and failure). idemkit is built around the hard part:

- **One atomic claim.** Two duplicates at the same instant, exactly one runs.
- **Crash-safe.** A worker that dies mid-run cannot wedge the key or run twice (a lease expires it, a fencing token rejects its late write).
- **Proven, not promised.** A conformance suite runs the same correctness checks on real Redis and PostgreSQL.
- **No lock-in.** Zero-dependency core, pluggable backends, drops into FastAPI, Flask, Django, or any worker.

A per-behavior comparison with Stripe, AWS Powertools, and the durable-execution frameworks is in the [spec](./spec/idemkit-unified-spec.md) (§10).

## Status

All three surfaces work in Python and pass their conformance vectors on real Redis and PostgreSQL. HTTP has the most production mileage; the queue and function surfaces are newer. Not on PyPI yet, so install from source. The spec is language-neutral, so a future port can reuse the same test vectors.

## Docs

- [Python quickstart and guides](./python/README.md)
- [Runnable examples](./python/examples/)
- [Engineering spec](./spec/idemkit-unified-spec.md)

## Contributing

Bug reports, spec feedback, code, and docs are all welcome. See [contributing](./python/README.md#contributing) for setup, or open an issue.

Apache-2.0. See [`LICENSE`](./LICENSE).
