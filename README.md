# idemkit

[![PyPI](https://img.shields.io/pypi/v/idemkit.svg)](https://pypi.org/project/idemkit/)
[![CI](https://github.com/idemkit/idemkit/actions/workflows/ci.yml/badge.svg)](https://github.com/idemkit/idemkit/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](./LICENSE)
[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org)
[![Downloads](https://img.shields.io/pypi/dm/idemkit.svg)](https://pypi.org/project/idemkit/)

**idemkit makes any operation safe to retry: no double charges, no duplicate emails, no repeated side effects.** It is a specification of crash-safe idempotency with a reference implementation in Python. When a client retries, a broker redelivers, or an agent repeats a step, your code runs once and the duplicates get the first result back. One idea, three surfaces: HTTP requests, queue messages, and function calls.

## Quickstart (Python)

```bash
pip install "idemkit[asgi]" fastapi
```

`[asgi]` is an optional extra: it adds the one dependency idemkit needs for HTTP middleware (Starlette). `fastapi` is your own web framework. idemkit's core has no dependencies, and every backend and surface is an opt-in extra like this: `[redis]`, `[postgres]`, `[mongo]`, `[dynamodb]`.

```python
from fastapi import FastAPI
from idemkit import IdempotencyMiddleware, InMemoryBackend

app = FastAPI()
app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend())

@app.post("/charge")
async def charge():
    return {"charged": True}   # runs once per Idempotency-Key; retries replay it
```

For production, swap `InMemoryBackend` for `RedisBackend` or `PostgresBackend` and add a `scope`. The full guide is in [python/README.md](./python/README.md).

## What it gives you

- **Exactly one run, even in a race.** Two duplicates that hit the same key at the same instant resolve to a single run, and the rest replay its result. No "both checked, both ran" gap.
- **A dead worker can't wedge the key or double-run.** If a worker dies mid-run, its lease expires, the next attempt takes over, and a fencing token throws out the dead worker's late write.
- **Duplicates wait and replay, they don't error.** A duplicate that arrives while the first is still running waits for that result instead of getting a conflict back.
- **A reused key with a changed body is rejected,** not answered with the wrong stored response.
- **A failed attempt isn't cached and replayed forever.** A decline or a transient error releases the key instead of being stored, so a retry runs again for real. Only a genuine success is replayed.
- **One core, three surfaces, five backends.** HTTP, queues, and function calls, on in-memory, Redis, Postgres, Mongo, or DynamoDB, sync or async.
- **Verified, not just claimed.** Every guarantee runs as a conformance test against real Redis, Postgres, Mongo, and DynamoDB, and it stays honest that this is effectively-once, not exactly-once, with the limits written down.

## Why not just use SETNX?

Because the safe version is more than one write. Here is the version most of us reach for first:

```python
async def charge(key, body):
    if not await redis.setnx(key, "running"):     # claim the key
        return await redis.get(f"{key}:result")   # someone else has it, replay
    result = await do_charge(body)                # the side effect
    await redis.set(f"{key}:result", result)
    return result
```

It looks right, and it breaks in four ways idemkit is built to handle:

1. **A duplicate that lands mid-charge gets nothing.** While the first call is still charging, the second finds the key already set but no result written yet, so it hands back an empty response. To answer it correctly, it has to wait for the first call to finish.
2. **A crash leaves the key stuck.** If the worker dies after `do_charge` but before writing the result, the key sits at `"running"` forever. Retries hang, or you clear it by hand and charge the card again. What's missing is a lease that expires on its own.
3. **A slow worker overwrites a good result.** Add a TTL so stuck keys expire, and a worker that wakes up after its TTL writes its stale result over a newer one. That takes a fencing token to reject the late write.
4. **The same key with a different body replays the wrong answer.** Key on the token alone and a $500 request cheerfully gets the $200 response back. You fingerprint the request and reject the mismatch.

idemkit does all four, on every backend. It's still effectively-once, not exactly-once, and it doesn't pretend otherwise: if a worker charges the card and then dies before recording it, idemkit throws out the dead worker's write but can't un-charge the card. For money, pass the key downstream too.

## Background and details

Why a key on its own isn't a guarantee: [Why an idempotency key isn't an idempotency guarantee](https://www.infoworld.com/article/4191741/why-an-idempotency-key-isnt-an-idempotency-guarantee.html). The key alone doesn't stop a double charge; the design around it does, and idemkit writes that design down as a spec and implements it for real.

- **Full Python guide and examples:** [python/README.md](./python/README.md), [python/examples/](./python/examples/)
- **The spec and behavioral contract:** [spec/idemkit-unified-spec.md](./spec/idemkit-unified-spec.md), with language-neutral [conformance vectors](./spec/conformance.yaml)

The public API is stable as of 1.0 and follows semantic versioning: a breaking change bumps the major. Apache-2.0, see [`LICENSE`](./LICENSE). Bug reports, spec feedback, and PRs (including implementations in other languages) welcome.
