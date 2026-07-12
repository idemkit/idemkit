# idemkit for Python

Make any operation safe to retry. When a client retries, a broker redelivers, or an agent re-plans, idemkit runs your code **once** and replays the first result to the duplicates, even when they arrive at the same instant.

One core covers the three places a retry turns into a duplicate: **HTTP requests**, **queue messages**, and **plain function calls** (agents, jobs, internal calls).

> 🚧 **Pre-release.** Not on PyPI yet, so [install from source](#install). HTTP has the most production mileage. Queue and method surfaces are newer but pass the same correctness vectors on real Redis and PostgreSQL.

## What you get

- **One atomic claim, not check-then-set.** Two duplicates racing at the same instant, exactly one runs.
- **Crash-safe.** A worker that dies mid-operation cannot wedge the key or double-execute. A storage-clock lease expires it, and a fencing token rejects the zombie's late write.
- **Duplicates wait and replay.** A concurrent retry waits for the in-flight one to finish and gets its result, instead of an immediate conflict error.
- **Cross-tenant safe.** Scope isolates callers, so one user never sees another's stored response.
- **async-native, zero-dependency core.** Each backend is one opt-in extra. Nothing is imported until you ask.
- **Every guarantee has a test.** A [conformance suite](#conformance) runs the same correctness vectors against in-memory, real Redis, and real PostgreSQL.

## Install

```bash
git clone https://github.com/idemkit/idemkit && cd idemkit/python
pip install -e ".[asgi,redis]"        # pick the extras you need
```

Once published: `pip install "idemkit[redis]"` (or `[postgres]`, `[asgi]`). The core has no third-party dependencies. Runs on Python 3.10 to 3.13, Redis 6+/Cluster, PostgreSQL 12+, any ASGI 3 or WSGI app.

## Pick your surface

| Your duplicate is | Deduped on | You add | Jump to |
|---|---|---|---|
| a client retrying `POST`/`PATCH` | the `Idempotency-Key` header | middleware (one line) | [HTTP](#http) |
| a broker redelivering a message | the broker's message id | `IdempotentConsumer` | [Queue](#queue-consumers) |
| a function called again (agent, job, internal call) | the function's arguments | the `@idempotent` decorator | [Method calls](#method-calls-and-ai-tools) |

---

## HTTP

A client sends `Idempotency-Key: abc-123` on a `POST`. If it retries with the same key, idemkit replays the first response instead of running your handler again.

The middleware wraps the whole app, but by default it only acts on `POST` and `PATCH`, the methods where a retry causes a duplicate. `GET`, `PUT`, `DELETE`, and the rest pass straight through untouched. Change that with `applicable_methods`.

Works in 30 seconds, no infrastructure:

```python
from fastapi import FastAPI
from idemkit import IdempotencyMiddleware, InMemoryBackend

app = FastAPI()
app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend())

@app.post("/charge")
async def charge():
    return {"charged": True}     # runs once per Idempotency-Key; retries replay it
```

```bash
curl -X POST localhost:8000/charge -H "Idempotency-Key: k1" -H "Content-Type: application/json" -d '{}'
# run it again: same response, plus header  idempotency-replayed: true  (the handler did not run)
```

For production, swap the backend and add `scope`:

```python
from idemkit import HttpConfig, IdempotencyMiddleware, RedisBackend

app.add_middleware(
    IdempotencyMiddleware,
    backend=RedisBackend.from_url("redis://localhost:6379"),
    config=HttpConfig(scope=lambda req: req.headers["x-user-id"]),   # isolate tenants
)
```

Without `scope`, idemkit runs single-tenant and warns once. Set `scope_mode="strict"` to turn a missing id into a hard error in CI.

> In the middleware, `scope` and `key` receive a lightweight proxy, not a full `Request`. Read identity from `req.headers` or `req.scope`, not `req.state`. If you need `request.state` or typed exceptions, use the per-route decorator below, which gets the real `Request`.

The middleware returns `application/problem+json` on the unhappy paths. Branch on the `type` URI, not the status:

| Status | When | Client should |
|---|---|---|
| replay | original status + `idempotency-replayed: true` | use it |
| `422` | same key, different body | resend the original body, or use a new key |
| `423` | another request with this key is in flight | retry after `Retry-After` |
| `503` | storage down (fail-closed) | retry after `Retry-After` |
| `400` | key missing (with `require_key_for_mutations`) or too long | fix the request |

**Other ways to wire HTTP** (same `HttpConfig`, pick by need):

- Whole app, sync (Flask / Django / any WSGI): `WSGIIdempotencyMiddleware`. [flask_wsgi.py](examples/http/flask_wsgi.py)
- FastAPI routes that return a `dict` and read `request.state`: `contrib.fastapi.idempotent_route` (a route class). [fastapi_route.py](examples/http/fastapi_route.py)
- One route, catch typed exceptions (e.g. a webhook): the `Idempotency(...).protect` decorator. [route_decorator.py](examples/http/route_decorator.py)
- Django REST Framework view (reads `request.user`): `contrib.drf.idempotent_view` (a mixin). [drf_view.py](examples/http/drf_view.py)

Full middleware walkthrough with scope, `body_fingerprint`, and a redactor: [fastapi_middleware.py](examples/http/fastapi_middleware.py).

<details>
<summary><b>All HTTP options</b> (you rarely set past `scope`)</summary>

| Option | Default | What it's for | When to touch it |
|---|---|---|---|
| `scope` | none (single-tenant) | Tenant/user id so two users never collide on one key | The moment you have more than one tenant |
| `scope_mode` | `"warn"` | No-scope behavior: `warn`, `single_tenant` (silent), `strict` (error) | `strict` in CI; `single_tenant` if you truly are one tenant |
| `require_key_for_mutations` | `False` | Reject a POST/PATCH with no `Idempotency-Key` | To force clients to send a key |
| `body_fingerprint` | whole body | Fingerprint only the fields that define the operation | Honest retries 422 because the body has a timestamp/nonce |
| `response_redactor` | none | Strip PII from the stored copy before caching | Payments/PII: scrub card numbers, SSNs |
| `response_hook` | none | Modify the response served to a duplicate (runs only on replay) | Tag a replayed response, tweak a cache header |
| `applicable_methods` | `{POST, PATCH}` | Which HTTP methods get idempotency | You also dedupe PUT/DELETE |
| `compat_mode` | `"default"` | `"stripe"` returns 409 instead of 422/423 | Only to match Stripe's exact status codes |
| `key` | the header | Read the key from elsewhere (JWT claim, query, body) | Rare; the key isn't in the standard header |
| `cacheable_status` | `{200,201,202}` | Which statuses are stored and replayed. Add a client error (e.g. `{200,201,202,422}`) to replay it Stripe-style instead of re-running the handler | You want a deterministic 4xx replayed, not re-executed |
| `max_body_bytes` / `max_request_body_bytes` | 1 MiB each | Caps on the cached response / buffered request (memory guard) | Rare; larger payloads |
| `header_allow` / `header_deny` | safe defaults | Which response headers are stored and replayed | Rare; custom header policy |

Plus the shared [core options](#configuration).
</details>

---

## Queue consumers

At-least-once brokers redeliver the same message by design. `IdempotentConsumer` wraps your handler so its side effect runs once per dedup id, even under redelivery, concurrent consumers, and crashes. You read the broker's id and visibility timeout, and it tells you to ack or redeliver.

```python
from idemkit import IdempotentConsumer, ConsumerAction, InMemoryBackend, QueueConfig

consumer = IdempotentConsumer(
    backend=InMemoryBackend(),
    config=QueueConfig(
        dedup_id=lambda msg: msg.message_id, # however YOUR broker exposes the id
        visibility_timeout_seconds=30,       # the lease derives from this, kept shorter
    ),
)

@consumer.handle
async def process(msg) -> None:
    await charge_customer(msg.body)          # runs once per message_id

# in your poll loop:
result = await consumer.dispatch(msg)
broker.ack(msg) if result.action is ConsumerAction.ACK else broker.nack(msg)
```

A sync worker (threaded SQS/Kafka, Celery) calls `consumer.dispatch_sync(msg)` instead. See [`examples/queue/getting_started.py`](examples/queue/getting_started.py), [`dead_letter.py`](examples/queue/dead_letter.py), and [`cache_result.py`](examples/queue/cache_result.py).

**What `dispatch` does with a duplicate**, so you know what to ack:

| The message is | `result.action` | The handler | `result.result` |
|---|---|---|---|
| new | `ACK` | runs | its return value |
| a redelivery of a **completed** one | `ACK` | does not run | replayed (if `cache_result`) |
| a **concurrent** duplicate, first still running | `RETRY` | does not run | none (redeliver; it replays once the first finishes) |
| a handler that raised, under `max_attempts` | `RETRY` | ran and failed | none (redeliver) |
| a handler that raised, at `max_attempts` | `ACK` (`exhausted`) | given up on | none (`on_exhausted` fired) |

`ACK` means remove it from the broker; `RETRY` means leave it for redelivery. A handler that returns `None` records a "processed" marker, so its redelivery is a no-op skip.

**SQS and Kafka come wired.** `idemkit.contrib` presets the dedup id, attempt count, and ack glue so you don't hand-roll it:

```python
import boto3
from idemkit import RedisBackend
from idemkit import QueueConfig
from idemkit.contrib.sqs import sqs_consumer, run_forever

sqs = boto3.client("sqs")
consumer = sqs_consumer(
    backend=RedisBackend.from_url("redis://..."),
    visibility_timeout_seconds=30,
    config=QueueConfig(
        max_attempts=5,
        on_exhausted=lambda msg, exc: sqs.send_message(QueueUrl=DLQ, MessageBody=msg["Body"]),
    ),
)

@consumer.handle
def process(msg) -> None:
    charge_customer(msg["Body"])             # once per SQS MessageId

run_forever(consumer, sqs_client=sqs, queue_url=QUEUE, visibility_timeout=30)  # deletes on ack
```

Kafka is the same shape with `from idemkit.contrib.kafka import kafka_consumer` (dedup id is `topic:partition:offset`, and you pass `group_id`). Neither imports a broker SDK. You install and create the client yourself. Runnable versions: [`queue/sqs.py`](examples/queue/sqs.py), [`queue/kafka.py`](examples/queue/kafka.py).

<details>
<summary><b>All QueueConfig options</b></summary>

Everything lives on one `QueueConfig`. `dedup_id` and `visibility_timeout_seconds` are the required wiring; everything else has a default.

| Option | Default | What it's for | When to touch it |
|---|---|---|---|
| **Required wiring** | | | |
| `dedup_id` | **required** | How to read the broker's dedup id from a message | Always |
| `visibility_timeout_seconds` | **required** | The broker's visibility window; the lease derives from it (kept shorter) | Always (match your broker) |
| **Queue-specific** | | | |
| `scope` | single | Isolation namespace (per queue / consumer group) | Several queues share one backend |
| `max_attempts` | `5` | Retries before a message is given up on | Tune to your DLQ policy |
| `on_exhausted` | none | Called when `max_attempts` is hit (DLQ, alert) | Set it to route poison messages |
| `receive_count` | none | Read the broker's native delivery count | Set it (SQS `ApproximateReceiveCount`, ...) so attempts count across processes |
| `attempt_store` | in-process | Durable attempt counter when the broker has none | Multi-consumer with no broker counter |
| `cache_result` | `False` | Cache and replay the handler's return value | The handler returns something you want back on redelivery |
| `result_codec` | json | How that return value is serialized | Non-JSON return types |
| `validation_fingerprint` | none | Bytes that must match on a dedup-id hit; a reused id with a different body raises `PayloadMismatch` and is routed to `on_exhausted` | A producer might reuse a message id with a different body |

A handler returning `None` records a "processed" marker, so a redelivery is skipped. The `contrib` helpers (`sqs_consumer`, `kafka_consumer`) preset `dedup_id` and `visibility_timeout_seconds` for you. Plus the shared [core options](#configuration).
</details>

---

## Method calls and AI tools

Some duplicates are a plain function call, and the caller has no key to give you: an LLM agent re-emitting a tool call, a job that overlaps, an internal call. `@idempotent` dedupes on the **arguments**.

```python
from idemkit import MethodConfig, idempotent, RedisBackend

@idempotent(backend=RedisBackend.from_url("redis://..."), config=MethodConfig(key_fields=["order_id", "amount"]))
async def refund(*, order_id, amount):
    return await payments.refund(order_id, amount)   # runs once per (order_id, amount)
```

Call it twice with the same `order_id` and `amount`, and the refund happens once. The second call replays the first result. Sync code uses `idempotent_sync`. Like HTTP, `scope` is optional: with none idemkit runs single-tenant and warns once, and you pass it to isolate callers (per user, or per agent session). See [`method/getting_started.py`](examples/method/getting_started.py), [`method/agent_loop.py`](examples/method/agent_loop.py), and [`method/sync_function.py`](examples/method/sync_function.py).

**Nested fields, without a query language.** A `key_fields` entry can be a dotted path into a nested dict or object, so you don't need a callback for the common case:

```python
@idempotent(backend=backend, config=MethodConfig(key_fields=["order.id", "customer.email"]))
async def book(*, order, customer): ...
```

`order.id` walks `order["id"]` (a dict) or `order.id` (an object). Plain dots, no expression language to learn.

> **Dedupe on the arguments, never a per-call id.** An LLM mints a fresh `tool_call_id` every turn (OpenAI `call_…`, Anthropic `toolu_…`). Passing that as the key defeats the purpose, because a retry carries a new id and runs the side effect again. The stable identity of a call is its arguments, so the default is `key_fields`. idemkit warns if you hand it a key that looks like a per-turn id.

**This is not memoization.** `functools.lru_cache` lives in one process, forgets on restart, and has no concept of a concurrent call. idemkit's claim is atomic across processes and survives a crash (lease plus fencing). A parallel duplicate waits for the first to finish, and each caller is scoped separately.

**MCP and agent tools.** MCP tools can advertise `idempotentHint`, but nothing enforces it. `idemkit.contrib.mcp` makes it real ([example](examples/method/mcp.py)):

```python
from mcp.server.fastmcp import FastMCP
from idemkit.contrib.mcp import mcp_idempotent

mcp = FastMCP("payments")

@mcp.tool()
@mcp_idempotent(backend=backend, config=MethodConfig(key_fields=["order_id", "amount"]))
async def refund(order_id: str, amount: int) -> dict:
    return await payments.refund(order_id, amount)   # agent re-plan replays, no double refund
```

Anthropic tool use and OpenAI function calling work the same way. Parse the tool arguments, call the decorated function, feed the result back. The `tool_call_id` only links the result to the call, and idemkit dedupes on the arguments.

<details>
<summary><b>All method options</b></summary>

| Option | Default | What it's for | When to touch it |
|---|---|---|---|
| `key_fields` | none | The argument names that make two calls "the same"; entries may be dotted paths into nested dicts/objects (`"order.id"`) | Almost always (this is your key) |
| `scope` | single | Isolation namespace. Ambient (`lambda: ctx.get()`) keeps it out of the signature (best for agents) | Multi-tenant / per-conversation |
| `result_codec` | `"json"` | Return value storage: `json`, `dataclass`, `pydantic`, custom `(to_dict, from_dict)`, or `pickle` (opt-in, warns) | Non-JSON return types |
| `idempotency_key` | none | An explicit key that overrides `key_fields` | You have a stable id (order id); never a per-turn id |
| `validation_fingerprint` | none | A field that must match on a key hit but isn't part of the key | Catch a reused id with a changed amount (raises `PayloadMismatch`) |
| `version` | `"1"` | Bump to invalidate cached results | The function's behavior changes |
| `normalize_args` | none | Derive the key from a function of the args | `key_fields` can't express it (nested fields) |
| `strict_keys` | `True` | Warn when a volatile-looking field lands in the key | Leave on; disable only for a false-positive warning |
| `require_key` | `False` | Refuse to derive the key from all arguments; the call must name its key (`key_fields`, `normalize_args`, or a per-call `idempotency_key`), else `IdempotencyKeyMissing` | Enforce, don't just warn, that every function names its key |
| `cache_exceptions` | `()` | Exception types that are deterministic: cache and re-raise them on a duplicate instead of re-running the handler | A validation/business-rule error should replay, not re-execute |

Keep one irreversible side effect per function: idemkit dedupes the call, not the steps inside it. Plus the shared [core options](#configuration).
</details>

---

## Examples

Clean, copy-paste snippets in [`examples/`](examples/), grouped by surface (`http/`, `queue/`, `method/`, `shared/`). Each shows only the integration code, uses `InMemoryBackend` so it needs no setup, and is covered by a test, so a broken example fails CI.

Browse the full "I want to..." index in [`examples/README.md`](examples/README.md). Each folder has a `getting_started.py`, an `all_options.py`, and one file per use case (SQS, DLQ, MCP tools, PII redaction, ...). Run them with `pytest tests/examples`.

## Backends

| Backend | Use it for |
|---|---|
| `InMemoryBackend` | Dev, tests, prototypes. Not for production, since state is not shared across workers. |
| `RedisBackend` | Most production deployments. Redis 6+ and Cluster. |
| `PostgresBackend` | When you already run Postgres. Ships `idemkit init-pg` (schema) and `idemkit pg-vacuum` (cleanup). |

The same backend serves all three surfaces. Writing your own is a five-method `Protocol` (`claim`, `complete`, `release`, `renew`, `wait_for_completion`); validate it against the [conformance suite](#conformance) (example: [`custom_backend.py`](examples/shared/custom_backend.py)).

To share one datastore with other data, or run isolated instances on it, `RedisBackend(namespace="idemkit")` prefixes every key, and `PostgresBackend(table="idempotency_keys")` uses a custom table (create it with `idemkit init-pg <url> --table idempotency_keys`). Full setup for all three backends is in [`backends.py`](examples/shared/backends.py).

Redis and Postgres hold a pool and a background listener. The ASGI middleware closes the backend it was given on `lifespan` shutdown, so the one-line install does not leak (pass `manage_backend=False` to opt out). Outside ASGI, use the backend as an async context manager:

```python
async with RedisBackend.from_url("redis://...") as backend:
    ...   # closed automatically
```

`from_url` sets a default connection timeout so an unreachable or hung backend fails fast (fail-closed) instead of blocking a request: Redis gets `socket_connect_timeout`, Postgres gets `command_timeout`. Override either via a keyword.

On **Redis Cluster** no hash tags are needed: each atomic claim and complete is a single-key script, and the in-flight-wait pub/sub broadcasts cluster-wide. **Postgres needs a reaper:** correctness does not depend on it (expiry is enforced on read), but the table grows unbounded unless you schedule `idemkit pg-vacuum` (see [Configuration](docs/configuration.md#postgres-schedule-the-reaper)).

## Configuration

**You rarely set any of these.** The defaults are production-sane. Each surface takes one config object (`HttpConfig` / `QueueConfig` / `MethodConfig`), passed as `config=`; that is the one way in. The **datastore** (Postgres table, Redis namespace) is configured on the backend, not here.

The knobs you might actually reach for:

| Option | Default | When to touch it |
|---|---|---|
| `lease_ttl_seconds` | `30` HTTP / `60` method / derived (queue) | Raise it above your handler's p99. **HTTP does not heartbeat**, so this matters most on HTTP. |
| `expires_after_seconds` | `86400` (24h) | Set it above your longest honest retry gap. |
| `on_storage_error` | `"fail_closed"` | Switch to `fail_open` only if availability beats dedup. |
| `event_handlers` | `[]` | Wire a ready-made metrics/logging handler, or your own. |

Full reference, the three time-settings explained, and the observability/alerting guide: **[docs/configuration.md](docs/configuration.md)**. Ready-made exporters:

```python
from idemkit.contrib.prometheus import prometheus_handler   # pip install "idemkit[prometheus]"
from idemkit.contrib.logging import logging_handler         # zero deps

config = HttpConfig(event_handlers=(prometheus_handler(), logging_handler()))
```

## Limitations and when not to use it

idemkit is deliberately narrow. Know these before you put it on a money path:

- **It dedupes result delivery, not downstream side effects.** A handler that keeps running after it loses its claim (a network partition, a blocking call that ignores cancellation) can still fire its side effect. For money, pass the idempotency key downstream (Stripe-style) or use a transactional outbox.
- **One irreversible side effect per function or handler.** idemkit dedupes the whole call, not steps inside it. A function that does two side effects and crashes between them re-runs both on retry. Split them, or key the downstream.
- **`fail_open` trades safety for availability.** On a storage outage it runs unprotected, so a duplicate can slip through during the outage. `fail_closed` (the default) rejects instead.
- **`expires_after_seconds` bounds the replay window.** A retry after the TTL re-executes. Size it above your longest honest retry gap.
- **Not a workflow engine.** For multi-step orchestration with recovery points, use Temporal, DBOS, or Restate. idemkit makes *one* operation safe to retry.
- **Effectively-once, not exactly-once.** At-least-once delivery plus idempotent execution. No system delivers exactly-once.
- **Postgres needs a reaper.** Correctness is fine without one (expiry is enforced on read), but the table grows unbounded until you schedule `idemkit pg-vacuum`. Redis keys self-expire, so Redis needs nothing.
- **A raised exception is not cached (method/queue).** `@idempotent` caches a returned value; a handler that *raises* releases the claim so a retry re-runs it. That is right for transient errors, but a deterministic exception re-executes each retry. Return a result instead of raising if you want it replayed. (HTTP can replay a client error via `cacheable_status`.)
- **The sync API runs on a shared background loop.** `idempotent_sync` / `dispatch_sync` bridge to the async core through one process-wide event loop. It is correct and fine for typical Flask/Django/Celery loads, but it is a shim over the async core rather than a native sync path. Under very high sync concurrency, prefer the async API.
- **HTTP does not renew the lease.** Queue and method handlers heartbeat, so a slow one keeps its claim. HTTP does not: an HTTP handler that runs longer than `lease_ttl_seconds` (default 30s) can have its claim reclaimed by a concurrent retry, which then runs a second time (the first handler's write is fenced, not cancelled). Size `lease_ttl_seconds` above your HTTP handler's p99, or push the idempotency key downstream for a slow charge.
- **A non-2xx HTTP response re-executes on retry.** By default only `{200,201,202}` are cached; any other status releases the claim, so a retry re-runs the handler. That is usually right (a `409` may succeed later), but a handler that fires a side effect then returns a non-2xx will re-fire it. Add the status to `cacheable_status` to replay it instead.
- **The in-flight-wait channel can drop, then self-heals.** If the Redis pub/sub or Postgres LISTEN channel dies (a failover), waiting duplicates fall back to a bounded poll (correctness holds) and the channel is re-established on the next waiter. Expect a brief latency bump on conflicts during a failover, not a hang.

## Testing

Issue a duplicate in-process and assert the side effect fired once:

```python
async def test_charge_is_idempotent():
    r1 = await client.post("/charge", headers={"Idempotency-Key": "k"})
    r2 = await client.post("/charge", headers={"Idempotency-Key": "k"})
    assert r1.json() == r2.json()
    assert r2.headers["idempotency-replayed"] == "true"
    assert charges_in_db() == 1
```

For lease expiry and TTL behavior, pass a `ManualClock` to `InMemoryBackend` and advance it instead of sleeping ([example](examples/method/record_expiration.py)).

## Conformance

The correctness core (atomic claim, fencing, lease reclaim, in-flight wait, TTL expiry, lease renewal) is a runnable suite any backend can check itself against:

```bash
idemkit conformance --redis redis://localhost:6379 --postgres postgresql://user:pass@localhost/db
```

Every surface's vectors pass on real Redis and PostgreSQL. The language-neutral descriptions live in [`spec/conformance.yaml`](../spec/conformance.yaml).

Beyond the fixed vectors, three checks run against every backend:

- **Property-based model check** (Hypothesis) drives random operation sequences against the backend and a reference model, and asserts they never diverge.
- **Fault injection** raises transient storage errors under concurrency to confirm the guarantees hold.
- **Clock-skew tests** prove lease decisions follow the storage clock, not the app's.

The full picture, including the honest limits, is in [CORRECTNESS.md](CORRECTNESS.md).

## Contributing

```bash
cd python/ && python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
make check                 # the gate CI runs: lint + typecheck + full suite
```

Setup, the `Makefile` targets, running the e2e brokers, and the house rules are in [`CONTRIBUTING.md`](CONTRIBUTING.md).

Apache-2.0. See [`LICENSE`](../LICENSE). Design and rationale are in [`spec/idemkit-unified-spec.md`](../spec/idemkit-unified-spec.md).
