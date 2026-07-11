# idemkit for Python

Make any operation safe to retry. When a client retries, a broker redelivers, or an agent re-plans, idemkit runs your code **once** and replays the first result to the duplicates, even when they arrive at the same instant.

One core covers the three places a retry turns into a duplicate: **HTTP requests**, **queue messages**, and **plain function calls** (agents, jobs, internal calls).

> 🚧 **Pre-release.** Not on PyPI yet, so [install from source](#install). HTTP has the most production mileage. Queue and method surfaces are newer but pass the same correctness vectors on real Redis and PostgreSQL.

## What you get

- **One atomic claim, not check-then-set.** Two duplicates racing at the same instant, exactly one runs.
- **Crash-safe.** A worker that dies mid-operation cannot wedge the key or double-execute. A storage-clock lease expires it, and a fencing token rejects the zombie's late write.
- **Duplicates wait and replay** instead of erroring. The concurrent retry gets the real result, not a 409.
- **Cross-tenant safe.** Scope isolates callers, so one user never sees another's stored response.
- **async-native, zero-dependency core.** Each backend is one opt-in extra. Nothing is imported until you ask.
- **Tested, not asserted.** A [conformance suite](#conformance) runs the same vectors against in-memory, real Redis, and real PostgreSQL. That is the part most libraries skip and get wrong.

Every feature has a runnable example in [`examples/`](examples/), grouped by surface. See [Examples](#examples).

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
from idemkit import IdempotencyMiddleware, RedisBackend

app.add_middleware(
    IdempotencyMiddleware,
    backend=RedisBackend.from_url("redis://localhost:6379"),
    scope=lambda req: req.headers["x-user-id"],   # isolate tenants
)
```

Without `scope`, idemkit runs single-tenant and warns once. Set `scope_mode="strict"` to turn a missing id into a hard error in CI.

> In the middleware, `scope` and `key` receive a lightweight proxy, not a full `Request`. Read identity from `req.headers` or `req.scope`, not `req.state`. If you need `request.state` or typed exceptions, use the per-route decorator below, which gets the real `Request`.

**Flask, Django, or any WSGI app** use the same config with the sync middleware ([example](examples/http/flask_wsgi.py)):

```python
from flask import Flask
from idemkit import WSGIIdempotencyMiddleware, RedisBackend

app = Flask(__name__)
app.wsgi_app = WSGIIdempotencyMiddleware(
    app.wsgi_app,
    backend=RedisBackend.from_url("redis://localhost:6379"),
    scope=lambda req: req.headers["x-user-id"],
)
```

**One route instead of the whole app** uses the `Idempotency` decorator. It gets the real Starlette `Request` and raises typed exceptions you can catch:

```python
from idemkit import Idempotency, RedisBackend
from starlette.responses import JSONResponse

idem = Idempotency(backend=RedisBackend.from_url("redis://..."), scope=lambda req: req.state.user.id)

@app.post("/charge")
@idem.protect
async def charge(request) -> JSONResponse:
    return JSONResponse({"ok": True}, status_code=201)
```

Each outcome is `application/problem+json`. Branch on the `type` URI, not the status:

| Status | When | Client should |
|---|---|---|
| replay | original status + `idempotency-replayed: true` | use it |
| `422` | same key, different body | resend the original body, or use a new key |
| `423` | another request with this key is in flight | retry after `Retry-After` |
| `503` | storage down (fail-closed) | retry after `Retry-After` |
| `400` | key missing (with `require_key_for_mutations`) or too long | fix the request |

Full walkthrough with scope, `body_fingerprint`, and a redactor: [`examples/http/fastapi_app.py`](examples/http/fastapi_app.py).

<details>
<summary><b>All HTTP options</b> (you rarely set past `scope`)</summary>

| Option | Default | What it's for | When to touch it |
|---|---|---|---|
| `scope` | none (single-tenant) | Tenant/user id so two users never collide on one key | The moment you have more than one tenant |
| `scope_mode` | `"warn"` | No-scope behavior: `warn`, `single_tenant` (silent), `strict` (error) | `strict` in CI; `single_tenant` if you truly are one tenant |
| `require_key_for_mutations` | `False` | Reject a POST/PATCH with no `Idempotency-Key` | To force clients to send a key |
| `body_fingerprint` | whole body | Fingerprint only the fields that define the operation | Honest retries 422 because the body has a timestamp/nonce |
| `response_redactor` | none | Strip PII from the stored copy before caching | Payments/PII: scrub card numbers, SSNs |
| `applicable_methods` | `{POST, PATCH}` | Which HTTP methods get idempotency | You also dedupe PUT/DELETE |
| `compat_mode` | `"default"` | `"stripe"` returns 409 instead of 422/423 | Only to match Stripe's exact status codes |
| `key` | the header | Read the key from elsewhere (JWT claim, query, body) | Rare; the key isn't in the standard header |
| `cacheable_status` | `{200,201,202}` | Which statuses are stored for replay (5xx never is) | Rare; to cache other 2xx |
| `max_body_bytes` / `max_request_body_bytes` | 1 MiB each | Caps on the cached response / buffered request (memory guard) | Rare; larger payloads |
| `header_allow` / `header_deny` | safe defaults | Which response headers are stored and replayed | Rare; custom header policy |

Plus the shared [core options](#configuration).
</details>

---

## Queue consumers

At-least-once brokers redeliver the same message by design. `IdempotentConsumer` wraps your handler so its side effect runs once per dedup id, even under redelivery, concurrent consumers, and crashes. You read the broker's id and visibility timeout, and it tells you to ack or redeliver.

```python
from idemkit import IdempotentConsumer, ConsumerAction, InMemoryBackend

consumer = IdempotentConsumer(
    backend=InMemoryBackend(),
    key=lambda msg: msg.message_id,          # however YOUR broker exposes the id
    visibility_timeout_seconds=30,           # the lease derives from this, kept shorter
)

@consumer.handle
async def process(msg) -> None:
    await charge_customer(msg.body)          # runs once per message_id

# in your poll loop:
result = await consumer.dispatch(msg)
broker.ack(msg) if result.action is ConsumerAction.ACK else broker.nack(msg)
```

A sync worker (threaded SQS/Kafka, Celery) calls `consumer.dispatch_sync(msg)` instead. See [`examples/queue/quickstart.py`](examples/queue/quickstart.py), [`dead_letter.py`](examples/queue/dead_letter.py), and [`cache_result.py`](examples/queue/cache_result.py).

**SQS and Kafka come wired.** `idemkit.contrib` presets the dedup id, attempt count, and ack glue so you don't hand-roll it:

```python
import boto3
from idemkit import RedisBackend
from idemkit.contrib.sqs import sqs_consumer, run_forever

sqs = boto3.client("sqs")
consumer = sqs_consumer(
    backend=RedisBackend.from_url("redis://..."),
    visibility_timeout_seconds=30,
    max_attempts=5,
    on_exhausted=lambda msg, exc: sqs.send_message(QueueUrl=DLQ, MessageBody=msg["Body"]),
)

@consumer.handle
def process(msg) -> None:
    charge_customer(msg["Body"])             # once per SQS MessageId

run_forever(consumer, sqs_client=sqs, queue_url=QUEUE, visibility_timeout=30)  # deletes on ack
```

Kafka is the same shape with `from idemkit.contrib.kafka import kafka_consumer` (dedup id is `topic:partition:offset`, and you pass `group_id`). Neither imports a broker SDK. You install and create the client yourself. Runnable versions: [`queue/sqs.py`](examples/queue/sqs.py), [`queue/kafka.py`](examples/queue/kafka.py).

<details>
<summary><b>All queue options</b></summary>

| Option | Default | What it's for | When to touch it |
|---|---|---|---|
| `key` | **required** | How to read the broker's dedup id from a message | Always |
| `visibility_timeout_seconds` | **required** | The broker's visibility window; the lease derives from it (kept shorter) | Always (match your broker) |
| `scope` | single | Isolation namespace (per queue / consumer group) | Several queues share one backend |
| `max_attempts` | `5` | Retries before a message is given up on | Tune to your DLQ policy |
| `on_exhausted` | none | Called when `max_attempts` is hit (DLQ, alert) | Set it to route poison messages |
| `receive_count` | none | Read the broker's native delivery count | Set it (SQS `ApproximateReceiveCount`, ...) so attempts count across processes |
| `attempt_store` | in-process | Durable attempt counter when the broker has none | Multi-consumer with no broker counter |
| `cache_result` | `False` | Cache and replay the handler's return value | The handler returns something you want back on redelivery |
| `result_codec` | json | How that return value is serialized | Non-JSON return types |

A handler returning `None` records a "processed" marker, so a redelivery is skipped. Plus the shared [core options](#configuration).
</details>

---

## Method calls and AI tools

Some duplicates are a plain function call, and the caller has no key to give you: an LLM agent re-emitting a tool call, a job that overlaps, an internal call. `@idempotent` dedupes on the **arguments**.

```python
from idemkit import idempotent, RedisBackend

@idempotent(backend=RedisBackend.from_url("redis://..."), key_fields=["order_id", "amount"])
async def refund(*, order_id, amount):
    return await payments.refund(order_id, amount)   # runs once per (order_id, amount)
```

Call it twice with the same `order_id` and `amount`, and the refund happens once. The second call replays the first result. Sync code uses `idempotent_sync`. Like HTTP, `scope` is optional: with none idemkit runs single-tenant and warns once, and you pass it to isolate callers (per user, or per agent session). See [`method/quickstart.py`](examples/method/quickstart.py), [`method/agent_loop.py`](examples/method/agent_loop.py), and [`method/sync.py`](examples/method/sync.py).

**Nested fields, without a query language.** A `key_fields` entry can be a dotted path into a nested dict or object, so you don't need a callback for the common case:

```python
@idempotent(backend=backend, key_fields=["order.id", "customer.email"])
async def book(*, order, customer): ...
```

`order.id` walks `order["id"]` (a dict) or `order.id` (an object). Plain dots, no expression language to learn.

> **Dedupe on the arguments, never a per-call id.** An LLM mints a fresh `tool_call_id` every turn (OpenAI `call_…`, Anthropic `toolu_…`). Passing that as the key defeats the purpose, because a retry carries a new id and runs the side effect again. The stable identity of a call is its arguments, so the default is `key_fields`. idemkit warns if you hand it a key that looks like a per-turn id.

**This is not memoization.** `functools.lru_cache` lives in one process, forgets on restart, and has no concept of a concurrent call. idemkit's claim is atomic across processes, survives a crash (lease plus fencing), makes a parallel duplicate *wait* for the first to finish, and scopes per caller. That is the whole point.

**MCP and agent tools.** MCP tools can advertise `idempotentHint`, but nothing enforces it. `idemkit.contrib.mcp` makes it real ([example](examples/method/mcp.py)):

```python
from mcp.server.fastmcp import FastMCP
from idemkit.contrib.mcp import mcp_idempotent

mcp = FastMCP("payments")

@mcp.tool()
@mcp_idempotent(backend=backend, key_fields=["order_id", "amount"])
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

Keep one irreversible side effect per function: idemkit dedupes the call, not the steps inside it. Plus the shared [core options](#configuration).
</details>

---

## Examples

Runnable, self-contained scripts in [`examples/`](examples/), grouped by surface. Most use `InMemoryBackend` with fake brokers or models, so they run with no infrastructure:

```bash
pip install -e ".[asgi]"
python examples/method/quickstart.py
```

| Folder | What's inside |
|---|---|
| [`http/`](examples/http/) | `double_charge.py` (the 30-second pitch), `fastapi.py`, `flask_wsgi.py`, `per_route.py` |
| [`queue/`](examples/queue/) | `quickstart.py`, `sqs.py`, `kafka.py`, `dead_letter.py`, `cache_result.py` |
| [`method/`](examples/method/) | `quickstart.py`, `sync.py`, `agent_loop.py` (LLM agent), `mcp.py`, `manual_clock.py` (testing) |

## Backends

| Backend | Use it for |
|---|---|
| `InMemoryBackend` | Dev, tests, prototypes. Not for production, since state is not shared across workers. |
| `RedisBackend` | Most production deployments. Redis 6+ and Cluster. |
| `PostgresBackend` | When you already run Postgres. Ships `idemkit init-pg` (schema) and `idemkit pg-vacuum` (cleanup). |

The same backend serves all three surfaces. Writing your own is a five-method `Protocol` (`claim`, `complete`, `release`, `renew`, `wait_for_completion`). Validate it against the [conformance suite](#conformance).

Redis and Postgres hold a pool and a background listener. Close them on shutdown via the ASGI lifespan, or use the backend as an async context manager:

```python
async with RedisBackend.from_url("redis://...") as backend:
    ...   # closed automatically
```

On **Redis Cluster** no hash tags are needed: each atomic claim and complete is a single-key script, and the in-flight-wait pub/sub broadcasts cluster-wide. On **Postgres**, run `idemkit pg-vacuum` on a cron (e.g. daily) to drop expired records, or the table grows unbounded.

## Configuration

**You rarely set any of these.** The defaults are production-sane; reach for a knob only when you have a reason. Every surface takes plain keyword arguments (no config object), and the core-policy names are the same on all three surfaces. A few defaults differ per surface, shown below.

The three time settings do different jobs. Don't confuse the in-flight lease with the result-retention TTL:

```
lease   : how long ONE run may hold the key       (queue: kept shorter than the visibility timeout)
wait    : how long a DUPLICATE waits for that run before giving up
result  : how long the FINISHED result is kept for replay   (a different thing entirely)
```

| Option | Default | What it does, and when to change it |
|---|---|---|
| `lease_ttl_seconds` | `30` HTTP / `60` method / derived (queue) | How long one execution may hold the key before it's assumed dead. Renewed by a heartbeat on queue and method. Raise it above your handler's p99. |
| `wait_timeout_seconds` | `10` HTTP and method / `5` queue | How long a concurrent duplicate waits for the in-flight run before giving up (HTTP returns 423; queue retries). Raise it if honest duplicates give up too soon. |
| `expires_after_seconds` | `86400` (24h) | How long the finished result is kept for replay. Set it above your longest honest retry gap. |
| `on_storage_error` | `"fail_closed"` (the safe default) | Backend down: `fail_closed` rejects the request; `fail_open` runs it unprotected, so a duplicate can slip through. Switch to `fail_open` only if availability beats dedup. |
| `event_handlers` | `[]` | One structured event per operation. **You** route it to Prometheus, OpenTelemetry, or logs; nothing is exported for you (see below). |

**Set it once, reuse it.** If you have many consumers or decorators, build an `IdempotencyPolicy` once and pass `config=` instead of repeating these keywords. A keyword passed directly still overrides the policy for that one:

```python
from idemkit import IdempotencyPolicy, idempotent, IdempotentConsumer

policy = IdempotencyPolicy(expires_after_seconds=3600, on_storage_error="fail_open")

@idempotent(backend=backend, key_fields=["order_id"], config=policy)
async def refund(*, order_id): ...

consumer = IdempotentConsumer(backend=backend, key=..., visibility_timeout_seconds=30, config=policy)
```

The same policy works on the HTTP middleware too (it fills in the HTTP defaults); pass HTTP-only options like `scope` as keywords alongside it:

```python
app.add_middleware(IdempotencyMiddleware, backend=backend, config=policy, scope=lambda req: req.headers["x-user-id"])
```

Each surface adds its own options, fully listed (with defaults and when to touch them) in the collapsible in its section: [HTTP](#http), [Queue](#queue-consumers), [Method](#method-calls-and-ai-tools). The one shared advanced knob is `use_local_cache` / `local_cache_max_items`: an in-process replay cache, off by default, that skips a backend round-trip for a key already seen in the same process. Almost nobody needs it.

Each event carries the decision (`new`, `replayed`, `in_flight_wait`, `conflict`, and so on), the hashed key (safe to log), latency, and backend. Every surface uses the same event, so one dashboard covers all three; you supply the exporter.

## Limitations and when not to use it

idemkit is deliberately narrow. Know these before you put it on a money path:

- **It dedupes result delivery, not downstream side effects.** A handler that keeps running after it loses its claim (a network partition, a blocking call that ignores cancellation) can still fire its side effect. For money, pass the idempotency key downstream (Stripe-style) or use a transactional outbox.
- **One irreversible side effect per function or handler.** idemkit dedupes the whole call, not steps inside it. A function that does two side effects and crashes between them re-runs both on retry. Split them, or key the downstream.
- **`fail_open` trades safety for availability.** On a storage outage it runs unprotected, so a duplicate can slip through during the outage. `fail_closed` (the default) rejects instead.
- **`expires_after_seconds` bounds the replay window.** A retry after the TTL re-executes. Size it above your longest honest retry gap.
- **Not a workflow engine.** For multi-step orchestration with recovery points, use Temporal, DBOS, or Restate. idemkit makes *one* operation safe to retry.
- **Effectively-once, not exactly-once.** At-least-once delivery plus idempotent execution. No system delivers exactly-once.

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

For lease expiry and TTL behavior, pass a `ManualClock` to `InMemoryBackend` and advance it instead of sleeping ([example](examples/method/manual_clock.py)).

## Conformance

The correctness core (atomic claim, fencing, lease reclaim, in-flight wait, TTL expiry, lease renewal) is a runnable suite any backend can check itself against:

```bash
idemkit conformance --redis redis://localhost:6379 --postgres postgresql://user:pass@localhost/db
```

Every surface's vectors pass on real Redis and PostgreSQL. The language-neutral descriptions live in [`spec/conformance.yaml`](../spec/conformance.yaml).

On top of the example-based suite, a **property-based, model-based test** (Hypothesis, in `tests/test_property_stateful.py`) drives random sequences of claim / complete / release / renew / clock-advance against each backend and a reference model, and asserts they agree at every step plus a replay-stability invariant. It runs on in-memory (with deterministic clock control), real Redis (Lua), and real PostgreSQL (SQL), so the fencing + lease + TTL state machine is checked across orderings and timings a hand-written test would miss, not just asserted.

## Contributing

```bash
cd python/ && python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                     # Docker-free; Redis tests use fakeredis
```

fakeredis is not real Redis. Before a release, run against real servers:

```bash
docker run -d --rm -p 6379:6379 redis:7-alpine
docker run -d --rm -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
IDEMKIT_TEST_REDIS_URL=redis://localhost:6379 \
IDEMKIT_TEST_PG_URL=postgresql://postgres:test@localhost:55432/postgres pytest
```

Every example is run by `tests/test_examples.py`, so an example that breaks fails CI. End-to-end tests against locally dockerized brokers (SQS, Kafka, RabbitMQ) live in `tests/e2e/`; they are excluded from the default run:

```bash
docker compose -f tests/e2e/docker-compose.yml up -d
pip install -e ".[dev,e2e]" && pytest -m e2e
```

House rules: concurrency changes need a race test (N concurrent, exactly one execution); spec changes update `conformance.yaml`; bug fixes include a regression test that fails before the fix.

Apache-2.0. See [`LICENSE`](../LICENSE). Design and rationale are in [`spec/idemkit-unified-spec.md`](../spec/idemkit-unified-spec.md).
