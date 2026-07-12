# idemkit for Python

Make any operation safe to retry. When a client retries, a broker redelivers, or an agent re-plans, idemkit runs your code **once per key** and replays the first result to the duplicates, even when they arrive at the same instant.

One core covers the three places a retry turns into a duplicate: **HTTP requests**, **queue messages**, and **plain function calls** (agents, jobs, internal calls).

> 🚧 **Pre-release (v0.1).** The API may still shift before 1.0. All three surfaces pass the same correctness suite on real Redis and Postgres; HTTP has the most production mileage so far.

## What you get

New to the vocabulary (*lease*, *fencing token*, *scope*, *fingerprint*)? Each links to the [Glossary](#glossary).

- **One atomic claim, not check-then-set.** Two duplicates racing at the same instant: exactly one wins the claim and runs, the rest replay its result. No read-then-write gap where both see "not done" and both run.
- **Crash-safe.** A worker that dies mid-operation can't block the key or double-execute. Its claim expires on a [lease](#glossary), and a [fencing token](#glossary) makes its late write get rejected instead of overwriting the real result.
- **Duplicates wait and replay.** A concurrent retry waits for the in-flight call to finish and gets its result, instead of an immediate conflict error.
- **Cross-tenant safe.** A [`scope`](#glossary) isolates callers, so one user never sees another's stored response.
- **async-native, zero-dependency core.** Each backend is one opt-in extra. Nothing is imported until you ask.
- **Every guarantee has a test.** The same [conformance vectors](#conformance) run on all five backends — in-memory, Redis, Postgres, Mongo, DynamoDB (Dynamo skips only the clock-skew one). What isn't covered yet — a multi-node partition sim, a formal model — is written down too, not hidden.

## Install

```bash
pip install "idemkit[redis]"          # or [postgres], [mongo], [dynamodb], [asgi]
```

> **Not on PyPI yet.** Until the first release, install from source:
> ```bash
> git clone https://github.com/idemkit/idemkit && cd idemkit/python
> pip install -e ".[asgi,redis]"      # pick the extras you need
> ```

The core has no third-party dependencies. Runs on Python 3.10 to 3.13, Redis 6+/Cluster, PostgreSQL 12+, MongoDB 4.2+, DynamoDB, any ASGI 3 or WSGI app.

## Pick your surface

| Your duplicate is | Deduped on | You add | Jump to |
|---|---|---|---|
| a client retrying `POST`/`PATCH` | the `Idempotency-Key` header | middleware (one line) | [HTTP](#http) |
| a broker redelivering a message | the broker's message id | `IdempotentConsumer` | [Queue](#queue-consumers) |
| a function called again (agent, job, internal call) | the function's arguments | the `@idempotent` decorator | [Method calls](#method-calls-and-ai-tools) |

---

## How it compares

The Python ecosystem has focused, single-purpose idempotency tools. idemkit trades that focus for one core that spans surfaces and backends, which changes a few concrete behaviors:

- **Concurrent duplicate: wait vs error.** Several HTTP libraries (e.g. `asgi-idempotency-header`) return a `409` to a duplicate that arrives while the first request is still in flight. idemkit waits for the in-flight one and replays its result.
- **Crash recovery: lease vs plain lock.** Lock-based dedupers (e.g. `celery-singleton`) release the lock only when the task finishes, so a `SIGKILL`, OOM, or timeout can leave a key locked. idemkit's claim is a storage-clock lease: it expires on its own, the next attempt reclaims it, and a fencing token rejects the dead worker's late write.
- **Body mismatch.** Many response-caching libraries key on the idempotency value alone. idemkit also fingerprints the body and rejects a reused key that carries a different payload (`422`) instead of replaying the wrong response.
- **One core, not one library per stack.** HTTP, queue, and method surfaces share the same framework-neutral core and five backends (in-memory, Redis, Postgres, Mongo, DynamoDB), sync or async — rather than a separate package per framework.

These are trade-offs, not verdicts: a single-framework tool is smaller and simpler when it's all you need. idemkit is the fit when you want the same guarantees across surfaces and a crash-safe lease rather than a plain lock.

---

## HTTP

A client sends `Idempotency-Key: abc-123` on a `POST`. If it retries with the same key, idemkit replays the first response instead of running your handler again.

The middleware wraps the whole app, but by default it only acts on `POST` and `PATCH`, the methods where a retry causes a duplicate. `GET`, `PUT`, `DELETE`, and the rest pass straight through untouched. Change that with `applicable_methods`.

Works in 30 seconds, no infrastructure (the `[asgi]` extra ships Starlette; this snippet also uses FastAPI):

```bash
pip install -e ".[asgi]" fastapi uvicorn
```

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
    config=HttpConfig(scope=lambda req: req.headers.get("x-user-id", "anonymous")),  # isolate tenants
)
```

Without `scope`, idemkit runs single-tenant and warns once. Set `scope_mode="strict"` to turn a missing id into a hard error in CI.

> In the middleware, `scope` and `key` receive a lightweight proxy, not a full `Request`. Read identity from `req.headers` or `req.scope`, not `req.state`. If you need `request.state` or typed exceptions, use the per-route decorator below, which gets the real `Request`.

The middleware returns `application/problem+json` on the unhappy paths. Branch on the `type` URI, not the status:

| Outcome | Status | `type` URI | Client should |
|---|---|---|---|
| replay | original status + `idempotency-replayed: true` | — | use it |
| same key, different body | `422` | `urn:idemkit:payload-mismatch` | resend the original body, or use a new key |
| another request with this key is in flight | `423` | `urn:idemkit:in-progress` | retry after `Retry-After` |
| storage down (fail-closed) | `503` | `urn:idemkit:storage-error` | retry after `Retry-After` |
| key missing (with `require_key_for_mutations`) or too long | `400` | `urn:idemkit:missing-key` | fix the request |

**Other ways to wire HTTP.** Not sure which? Pick by your framework and how much you want to wrap — all take the same `HttpConfig`:

| You're on… | You want… | Use | Example |
|---|---|---|---|
| FastAPI / Starlette / any ASGI | the whole app | `IdempotencyMiddleware` (shown above) | [fastapi_middleware.py](examples/http/fastapi_middleware.py) |
| Flask / Django / any WSGI (sync) | the whole app | `WSGIIdempotencyMiddleware` | [flask_wsgi.py](examples/http/flask_wsgi.py), [django_wsgi.py](examples/http/django_wsgi.py) |
| FastAPI | one route that returns a `dict` and reads `request.state` | `contrib.fastapi.idempotent_route` (route class) | [fastapi_route.py](examples/http/fastapi_route.py) |
| any ASGI | one route, and to catch typed exceptions (e.g. a webhook) | `Idempotency(...).protect` (decorator) | [route_decorator.py](examples/http/route_decorator.py) |
| Django REST Framework | one view that reads `request.user` | `contrib.drf.idempotent_view` (mixin) | [drf_view.py](examples/http/drf_view.py) |

Rule of thumb: **whole app → middleware; one route → the route class (FastAPI) or `.protect` (any ASGI).** The full middleware walkthrough with scope, `body_fingerprint`, and a redactor is in [fastapi_middleware.py](examples/http/fastapi_middleware.py).

Full HTTP option reference (you rarely set past `scope`): **[docs/configuration.md → HTTP options](docs/configuration.md#http-options)**.

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

A sync worker (threaded SQS/Kafka, Celery) calls `consumer.dispatch_sync(msg)` instead. See [`examples/queue/getting_started.py`](examples/queue/getting_started.py), [`generic_broker.py`](examples/queue/generic_broker.py) (RabbitMQ/NATS), [`dead_letter.py`](examples/queue/dead_letter.py), and [`cache_result.py`](examples/queue/cache_result.py).

> **Size the visibility timeout above your handler's runtime.** The lease derives from `visibility_timeout_seconds`, and every at-least-once broker makes a message visible again if you don't ack within its window. If a handler — or a retry backoff, or a Celery `eta`/`countdown` — runs longer than the visibility timeout, the broker hands the same message to a *second* consumer while the first is still running. That is the usual source of "my task ran twice" (SQS default 30s, Celery Redis 1h). Set the visibility timeout above your handler's p99, keep any delay/backoff shorter than it, and — since the dedup record lives for `expires_after_seconds` — keep that window longer than the broker's whole redelivery horizon. idemkit still fences the late finisher, but you avoid the needless second run.

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

Full `QueueConfig` reference (`max_attempts`, `on_exhausted`, `cache_result`, `validation_fingerprint`, ...): **[docs/configuration.md → Queue options](docs/configuration.md#queue-options)**.

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

**Nested args:** a `key_fields` entry can be a dotted path — `key_fields=["order.id", "customer.email"]` walks `order["id"]` (dict) or `order.id` (object), no callback or expression language to learn.

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

Full `MethodConfig` reference (`version`, `normalize_args`, `require_key`, `cache_exceptions`, ...): **[docs/configuration.md → Method options](docs/configuration.md#method-options)**.

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
| `MongoBackend` | When you already run MongoDB. Server-clock leases (`$$NOW`); a TTL index self-reaps, so no vacuum. Needs the `mongo` extra. |
| `DynamoBackend` | When you already run on DynamoDB (managed, nothing to operate). Conditional writes + a TTL attribute. Leases use the **client** clock (see the note in Limitations). Needs the `dynamodb` extra. |

All five pass the same [conformance suite](#conformance) on real servers (DynamoDB passes every vector except the clock-skew one, since its leases use the client clock — see [Limitations](#limitations-and-when-not-to-use-it)). The same backend serves all three surfaces. Writing your own is a five-method `Protocol` (`claim`, `complete`, `release`, `renew`, `wait_for_completion`); validate it against the conformance suite (example: [`custom_backend.py`](examples/shared/custom_backend.py)).

To share one datastore with other data, or run isolated instances on it, `RedisBackend(namespace="idemkit")` prefixes every key, `PostgresBackend(table="idempotency_keys")` / `MongoBackend.from_url(url, collection=...)` use a custom table/collection, and `DynamoBackend("idempotency_keys")` a custom table. Full setup for every backend is in [`backends.py`](examples/shared/backends.py).

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

idemkit is deliberately narrow. It gives **effectively-once**: at-least-once delivery plus idempotent execution, not the impossible exactly-once. So the one question a money path must answer is:

### When can my code actually run twice?

| Cause | Which surface | Guard it with |
|---|---|---|
| A completion write is lost (storage blips at the instant the result is recorded) | all | pass the idempotency key downstream (Stripe-style) so the whole chain dedupes |
| An HTTP handler runs longer than `lease_ttl_seconds` (no heartbeat on HTTP) | HTTP only | size `lease_ttl_seconds` above your handler's p99, or push the key downstream |
| A storage outage while `on_storage_error="fail_open"` | all | keep the default `fail_closed` (rejects instead of running unprotected) |
| A non-2xx response (or a *raised* exception in method/queue) releases the claim | all | return a result instead of raising; add the status to `cacheable_status` to replay it |
| A handler fires two side effects and crashes between them | all | split them, or key the downstream call |

In every one of these the stored record stays correct and duplicates still replay it — what can repeat is the **side effect** (the charge, the email). idemkit fences the record; it can't un-send an email a zombie worker already sent. The detail behind each row:

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
- **DynamoDB leases use the client clock.** Redis, Postgres, and Mongo decide lease expiry with the storage server's own clock, so a skewed app clock cannot cause a wrongful reclaim. DynamoDB has no server clock in a condition expression, so `DynamoBackend` uses the client clock. With NTP-synced hosts this is fine; a badly skewed host could misjudge a lease. Pick Redis/Postgres/Mongo if that matters.
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
- **Clock-skew tests** check that lease decisions follow the storage clock, not the app's.

The full picture, including the honest limits, is in [CORRECTNESS.md](docs/correctness.md).

## Performance

Idempotency cost is one backend round-trip on the happy path (claim + complete) and one on a replay, so throughput tracks your store's latency, not the library. We publish no numbers because ours wouldn't predict yours — measure on your own backend with [`benchmarks/bench.py`](benchmarks/bench.py) (reports ops/sec and p50/p99 for in-memory, Redis, or Postgres). Details in [`benchmarks/README.md`](benchmarks/README.md).

## Glossary

The terms the rest of this README leans on:

- **Idempotency key** — the token that says "these two calls are the same call." On HTTP it's the `Idempotency-Key` header; on a queue it's the broker's message id; on a method call it's derived from the arguments.
- **Claim** — winning the atomic right to run the code for a key. Exactly one caller claims it and runs; everyone else waits and replays that caller's result.
- **Scope** — an isolation namespace layered on top of the key, usually a tenant or user id. Two callers can send the same key and never collide, because their scopes differ. Omit it only for a single-tenant service.
- **Fingerprint** — a hash of the request body (or chosen fields). If the same key arrives with a *different* fingerprint, idemkit rejects it (`422`) instead of replaying the wrong response. `body_fingerprint` lets you fingerprint only the stable fields.
- **Lease** — a time-boxed hold on a key while your code runs. If the worker dies, the lease expires (decided by the storage server's own clock, so a skewed app clock can't misfire it) and the next attempt may reclaim the key. This is what stops a dead worker from blocking a key forever.
- **Fencing token** — a token handed to whoever holds the current lease. If a zombie worker (one that lost its lease but kept running) tries to write its result, the token no longer matches and the write is rejected — so its late write can't overwrite the real result.

Deeper: **effectively-once** (at-least-once delivery + idempotent execution, the honest alternative to impossible exactly-once) is explained under [Limitations](#limitations-and-when-not-to-use-it); the mechanics live in [CORRECTNESS.md](docs/correctness.md).

## Security

idemkit stores a **hash** of the idempotency key, never the raw key, and never logs the raw key. It does store the **response body verbatim** so duplicates can replay it — so on a money path, redact sensitive fields (`response_redactor`, opt-in) and enable encryption at rest on your backend. The default codec is JSON; the pickle codec is opt-in and an RCE risk. Full details and how to report a vulnerability: [SECURITY.md](SECURITY.md).

## Contributing

```bash
cd python/ && python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
make check                 # the gate CI runs: lint + typecheck + full suite
```

Setup, the `Makefile` targets, running the e2e brokers, and the house rules are in [`CONTRIBUTING.md`](CONTRIBUTING.md).

Apache-2.0. See [`LICENSE`](../LICENSE). Design and rationale are in [`spec/idemkit-unified-spec.md`](../spec/idemkit-unified-spec.md).
