# idemkit for Python

**Stop your API from charging twice.** When a client retries a request, idemkit runs your handler once and replays the original response to the duplicates — even when the retries arrive at the same instant.

Works with FastAPI, Starlette, or any ASGI 3 framework. The core is framework-agnostic; the concurrency and crash handling are the parts most libraries skip.

> 🚧 **Pre-release.** Not on PyPI yet — install from source (see [Install](#install)). One core, three surfaces — HTTP, queue consumers, and method-level (keyless) calls — each passing the same correctness vectors on real Redis and PostgreSQL. HTTP is the most battle-tested; queue and method-level are newer.

---

## Which surface do you need?

idemkit guards three places a retry turns into a duplicate. They share one core but differ in what triggers the dedupe. Pick the row that matches where your duplicate comes from — then **read only that section**.

| | **HTTP requests** | **Queue consumers** | **Method calls (keyless callers)** |
|---|---|---|---|
| The duplicate is… | a client retrying a `POST`/`PATCH` | a broker redelivering a message | a keyless caller re-invoking a function (LLM agent, job, internal call) |
| Deduped on… | the `Idempotency-Key` header | the broker's message id | the function's arguments (`key_fields`) |
| You add… | middleware (one line) or a `@protect` decorator | `IdempotentConsumer` around your handler | the `@idempotent` decorator |
| idemkit gives back… | the replayed HTTP response | ack vs. redeliver | the cached return value |
| Real example | a payments API that must not charge a card twice on a timeout retry | an SQS worker that must not email a customer twice on redelivery | an AI agent that must not book a flight twice when it re-plans |
| Go to | [HTTP requests](#http-requests) | [Queue consumers](#queue-consumers) | [Method calls](#method-level-idempotency) |

[Install](#install), [Backends](#backends), and [Troubleshooting](#troubleshooting) apply to all three.

---

## Core options (every surface)

Every surface is configured with **plain keyword arguments** — there's no config object to build. The same four ideas recur on all three: `backend` (where state lives), `key`/`key_fields` (what identifies the operation), `scope` (the isolation namespace), and the **core policy** below. The policy keywords have the **same names on every surface**:

```python
app.add_middleware(IdempotencyMiddleware, backend=redis, scope=lambda r: r.headers["x-user-id"], lease_ttl_seconds=30)
IdempotentConsumer(backend=redis, key=lambda m: m.message_id, visibility_timeout_seconds=30, completed_ttl_seconds=86_400)
idempotent(backend=redis, key_fields=["order_id", "amount"], scope=lambda a: a["session_id"], on_storage_error="fail_closed")
```

| Core option | Default | What it does |
|---|---|---|
| `lease_ttl_seconds` | `30` † | In-flight lease; set above your handler's p99 latency. |
| `wait_timeout_seconds` | `10` † | How long a duplicate waits for the in-flight one before giving up. |
| `completed_ttl_seconds` | `86400` | How long a result is kept for replay (24h). |
| `on_storage_error` | `"fail_closed"` | `fail_closed` → reject (503 on HTTP); `fail_open` → pass through unprotected. |
| `use_local_cache` | `False` | Warm-path replay without a backend round-trip (single-process); `local_cache_max_items` bounds it. |
| `event_handlers` | `[]` | One structured event per operation, for metrics/tracing. |

† Surface defaults differ where it matters: the queue surface uses `wait_timeout_seconds=5` and **derives** `lease_ttl_seconds` from the visibility timeout; the method surface defaults `lease_ttl_seconds=60`. Each surface adds its own options (HTTP options below; queue and method options in their sections).

---

## Install

idemkit isn't on PyPI yet, so the `pip install idemkit` form does *not* resolve today — install from source:

```bash
git clone https://github.com/idemkit/idemkit && cd idemkit/python
pip install -e ".[asgi,redis]"       # pick the extras you need
pip install fastapi uvicorn          # only for the HTTP quickstart below
```

Once published, the extras become plain installs: `pip install "idemkit[redis]"` (or `[postgres]`, `[asgi]`). The core has **zero third-party dependencies** — nothing pulls in `redis`, `asyncpg`, `starlette`, or `pydantic` unless you ask. (The `asgi` extra installs Starlette, not FastAPI.)

| | Supported |
|---|---|
| Python | 3.10 – 3.13 |
| Web (`asgi`) | Starlette ≥ 0.30, any ASGI 3 app; FastAPI works as-is |
| Redis (`redis`) | `redis` ≥ 5.0; Redis 6+ and Redis Cluster |
| PostgreSQL (`postgres`) | `asyncpg` ≥ 0.29; PostgreSQL 12+ |

---

# HTTP requests

Dedupe inbound HTTP requests. A client sends `Idempotency-Key: abc-123` on a `POST`/`PATCH`; if it retries with the same key, idemkit replays the first response instead of running your handler again.

| The key arrives… | idemkit does |
|---|---|
| first time | runs your handler normally |
| again, same body | replays the stored response (handler does not run) |
| again, **different** body | returns **422** (the original is never replayed by mistake) |
| twice at once | runs exactly one handler; the other waits, then gets the replay |
| after a crash mid-request | the lease expires on its own; the next retry starts over |

You also get cross-tenant isolation once you set `scope`, faithful replay (status + headers + body), 5xx never cached, and one structured event per request.

**Error responses** (all `application/problem+json`; the `type` URI is stable, so branch on it, not the status):

| Status | When | Client should |
|---|---|---|
| `400` | key missing (with `require_key_for_mutations`) or > 255 bytes | fix the request |
| `422` | same key, different body | resend the original body, or use a new key |
| `423` | another request with this key is still in flight | retry after `Retry-After` |
| `503` | storage unavailable (fail-closed) | retry after `Retry-After` |
| `500` `urn:idemkit:identity-unavailable` | couldn't resolve a caller identity | surface; it's a server bug |

In Stripe-compat mode (`compat_mode="stripe"`), `422`/`423` become `409`.

## Quickstart

The smallest thing that works — in-memory backend, no config:

```python
from fastapi import FastAPI
from idemkit import IdempotencyMiddleware, InMemoryBackend

app = FastAPI()
app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend())

@app.post("/orders")
async def create_order(order: dict):
    # Runs ONCE per Idempotency-Key; duplicates replay this response.
    return {"order_id": "abc-123", "status": "created"}
```

> **Before production:** if you have more than one user/tenant, add `scope=lambda req: req.headers["x-user-id"]` to `add_middleware(...)` so two users can't collide on one key. idemkit warns at startup until you do; `strict_scope=True` makes it a hard error in CI.

Run it (save as `hello.py`), then retry the same request:

```bash
uvicorn hello:app --reload    # http://localhost:8000

# Keep Content-Type: application/json — without it FastAPI 422s the body before idemkit runs.
curl -X POST http://localhost:8000/orders \
     -H "Idempotency-Key: my-order-1" -H "X-User-Id: user-42" \
     -H "Content-Type: application/json" -d '{"item": "widget"}'
# {"order_id":"abc-123","status":"created"}
```

Run the exact same command again: the handler does **not** run, and the response comes back with an `idempotency-replayed: true` header.

## One route instead of the whole app

The middleware protects every in-scope request. To protect specific routes, bind the `Idempotency` decorator once and apply it:

```python
from idemkit import Idempotency, RedisBackend
from starlette.requests import Request
from starlette.responses import JSONResponse

idem = Idempotency(
    backend=RedisBackend.from_url("redis://localhost:6379"),
    scope=lambda req: req.state.user.id,
)

@app.post("/charge")
@idem.protect
async def charge(request: Request) -> JSONResponse:
    return JSONResponse({"ok": True}, status_code=201)
```

**The decorator raises; the middleware returns.** Where the middleware emits `problem+json` directly, the decorator raises typed exceptions — `PayloadMismatch`, `IdempotencyConflict`, `IdempotencyKeyMissing`, `StorageUnavailable` — so you can catch and branch on them. Uncaught, they surface as a 500. To get the middleware's `problem+json` responses, register the handler once:

```python
from idemkit.adapters.route import idempotency_problem_handler
from idemkit.core.exceptions import IdempotencyError

app.add_exception_handler(IdempotencyError, idempotency_problem_handler)
```

Two rules for a decorated endpoint:

1. **Declare a `request: Request` parameter** — the decorator and your `scope` / `key` get the real Starlette `Request` (so `request.state`, `request.client`, case-insensitive headers all work, unlike the middleware's lightweight proxy).
2. **Return a `Response`** (e.g. `JSONResponse`) — a returned dict relies on FastAPI serialization that runs after the decorator and can't be captured.

By default only POST and PATCH are deduplicated; narrow with `applicable_methods={"POST"}`.

## Production: Redis or PostgreSQL

In production use Redis or PostgreSQL so the idempotency state survives restarts and is shared across worker processes.

> **Middleware ordering.** `add_middleware` runs outermost-first — add idemkit *before* your auth middleware so the user is resolved when idemkit reads `scope`. The middleware hands extractors a lightweight proxy, not a `Request`: read identity from a header, or from `req.scope`.

**Redis** (recommended; Redis 6+ and Cluster):

```python
from idemkit import IdempotencyMiddleware, RedisBackend

app.add_middleware(
    IdempotencyMiddleware,
    backend=RedisBackend.from_url("redis://localhost:6379"),
    scope=lambda req: req.headers["x-user-id"],   # MUST return a non-empty string
    require_key_for_mutations=True,
)
```

**PostgreSQL** — same as Redis with a different backend (the pool opens lazily on the first request). Create the schema once, and run the TTL cleanup on a cron:

```bash
idemkit init-pg postgresql://user:pass@localhost/mydb     # once
idemkit pg-vacuum postgresql://user:pass@localhost/mydb   # periodically (e.g. daily)
```
```python
from idemkit import PostgresBackend
# ...the same add_middleware as above, but:
backend = PostgresBackend.from_url("postgresql://user:pass@localhost/mydb")
```

**Closing on shutdown.** Redis/Postgres backends hold a pool plus a background subscriber/`LISTEN`. Close them via the ASGI lifespan:

```python
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    await backend.aclose()

app = FastAPI(lifespan=lifespan)
```

In a script or test (no lifespan), every backend is also an async context manager — the simplest way to avoid a leaked pool and the `Event loop is closed` traceback a forgotten `aclose()` prints:

```python
async with RedisBackend.from_url("redis://localhost:6379") as backend:
    ...   # closed automatically on exit
```

(`InMemoryBackend` needs no teardown.)

## HTTP-only options

Plain keywords on `add_middleware(IdempotencyMiddleware, ...)` / the `Idempotency` decorator, besides the [core options](#core-options-every-surface). The one you'll almost always set is `scope`.

| HTTP option | Default | What it does |
|---|---|---|
| `scope` | none → single-tenant | Extracts the user/tenant id for isolation. Without it, one shared namespace + a startup warning; `strict_scope=True` makes a missing id a hard error. |
| `key` | reads `Idempotency-Key` | Read the key from elsewhere (see [Patterns](#patterns)). |
| `applicable_methods` | `{"POST","PATCH"}` | Which methods get idempotency. |
| `cacheable_status` | `{200,201,202}` | Which statuses are cached for replay. **5xx is never cached.** |
| `max_body_bytes` | `1 MiB` | Bigger responses pass through uncached (memory-DoS guard). |
| `require_key_for_mutations` | `False` | Reject mutating requests with no `Idempotency-Key`. |
| `body_fingerprint` | none → whole body | Callback selecting which body bytes go into the fingerprint (drop volatile fields, or return `b""` to ignore the body); see [Patterns](#patterns). |
| `compat_mode` | `"default"` | `"stripe"` → 409 instead of 422/423, `Idempotent-Replayed` header. |
| `response_redactor` | — | Strip sensitive fields from the stored copy (see [Patterns](#patterns)). |

## Patterns

**Money paths.** The middleware dedupes response *delivery*; it does not stop a handler that's mid-execution from finishing its side effect after a client disconnect. For money, also pass the key to your downstream API so the whole chain dedupes:

```python
@app.post("/charge")
async def charge(req: ChargeRequest, idempotency_key: str = Header()):
    return stripe.PaymentIntent.create(amount=req.amount, idempotency_key=idempotency_key)
```

**PII / PCI redaction.** Strip sensitive fields from the *stored* copy (the first client still gets the full response):

```python
def redactor(body: bytes, headers: dict, status: int) -> tuple[bytes, dict]:
    if headers.get("content-type", "").startswith("application/json"):
        data = json.loads(body)
        data.pop("card_number", None); data.pop("ssn", None)
        body = json.dumps(data).encode()
    return body, headers

app.add_middleware(IdempotencyMiddleware, backend=..., scope=..., response_redactor=redactor)
```

**Bodies that change between retries.** The body is part of the *fingerprint* by default (not the key itself — the key is the `Idempotency-Key`), so the same key with a *different* body returns 422. If a client puts a volatile value in the body (timestamp, nonce, generated id), an honest retry looks "different" and trips a surprising 422. The fix is `body_fingerprint` — fingerprint only the fields that define the operation:

```python
def select(body: bytes, content_type: str | None) -> bytes:
    data = json.loads(body)
    return json.dumps({"amount": data["amount"], "currency": data["currency"]},
                      sort_keys=True).encode()   # ts / nonce ignored

app.add_middleware(IdempotencyMiddleware, backend=..., scope=..., body_fingerprint=select)
```

The selector returns the bytes idemkit hashes. To ignore the body wholesale (dedupe on key + caller + method + path), return `b""` — then a reused key replays even with a genuinely different payload, so do that only when the key itself is the contract.

**Custom key extraction.** To read the key from a JWT claim, query string, or body field:

```python
app.add_middleware(IdempotencyMiddleware, backend=..., scope=..., key=lambda req: req.headers.get("x-request-id"))
```

`key` and `scope` receive a lightweight proxy, not a `Request`: `req.headers` (lowercase names), `req.scope` (raw ASGI scope), `req.body` (`key` only). A configured `scope` MUST return a non-empty string, or idemkit refuses the request (500 `urn:idemkit:identity-unavailable`) rather than bucket it into the shared namespace.

**Observability.** Each request emits one structured event for its terminal decision:

```python
def event_handler(event):
    # event.decision: new | replayed | in_flight_wait | conflict | payload_mismatch | ...
    # event.effective_key: SHA-256 hash, safe to log (never the raw key)
    # also: event.latency_seconds, event.backend_name
    prometheus_counter.labels(decision=event.decision.value).inc()

app.add_middleware(IdempotencyMiddleware, backend=..., scope=..., event_handlers=[event_handler])
```

**Stripe wire format.** `compat_mode="stripe"` returns 409 instead of 422/423 and emits the `Idempotent-Replayed` header.

---

# Queue consumers

Dedupe redelivered broker messages. At-least-once brokers redeliver the same message — that's their contract — and the consumer must not act on it twice. `IdempotentConsumer` wraps your handler so its side effect runs once per dedup id, even under redelivery, concurrent consumers in a group, and crashes. You read the broker's dedup id and visibility timeout; it tells you whether to ack the message or leave it for redelivery.

**Which brokers?** Any at-least-once broker — idemkit is broker-agnostic and ships no broker client. It's been exercised against **SQS, Kafka, RabbitMQ, Redis Streams, and Google Pub/Sub**, but never talks to the broker itself: you supply small callables that read the id, scope, and (optionally) delivery count from whatever message object your library hands you, and you keep your own poll loop and ack/nack calls. So unlike the HTTP middleware this isn't a one-liner — "zero infra" below means no Redis/Postgres to set up, not zero wiring.

**Dev** (no Redis/Postgres needed):

```python
from idemkit import IdempotentConsumer, ConsumerAction, InMemoryBackend

consumer = IdempotentConsumer(
    backend=InMemoryBackend(),
    key=lambda msg: msg.message_id,        # however YOUR broker exposes the id
    visibility_timeout_seconds=30,         # the lease is derived from this, kept shorter
)

@consumer.handle
async def process(msg) -> None:
    await charge_customer(msg.body)         # runs once per message_id

# In your own poll loop, let the consumer decide ack vs redeliver:
result = await consumer.dispatch(msg)
broker.ack(msg) if result.action is ConsumerAction.ACK else broker.nack(msg)
```

> **Sync workers.** An `async def` handler is awaited; a plain `def` handler runs on a worker thread. From a **synchronous poll loop** (no event loop — a threaded SQS/Kafka worker, a Celery task), call `consumer.dispatch_sync(msg)` instead of `await consumer.dispatch(msg)` — same flow, returns the `ConsumerResult` synchronously. Don't call `dispatch_sync` from inside a running loop.

**Production** — swap the backend, scope per queue/group, wire the broker's receive count and a dead-letter boundary:

```python
consumer = IdempotentConsumer(
    backend=RedisBackend.from_url("redis://prod"),
    key=lambda msg: msg.message_id,
    scope=lambda msg: (msg.queue, msg.consumer_group),   # isolation across queues
    visibility_timeout_seconds=30,
    receive_count=lambda msg: msg.receive_count,         # broker-native attempt count
    max_attempts=5,
    on_exhausted=lambda msg, exc: dlq.send(msg),         # poison-message boundary
)
```

- **idemkit guarantees:** the side effect runs once per dedup id under redelivery, concurrent consumers, and crashes; the lease is kept shorter than the visibility timeout and renewed by a heartbeat, so a redelivery can't race a still-running handler.
- **You handle:** ack on `ACK`, redeliver on `RETRY`; you decide `max_attempts` and `on_exhausted` (the DLQ boundary).

**Queue options** (besides the [core options](#core-options-every-surface)): `key` (the dedup id), `visibility_timeout_seconds` (required; the lease derives from it), `scope`, `max_attempts` + `on_exhausted` (DLQ boundary), `receive_count` / `attempt_store` (how attempts are counted), `result_bearing` + `result_codec`. A handler returning `None` records a "processed" marker (a redelivery is skipped). To cache and replay a value, set `result_bearing=True` and read `result.result`. Attempts come from `receive_count` when you supply it; without it, idemkit counts in-process and warns that it only holds within one process.

**Testing** (a message is any object your `key` can read):

```python
from types import SimpleNamespace

async def test_processes_once():
    seen = 0
    @consumer.handle
    async def process(msg) -> None:
        nonlocal seen; seen += 1

    msg = SimpleNamespace(message_id="m1", body=b"...")
    for _ in range(3):                  # three deliveries of the same message
        await consumer.dispatch(msg)
    assert seen == 1
```

---

# Method-level idempotency

*Make any function run once per call, keyed on its arguments — for callers that don't supply an idempotency key. The decorator is `@idempotent` (the old name `@idempotent_tool` still works as a deprecated alias).*

This is **general-purpose function idempotency**: `@idempotent` wraps one of *your* functions so that calling it again with the same arguments replays the first result instead of re-running the side effect. Nothing about it is AI-specific — it fits any **keyless caller**:

- an **LLM agent** emitting tool calls — OpenAI function calling, Anthropic tool use, the Model Context Protocol (MCP), or LangChain / LlamaIndex tools. It re-emits the same call on a retry, a re-plan, or a parallel branch, and the per-turn ids it gives you change every time. *(This is the leading example, worked end-to-end below.)*
- a **background job, cron step, or Celery task** that can run again after a failure or an overlap;
- an **internal call** — another service, a worker, a CLI — that carries no idempotency convention;
- a function reached from **several entry points** where you want the dedupe to live once, with the method.

In all of these there's no caller-supplied key, so idemkit dedupes on the function's **arguments** (`key_fields`). It's a decorator, not a workflow engine — the light option when you don't want Temporal or DBOS just to make one call retry-safe.

**When NOT to use it: an HTTP client with a key.** If the function is called from an HTTP endpoint where the client sends an `Idempotency-Key`, put idempotency at the [HTTP layer](#http-requests) instead — the client's key is the better signal and stops the work at the edge. `idempotent` is for the cases where no such key exists.

**What a caught duplicate looks like.** A support agent refunds with `issue_refund(order_id, amount)`:

1. The agent emits `issue_refund("A123", 50)`, id `call_abc`. Your loop runs it — refund posted.
2. Two turns later it re-plans and emits the **same** `issue_refund("A123", 50)`, now with a **new** id `call_xyz`.
3. Without dedup: a second refund. With `idempotent` keyed on `["order_id", "amount"]`, step 2 replays step 1 — the refund runs once.

> **⚠️ Dedupe on the arguments, not a per-call id.** A keyless caller has no stable key, and an LLM in particular mints a **fresh `tool_call_id` every turn** (OpenAI `call_…`, Anthropic `toolu_…`). Passing that id as the key does the opposite of what you want — a retry carries a new id, misses the dedupe, and runs the side effect again. The stable identity of a call is its **arguments**, so the default is `key_fields`; idemkit warns if you hand it a key that looks like a per-turn id.

The smallest form — key off the arguments that define the operation:

```python
from idemkit import idempotent, InMemoryBackend

@idempotent(backend=InMemoryBackend(), key_fields=["origin", "destination", "date"])
async def book_flight(*, origin, destination, date):
    return await airline.book(origin, destination, date)   # runs once per (origin, dest, date)
```

## Leading example: an LLM agent loop (OpenAI)

The model returns `message.tool_calls`, each with a `function.name`, a `function.arguments` JSON string, and a per-turn `id`. Parse the arguments, dispatch to the decorated function, feed the result back — idemkit dedupes on the arguments, and the `id` only links the result to the call. (Anthropic tool use and MCP have the same shape; only the field names differ.)

```python
import contextvars, json
from openai import AsyncOpenAI
from idemkit import idempotent, RedisBackend

client = AsyncOpenAI()
current_session = contextvars.ContextVar("current_session")

@idempotent(
    backend=RedisBackend.from_url("redis://prod"),
    scope=lambda: current_session.get(),   # ambient: per-conversation scope
    key_fields=["origin", "destination", "date"],    # dedupe on the args, not the call id
)
async def book_flight(*, origin, destination, date):
    return await airline.book(origin, destination, date)

TOOLS = {"book_flight": book_flight}

# The schema the model sees — NO session_id field, because ambient identity keeps
# the dedup scope out of the tool the model has to fill.
TOOL_SCHEMAS = [{
    "type": "function",
    "function": {
        "name": "book_flight",
        "description": "Book a flight.",
        "parameters": {
            "type": "object",
            "properties": {
                "origin": {"type": "string"},
                "destination": {"type": "string"},
                "date": {"type": "string"},
            },
            "required": ["origin", "destination", "date"],
        },
    },
}]

async def run_turn(messages: list, session_id: str) -> list:
    current_session.set(session_id)             # ambient scope for this turn
    resp = await client.chat.completions.create(
        model="gpt-4o", messages=messages, tools=TOOL_SCHEMAS,
    )
    msg = resp.choices[0].message
    messages.append(msg)
    for tc in (msg.tool_calls or []):
        args = json.loads(tc.function.arguments)        # arguments is a JSON string
        result = await TOOLS[tc.function.name](**args)  # ← deduped on key_fields, runs once
        messages.append({
            "role": "tool",
            "tool_call_id": tc.id,                       # the ONLY use of tc.id: link the result
            "content": json.dumps(result),
        })
    return messages
```

A re-planned call with a new `tc.id` replays the first result instead of booking twice.

## Method options

Plus the [core options](#core-options-every-surface) (lease, TTL, `on_storage_error`, events — `lease_ttl_seconds` is renewed by a heartbeat while the call runs).

- **`key_fields`** — the arguments that make two calls "the same"; the default, and almost always the right choice.
- **`validation_fingerprint`** — `lambda args: bytes` that must *match* on a key hit but isn't part of the key (the method analogue of HTTP's `body_fingerprint`). A reused key whose fingerprint differs raises `PayloadMismatch` instead of a wrong replay — e.g. key on `order_id`, but `validation_fingerprint=lambda a: str(a["amount"]).encode()` to catch a client that reuses an order id with a different amount. (It's not in the key, so a mismatch *errors* rather than creating a new dedupe bucket.)
- **`scope`** — the dedup scope (arity auto-detected). *Ambient* (`lambda: session_ctx.get()`) keeps the scope out of the function's signature — recommended for agents; *arg-derived* (`lambda args: args["session_id"]`) when the scope genuinely is an argument.
- **`idempotency_key`** (per call) — overrides `key_fields`; use only for a value stable across retries (an order id), never the per-turn `tool_call_id`. idemkit warns when a volatile-looking value (a `*_id`, timestamp, UUID, call id) lands in the key, by any path.
- **`normalize_args`** — derive the key from a function of the arguments (e.g. select nested fields from an object), when `key_fields` can't express it.
- **`version`** — bump to invalidate cached results when the function's behavior changes. (The old name `tool_version` still works as a deprecated alias.)
- **`result_codec`** — how the return value is stored: `"json"` (default), `"dataclass"`/`"pydantic"` (type from the return annotation; v1 and v2), a `(to_dict, from_dict)` pair, or `"pickle"` (opt-in, warns — RCE on decode). An unserializable result fails closed: it raises rather than caching garbage, and holds the claim so a retry doesn't immediately re-run.

idemkit guarantees one real execution per `(function, version, key, scope)`; retries and parallel identical calls get the cached result; a crash mid-call lets a retry re-run once.

> **Keep one irreversible side effect per function.** idemkit dedupes the *call*, not the steps inside it. If a function does two side effects (refund **and** email) and crashes between them, the claim is released and a retry re-runs the *whole* function — re-doing the first effect. Either split them into separate decorated functions, or pass an idempotency key to the downstream so the redo collapses there (the [Money paths](#patterns) pattern). idemkit can't un-send an email a half-finished function already sent.

> **Sync codebases:** use `idempotent_sync` — same options, but it decorates a plain `def` and returns a normal callable (no `await`; the dedup core runs on a background loop, your function on a worker thread). The async `idempotent` rejects a sync `def`. Don't call the sync form from inside a running event loop.

**Testing:**

```python
async def test_books_once():
    calls = 0
    @idempotent(backend=InMemoryBackend(), key_fields=["x"], scope=lambda a: "s1")
    async def charge(*, x):
        nonlocal calls; calls += 1
        return {"r": x}

    await charge(x="v")
    await charge(x="v")         # same args -> replayed, not re-run
    assert calls == 1
```

---

# Shared

## Backends

| Backend | When to use |
|---|---|
| `InMemoryBackend` | Development, tests, prototypes. **Not for production** with multiple workers (state isn't shared). |
| `RedisBackend` | Recommended for most production deployments. Redis 6+ and Redis Cluster. |
| `PostgresBackend` | When you already run PostgreSQL. Ships with `idemkit init-pg` (schema) and `idemkit pg-vacuum` (cleanup). |

The same backend serves all three surfaces. Writing your own is one Python `Protocol` (`claim`, `complete`, `release`, `renew`, `wait_for_completion`); validate it against the [conformance suite](#conformance-suite) below.

## Performance & limits

idemkit adds one backend round-trip on the way in (the atomic claim) and one on completion (a Redis Lua `EVALSHA`, or a Postgres `INSERT` then `UPDATE`); a replayed duplicate is a single read. Rough numbers from a developer laptop against **local Redis 7** — dominated by network round-trips, so treat as order-of-magnitude, not a benchmark:

- new request (claim + complete): **~1 ms** added latency
- replayed duplicate (one read): **~0.5 ms**
- **~3–4k ops/s per Python process**, scaling with the connection pool, in-flight requests, and worker processes

The overhead is one or two round-trips to your store — negligible next to anything that charges a card or calls an external API. Measure against *your* Redis/Postgres; a remote backend adds its real network latency.

**Two limits to size deliberately:**

- **`completed_ttl_seconds` vs your retry window.** A completed response is kept for `completed_ttl_seconds` (24h default). If a client's retry arrives *after* it expires, the record is gone and the handler runs again — a second side effect. Set the TTL comfortably above the longest realistic gap between an original request and an honest retry, but no longer (a longer TTL is more storage and more attack surface, not more safety).
- **Key cardinality is caller-controlled.** The dedupe key comes from the caller (an `Idempotency-Key` header on HTTP; a broker id or call arguments elsewhere), so a hostile or buggy source can mint unbounded keys and grow the store. TTLs bound this, `InMemoryBackend` enforces `max_size`, and Redis/Postgres inherit their own limits; for untrusted callers, rate-limit key creation upstream. idemkit imposes no per-caller key quota in v0.1.

## Testing your idempotency

Each surface's "Testing" example above issues a duplicate in-process and asserts the side effect fired once. For time-dependent behavior (lease expiry, completed-TTL), use **`ManualClock`** instead of real sleeps — pass it to `InMemoryBackend` and advance it:

```python
from idemkit import InMemoryBackend, ManualClock

clock = ManualClock()
backend = InMemoryBackend(clock=clock)
# ... run your handler once (claim + complete) ...
clock.advance(86_401)   # past completed_ttl_seconds — no real sleep
# ... the next call re-executes, deterministically
```

To force a record into a state directly, call the backend Protocol methods (`backend.claim(...)` → CLAIMED, then `backend.complete(...)` → COMPLETED) before exercising your handler.

## Troubleshooting

Entries are tagged `(queue)` / `(method)` where surface-specific; the rest are HTTP.

**Startup warning: `SINGLE-TENANT MODE — no scope configured`** — all callers share one namespace. Fine for a single-tenant service. Otherwise set `scope`; silence with `scope_optional=True`, or make it a hard error with `strict_scope=True`.

**`500` / `urn:idemkit:identity-unavailable`** — a configured `scope` returned empty/`None` (or raised). idemkit refuses rather than bucket the request into the shared namespace. Always return a stable, non-empty id.

**`ConfigurationError: PostgreSQL table 'idemkit_records' does not exist`** — run `idemkit init-pg <url>` once. (Postgres connects lazily, so this surfaces on the first request as a 503, not at startup.)

**A large or streamed response isn't replayed** — responses over `max_body_bytes` and streamed responses (`StreamingResponse`, SSE) pass through uncached with an `Idempotency-Replay-Unavailable` header; a retry re-executes. Raise `max_body_bytes` to cache larger single-shot responses.

**Duplicate returns 423 instead of replaying** — the first request is still running and the wait timed out. Raise `wait_timeout_seconds` above your handler's p99, or retry after `Retry-After`.

**Handler outlives `lease_ttl_seconds`** — raise it. If the lease expires mid-handler, another retry can claim the key and your completion is fenced out via the `claim_token` check — work wasted, but no incorrect state committed. (The queue and method-call surfaces renew the lease with a heartbeat.)

**A message or call I'm testing locally is skipped — the handler never runs, but it's ACKed** — you're re-running against a Redis/Postgres backend that still holds the dedup record from a *previous* run (`completed_ttl` defaults to 24h), so the id is treated as already-done and replayed. Correct behavior, not a dropped message (the event is `replayed`, not `new`). Use a unique id per run, flush the store, or lower the TTL.

**`ConfigurationError: lease_ttl_seconds must be shorter than visibility_timeout_seconds`** (queue) — the lease must be strictly shorter than the broker's visibility timeout, or a redelivery races a running handler. Leave `lease_ttl_seconds` unset for a safe default.

**`on_exhausted` fired twice** (queue) — it's best-effort, not exactly-once: a broker that over-delivers around the exhaustion boundary exhausts the message again. Make the DLQ side idempotent (key it on the dedup id).

**Warning: `counting delivery attempts in-process`** (queue) — you gave neither `receive_count` nor a durable `attempt_store`, so `max_attempts` is counted per-process only. Pass `receive_count=lambda msg: msg.receive_count` or a durable `AttemptStore`.

**`IdempotencyConflict` from a decorated function** (method) — another call with the same key is in progress and didn't finish within `wait_timeout_seconds`, or the running call lost its lease. Retry with bounded backoff; don't loop forever.

**`ConfigurationError: idempotent requires an 'async def' ...`** (method) — you decorated a sync function with the async `idempotent`. Use `idempotent_sync` for sync code, or offload the blocking call inside an async function with `await asyncio.to_thread(...)`.

## Conformance suite

The shared correctness core — atomic claim, fencing, lease reclaim, in-flight wait, TTL expiry, lease renewal — is a runnable suite any backend can validate itself against:

```python
from idemkit.conformance import BackendConformance
report = await BackendConformance(MyBackend(...)).run()
assert report.passed, report.report()
```

Or from the CLI against idemkit's own backends:

```bash
idemkit conformance --redis redis://localhost:6379 --postgres postgresql://user:pass@localhost/mydb
```

Every surface's vectors pass on real Redis and PostgreSQL. The language-neutral descriptions live in [`spec/conformance.yaml`](../spec/conformance.yaml).

## Contributing

```bash
cd python/ && python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                          # Docker-free; Redis tests use fakeredis
```

**fakeredis is not real Redis** — it doesn't reproduce server-side atomicity. To run the Lua scripts and SQL against real servers (CI does this before release):

```bash
docker run -d --rm --name idemkit-pg -e POSTGRES_PASSWORD=test -p 55432:5432 postgres:16-alpine
docker run -d --rm --name idemkit-redis -p 6379:6379 redis:7-alpine
IDEMKIT_TEST_PG_URL=postgresql://postgres:test@localhost:55432/postgres \
IDEMKIT_TEST_REDIS_URL=redis://localhost:6379 pytest
```

House rules: concurrency changes need a race test (N concurrent → exactly-once); spec-affecting changes update [`spec/conformance.yaml`](../spec/conformance.yaml); bug fixes include a regression test that fails before the fix.

---

- **Engineering spec** (the primitive, three surfaces, state machine, comparison with Stripe / AWS Powertools / IETF) → [`spec/idemkit-unified-spec.md`](../spec/idemkit-unified-spec.md)
- **Issues** → [github.com/idemkit/idemkit/issues](https://github.com/idemkit/idemkit/issues)

Apache-2.0. See [`LICENSE`](../LICENSE).
