# Configuration reference

**You rarely set any of these.** The defaults are production-sane; reach for a knob only when you have a reason. Every surface takes one config object (`HttpConfig` / `QueueConfig` / `MethodConfig`), passed as `config=`. That is the one way in. To reuse settings across surfaces, write a factory that returns the config.

The **datastore** (Postgres table, Redis namespace) is configured on the backend, not here: the backend is *where* dedup state lives; the config is *how* a duplicate is judged and replayed.

> New to *lease*, *fencing token*, *visibility timeout*, *heartbeat*, or *fail_open*? See the [Glossary](../README.md#glossary).

## The three time settings

They do three different jobs. The two that get confused are `lease_ttl_seconds` (the *running* phase) and `expires_after_seconds` (the *finished* result):

```
lease_ttl_seconds     WHILE RUNNING: how long one execution may hold the key before
                      a crash is assumed and another worker may reclaim it. Queue and
                      method renew it (heartbeat); HTTP does not, so size HTTP above
                      your handler's p99. Scale: seconds to minutes.
wait_timeout_seconds  a concurrent DUPLICATE that arrives mid-run waits this long for
                      the first to finish, then gives up (HTTP -> 423; queue -> redeliver).
expires_after_seconds AFTER IT FINISHES: how long the stored result is kept, so a
                      later retry REPLAYS it instead of re-running. Scale: hours to
                      days. Set it above your longest honest retry gap.
```

## Shared options (same names on every surface)

| Option | Default | What it does, and when to change it |
|---|---|---|
| `lease_ttl_seconds` | `30` HTTP / `60` method / derived (queue) | How long one execution may hold the key before it's assumed dead. Queue and method renew it with a heartbeat; **HTTP does not**, so on HTTP raise it above your handler's p99. |
| `wait_timeout_seconds` | `10` HTTP and method / `5` queue | How long a concurrent duplicate waits for the in-flight run before giving up (HTTP returns 423; queue retries). Raise it if honest duplicates give up too soon. |
| `expires_after_seconds` | `86400` (24h) | How long the finished result is kept for replay. Set it above your longest honest retry gap. |
| `on_storage_error` | `"fail_closed"` (the safe default) | Backend down: `fail_closed` rejects the request; `fail_open` runs it unprotected, so a duplicate can slip through. Switch to `fail_open` only if availability beats dedup. |
| `use_local_cache` | `False` | An in-process replay cache that almost nobody needs. |
| `event_handlers` | `[]` | One structured event per operation (see Observability below). |

Each surface adds its own options on top of these; the full tables are under [Per-surface options](#per-surface-options) below, and there's a runnable `all_options` example for each ([http](../examples/http/all_options.py), [queue](../examples/queue/all_options.py), [method](../examples/method/all_options.py)).

```python
from idemkit import MethodConfig, QueueConfig, idempotent, IdempotentConsumer

@idempotent(backend=backend, config=MethodConfig(key_fields=["order_id"], expires_after_seconds=3600))
async def refund(*, order_id): ...

consumer = IdempotentConsumer(
    backend=backend,
    config=QueueConfig(
        dedup_id=..., visibility_timeout_seconds=30,
        max_attempts=3, on_storage_error="fail_open",
    ),
)
```

## Per-surface options

Each table adds to the shared options above. You rarely set past the first row or two.

### HTTP options

On `HttpConfig`:

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

### Queue options

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

A handler returning `None` records a "processed" marker, so a redelivery is skipped. The `contrib` helpers (`sqs_consumer`, `kafka_consumer`) preset `dedup_id` and `visibility_timeout_seconds` for you.

### Method options

On `MethodConfig`:

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

Keep one irreversible side effect per function: idemkit dedupes the call, not the steps inside it.

## Observability

Every operation emits one `IdempotencyEvent` carrying the decision (`new`, `replayed`, `conflict`, ...), the hashed key (safe to log), latency, and backend. Every surface uses the same event, so one dashboard covers all three.

Two ready-made handlers ship in `idemkit.contrib`, or write your own (an `EventHandler` is just `Callable[[IdempotencyEvent], None]`):

```python
from idemkit.contrib.prometheus import prometheus_handler   # pip install "idemkit[prometheus]"
from idemkit.contrib.logging import logging_handler         # zero deps

config = HttpConfig(event_handlers=(prometheus_handler(), logging_handler()))
```

`prometheus_handler` records `idemkit_operations_total{decision,surface,backend}` plus latency/wait histograms, so replay/conflict/storage-error rates come straight off the `decision` label. Runnable: [`examples/shared/observability.py`](../examples/shared/observability.py).

The `decision` label separates the outcomes an operator alerts on:

| Decision | Means | Alert when |
|---|---|---|
| `ran_unprotected` | `fail_open` ran this request WITHOUT protection (storage was down) | Any: it sizes the blast radius of a fail_open outage |
| `storage_error` | `fail_closed` refused the request (storage down) | Rate spikes |
| `complete_failed` | The side effect ran but its result wasn't recorded, so a retry re-runs it | Any on a money path (effectively-once degraded to at-least-once) |
| `lease_lost` | A handler outlived its lease and was cancelled mid-run | Rate rises (partition hazard) |
| `conflict` | A duplicate gave up waiting on an in-flight run | Clients see 423s |

## Postgres: schedule the reaper

Correctness does not depend on it (expiry is enforced on read), but the table grows unbounded unless you drop expired rows. Run `idemkit pg-vacuum` from OS cron (it matches your `expires_after_seconds`):

```cron
0 3 * * *  idemkit pg-vacuum "$DATABASE_URL"   # daily at 03:00
```
