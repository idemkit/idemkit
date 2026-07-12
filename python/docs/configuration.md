# Configuration reference

**You rarely set any of these.** The defaults are production-sane; reach for a knob only when you have a reason. Every surface takes one config object (`HttpConfig` / `QueueConfig` / `MethodConfig`), passed as `config=`. That is the one way in. To reuse settings across surfaces, write a factory that returns the config.

The **datastore** (Postgres table, Redis namespace) is configured on the backend, not here: the backend is *where* dedup state lives; the config is *how* a duplicate is judged and replayed.

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

Each surface adds its own options; see its `all_options` example ([http](../examples/http/all_options.py), [queue](../examples/queue/all_options.py), [method](../examples/method/all_options.py)) and the per-surface option tables in the [README](../README.md).

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
