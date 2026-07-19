# Running idemkit in production

Two things a platform team wants before it depends on this: what to watch, and what
it can pin to. Both are here. The full option reference is in
[configuration.md](configuration.md); this page is the operational side.

## What to alert on

Every operation emits one `IdempotencyEvent` carrying a `decision`. Wire the
`event_handlers` (the [Prometheus handler](configuration.md#observability) tags each
decision), then alert off the decision rate. The ones worth a page, and what to do:

| Decision | What happened | Alert when | First response |
|---|---|---|---|
| `storage_error` | The backend was unreachable and `fail_closed` rejected the request (`503`). | Rate climbs above your normal backend-error floor. | It's a backend outage, not an idemkit bug — check Redis/Postgres/Mongo/DynamoDB health and connectivity. Requests are being *rejected*, not silently duplicated. |
| `ran_unprotected` | `fail_open` ran a request **without** dedup because the backend was down. | Any sustained rate. | Same root cause as `storage_error`, but here duplicates *can* slip through. This is the blast radius of running `fail_open` — fix the backend fast, and reconcile any money paths for the window. |
| `complete_failed` | The handler ran, but recording its result failed (a storage blip at the worst instant). | Any, on a money path. | Effectively-once degraded to at-least-once for that key: the side effect may repeat on retry. If the downstream isn't idempotent, reconcile. Investigate backend write reliability. |
| `lease_lost` | A handler outlived its lease and was cancelled mid-run (or its `renew` failed). | Rate rises. | Handlers are running longer than the lease. Raise `lease_ttl_seconds` (HTTP) or the visibility timeout (queue) above the handler's p99, or check for backend/network stalls breaking the heartbeat. |
| `conflict` | A duplicate gave up waiting on an in-flight run (`423`). | Clients see a spike of `423`s. | Usually fine (a genuine concurrent retry), but a sustained spike means handlers are slow or `wait_timeout_seconds` is too short for your p99. |
| `corrupt_record` | A stored record was unreadable and was treated as absent, then re-run. | Any non-zero rate. | Rare. Points at a serialization/version mismatch or a partially-written record. Capture the effective key and inspect the backend row. |

The healthy decisions — `new`, `replayed`, `lease_reclaimed`, `in_flight_wait` — are
your signal, not your alarm. Watch the **replay rate** as a duplicate-traffic gauge;
a sudden jump often means a client or broker is retrying harder than usual.

## Routine operations

- **Postgres: schedule the reaper.** Correctness never depends on it (expiry is
  enforced on read), but the table grows until you drop expired rows. Run
  `idemkit pg-vacuum "$DATABASE_URL"` from cron, daily is fine. Redis, Mongo, and
  DynamoDB self-expire (key TTL / TTL index / TTL attribute), so they need nothing.
- **Size `expires_after_seconds` to your real retry window, not longer.** It bounds
  how long a completed result can be replayed. Too short and a late retry re-executes;
  too long is wasted storage and more attack surface. A day is a common default; match
  it to how long your clients (or a webhook provider) actually retry.
- **In-flight-wait channel failover is a latency bump, not a hang.** If the Redis
  pub/sub or Postgres `LISTEN` channel drops (a failover), waiting duplicates fall back
  to a bounded poll and the channel re-establishes on the next waiter. Expect a brief
  latency rise on conflicts during a failover, nothing worse.
- **DynamoDB uses the client clock.** Its lease decisions rely on your hosts' clocks,
  not a storage clock. Keep NTP healthy; on badly-skewed hosts prefer Redis/Postgres/Mongo.

## Versioning and stability

idemkit is **pre-1.0 and not yet on PyPI.** Until then:

- **Pin to a commit.** Installing from source, pin the git SHA in your lockfile; the
  API can change between commits before 1.0.
- **The public API** is what's exported from the top-level `idemkit` package
  (`__all__`) plus the documented `idemkit.contrib.*` helpers. Anything under a
  leading underscore, or reached through a private module path, is internal and may
  change without notice.
- **Breaking changes are called out in the [CHANGELOG](../CHANGELOG.md).** Before 1.0 a
  minor version may carry one; each is listed there.
- **Record format.** The stored record layout and the effective-key hash carry a
  `fingerprint_version`. A change that would break in-flight cached records across a
  rolling deploy bumps it, so old and new records don't silently mismatch.

After 1.0 the project follows semantic versioning: no breaking change to the public
API without a major bump, and a deprecation is warned for at least one minor release
before removal.
