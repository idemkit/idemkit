# How idemkit is verified

Idempotency looks simple and fails in ways you only see under load or during a crash:
two requests race and both run, or a dead worker's late write clobbers a newer result.
This is the honest account of what idemkit promises and how each promise is checked, so
you can trust it on a money path without taking our word for it.

## What it guarantees

- **One atomic claim.** Two duplicates at the same instant resolve to exactly one
  execution. No read-then-write window where both see "not done" and both run.
- **Crash safety.** A worker that dies mid-operation does not wedge the key. A
  storage-clock lease expires the claim, the next attempt reclaims it, and a fencing
  token rejects the dead worker's late write.
- **No wrong or lost result.** Once an operation completes, every duplicate replays
  that stored result. A key reused with a different body is rejected, not answered with
  the wrong response.
- **Cross-tenant isolation.** With a `scope`, one caller never sees another's result.

## What it does not guarantee

The part most libraries stay quiet about.

- **Effectively-once, not exactly-once.** Exactly-once *delivery* is impossible in a
  distributed system. idemkit gives at-least-once delivery plus idempotent execution. If
  a completion is lost (storage blips at the instant the result is recorded), the handler
  can run again. That run returns the same result and the record never corrupts, but the
  side effect happened twice. For money, pass the idempotency key downstream so the whole
  chain dedupes.
- **It fences the record, not a side effect already fired.** idemkit can reject a stale
  worker's write. It cannot un-send an email that worker already sent.
- **HTTP handlers are not heartbeated.** Queue and method handlers renew the lease while
  they run; the HTTP path does not. An HTTP handler that runs past `lease_ttl_seconds`
  (default 30s) can have its claim reclaimed by a concurrent retry that then executes a
  second time. The original's write is fenced, but its side effect already fired. Size the
  HTTP lease above your handler's p99, and push the key downstream for a slow money path.

## How it is checked

Five layers, from fixed examples to random adversarial search. Each runs against every
backend: in-memory, Redis, Postgres, MongoDB, and DynamoDB. If a behavior passes here for
all of them, the spec is satisfied uniformly, which is the whole promise of the conformance
vector file (`../../spec/conformance.yaml`) made concrete.

**1. Conformance suite.** A fixed set of behavioral vectors: atomic claim, conditional
complete, fencing a wrong token, lease reclaim, race-free in-flight wait, TTL expiry,
lease renewal. Code in `tests/conformance/`, language-neutral vectors in
`../../spec/conformance.yaml`. Run: `idemkit conformance --redis ... --postgres ...`.

**2. Concurrency and crash, on real backends.** Fire N identical claims at once and
assert exactly one wins. Simulate a crash (claim, let the lease lapse, reclaim from
another worker, then try to complete with the stale token) and assert the stale write is
fenced. Runs on real Redis, Postgres, MongoDB, and DynamoDB, because a fake does not
reproduce server-side atomicity. Code in `tests/backends/`; set the backend endpoints
(`IDEMKIT_TEST_REDIS_URL`, `IDEMKIT_TEST_PG_URL`, `IDEMKIT_TEST_MONGO_URL`,
`IDEMKIT_TEST_DYNAMODB_ENDPOINT`), then `pytest` (or `make test`).

**3. Property-based model checking.** Hypothesis generates random sequences of claim,
complete, release, renew, and advance-clock, and drives a real backend and a reference
model in lockstep, asserting they agree at every step and that a completed record always
replays its exact result. On a divergence it shrinks to a minimal repro. Runs in-memory
with a controllable clock, and against real Redis, Postgres, MongoDB, and DynamoDB. Code in
`tests/correctness/test_property_stateful.py`.

**4. Fault injection.** A wrapper backend raises a transient storage error before chosen
operations. With faults only on the claim, the side effect still runs once. With faults
on every operation including the completion write, a caller never gets a *wrong* result,
though the handler may run more than once (a lost completion is at-least-once, as above).
Seeded, so a failure reproduces. Code in `tests/correctness/test_fault_injection.py`.

**5. Clock skew.** Every lease decision uses the backend's own clock (Postgres `NOW()`,
Redis `TIME`, Mongo `$$NOW`, or the injected in-memory clock), never the app server's, so
two nodes with disagreeing clocks cannot cause a wrongful reclaim. The test skews the app
clock a million seconds mid-claim and confirms the lease is unmoved, then lets the server
clock genuinely pass the lease and confirms the reclaim fences the old owner. Code in
`tests/correctness/test_clock_skew.py`.

The one exception is `DynamoBackend`: DynamoDB has no server clock in a condition
expression, so its leases use the client clock. It passes every
other vector but is deliberately excluded from this one; use Redis/Postgres/Mongo if you
need the storage-clock guarantee.

## Run all of it

```bash
docker compose -f tests/e2e/docker-compose.yml up -d   # Redis, Postgres, Mongo, DynamoDB
make check            # lint, types, and the full suite across all five backends
```

## Not done yet

Being straight about the gaps:

- **Network-partition simulation.** The in-flight wait falls back to polling when a
  notification is dropped, and that path is tested; a full multi-node partition (with
  toxiproxy) is not.
- **A machine-checked model.** A TLA+ spec of the claim, lease, and fence state machine
  would be the strongest statement possible. On the list, not in the repo.

Found a case these miss? That is exactly the bug report worth opening.
