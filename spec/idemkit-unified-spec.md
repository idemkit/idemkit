# idemkit: Idempotent Execution for Python

**Idempotent execution for HTTP, queue consumers, and AI tool calls: one correct core underneath all three, with conformance vectors that run against real Redis, PostgreSQL, MongoDB, and DynamoDB.**

> All three surfaces have reference implementations (HTTP, queue consumers, AI tool calls), each passing its conformance vectors against the real backends. The unified three-surface primitive is the architecture and the central contribution of this document. The HTTP surface has the most production mileage; the queue and AI surfaces are newer and less battle-tested.

**Version:** draft (tracks the current pre-1.0 implementation)
**Date:** 2026-05-31
**Author:** Violetta Pidvolotska
**Status:** all three surfaces implemented and tested against the real backends (Redis, PostgreSQL, MongoDB, DynamoDB): HTTP (Appendix A), queue consumers (§7, with the §7.4 vectors), and AI tool calls (§8, with the §8.4 vectors), on a shared surface-neutral core with lease renewal (§5.3.1). The shared backend-Protocol vectors ship as a runnable conformance suite (§9, §13). HTTP remains the most production-hardened surface.
**License:** CC BY 4.0
**Scope of this document:** the reference idemkit specification, the one the implementation is built against. It defines the shared core and the three surfaces (HTTP, queues, AI tool calls); the full, implemented HTTP behavioral contract is **Appendix A**. The user-facing READMEs (repo root and `python/`) are the adoption docs and are kept separate from this spec. The language-neutral test vectors live in `conformance.yaml` (this directory).

---

## 1. Thesis

HTTP idempotency, duplicate-safe queue processing, and safe retries of AI/LLM tool calls look like three different problems. They are the same problem in three settings.

In every case the shape is identical:

> a **key** identifies an operation → an **atomic claim** grants one executor the right to run it → the operation runs **once** → its **result is stored** → every **retry replays** the stored result instead of running again.

What stays the same across all three, and what is hard, is the distributed-correctness core: claiming atomically under concurrency, fencing out a stale executor whose lease expired, surviving a process that crashes mid-operation, and never serving the wrong stored result. That core is genuinely one thing.

What is **not** the same is the *semantics of the key*, and the spec is explicit about this rather than papering over it. On HTTP the client deliberately chooses the `Idempotency-Key`; the key carries the client's *intent* ("these two requests are the same operation"). On queues the key is the broker's dedup id. On AI tool calls there is no client-chosen key by default, so one is **derived** from the tool name and arguments, which means two genuinely-distinct calls with identical arguments would collapse unless the caller supplies an explicit key. The shared primitive is the claim/lease/fence *mechanism*; key derivation, and therefore what "same operation" means, is a per-surface, caller-controllable choice (§5.1, §8.2).

idemkit is built as **one correct core plus thin adapters**, not three libraries and not one bloated middleware. This document specifies that core and the three surfaces on top of it.

---

## 2. The problem, and why partial solutions keep failing

Any system that charges money, creates resources, sends messages, or calls a paid/irreversible API has to answer one question: *what happens when the same operation arrives twice?* It always arrives twice eventually: a client retries on a timeout, a broker redelivers after a missed ack, an LLM re-emits a tool call, a pod restarts mid-flight.

The naïve fix ("check if we've seen this key, if not do the work, then record the key") is wrong in ways that only show up under load or failure:

- Two requests check "have we seen this key?" simultaneously, both see "no," both execute. (No atomic claim.)
- A worker claims the key, then crashes before recording completion. The key is now stuck forever, or, worse, gets cleared and the operation runs twice. (No lease, or no fencing.)
- A worker stalls past its deadline, a second worker takes over, then the first wakes up and overwrites the second's result. (No fencing token.)
- The stored result is keyed only on the client-chosen key, so two tenants using the same key see each other's data. (No scoping.)
- A transient 5xx gets cached and replayed forever, so retries can never succeed. (No status policy.)

These are not exotic; they are the failure modes that recur across ad-hoc and library implementations alike. HTTP idempotency middleware commonly caches the response but skips crash recovery, so a handler that dies mid-run can leave a key stuck. On the queue side there is no common idempotent-consumer primitive: teams hand-roll dedup on top of broker features and get the lease/visibility-timeout interaction wrong. On the AI side, the usual "safe retry" answer is a full durable-execution framework, which is a large dependency and an architectural commitment.

idemkit's claim is narrow and testable: **ship the correct core once, and close, per surface, the specific failure modes listed above, with a conformance suite that exercises the behavior rather than only asserting it.**

---

## 3. Positioning and non-goals

### 3.1 What idemkit is

A lightweight, drop-in idempotency layer for Python services and agents. You add a middleware, a decorator, or a consumer wrapper. You do not restructure your code, run a separate engine, or learn a workflow DSL.

### 3.2 What idemkit is deliberately not

- **Not a durable-execution / workflow engine.** Temporal, DBOS, and Restate solve a larger problem (multi-step orchestration, recovery points, deterministic replay of whole workflows) and are the right tool when you need it. idemkit is the lighter answer for the common case: make *one* operation safe to retry. The niche is precisely the gap between "I copy-paste dedup by hand" and "I adopt Temporal."
- **Not a delivery guarantee.** This is the most important boundary in this spec. **Exactly-once *delivery* is impossible** in a distributed system; anyone who claims it is selling something. What idemkit provides is **at-least-once delivery + idempotent execution = effectively-once side effects.** We use the term *effectively-once* throughout and never *exactly-once*.
- **Not a guarantee about side effects a handler performs after it loses its claim.** idemkit fences the *record*; it cannot un-send an email a zombie handler already sent. See §5.7 and the per-surface mitigations.

### 3.3 Requirement keywords

The key words **MUST**, **SHOULD**, **MAY**, etc. are interpreted per RFC 2119 and RFC 8174.

### 3.4 Design principles (the user contract, in one place)

These hold on every surface. They are what a developer, a platform team, and an operator can rely on.

1. **Correctness before everything.** The core guarantee is a *rigorously-tested atomic claim with crash recovery*. Wrong idempotency is worse than none (it silently loses or duplicates operations), so the claim is a single atomic step, the record is a state machine (not a cache flag), and a crashed or fenced-out executor can never corrupt a later attempt. ("Rigorously tested" is deliberate: an extensive example-based and cross-backend suite against real Redis, PostgreSQL, MongoDB, and DynamoDB, plus property-based (Hypothesis) model checking and fault injection. We do not claim formal proof.) If only one thing in this library is bulletproof, it is this.
2. **Safe by default, without blocking the first run.** Scope is `caller + operation` (plus the request body on HTTP), never a silent global namespace. With no identity configured the library runs in **single-tenant mode** and emits a loud, repeated warning that this collapses all callers into one namespace; you set `scope` for multi-tenant isolation, and `scope_mode="strict"` turns the missing-identity case into a hard error for CI/production gates. This is a deliberate balance: value on the first run, with safety as a loud net plus an opt-in enforcement switch, rather than a hard-fail that costs adopters at the moment of peak enthusiasm. (A configured extractor that then yields nothing at request time still fails closed, see §5.1, because that is a runtime ambiguity, not a declared single-tenant choice.)
3. **Zero-config in development.** `InMemoryBackend` works out of the box. One decorator or one middleware, no infrastructure. The quick, no-infrastructure start holds for **HTTP** specifically; queues (you must wire the broker's dedup id and visibility timeout) and AI tools (you must reason about argument normalization) take more thought, and the docs say so rather than implying one-line magic everywhere. You add Redis or PostgreSQL only when you go multi-process.
4. **Pluggable backends, minimal footprint.** In-memory, Redis, PostgreSQL, MongoDB, and DynamoDB today, anything else via a five-method `Protocol` (`claim`, `complete`, `release`, `renew`, `wait_for_completion`; `renew` drives the lease heartbeat, §5.3.1). The core has **zero third-party dependencies**; each backend is one opt-in extra (`idemkit[redis]`, `idemkit[postgres]`, `idemkit[asgi]`). The buy/no-buy facts a platform team checks first, install footprint and the Python/Starlette/Pydantic/Redis/PostgreSQL compatibility matrix, live in the README and `pyproject.toml`, not buried in this spec.
5. **Explicit failure policy.** `fail_open` vs `fail_closed` on storage outage, and exactly which outcomes are cacheable, are configuration you set deliberately, never a surprise the library decides for you.
6. **Observable.** One structured event per operation, tagged with the decision (`new`/`replayed`/`in_flight_wait`/`conflict`/…) and the surface, with fields you route to Prometheus, OpenTelemetry, or logs, so one dashboard covers HTTP, queues, and tools. (A native OpenTelemetry span emitter is specified in Appendix A §4.15.1 but not yet implemented; today you bridge the event into your own tracer.)
7. **Testable.** First-class helpers to exercise a duplicate and to force a record into a given state, so your own test suite can prove your handler is idempotent. See §6.2 and §9.4.
8. **Drop-in, not a framework.** The boundary in §3.2 is load-bearing. The moment idempotency turns into workflow orchestration, it competes with Temporal/DBOS and loses. idemkit stays a decorator/middleware/wrapper.

### 3.5 Adoption path and migration

- **Day one (dev):** `InMemoryBackend` + the decorator/middleware. No infra. Idempotency is exercised in tests immediately.
- **Production:** swap the backend to `RedisBackend`/`PostgresBackend` and supply `scope`. No code change beyond construction.
- **Migrating off hand-rolled idempotency:** this is **re-keying, not a drop-in swap**. idemkit's effective key is a SHA-256 derivation; it will not match an arbitrary legacy schema stored in your own database, so existing keys are not transparently reused. The realistic, low-risk path: run idemkit in parallel in `fail_open` (it never blocks traffic), watch its events to confirm hit/miss rates look right, then cut over. During the overlap both systems may dedup independently, so size a cutover window where a small number of operations could be seen as new by one system and replayed by the other; for money paths, keep downstream idempotency on during the window. There is no zero-window, zero-rekey migration.

---

## 4. Architecture

```
                        ┌─────────────────────────────────────────────┐
   Surface adapters     │  HTTP middleware /   Queue consumer /   AI    │
   (thin, per-transport)│  HTTP route deco     consumer wrapper   tool  │
                        │                                        deco   │
                        └───────────────┬─────────────────────────────-┘
                                        │  (key, fn, result-codec, policy)
                        ┌───────────────▼─────────────────────────────┐
   Idempotent execution │  claim → lease/fence → run once → store →    │
   core (surface-neutral)│  replay;  state machine;  storage-error pol. │
                        └───────────────┬─────────────────────────────┘
                                        │  backend Protocol (5 methods)
                        ┌───────────────▼─────────────────────────────┐
   Backends             │  InMemory · Redis · PG · Mongo · DynamoDB    │
   (distributed-correct)│  claim + storage-clock lease + fencing token │
                        └─────────────────────────────────────────────┘
```

The boundary that makes this real: **the backends are already surface-neutral.** A stored record holds an opaque result blob (bytes); the claim/lease/fencing logic knows nothing about HTTP. The only HTTP-specific code lives in the HTTP adapter (status codes, header allow/deny, Problem Details, wire format). Queue and AI adapters reuse the same backends and the same core with a different *result codec* and a different *policy*.

This is verifiable today: the same `InMemoryBackend`, `RedisBackend`, `PostgresBackend`, `MongoBackend`, and `DynamoBackend` that drive the HTTP middleware also drive the queue-consumer and AI-tool adapters, including under concurrency and after a simulated crash.

---

## 5. The idempotent execution core (surface-neutral)

This section generalizes the HTTP behavioral contract in Appendix A §4 to a transport-independent primitive. Where HTTP specifics are referenced, they are normative for the HTTP surface only.

### 5.1 The operation key and scope

Every operation is identified by an **effective key**: a collision-resistant hash of the surface-appropriate scope components.

- **HTTP:** `(idempotency_key, scope, method, path)`.
- **Queue:** `(message_dedup_id, queue_or_topic, consumer_group)`.
- **AI tool:** `(tool_name, version, canonical_hash(arguments), scope)`.

Composition MUST be **length-prefixed** (`SHA-256(len_be32(c0)‖c0‖…)`) so that user-controlled components containing `0x00` bytes cannot induce collisions (see Appendix A §4.5-§4.6). A silently-shared key namespace is the most-reported security defect in this class (cross-tenant replay). The core handles a missing scope source as follows (chosen to give value on the first run while never leaking silently): with **no identity configured**, it runs in single-tenant mode and logs a loud, repeated warning; `scope_mode="single_tenant"` acknowledges single-tenant and silences it; `scope_mode="strict"` makes a missing identity a hard error. A configured extractor that yields nothing for a given request is a different case and **MUST fail closed** (see Identity guardrails), it is a runtime ambiguity, not a declared single-tenant choice.

**Client-chosen vs derived keys (the semantic seam).** Where the key encodes client *intent* (HTTP `Idempotency-Key`, broker dedup id), "same key" means "same operation" by the caller's own declaration, and the design is sound. Where the key is *derived* from arguments (the AI-tool default), the library is asserting intent the caller never stated, and two legitimately-distinct calls with identical arguments will collapse into one. Therefore: any surface that derives a key MUST also accept an explicit caller-supplied key, and the derived-key default MUST be documented as a footgun, not a feature (§8.2).

**Identity guardrails.** Because scoping rides on a user-supplied callable, the core SHOULD reject or loudly warn on obviously-unsafe identities: an empty/`None` identity in production (already fail-closed on HTTP), and a *constant* identity (e.g. a literal that collapses all callers into one bucket) SHOULD be detectable in a strict/lint mode at construction time. Silent misconfiguration here reintroduces the exact cross-tenant bug the library exists to prevent.

**Key selector + validation selector (allow-list).** For surfaces with a structured payload (queue messages, AI tool arguments), the recommended way to define what "same operation" means is two explicit selectors, not "hash everything and subtract":

- a **key selector**: which fields form the idempotency key (e.g. `["user_id", "product_id"]`);
- a separate **validation selector**: which fields must *match* on a key hit but are not part of the key (e.g. `amount`).

Fields named in neither are simply ignored, which is the clean answer to volatile fields like timestamps and request ids: you never list them, so they never affect the key. This **allow-list** model is safer than a deny-list normalizer (you opt fields *in*; you cannot forget to strip one out), and it splits "is this the same call?" from "did the payload tamper?", a key hit with a mismatched validation field yields a payload-mismatch error, not a wrong replay. idemkit adopts this model (selectors expressed as a path expression or a callable) for the queue and AI surfaces; HTTP keeps its existing client-key + body-fingerprint model (§4.5) and exposes a **`body_fingerprint` callback** that selects which body bytes are fingerprinted (the field-selector form of this model, drop volatile timestamps/nonces, keep the fields that define the operation).

### 5.2 Record and atomic claim

Each effective key maps to a **single record** (the two-key claim+result layout is forbidden; it opens a window where a reader observes `ABSENT` mid-completion). Fields: `state` (`CLAIMED`/`COMPLETED`), `fingerprint` + `fingerprint_version`, `claim_token` (128-bit random), `claimed_at`, `lease_until`, and on completion the opaque `result_blob` + `result_meta`.

The claim MUST be a single-shot atomic operation against the backend (Redis `SET NX`, PostgreSQL `INSERT … ON CONFLICT DO NOTHING`, in-memory lock). No check-then-set.

A key is a **state machine**, not a presence-in-cache flag. The distinction is the whole point: without an explicit in-progress state, two concurrent duplicates both see "absent" and both run the side effect.

```
ABSENT ──atomic claim──▶ CLAIMED (in-progress; holds claim_token + lease_until)
  ▲                          │
  │                          ├── success & cacheable ─────────▶ COMPLETED ──completed_ttl──▶ ABSENT
  │  completed_ttl           │
  │                          ├── failure / non-cacheable ─────▶ release ─────────────────▶ ABSENT
  │                          │
  │                          ├── lease_until elapsed ─────────▶ reclaimable (next claim wins, new token)
  │                          │
  └──────────────────────────┴── stale owner completes with old token ─▶ REJECTED (fenced; logged)
```

A duplicate that arrives while the key is `CLAIMED` does not execute; it waits for completion (§5.5) or, per surface, declines and retries later.

### 5.3 Lease and fencing (crash recovery)

The lease MUST be enforced by the **storage backend's authoritative clock**, never the app server's clock, to survive cross-node clock skew. A record whose `lease_until` precedes the storage clock is `ABSENT` and re-claimable.

Completion is conditional: a `CLAIMED → COMPLETED` transition applies **only if `claim_token` still matches**. If a slow executor's lease expired and another executor reclaimed the key, the slow executor's delayed completion is rejected (the *fencing* guarantee). This is what makes a crash mid-operation safe: the dead executor's record auto-expires, the next attempt reclaims, and a zombie's late write cannot corrupt the new attempt.

#### 5.3.1 Lease renewal is not optional (it is what makes crash-safety work for queue and AI)

A fixed lease is a trap for any operation that can run longer than a tight TTL, and that is the common case on the queue and AI surfaces. The dilemma:

- **Lease too long:** a real crash leaves the message/operation wedged for the whole lease before anything can retry, so crash recovery is effectively defeated.
- **Lease too short:** a legitimately slow handler loses its claim mid-flight. Fencing then correctly rejects its *completion*, but the handler is **still running and will still perform its side effect**, while a second executor also runs, a double execution. Fencing protects the record, not the side effect (§5.7).

"Size the lease above p99" is not a fix: p99 is not a maximum, and the tail is exactly where this bites. The correct answer is **lease renewal (heartbeat)**: while a handler runs, the executor periodically extends `lease_until` (conditional on still holding `claim_token`), so the lease tracks actual progress instead of a guessed upper bound. A crash stops the heartbeat and the lease lapses promptly; a slow-but-alive handler keeps its claim.

**Why a platform-deadline shortcut is not enough off-platform.** A serverless model can derive its in-progress expiry from the platform's *remaining execution time*, which works because the platform enforces a hard timeout, so the remaining time *is* a true maximum and no renewal is ever needed. idemkit uses the same idea where a deadline exists (a queue lease SHOULD be derived from the broker's visibility timeout, an AI lease from any deadline available), but off such a platform there is usually **no hard deadline**, so a single derived value is only an estimate and renewal is required to be correct. Renewal is the cost of running anywhere, not only where the runtime kills you at a known time.

**Completion fencing.** A hard-deadline model can complete the record unconditionally, relying on the runtime having killed any timed-out executor. idemkit instead fences completion with the `claim_token`, so even a *stalled-but-alive* worker (the normal case without a hard deadline) cannot overwrite a newer execution's result after its lease was reclaimed.

**Renewal introduces a partition hazard, and the mitigation is bounded by Python's cancellation model.** Heartbeating creates a new failure mode: the handler is alive but the link to storage blips, `renew()` fails, the lease lapses, and a second executor reclaims, and now two live executors run the same operation. Fencing protects the *record* (only one completion wins) but not the *side effect* (both may have already fired it). The mitigation is **cooperative cancellation**: when `renew()` fails and the lease can't be confirmed extended, the executor signals cancellation into its own handler before `lease_until` instead of pressing on.

To be precise (this would be an overclaim as an absolute): asyncio cancellation only takes effect at an `await` point. A handler that `await`s regularly will observe the `CancelledError` and can stop and clean up before the lease expires. For those, cancellation closes the partition window. A handler that is blocked in a synchronous call (a sync DB driver, `requests`, a CPU-bound loop) will **not** see the cancellation until it next reaches an `await`, which may be after the lease has already lapsed. For those, renewal degrades back to the partition hazard, exactly like §5.7's broader cancellation limit. So: implementations MUST issue the cancellation and MUST size the renewal interval with margin below the lease to detect a failed renew early; but they MUST also document that the guarantee holds only for cooperatively-scheduled handlers, and that blocking/CPU-bound handlers on a money path still need downstream idempotency (§5.7). Renewal turns a crash hazard into a partition hazard that is *closed for well-behaved handlers and merely mitigated for blocking ones*, not eliminated for all.

Therefore renewal is **normative for long-running operations**, not a "later" nicety:

- The backend Protocol includes a `renew(effective_key, claim_token, lease_ttl) -> bool` operation (conditional on the token, like `complete`/`release`).
- The queue and AI adapters heartbeat for the duration of the handler, at an interval well under the lease.
- On the queue surface this relaxes the §7.2 constraint: the lease can be short (so crash recovery is fast) *and* a slow handler is safe, with the lease kept under the broker's visibility timeout by renewal rather than by a single large value.
- The HTTP surface (short handlers) does not heartbeat; an HTTP handler that outlives its lease can be reclaimed, so size the HTTP lease above its p99 or push the key downstream (§5.7).

### 5.4 Result codec (the per-surface seam)

The stored result is opaque to the core. Each surface supplies a `ResultCodec`:

- **HTTP:** status + permitted headers + body bytes (full-fidelity replay).
- **Queue (side-effect-only):** a tiny "processed" marker; there is no payload to replay, so a `COMPLETED` record means *skip*, not *return garbage*.
- **Queue (result-bearing) / AI tool:** a serialized return value. The result is always stored as JSON in the backend, and a pluggable **output serializer** maps the typed return value to/from that JSON: a default JSON codec, plus **dataclass** and **pydantic** serializers that infer the type from the function's return annotation, plus a custom `(to_dict, from_dict)` codec. `pickle` is **opt-in only** and MUST emit a security warning (it is an RCE vector); an unserializable result MUST fail closed (do not silently re-run an operation that has side effects). Typed in, typed out, JSON on the wire, no surprises.

### 5.5 In-flight handling (race-free wait)

When a duplicate observes a live `CLAIMED` record, it MUST **subscribe before re-reading state**, then wait for the completion notification or a timeout. Subscribe-after-read loses notifications published in the gap and hangs the waiter for the full timeout. Backends implement this with Redis Pub/Sub on a shared channel and PostgreSQL `LISTEN/NOTIFY` demultiplexed in-process (see Appendix A §4.3).

**Notifications are best-effort transport, so the wait MUST NOT depend on them for correctness.** Redis Pub/Sub and PostgreSQL `LISTEN/NOTIFY` are at-most-once: a notification can be dropped on a connection blip, replica failover, or cluster reshard. Subscribe-before-read closes the *logical* race, but a lost notification would otherwise hang a waiter for the full timeout. Therefore a waiter MUST also **poll the record on a bounded, backed-off schedule** (e.g. re-read at increasing intervals up to the wait timeout); the notification is a latency optimization, the poll is the correctness floor. The completion record itself, written before the notification fires, is the source of truth.

**Shared-listener lifecycle.** The single per-process `LISTEN`/subscriber connection is a deliberate scalability choice (per-request `LISTEN` exhausts the pool), but it is a shared resource and a contention/failure point. Its lifecycle MUST be defined: it is created lazily on first wait, its subscriber task is supervised and restarted if it dies, on reconnect it re-subscribes and waiters fall back to polling for the gap, and it is torn down on backend `aclose()`. A crashed listener MUST degrade to polling, never silently stop waking waiters.

The waiter's behavior on timeout is surface-specific (HTTP returns 423; a queue consumer typically declines to ack and lets the broker redeliver).

### 5.6 Cacheable-outcome policy

Not every outcome should be cached. The core MUST let each surface declare which outcomes are replayable:

- **HTTP:** 2xx by default; 5xx MUST NOT be cached by default (replaying a transient error blocks retries forever).
- **Queue / AI:** success is cached; a raised exception releases the claim so a retry re-runs, bounded by a max-attempts policy (§7.4, §8.x).

### 5.7 Corrupt records, storage errors, and the cancellation limit

- A record that fails to deserialize MUST be treated as `ABSENT` (re-execute), never a 500, and MUST emit a `corrupt_record` event.
- Storage-error policy is explicit per deployment: `fail_closed` (refuse the operation) or `fail_open` (run without protection). Default `fail_closed`.
- **Cancellation limit (applies to every surface):** idemkit's caching layer protects against duplicate *result delivery*, not against duplicate downstream *side effects* originating from an executor that kept running after it lost its claim. The mitigation is the same everywhere: pass the key downstream (Stripe-style), use a transactional outbox, or make the side effect itself check-before-act. This is documented, not hidden.

### 5.8 Observability

Each operation emits a structured event for its terminal decision (`new`, `replayed`, `in_flight_wait`, `conflict`, `payload_mismatch`, `lease_reclaimed`, `lease_reclaimed_loss`, `storage_error`, `corrupt_record`), carrying the hashed effective key (never the raw key), latency, backend, and `fingerprint_version`. The surface name (`http` / `queue` / `ai_tool`) is included so one dashboard covers all three.

A `response_hook` MAY post-process a replayed result before it is returned, e.g. to stamp a "this was a replay" marker, without the caller re-deriving that from the decision.

### 5.9 Local cache (warm-path optimization)

An optional in-process LRU cache (`use_local_cache`, `local_cache_max_items`) lets a repeat of a key seen in the same process skip the backend round-trip. Two rules are normative:

- **Only `COMPLETED` records are cacheable locally; an `INPROGRESS` claim MUST NOT be cached** (its state is volatile and owned elsewhere).
- The local cache is a latency optimization, never the source of truth: it is bounded, TTL-checked on read, and invalidated on release. Correctness still comes from the backend; the cache only short-circuits a known-`COMPLETED` replay. In a multi-process deployment a local cache cannot see another process's completion, so it only ever *adds* a backend read it could skip, it never causes a miss to become a wrong hit.

---

## 6. Surface A: HTTP idempotency (implemented)

The HTTP surface is fully specified in Appendix A §4-§9 and implemented. Summary of what it closes and how it is tested.

**Closes:** atomic claim under concurrent retries; race-free in-flight wait; full response fidelity (status + permitted headers + body); payload-fingerprint mismatch → 422/409 (never replay the wrong response); crash recovery via lease + fencing; cross-tenant isolation *once `scope` is configured*, with the no-identity default running single-tenant behind a loud, repeated warning (and `scope_mode="strict"` to hard-fail instead, §5.1); 5xx never cached; response size enforced by streamed bytes (not `Content-Length`); streaming responses bypassed; corrupt record → re-execute; quoted/unquoted `Idempotency-Key` treated identically; case-insensitive extractor headers.

**Surfaces it exposes:** ASGI middleware (app-wide) and the `Idempotency` decorator (per-route, with the real Starlette `Request`).

**Fingerprint cost (hot path).** Hashing the request body on every call has a CPU cost that grows with body size and shows up at p99 for large payloads. idemkit bounds it: the body fingerprint is capped by `max_request_body_bytes` (default 1 MiB), a larger request bypasses idempotency rather than being hashed unbounded (§7-style streamed counting, never trusting `Content-Length`), and a `body_fingerprint` callback returning `b""` keys on method + path + key + caller when the body is large or volatile. So the fingerprint is never an unbounded hot-path hash; it is either small, capped, or skipped by configuration.

**Tests:** the HTTP surface suite runs against InMemory, real Redis, PostgreSQL, MongoDB, and DynamoDB, including the cross-backend contract suite, concurrency (N-at-once → exactly one execution), crash/lease-reclaim, payload mismatch, header filtering, size/streaming bypass, redactor-failure-does-not-persist, and the per-route decorator.

### 6.1 Usage contract

**Dev (zero infra), works in tests in five minutes:**

```python
from idemkit import IdempotencyMiddleware, InMemoryBackend

# Single-tenant by default; logs a one-time warning until you add scope.
app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend())
```

**Production, only the backend and identity change:**

```python
from idemkit import IdempotencyMiddleware, RedisBackend

app.add_middleware(
    IdempotencyMiddleware,
    backend=RedisBackend.from_url("redis://prod"),
    scope=lambda req: req.state.user.id,   # real identity
    on_storage_error="fail_closed",
)
```

**Per route instead of app-wide** (gets the real Starlette `Request`):

```python
idem = Idempotency(backend=RedisBackend.from_url("redis://prod"),
                   scope=lambda req: req.state.user.id)

@app.post("/charge")
@idem.protect
async def charge(request: Request) -> JSONResponse:
    return JSONResponse({"id": charge_once()}, status_code=201)
```

- **You write:** the client sends `Idempotency-Key: <uuid>`; you supply `scope` once you have more than one tenant (single-tenant works with none).
- **idemkit guarantees:** one execution per `(key, caller, method, path)`; duplicates replay the exact stored status + headers + body with `Idempotency-Replayed: true`; same key + different body never replays the wrong response.
- **You handle (all `application/problem+json`):** 422 payload mismatch · 423 in-flight timeout · 400 missing/oversized key · 503 storage (fail-closed) · 500 `identity-unavailable` (server-side config). 409 replaces 422/423 in `compat_mode="stripe"`; branch on the `type` URI, not the status.

### 6.2 Testing your endpoint is idempotent

```python
async def test_charge_is_idempotent():
    r1 = await client.post("/charge", headers={"Idempotency-Key": "k"})
    r2 = await client.post("/charge", headers={"Idempotency-Key": "k"})
    assert r1.json() == r2.json()
    assert r2.headers["idempotency-replayed"] == "true"
    assert charges_in_db() == 1            # the side effect happened once
```

This is the test idemkit's own suite runs against all five backends; the same shape checks *your* handler.

### 6.3 Client retry contract (what the caller does with each outcome)

Error messages and the client's response to them are a first-class UX surface, not an afterthought. The library MUST make outcomes actionable on both sides:

- **Server/app side, and the two paths differ, so the spec is explicit:** the **app-wide middleware** sits above your handler in the ASGI stack and cannot raise into your code; it returns the `problem+json` response directly (your code never sees the conflict). The **per-route decorator** wraps your handler, so it MUST raise **typed exceptions** (`IdempotencyConflict`, `PayloadMismatch`, `IdempotencyKeyMissing`, `StorageUnavailable`) that your code can catch and branch on. If you need to branch in application code, use the decorator; if you only need the wire response, the middleware suffices. Either way, messages MUST be human-readable and actionable, "key `…hash…` is in progress by another executor, lease expires in ~Ns", never a bare hash or a `KeyError`.
- **API-consumer side**, the contract per outcome:

| Outcome | Status (default / stripe) | Client action |
|---|---|---|
| Replay of a completed op | original status + `Idempotency-Replayed: true` | use it; it's the original result |
| In progress, wait timed out | `423` / `409` + `Retry-After` | retry after `Retry-After`, with capped exponential backoff; **bounded**: do not loop forever |
| Payload mismatch | `422` / `409` | do not retry as-is; resend the original body or use a new key |
| Storage unavailable | `503` + `Retry-After` | retry after `Retry-After` |
| Missing/oversized key | `400` | fix the request; non-retryable |

The `423`-loop hazard is called out explicitly: if the original holder is wedged, a naïve client retrying on `423` forever never makes progress. The `Retry-After` value is bounded by the lease, and once the lease expires the next retry reclaims and executes, so a client MUST use bounded backoff and surface a failure after a sensible attempt budget rather than spin. This guidance ships in the docs and in the typed exception's message.

---

## 7. Surface B: Queue consumer idempotency (implemented)

### 7.1 Why queues need their own depth

Brokers (SQS, Kafka, RabbitMQ, Redis Streams, GCP Pub/Sub) deliver **at-least-once**. The consumer is responsible for not acting twice. Teams usually dedup by storing seen message IDs, and that ad-hoc approach reproduces every core failure mode plus two that are unique to queues. idemkit's queue adapter MUST close all of them.

### 7.2 Gaps idemkit closes (and that ad-hoc dedup does not)

1. **Lease ↔ visibility-timeout coupling.** The number-one queue dedup bug: the broker redelivers a message after its visibility/ack deadline while the first consumer is still working, and both run. idemkit's lease MUST be derived from, and strictly shorter than, the broker's visibility timeout. The adapter MUST accept the visibility timeout, MUST set `lease_ttl < visibility_timeout`, and SHOULD warn at startup if the handler's observed p99 approaches the lease. The relationship is part of the contract, not left to the operator to discover in an incident.
2. **Crash-safe claim that outlives the consumer.** A consumer that dies after claiming but before acking MUST NOT wedge the message. The storage-clock lease + fencing token (§5.3) guarantee the record expires and the redelivered message is reclaimed and processed once; the dead consumer's late completion is fenced out.
3. **Side-effect-only mode.** Queue handlers usually return nothing. The adapter MUST support a "processed marker" codec (§5.4) so a `COMPLETED` record means *skip this redelivery*, not *replay a payload that does not exist*. Result-bearing handlers (e.g., compute-and-store) MAY cache the return value.
4. **Poison-message / DLQ boundary.** idempotency releases the claim on handler failure so the broker can redeliver, but unbounded. The adapter MUST track attempts and, after a configured `max_attempts`, stop releasing and invoke a `on_exhausted` hook (route to a dead-letter queue, alert, etc.). The spec defines the line: idempotency owns "run once on success"; poison-handling owns "give up after N failures."
5. **Concurrent redelivery across consumers.** Two consumers in a group that grab the same message (rebalance, double-delivery) MUST resolve to exactly one execution via the atomic claim; the loser declines without re-running.

6. **Attempt counting that survives release (a concrete design constraint).** On failure the claim is released so the broker can redeliver, but that deletes the record, and `max_attempts` needs a counter that *outlives* the deleted record across redeliveries. The core claim/lease Protocol has nowhere to put it, so the queue adapter MUST handle attempts explicitly, in priority order: (a) **use the broker's native receive count** when available (SQS `ApproximateReceiveCount`, Kafka delivery attempts, etc.), the simplest and most reliable source; (b) otherwise maintain a **separate attempts record** keyed by dedup id, with its own TTL ≥ the redelivery window, incremented atomically per delivery and distinct from the claim record. The spec forbids the naive approach of inferring attempts from the claim record (it is gone) or holding the claim record un-released to count (that wedges the message). This is called out because it is the kind of gap that looks handled and is not.

### 7.3 API shape (illustrative, not final)

```python
from idemkit import IdempotentConsumer, RedisBackend

consumer = IdempotentConsumer(
    backend=RedisBackend.from_url("redis://..."),
    key=lambda msg: msg.message_id,           # broker-native dedup id
    scope=lambda msg: (msg.queue, msg.consumer_group),
    visibility_timeout_seconds=30,                  # lease is derived from this
    max_attempts=5,
    on_exhausted=lambda msg, exc: dlq.send(msg),    # poison boundary
)

@consumer.handle
async def process(msg) -> None:                     # side-effect-only by default
    await charge_customer(msg.body)                 # runs exactly once per dedup id
```

The consumer MUST integrate with the broker's ack/nack: ack on success or on a deduplicated skip; nack/leave-for-redelivery on a transient failure within `max_attempts`; ack-and-DLQ on exhaustion.

- **You write:** how to read the broker's dedup id (`key`) and the visibility timeout; your handler.
- **idemkit guarantees:** the handler's side effect runs once per dedup id even under at-least-once redelivery, concurrent consumers, and consumer crashes; the lease is held shorter than the visibility timeout so a redelivery can't race a still-running handler.
- **You handle:** nothing in the happy path: the wrapper acks/nacks for you. You decide `max_attempts` and `on_exhausted` (the DLQ boundary).

### 7.4 Required conformance vectors (the "with tests" mandate)

A conforming queue adapter MUST pass, against at least one production backend:

- `queue-at-least-once-dedup`: same dedup id delivered N times → handler side effect occurs exactly once; every delivery acks.
- `queue-concurrent-redelivery`: N consumers receive the same message simultaneously → exactly one executes; the rest decline without running.
- `queue-crash-recovery`: a consumer claims then "crashes" (no ack, no complete); after the lease expires, redelivery reclaims and processes exactly once; the zombie's late completion is rejected.
- `queue-lease-shorter-than-visibility`: configuration with `lease_ttl >= visibility_timeout` is rejected (or warned) at construction.
- `queue-poison-dlq`: a handler that always raises is retried up to `max_attempts`, then `on_exhausted` fires exactly once and the message is not retried further.
- `queue-result-bearing-replay`: a result-bearing handler's return value is replayed on redelivery without re-execution.

These mirror the HTTP suite's structure so the same correctness is demonstrated, not assumed.

---

## 8. Surface C: AI / LLM tool-call idempotency (implemented)

### 8.1 Why this is the hardest surface, and where the line is

An LLM agent re-emits the same tool call for ordinary reasons: a retry after a timeout, a re-planning loop, parallel agents exploring the same step. If that tool charges a card, books a flight, or posts a message, the duplicate is a real-world incident. The heavyweight alternative here is a durable-execution engine or framework-level checkpointing. idemkit's position MUST stay: a **drop-in decorator for a single side-effectful tool**, with no workflow engine and no runtime to operate.

### 8.2 Gaps idemkit closes

1. **Explicit key first, derived key as a documented fallback.** Deriving the key from arguments asserts an intent the caller never stated: two genuinely-distinct calls with identical arguments (a re-planning loop where the user really does want a second booking) would collapse into one. This is the seam from §1/§5.1, and the spec resolves it loudly: the decorator MUST accept an **explicit idempotency key** (e.g. an `idempotency_key=` argument or a key supplied from agent/run context) that, when present, is authoritative. Only when no explicit key is given does it fall back to deriving `(tool_name, version, canonical_hash(arguments), scope)`, and that fallback MUST be documented as a footgun, not a default to lean on.

2. **Key/validation selectors, with strict-mode rails.** When no explicit key is given, the preferred way to derive one is the allow-list selector model from §5.1: name the argument fields that form the key, and (separately) the fields that must merely *match*. Volatile fields you never name simply don't affect the key, the clean fix for timestamps/nonces, and safer than a deny-list normalizer (which idemkit still accepts as a fallback). Canonicalization uses the length-prefixed construction and inherits the documented limits of JSON sorted-keys hashing (numeric `1` vs `1.0`, Unicode NFC; see Appendix A §4.5). Because mis-keying here silently causes duplicate side effects or lost operations, the library MUST ship a **strict mode** that warns when a volatile-looking value (`request_id`, `*_id`, `timestamp`, `nonce`, UUID- or ISO-8601-shaped) ends up in the key with no selector or normalizer configured. The rail turns a silent mis-key into a loud warning.
3. **Serialization safety, fail-closed.** Tool results are arbitrary Python objects. Default codecs are JSON or a typed codec; `pickle` is opt-in with a loud security warning; a result that cannot be serialized MUST fail closed (run once, do not cache, and do not silently permit a second side-effecting run). The library MUST NOT quietly downgrade to "re-run on every retry" for a side-effectful tool.
4. **Side-effect classification is the caller's, explicitly.** A pure tool needs no idempotency and wrapping it only wastes storage; a side-effectful tool needs it. The decorator MUST NOT guess. It applies to whatever the developer decorates, and the docs make the distinction loud.
5. **Concurrent-call dedup with in-flight wait.** When the same tool call is in flight and a duplicate arrives (parallel agents, fast retry), only one executes; the duplicate waits and replays via the subscribe-before-read pattern (§5.5). This is the agent-world analogue of HTTP concurrent retries.
6. **Non-determinism, stated plainly.** Idempotency caches the *first* result for a given key; it does not make a nondeterministic tool deterministic, and a different key (different arguments) is a different call. This is documented so users do not mistake idempotency for memoization-of-meaning.
7. **Long-running tools.** Tool calls (especially ones that themselves call slow APIs or models) routinely outlast a tight lease, so this surface depends on **lease renewal/heartbeat** (§5.3.1), not on guessing a lease above p99. Renewal keeps a live-but-slow tool's claim while still letting a crashed one's lease lapse promptly.

### 8.3 API shape (illustrative, not final)

The recommended form uses **declarative selectors** (§5.1): you name the argument fields that form the key, and volatile fields like `request_id` are simply never listed.

```python
from idemkit import idempotent, RedisBackend

backend = RedisBackend.from_url("redis://...")

@idempotent(
    backend=backend,
    version="1",
    # scope receives the call's bound arguments as a dict (name -> value),
    # so you select the scope field by name. Per-agent / per-session scope:
    scope=lambda args: args["session_id"],
    key_fields=["origin", "destination", "date"],   # request_id is NOT listed, so it's ignored
    result_codec="json",                            # pickle is opt-in + warned
    lease_ttl_seconds=120,                           # renewed while running (§5.3.1)
)
async def book_flight(*, origin, destination, date, session_id, request_id):
    return await airline_api.book(origin, destination, date)  # charged once
```

A side-effectful tool wrapped this way executes once per `(tool, version, selected-args, caller)`; concurrent or retried calls with the same key get the cached result. For full control there is also an explicit `idempotency_key=` argument (authoritative when present, §8.2), and a `normalize_args=` callable as an advanced fallback when a selector can't express the rule, but reach for the selector first; it's declarative, visible in the signature, and testable.

- **You write:** the decorator on a side-effectful tool, naming `key_fields` (or an explicit key). Pure tools you simply don't decorate.
- **idemkit guarantees:** one real execution per `(tool, version, selected-args, caller)`; agent retries and parallel identical calls get the cached result; a crash mid-call lets a retry re-run once.
- **You handle:** an unserializable result surfaces an error instead of silently risking a second side effect (fail-closed); you opt into `pickle` only with the warning.

### 8.4 Required conformance vectors (the "with tests" mandate)

- `tool-retry-dedup`: same tool + same normalized args called N times → underlying side effect occurs once; later calls return the cached result.
- `tool-arg-normalization`: calls differing only in a normalized-out field (e.g. `request_id`) dedupe together; calls differing in a semantic field do not.
- `tool-concurrent-calls`: N concurrent identical calls → one execution, the rest wait and replay.
- `tool-crash-recovery`: a tool that "crashes" mid-call releases/leases correctly so a retry re-runs once.
- `tool-unserializable-result-fails-closed`: a non-JSON result with the JSON codec does not silently re-run on retry; it surfaces an error rather than risking a second side effect.
- `tool-pickle-opt-in-warns`: enabling the pickle codec emits a security warning.

---

## 9. Cross-surface conformance and testing strategy

The originality and the credibility both rest here. idemkit ships a **language-neutral conformance vector file** and runs the shared core vectors plus each surface's vectors against every production backend.

- **Shared core vectors** (already implemented for the backend Protocol): atomic claim, duplicate → already-claimed, conditional complete, fencing on wrong token, lease reclaim, crash-then-reclaim, concurrent-claim-exactly-one, in-flight wait, wait timeout, completed-TTL expiry, corrupt-record recovery. These run uniformly across InMemory, Redis, PostgreSQL, MongoDB, and DynamoDB (DynamoDB excepts only the clock-skew vector, as its leases use the client clock).
- **Per-surface suites**: HTTP (implemented), queue (§7.4), AI tool (§8.4).
- **The promise:** a behavior that passes the shared vectors on all backends, plus a surface's vectors, is correct on that surface, uniformly, and demonstrably. The same `claim/lease/fence` correctness is proven once and reused, rather than re-implemented (and re-broken) per surface. A cross-language conformance runner is future work (see Appendix A §8).

### 9.4 Test affordances for users (testability as a feature)

Idempotency is notoriously hard to test, so the library MUST make it easy for *users* to prove their own handlers are correct, not just trust idemkit's internal suite. The library MUST provide:

- A way to **issue a duplicate** in-process and assert the side effect fired once (the §6.2 shape, available on every surface). *Implemented* (the per-surface "Testing" examples).
- A way to **force a record into a state**: `CLAIMED` (to test the in-flight/conflict path), `COMPLETED` (to test replay), or expired-lease (to test crash recovery), without timing hacks or real sleeps. *Implemented*: call the backend Protocol directly (`claim` then `complete`/`release`), combined with the clock below for expiry.
- A **fake clock / advanceable time** hook on the in-memory backend so lease-expiry and TTL tests are deterministic, not flaky `sleep()`s. *Implemented*: `InMemoryBackend(clock=ManualClock())`, then `clock.advance(seconds)`.

These are first-class, documented helpers. Most idempotency tools are black boxes you cannot test against; making the failure modes reproducible in a unit test is itself a differentiator senior teams select on.

---

## 10. Related approaches and what they leave open

A category comparison, framed by capability rather than by naming products.

- **HTTP idempotency middleware / response caches.** Cache a response under the idempotency key and replay it. The common gap is crash recovery: many cache the result but do not fence a handler that dies mid-run, so a key can stick, or a stalled-but-alive worker can double-run. Concurrency is often handled as a hard `409` rather than a wait-and-replay.
- **Ad-hoc queue dedup.** Teams hand-roll a "seen this message id?" check on top of broker features. The recurring bug is the lease ↔ visibility-timeout coupling: if the handler outlives the broker's visibility window the message is redelivered and runs twice, and attempt counts are lost across releases.
- **Lock-based task dedupers.** A distributed lock keyed on task name + arguments blocks concurrent duplicates, but the lock is typically held until the task finishes, so a crash or hard-kill can strand the key, and there is usually no request-fingerprint check.
- **Durable-execution engines.** Correct and powerful, but they solve a larger problem (multi-step orchestration, deterministic replay) and require restructuring your code around a separate runtime — overkill when you only need one operation to be safe to retry.

idemkit's position is the correct, lightweight, portable middle: one atomic claim with a storage-clock lease and fencing token, a race-free in-flight wait, request fingerprinting, and lease renewal for handlers with no hard deadline — the same core across HTTP, queues, and AI tool calls, checked by a single public conformance suite. Prior designs it draws on are credited in §14.

---

## 11. Original contribution

Stated precisely, strongest first:

1. **A public, language-neutral conformance suite for idempotency correctness.** A reusable set of correctness vectors — atomic claim, fencing, crash/lease-reclaim, in-flight race, TTL, corrupt-record, and the per-surface cases — that any backend or language implementation can run against itself. It ships with a runnable runner and language-neutral vectors in `conformance.yaml`; extracting it into a standalone repository, so other libraries can validate against it, is the next step (§13). No comparable public conformance artifact for idempotency exists elsewhere.
2. **A single rigorously-tested core, exposed across three surfaces.** Atomic claim, storage-clock lease, fencing token, lease renewal, crash recovery, corrupt-record safety, and a race-free (notification-plus-poll) in-flight wait — implemented once and reused, rather than re-implemented and re-broken per surface.
3. **Per-surface depth that closes enumerated, real failure modes** (§6, §7.2, §8.2): the queue lease/visibility-timeout coupling and attempt-counting across releases, and the AI explicit-key/normalization and serialization-safety problems.
4. **A lightweight, framework-agnostic, conformance-verified implementation of the unified primitive across all three surfaces** — portable, dependency-light, and publicly conformance-tested across HTTP, queues, and AI tool calls.

The single load-bearing engineering element underneath all of this is a **rigorously-tested atomic claim with crash recovery** (atomic claim + state machine + storage-clock lease + fencing + renewal). It is where the engineering value converges: an architect trusts it, a platform team adopts because of it, a user stops losing operations.

This is an engineering synthesis, a documented operational profile, and a public conformance suite, backed by a working reference implementation. It is **not** a claim to have invented idempotency, **not** a claim of formal proof (it is rigorously tested, not proven), and **not** a claim of exactly-once delivery.

---

## 12. Security and compliance

- The raw key is never persisted, logged, or emitted; the hashed effective key is the audit-safe identifier.
- Cross-tenant collision is prevented by mandatory scope + length-prefixed hashing (§5.1).
- Stored results may contain sensitive data; a redactor hook operates on a copy before persistence, and a redactor failure MUST NOT persist unredacted data (it releases instead). PCI/GDPR/SOC2 notes carry over from Appendix A §9.
- The `pickle` result codec is a remote-code-execution risk and is opt-in with a warning; JSON/typed codecs are the default.
- idemkit is not a defense against replay-of-old-key beyond the configured TTL, and does not prevent duplicate side effects from an executor that survives losing its claim (§5.7). Pair high-value paths with downstream idempotency.
- **Key cardinality is attacker-controlled.** The `Idempotency-Key` (HTTP) and a derived AI key are chosen by the caller, so a hostile or buggy client can mint unbounded distinct keys and grow the store. Mitigations: every record carries a TTL (`expires_after_seconds`, and the lease TTL bounds in-flight rows); `InMemoryBackend` enforces `max_size` and rejects beyond it; Redis/PostgreSQL inherit their own memory/disk limits and the TTL-driven vacuum. For untrusted clients, additionally rate-limit or cap per-caller key creation upstream and keep `expires_after_seconds` no longer than your real retry window (a longer TTL is more attack surface, not more safety). idemkit does not itself impose a per-caller key quota in v0.1.

---

## 13. Roadmap and versioning

Implemented today (one pre-1.0, not-yet-published line):

- **HTTP surface** — decorator + middleware, five backends (in-memory, Redis, PostgreSQL, MongoDB, DynamoDB), and the conformance suite.
- **Queue consumer surface (§7)** with lease renewal / heartbeat (§5.3.1): the `renew()` Protocol method plus adapter heartbeating (crash-safety on long handlers depends on it), attempt-counting (§7.2, via broker receive count or a separate store), and the §7.4 vectors, tested against a generic broker harness on the real backends.
- **AI tool-call surface (§8)** with explicit-key support, the selector / `normalize_args` strict-mode rail (§8.2), the typed result serializers (§5.4: JSON / dataclass / pydantic v1+v2 / custom / opt-in pickle), and the §8.4 vectors.
- **Property-based (Hypothesis) model checking and fault injection** over the claim/lease/fence core; typed exceptions (`IdempotencyConflict`/`PayloadMismatch`/…) and the client-retry contract (§6.3); a strict/lint mode that flags an unsafe or constant `scope` and volatile-looking key fields (§5.1, §8.2).
- **Conformance vectors + runner** ship in-package (`idemkit/conformance/`) with the language-neutral surface vectors in `conformance.yaml`, so any backend implementing the Protocol can validate itself.

Not yet:

- Extracting the conformance suite into its own repository, and at least one third-party project running the vectors against itself.
- Additional broker and backend adapters; cross-language ports reusing the conformance vectors.

**Versioning.** The library is pre-1.0 and not yet published; treat the API as subject to change until 1.0. Behavioral clause numbers are preserved across revisions so test vectors and external references stay stable.

### 13.1 Licensing

The reference implementation code is **Apache-2.0** (see the repository `LICENSE`); this specification document is CC BY 4.0. Any future hosted/managed offering (dashboard, managed backend, enterprise conformance certification) would sit alongside the permissively-licensed core, not gate it, the correctness core stays open so the library is safe to depend on. The precise open-core boundary is not yet decided and is intentionally out of scope for this spec.

---

## 14. References

- RFC 2119, RFC 8174: requirement keywords
- RFC 9457: Problem Details (HTTP surface)
- RFC 8941: Structured Field Values (HTTP `Idempotency-Key`)
- `draft-ietf-httpapi-idempotency-key-header-07`: HTTP wire format (see Appendix A §10)
- Stripe idempotency design; Brandur Leach, *"Implementing Stripe-like Idempotency Keys in Postgres"*
- AWS Lambda Powertools (Python) idempotency utility
- Temporal, DBOS, Restate: durable-execution frameworks
- `hono-idempotency` (@paveg): HTTP idempotency middleware in the Node.js / Web-Standards ecosystem

---

## Appendix A. HTTP surface: detailed behavioral contract (normative)

This appendix is the complete, implemented contract for the HTTP surface (Surface A, §6 above): storage model and atomic claim, complete/release, fingerprinting and effective-key composition, wire format, backend requirements, security, and IETF alignment. It is the detail the HTTP test suite enforces. **Numbering note:** headings in this appendix keep the HTTP surface's own section numbers (its Glossary, Scope, and §4-§10); these are independent of the main specification's §1-§14 above, which cites them as "Appendix A, §4.x".

## Glossary

- **Effective key**: collision-resistant hash of `(idempotency_key, scope, method, path)`. Cross-tenant isolation key. See §4.6.
- **Fingerprint**: collision-resistant hash of the canonical request payload. Detects key reuse with a different body. See §4.5.
- **Claim**: atomic acquisition of execution rights for an effective key.
- **Claim token**: random 128-bit value generated at claim time, persisted in the record. Used to detect "delayed completion from an owner whose lease expired." See §4.1.
- **Lease**: time-bounded ownership of a `CLAIMED` record. Expires if the handler does not complete in time.
- **`len_be32(x)`**: the byte length of `x` encoded as a 4-byte big-endian unsigned integer. Used in length-prefixed hash construction.

---

## Scope

**In:** wire format (request/response headers, status codes), state machine, claim atomicity, response storage and replay, payload fingerprinting, observability event surface.

**Out:**

- Distributed transactions, sagas, change-data-capture, outbox pattern.
- **Multi-step operations with recovery points (Stripe-style `recovery_point` DAGs across foreign API calls).** idemkit assumes the handler is one atomic unit; multi-step orchestration belongs to saga frameworks (e.g., Temporal, DBOS).
- **Cancellation of in-progress handlers on client disconnect.** Discussed and explicitly NOT done in §4.4.
- Client-side retry policies (other than the retry-safety contract idemkit enables).
- General-purpose HTTP caching (idemkit keys on intent, not on URL).
- Streaming responses (SSE, chunked transfer without `Content-Length`). Implementations MUST bypass these with a warning (§4.9).

---

## 4. Behavioral contract

Numbering preserved across revisions so test vectors and external references stay stable.

### 4.1 Storage model and atomic claim

Each effective key maps to a **single record** in storage. The record fields:

| Field | Always present | Notes |
|---|---|---|
| `state` | yes | `CLAIMED` or `COMPLETED` |
| `fingerprint` | yes | SHA-256 hex of canonical request (§4.5) |
| `fingerprint_version` | yes | Integer; v0.1 = `1` |
| `claim_token` | yes | Cryptographically-random 128-bit value, generated at claim |
| `claimed_at` | yes | Timestamp |
| `lease_until` | yes | `claimed_at + lease_ttl` |
| `completed_at` | only on COMPLETED | Timestamp |
| `response_status` | only on COMPLETED | Integer |
| `response_headers` | only on COMPLETED | Filtered per §4.10 |
| `response_body` | only on COMPLETED | Raw bytes |

**A two-key layout (separate `:claim` and `:result` keys) is forbidden.** Such a layout creates a race window where the claim key has been deleted but the result key has not yet been written, during which a reader observes `ABSENT` and re-executes. Single record only.

#### Claim operation

Attempt to atomically insert the record. The operation MUST be atomic against the backend.

- **Redis:** `SET <eff_key> <serialized_record> NX PX <lease_ttl_ms>`. NX-success ⇒ NEW. Otherwise GET the existing record to read its state.
- **PostgreSQL** (`READ COMMITTED` isolation):
  ```sql
  INSERT INTO idemkit_records
      (effective_key, state, fingerprint, fingerprint_version,
       claim_token, claimed_at, lease_until)
  VALUES ($1, 'CLAIMED', $2, $3, $4, NOW(),
          NOW() + ($5 || ' milliseconds')::INTERVAL)
  ON CONFLICT (effective_key) DO NOTHING
  RETURNING *;
  ```
  Non-empty `RETURNING` ⇒ NEW. Empty ⇒ `SELECT * FROM idemkit_records WHERE effective_key = $1` to read existing.
- **In-memory:** per-effective-key `asyncio.Lock` + dict. Dev/test only. Implementations MUST log a warning when used outside test contexts.

#### Complete operation (conditional state transition)

Transition `CLAIMED → COMPLETED` **only if the record still belongs to this claim**. The lease may have expired while the handler was running and another process may have reclaimed the key; in that case our completion MUST be discarded so we don't overwrite the newer claim.

- **Redis:** Lua script that:
  1. `GET <eff_key>`, deserialize.
  2. If `state == "CLAIMED"` and `claim_token == <our_token>`: serialize a new record with state `COMPLETED` + response fields, `SET <eff_key> <new_record> EX <completed_ttl>`, then `PUBLISH idemkit_completions <eff_key>`.
  3. Otherwise: return without modification (caller logs `idempotency.lease_reclaimed_loss`).
- **PostgreSQL:**
  ```sql
  UPDATE idemkit_records
  SET state = 'COMPLETED', completed_at = NOW(),
      response_status = $2, response_headers = $3, response_body = $4
  WHERE effective_key = $1
    AND state = 'CLAIMED'
    AND claim_token = $5
  RETURNING effective_key;
  ```
  Empty `RETURNING` ⇒ claim was reclaimed; discard result. On success: `pg_notify('idemkit_completions', $1)` to wake any waiters.
- **In-memory:** the per-key lock + dict update with the same conditional check.

#### Release operation

When the handler raises or returns a non-cacheable status (§4.2), the claim MUST be released so the next request can retry. Same conditional pattern as Complete: only release if `state == CLAIMED AND claim_token == <our_token>`.

Implementations MAY skip explicit release and rely on lease expiry. Explicit release is an optimization for the common case where the handler fails predictably.

---

### 4.2 Cacheable status policy

- Default cacheable: **2xx only**.
- **5xx MUST NOT be cached by default.** If a transient error gets cached, every retry replays it and the client can never get through. This is a common and damaging mistake.
- 4xx: configurable; off by default. Operators MAY opt in for deterministic validation errors.
- A non-cacheable response (5xx by default; 4xx if not in `cacheable_status`) triggers Release per §4.1.

### 4.3 In-flight handling (race-free wait pattern)

When a duplicate request observes an existing record with state `CLAIMED`, the wait pattern MUST follow this exact ordering:

1. **Subscribe first.** Register for completion notifications. This step MUST complete before step 2.
2. **Re-read state.** GET / SELECT the record.
3. **Branch:**
   - state == `COMPLETED` → unsubscribe; replay the stored response.
   - state == `CLAIMED` → wait for either the completion notification (max `wait_timeout`) or timeout.
   - record absent (lease expired between original claim and re-read) → unsubscribe; retry the claim as a NEW request (§4.1).
4. **On notification arrival:** GET the record; replay if `COMPLETED`, otherwise loop to step 3.
5. **On `wait_timeout`:** unsubscribe; return 423 (default) or 409 (`compat_mode="stripe"`) + `Retry-After: <ceil(remaining_lease_seconds)>`.

**Subscribing AFTER reading state would lose a notification published in the window between the read and the subscribe, causing the waiter to hang for the full timeout.** Subscribe-first is mandatory.

#### Backend notification patterns

- **Redis:** One Pub/Sub channel name for all keys: `idemkit_completions`. The completing process publishes the effective key as the message payload. The waiting process subscribes once and filters by payload. In Redis Cluster, Pub/Sub messages broadcast cluster-wide; no hash tag concerns.
- **PostgreSQL:** **One dedicated `LISTEN` connection per app process** holds `LISTEN idemkit_completions`. The completing process emits `pg_notify('idemkit_completions', <effective_key>)`. The receiving process **demultiplexes notifications to in-process waiters** keyed by effective_key (e.g., `asyncio.Event` per effective_key). This uses a single PostgreSQL connection per process regardless of in-flight request count, per-request `LISTEN` would exhaust the connection pool at modest concurrency.
- **In-memory:** per-key `asyncio.Event`.

`wait_timeout` default: 10 seconds.

### 4.4 Crash, lease, and client disconnect

#### Lease expiry as the universal safety net

The backend MUST atomically enforce `lease_until` using the **storage backend's authoritative clock**, never the app server's clock. (Redis: `PEXPIRE` / `PX`. PostgreSQL: comparisons against `NOW()` inside SQL. In-memory: wall-clock of the single hosting process.) Cross-node clock skew is a real failure mode; relying on the storage clock for lease enforcement eliminates it.

Any record whose `lease_until` precedes the storage clock's current time is treated as `ABSENT` by §4.1 and re-claimable. If a process holding a claim dies, crashes, or stalls past `lease_until`, the next request claims successfully.

#### Explicit release

The middleware MUST call Release (§4.1) when:
- The handler raises an exception.
- The handler returns a non-cacheable status (5xx by default; §4.2).
- The ASGI client disconnects mid-handler (the framework signals `http.disconnect`).

Release is the fast path; lease expiry is the fallback.

#### Handler cancellation policy (explicit non-protection)

When the ASGI client disconnects, idemkit RELEASES the claim. **idemkit does NOT cancel the handler coroutine.** The handler may continue to execute and produce side effects (database writes, downstream API calls, queued messages).

**Consequence: handler-level side effects MAY occur once, twice, or not at all on retry. This is a known limitation, not a bug, of any HTTP-middleware-level idempotency layer.** idemkit's caching layer protects against duplicate response *delivery*, not against duplicate downstream *effects* originating from a handler that survived client disconnect.

**Mitigation for high-value paths.** Operators handling money, account state, or external API calls MUST pair idemkit with downstream idempotency:

- Pass idempotency keys to downstream APIs (Stripe SDK, payment gateways).
- Use a transactional outbox so DB writes and downstream calls share a transaction boundary.
- Make handlers themselves idempotent: detect "already processed" by querying state before performing the side effect.

The alternative, cancelling the handler on disconnect, leaves Python coroutines, database transactions, and `finally` blocks in indeterminate states; that failure mode is worse than the duplicate-side-effect case. A future spec revision MAY introduce optional cancellation behind a flag.

### 4.5 Fingerprinting

#### Algorithm v1

```
fingerprint = lowercase_hex(SHA-256(
    len_be32(method_uppercase) ‖ method_uppercase ‖
    len_be32(path_canonical)   ‖ path_canonical   ‖
    len_be32(query_canonical)  ‖ query_canonical  ‖
    len_be32(body_canonical)   ‖ body_canonical
))
```

Length-prefixing each component (rather than null-byte separation) makes the construction collision-resistant for arbitrary inputs including those containing `0x00` bytes. This matters because body bytes are user-controlled and may contain any byte sequence.

#### Canonicalization rules

- `method_uppercase`: HTTP method uppercased (`POST`, `PATCH`, `DELETE`, etc.).
- `path_canonical`: collapse repeated slashes; strip trailing slash except root `/`; normalize percent-encoding to **uppercase hex** (`%2F`, not `%2f`).
- `query_canonical`: percent-decode once, sort params lexicographically by name then value, re-encode with uppercase hex.
- `body_canonical` is computed from the **decoded** request body, after the adapter has applied `Content-Encoding` decompression. The request `Content-Type` determines canonicalization:
  - `application/json` → `json.dumps(parsed, sort_keys=True, separators=(",", ":"), ensure_ascii=False)`.
  - `application/x-www-form-urlencoded` → percent-decode, sort by name then value, re-encode.
  - `multipart/form-data` → parts sorted by name; each part body hashed inline.
  - Any other content type, or any content type whose body fails to parse → raw decoded body bytes. Implementations MUST NOT raise on parse failure; the fallback to raw bytes is normative.

#### Known limitations

JSON sorted-keys canonicalization is not type-canonical or text-canonical:

- **Numeric drift:** `{"a": 1}` and `{"a": 1.0}` produce different bytes.
- **Unicode normalization:** `{"a": "café"}` in NFC vs NFD produces different bytes for the same logical string.
- **Whitespace and escape choices** in original input do not survive parsing, but newly-introduced whitespace by re-serialization is also not preserved consistently across JSON libraries.

For applications where these matter (typically money handling), operators SHOULD normalize the request body (canonical types, NFC Unicode) before the handler returns control to idemkit. RFC 8785 (JSON Canonicalization Scheme) is the type-canonical alternative and is reserved for `fingerprint_version: 2`; v0.1 deliberately keeps a zero-dependency implementation.

#### Headers in the fingerprint

Request headers are **not** included by default. Applications where headers carry semantic meaning (e.g., `X-Currency: USD` changes the outcome) MUST use a custom extractor (§4.7) that incorporates those headers into either the key or the body before idemkit sees the request.

#### Versioning

- `fingerprint_version` (integer) MUST be persisted with the record. v0.1 = `1`.
- **Unknown `fingerprint_version` on read MUST be treated as `ABSENT`** (fresh execution). Prevents false-match replays on library upgrade.
- Mismatch MUST result in `422` / `409` and MUST NOT, under any circumstance, replay the stored response.

### 4.6 Effective key composition (cross-tenant safety)

```
effective_key = lowercase_hex(SHA-256(
    len_be32(idempotency_key)  ‖ idempotency_key  ‖
    len_be32(scope)  ‖ scope  ‖
    len_be32(method_uppercase) ‖ method_uppercase ‖
    len_be32(path_canonical)   ‖ path_canonical
))
```

Length-prefixing each component is required because `idempotency_key` is user-controlled and may contain any byte sequence including `0x00`. Null-byte separators alone would permit collisions: a malicious or accidental key of the form `<short_key>\x00<tenant>` could collide with `<tenant_prefix>\x00<rest>`.

- `scope` MUST be a non-empty string for authenticated requests. An empty string MUST be treated as missing.
- For anonymous requests, implementations SHOULD use a stable client identifier (e.g., source IP) and document the choice.
- **A missing `scope` extractor MUST be loud, and MUST be enforceable.** When no extractor is supplied the library runs in single-tenant mode (all callers share one namespace) and MUST log a prominent, repeated warning describing the cross-tenant risk. `scope_mode="single_tenant"` acknowledges single-tenant and silences the warning; `scope_mode="strict"` MUST make a missing extractor a `ConfigurationError` at initialization, for deployments that want to enforce identity in CI/production. (Revised from the original always-refuse rule: hard-failing the first run cost adoption with no safety gain over a loud warning plus an opt-in enforcement switch.) A configured extractor that yields an empty/absent identity at request time is a separate case and MUST fail closed (HTTP 500, `urn:idemkit:identity-unavailable`) rather than silently use the shared namespace.

Header-only keying without `scope` is the most-reported security issue in this category (cross-tenant cache replay).

### 4.7 Pluggable extraction

- Default key extractor: the `Idempotency-Key` request header.
- Implementations MUST support a custom key extractor, a callable taking the framework-specific request object and returning the key string (or signalling "no key" via a language-appropriate mechanism: `None`, a sentinel, an exception).
- Implementations MUST support a dynamic `scope` extractor, a callable returning the caller-identity string for this request from request context (auth user, tenant header, etc.).

### 4.8 Response storage and TTL

- The COMPLETED record stores: status code, response body (raw bytes as sent), permitted headers (§4.10), `fingerprint_version`, fingerprint, `claim_token`, `claimed_at`, `completed_at`.
- **`completed_ttl` clock starts at `completed_at`**, not at `claimed_at`, and MUST NOT be reset on read. (Lease renewal in a future spec version will explicitly affect `lease_until`, never `completed_ttl`.)
- Default `completed_ttl`: 24 h.
- Default `lease_ttl`: 30 s. SHOULD be greater than handler p99 latency. Implementations SHOULD emit a deployment-time warning if `lease_ttl < 5 s` or if observed handler p99 exceeds `lease_ttl`.

### 4.9 Response body size enforcement

- Default maximum cacheable **response** body: 1 MiB.
- Enforcement MUST count actual streamed response bytes; **do NOT trust the response `Content-Length` header**: it is absent on chunked and HTTP/2 responses, and trusting it permits DoS via unbounded responses.
- The middleware buffers the response body up to `max_body_bytes`. On hitting the limit, the middleware passes through the remaining bytes uncached and discards any partial buffer.
- When uncached: the response includes `Idempotency-Replay-Unavailable: size-exceeded`. The client knows a retry will re-execute.
- Streaming responses (SSE, chunked without `Content-Length`): bypass entirely with `Idempotency-Replay-Unavailable: streaming`.

### 4.10 Response header allow / deny

- Stored by default: `Content-Type`, `Content-Encoding`, `Content-Language`, `Content-Disposition`, `Location`, `ETag`, `Last-Modified`, `Link`, `Cache-Control`.
- NOT stored by default: `Set-Cookie`, `Authorization`, `WWW-Authenticate`, `Vary` (idemkit is not Vary-aware on replay), hop-by-hop headers (RFC 7230 §6.1).
- Both lists MUST be runtime-overridable.

### 4.11 Encoding on replay (v0.1)

v0.1 stores and replays the response body **verbatim**, including the original `Content-Encoding`. No re-encoding on replay. If the first client received a `gzip`-encoded response, the replay client also receives a `gzip`-encoded response regardless of its `Accept-Encoding`. The cross-client encoding mismatch is acceptable for v0.1; a future revision MAY add opt-in re-encoding.

### 4.12 Corrupt record recovery

- A stored record that fails to deserialize (corrupt bytes, schema drift, encryption key unavailable) MUST be treated as `ABSENT` (fresh execution), not as a 500.
- Implementation MUST emit `idempotency.corrupt_record` event so the operator can investigate without service degradation.

### 4.13 Eviction safety (in-memory backends)

In-memory backends use **per-key TTL only**, with no LRU eviction. Rationale: any LRU-style eviction that can remove `CLAIMED` records causes duplicate execution under load; safely partitioning eviction between `CLAIMED` and `COMPLETED` adds significant complexity for marginal benefit in a backend that is dev/test-only anyway.

- The InMemory backend MUST expose a `max_size` knob (default: `10_000` records).
- When `max_size` is reached, new claims MUST be rejected by raising the storage-error path (§4.1 reports an error; middleware respects `on_storage_error` per §4.16).
- The backend MUST log when rejection occurs so operators can detect under-sized caches.
- TTL-based expiry runs lazily on read (cheap) plus a periodic sweep (cadence: `lease_ttl / 4`) to bound memory.

### 4.14 PII redaction

- A `response_redactor: (StoredResponse) → StoredResponse` hook MUST be provided.
- The redactor receives a **copy** of the response and operates on the copy. The response delivered to the first client MUST NOT be modified, only the persisted copy is redacted.
- Runs immediately before persistence.

### 4.15 Observability events

Required event types: each MUST be emitted exactly once per request:

- `idempotency.new`: claim acquired, handler will execute
- `idempotency.replayed`: stored response served
- `idempotency.in_flight_wait`: duplicate is waiting for in-flight completion
- `idempotency.conflict`: wait exhausted; 423 / 409 returned
- `idempotency.payload_mismatch`: fingerprint disagreed
- `idempotency.lease_reclaimed`: expired lease re-acquired by this process
- `idempotency.lease_reclaimed_loss`: our completion was rejected because another process reclaimed (§4.1 Complete)
- `idempotency.storage_error`: backend unavailable
- `idempotency.corrupt_record`: stored record failed to deserialize

Each event MUST carry: effective-key (already a hash per §4.6), decision, latency, backend name, `fingerprint_version`.

#### 4.15.1 OpenTelemetry semantic conventions (planned, not yet implemented)

> **Status (v0.1-v0.3):** idemkit does **not** emit OpenTelemetry spans itself. It emits the structured event in §4.15, which you can bridge into a span in your own handler. The conventions below are the target shape for a future native emitter; when it lands, an implementation that has OpenTelemetry configured SHOULD emit one span per request wrapping the full lifecycle.

- **Span name:** `idempotency.handle`
- **Span kind:** `INTERNAL`
- **Required attributes:**
  - `idempotency.decision`: one of `new`, `replayed`, `conflict`, `payload_mismatch`, `in_flight_wait`, `lease_reclaimed`, `lease_reclaimed_loss`, `corrupt_record`, `storage_error`
  - `idempotency.backend`: backend name (`memory`, `redis`, `postgres`)
  - `idempotency.fingerprint_version`: integer
- **SHOULD attributes:**
  - `idempotency.effective_key`: hashed effective key (privacy-safe per §4.6; never the raw idempotency key)
  - `idempotency.wait_duration_ms`: when decision involved waiting
  - `idempotency.cache_hit`: boolean (alias for `decision == "replayed"`)

The single-span model keeps integration light. Implementations MAY add child spans for storage operations if useful (`idempotency.claim`, `idempotency.complete`).

### 4.16 Storage error handling

- `on_storage_error` (default `fail_closed`).
- `fail_closed`: respond with HTTP `503` + `Retry-After` + Problem Details `urn:idemkit:storage-error`.
- `fail_open`: pass the request through to the handler uncached; emit `idempotency.storage_error` event. Recommended only for low-stakes paths where availability dominates correctness.

---

## 5. State machine

```
ABSENT ──[atomic claim, §4.1]──▶ CLAIMED (with claim_token)
   ▲                                │
   │                                ├──[handler 2xx + conditional complete OK, §4.1/§4.2]──▶ COMPLETED
   │                                │
   │                                ├──[handler exception OR non-cacheable OR disconnect, §4.4]──▶ release ──▶ ABSENT
   │                                │
   │                                ├──[lease_until elapsed, §4.4]──▶ reclaim path (next request claims with NEW token)
   │                                │
   │                                └──[handler completed but claim_token mismatch on UPDATE]──▶ result discarded
   │                                                                                   (logged as lease_reclaimed_loss)
   │
   └──[completed_ttl elapsed]── COMPLETED ───────────────▶ ABSENT
```

---

## 6. Wire format

### 6.1 Request

- Header: `Idempotency-Key`. The IETF draft specifies an RFC 8941 Structured Field String; in practice most clients send the value unquoted. Servers MUST accept both quoted and unquoted forms.
- Header name MUST be matched case-insensitively.
- Key length: 1-255 **bytes** (byte count, not codepoints; multi-byte chars can exceed backend key limits).
- Malformed or oversized → HTTP 400 with `urn:idemkit:missing-key`.

### 6.2 Response headers

| Header | When | Semantics |
|---|---|---|
| `Idempotency-Replayed: true` | MUST on every replayed response (default mode) | Distinguishes stored replay from fresh execution. Absent ⇒ fresh. |
| `Idempotent-Replayed: true` | MUST on every replayed response (`compat_mode="stripe"`) | Stripe's deployed spelling. See §6.3.1. |
| `Idempotency-Key-Expires: <IMF-fixdate>` | SHOULD on idempotency-handled responses | Advertises eviction time. |
| `Idempotency-Replay-Unavailable: <reason>` | MUST when caching bypassed | Values: `size-exceeded`, `streaming`. |
| `Retry-After: <seconds>` | MUST on `423`, `503` | Standard back-off hint. |

### 6.3 Status codes and Problem Details

| Condition | Default mode | `compat_mode="stripe"` | `type` URI |
|---|---|---|---|
| Fresh successful execution | (per handler) | (per handler) | n/a |
| Replay of completed response | (original status) + `Idempotency-Replayed: true` | (original status) + `Idempotent-Replayed: true` | n/a |
| Same key, different payload | **422** | **409** | `urn:idemkit:payload-mismatch` |
| In-flight, wait timed out | **423 Locked** + `Retry-After` | **409** + `Retry-After` | `urn:idemkit:in-progress` |
| Missing required header | **400** | **400** | `urn:idemkit:missing-key` |
| Backend unavailable, fail-closed | **503** + `Retry-After` | **503** + `Retry-After` | `urn:idemkit:storage-error` |

#### 6.3.1 Stripe-compat mode

`compat_mode` (default `"default"`). When set to `"stripe"` (the boolean
`stripe_compat=true` is accepted as a deprecated alias):

- Mismatch and in-flight responses both return HTTP `409` instead of `422` / `423`.
- The replay-indicator response header MUST be emitted as `Idempotent-Replayed: true` (Stripe's deployed spelling, per Brandur Leach 2017), not `Idempotency-Replayed: true`. This achieves wire-level drop-in compatibility for clients written against Stripe's pattern.

The Problem Details `type` URI is set in both modes so clients can disambiguate via the URI independently of status code.

Example (default mode):

```http
HTTP/1.1 422 Unprocessable Content
Content-Type: application/problem+json

{
  "type":   "urn:idemkit:payload-mismatch",
  "title":  "Idempotency key payload mismatch",
  "status": 422,
  "detail": "The idempotency key was previously used with a different request payload. To retry safely, send the same payload or use a new idempotency key."
}
```

---

## 7. Backend requirements

### 7.1 General requirements

- MUST support the atomic claim, conditional complete, and conditional release operations per §4.1.
- MUST support **lazy initialization**: connections opened on first call, not at import. Required for serverless cold start and cheap test imports.
- SHOULD document recommended connection pool sizing. Defaults: Redis pool size = `max(min_size=4, cpu_count * 2)`; PostgreSQL pool size = `max(min_size=4, cpu_count * 4)` for the work pool, **plus one dedicated `LISTEN` connection per process** for completion notifications (§4.3).

### 7.2 PostgreSQL backend

Schema:

```sql
CREATE TABLE IF NOT EXISTS idemkit_records (
    effective_key       TEXT PRIMARY KEY,
    state               TEXT NOT NULL CHECK (state IN ('CLAIMED', 'COMPLETED')),
    fingerprint         TEXT NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    claim_token         TEXT NOT NULL,
    claimed_at          TIMESTAMPTZ NOT NULL,
    lease_until         TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    response_status     INTEGER,
    response_headers    JSONB,
    response_body       BYTEA,
    schema_version      INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idemkit_records_lease_until_idx
    ON idemkit_records (lease_until)
    WHERE state = 'CLAIMED';

CREATE INDEX IF NOT EXISTS idemkit_records_completed_at_idx
    ON idemkit_records (completed_at)
    WHERE state = 'COMPLETED';
```

#### LISTEN/NOTIFY pattern

A single dedicated connection per app process holds `LISTEN idemkit_completions`. The completing process emits `pg_notify('idemkit_completions', <effective_key>)`. The receiving process demultiplexes notifications to in-process waiters keyed by effective_key (e.g., one `asyncio.Event` per effective_key registered when wait begins, signalled by the demux on notification).

**Per-request `LISTEN` is forbidden:** opening a new LISTEN connection per duplicate request exhausts the PostgreSQL connection pool at modest concurrency. The single-connection demux is the only scalable pattern.

#### Migration CLI

The library MUST ship a `idemkit init-pg <database-url>` command:

- Creates the `idemkit_records` table and indexes per the schema above.
- Idempotent: safe to re-run; `CREATE IF NOT EXISTS` used throughout.
- Verifies `schema_version` and warns on drift; does NOT auto-migrate destructively.
- On library start, the PostgreSQL backend SHOULD verify the table exists and `schema_version` matches the expected value, raising a `ConfigurationError` with a clear message and the exact CLI invocation to run otherwise.

#### TTL cleanup runner

PostgreSQL has no native TTL. The library MUST ship both:

- `idemkit pg-vacuum <database-url>` CLI for cron-driven deletion.
- An opt-in in-process background task for single-instance deployments.

Recommended cadence: `≤ completed_ttl / 10`. The runner deletes:
- `state = 'COMPLETED' AND completed_at + interval '<completed_ttl>' < NOW()`
- `state = 'CLAIMED' AND lease_until + interval '<grace_seconds>' < NOW()` (grace handles delayed reclaim path).

### 7.3 Redis backend

- All state transitions beyond initial claim MUST use Lua scripts to ensure atomicity with the `claim_token` check.
- **Lua scripts MUST execute in O(constant) time.** Redis runs Lua single-threaded against the entire instance; a long script blocks all clients. Concretely: scripts MUST NOT iterate over data structures, MUST NOT call blocking commands, and MUST keep total execution under approximately 1 ms.
- **Pass large arguments (response body, headers) as `ARGV`**, never embed them in script source. The script source is cached by `SHA1`; including large payloads bloats the script cache.
- **Implementations MUST handle the `NOSCRIPT` reply** by falling back to `EVAL` (script send + execute) on first occurrence, then resuming `EVALSHA` on subsequent calls. This handles Redis instance restart, replica failover, and cluster reshard.
- Redis Cluster: the claim record key is a single string; no hash tag is required. Pub/Sub broadcasts cluster-wide.
- TTL is handled natively via `PX` / `EX` on each SET.

### 7.4 In-memory backend

- Per-effective-key `asyncio.Lock` + dict.
- MUST NOT be used across processes.
- MUST log a warning if used outside development/test environments.
- See §4.13 for eviction-safety requirements.

### 7.5 MongoDB backend

- One document per effective key (`_id`), the same single-record layout as PostgreSQL.
- The atomic claim is a single `findOneAndUpdate` with an aggregation-pipeline update that decides "take if new or lease-expired" using **`$$NOW` (the server clock)**, so lease enforcement uses the storage clock, not the app's (§4.4). `complete` / `release` / `renew` are conditional on `claim_token`.
- **TTL** is a TTL index on an `expire_at` field set to `lease_until + grace` (not on `lease_until` itself); the grace keeps a just-lapsed claim readable long enough to be reclaimed as `lease_reclaimed` rather than reaped and re-seen as `new`. Correctness never depends on TTL timing (expiry is enforced on read).
- **In-flight wait** polls on a bounded, backed-off schedule (§4.3's correctness floor); MongoDB change streams require a replica set, so no push channel is assumed.
- Requires MongoDB 4.2+ (aggregation-pipeline updates and `$$NOW`).

### 7.6 DynamoDB backend

- One item per effective key (the partition key). The atomic claim is an `UpdateItem` with a `ConditionExpression` (`attribute_not_exists(pk) OR lease_until < :now`) and `ReturnValues=ALL_OLD`, so a fresh claim is told from a lease reclaim by the old item's state. `complete` / `release` / `renew` are conditional on `claim_token`. Reads use `ConsistentRead=True`; numbers are stored as `Decimal`.
- **This is the one backend whose lease decisions use the CLIENT clock.** DynamoDB has no server clock available inside a condition expression, so it cannot offer the storage-clock guarantee (§4.4); with NTP-synced hosts this is fine, but a badly skewed host can misjudge a lease. Prefer Redis/PostgreSQL/Mongo where the storage-clock guarantee matters. This backend is therefore excluded from the clock-skew conformance vector and only that one.
- **TTL** is the native `ttl` attribute (epoch seconds), set to the logical expiry plus a grace pad; DynamoDB reaping is best-effort (up to ~48h lag), so correctness relies on expiry-on-read via `lease_until`, never on TTL timeliness. The grace pad (one-second `ttl` granularity plus lag) keeps a just-lapsed claim present long enough to be reclaimed as `lease_reclaimed`.
- The table is auto-created on first use (on-demand billing) unless `create_table=False`; in production, provision it out of band so the runtime role needs no `CreateTable` permission.

---

## 8. Test vectors

See [`conformance.yaml`](./conformance.yaml). The vector file describes the expected behavior of the reference implementation in language-neutral form, suitable as engineering documentation for any future port.

A formal cross-language conformance certification process is not specified by this version of the spec; building a full multi-language test runner is out of scope for v0.1. Future revisions MAY define a conformance runner once additional implementations exist.

---

## 9. Security

- Key length capped at 255 bytes (§6.1).
- Cross-tenant key collision prevented by §4.6, required `scope` in production mode, with length-prefixed hash construction preventing collisions from user-controlled inputs.
- Effective key hashing means the raw idempotency key is NEVER persisted, NEVER logged, NEVER emitted in observability events.
- Stored responses may contain sensitive data; the `response_redactor` hook (§4.14) is the primary mitigation.
- idemkit is not a defense against replay-of-old-key attacks beyond the configured TTL. Pair with request signing for high-value paths.
- **idemkit does not prevent duplicate handler-level side effects originating from a handler that survives client disconnect.** See §4.4 mitigation guidance.

### 9.1 Compliance considerations

idemkit's defaults are not regulation-specific; operators in regulated industries MUST configure additional safeguards.

- **PCI DSS.** When the cached response may contain Primary Account Numbers, magnetic-stripe data, CVV/CVC, or any cardholder data, the `response_redactor` hook (§4.14) MUST be configured to strip those fields before persistence. A PCI-scope deployment with `response_redactor=None` violates this spec.
- **GDPR and similar privacy regulations.** Stored responses are subject to data-retention rules. The default 24h `completed_ttl` may exceed permitted retention windows for some data classes. Operators MUST set `completed_ttl` consistent with their retention policy. Right-to-erasure (Art. 17) requests may require purging specific stored responses, implementations SHOULD expose a `delete(effective_key)` admin API.
- **SOC 2 and encryption at rest.** idemkit delegates encryption at rest to the storage backend (Redis TLS + AUTH + encrypted persistence; PostgreSQL TDE; cloud-managed disk encryption). Operators are responsible for enabling backend-level encryption; idemkit does not perform application-layer encryption.
- **Audit logging.** Every observability event (§4.15) carries the effective-key hash, never the raw idempotency key. This is the audit-safe identifier.

These guarantees are floors, not ceilings. A regulated deployment will typically pair idemkit with framework-level audit logging, request signing, and observability infrastructure outside idemkit's scope.

---

## 10. Relation to the IETF draft

`draft-ietf-httpapi-idempotency-key-header-07` (latest revision 2025-10-15, expired 2026-04-18) standardizes the wire format. It explicitly delegates operational behavior to implementers. Quoted verbatim from the `-07` text:

> "Uniqueness of the key MUST be defined by the resource owner and MUST be implemented by the clients of the resource."  (§2.2, *Uniqueness of Idempotency Key*)
>
> "An idempotency fingerprint MAY be used in conjunction with an idempotency key to determine the uniqueness of a request."  (§2.4, *Idempotency Fingerprint*)
>
> "The resource MAY require time based idempotency keys to be able to purge or delete a key upon its expiry."  (§2.3, *Idempotency Key Validity and Expiry*)

These map to the three areas this spec is most concerned with: cross-tenant scoping (§4.6), fingerprinting (§4.5), and TTL (§4.8). In each case the draft leaves the concrete choice to the implementer. It says nothing at all about in-flight handling, crash recovery, the shape of the stored response, how a replay is signaled, or Problem Details type URIs, and this spec defines all of those.

Where the draft is normative, idemkit follows it. Where it's silent, idemkit makes a documented choice, and several of those choices are written up as candidate input to the IETF working group.

---

