# The four assumptions, in code

idemkit exists to make one argument concrete: an idempotency key is not a guarantee.
The guarantee is the design around it. That argument is laid out in
[Why an idempotency key isn't an idempotency guarantee](https://www.infoworld.com/article/4191741/why-an-idempotency-key-isnt-an-idempotency-guarantee.html),
which names four assumptions a correct design has to hold. This page shows where each
one lives in the code, and how it is checked.

Three of the four can live in a library, and idemkit implements them. The fourth cannot,
because it reaches past your process into a system you do not control, and this page
says so plainly.

| Assumption | What can go wrong | idemkit's answer | Proof |
|---|---|---|---|
| **Claim** | Two requests race and both run | One atomic claim, not check-then-set | `concurrent-claim-exactly-one` vector, all backends |
| **Intent** | A key is reused for a different request | Fingerprint the request, reject a mismatch | `fingerprint.py`, `payload_validation` example |
| **Memory** | A cached decline replays forever | Cache only success; a failure releases the claim | `error_replay` example, README limitations |
| **Boundary** | The provider charged, your record does not know | The signal plus the playbook, not a fake guarantee | `contrib/reconciliation.py`, `reconciliation.py` example |

## 1. Claim: only one request can win the key

Two requests arrive in the same millisecond. Both check whether the key exists, both
find it free, both run. The article calls this the claim race, and it is why
check-then-write is the wrong shape.

idemkit makes writing the key the check. Every request tries to claim the key in one
atomic operation (a Postgres unique insert, a Redis single-key script, a DynamoDB
conditional write), and the store lets exactly one claim win. The winner runs; everyone
else waits for its result and replays it.

The `concurrent-claim-exactly-one` conformance vector fires 25 claims at the same key at
once and asserts that exactly one comes back new and the rest see it as already claimed.
It runs on all five backends, because only a real store reproduces server-side
atomicity. Crash safety rides on the same claim: if the winner dies, its lease lapses and
the next attempt reclaims the key, while a fencing token rejects the dead worker's late
write. Those are the `lease-reclaim` and `fenced-after-reclaim` vectors.

## 2. Intent: the same key always means the same request

A caller reuses one key for a $200 request and then a $500 request. A cache that keys on
the token alone hands back the first response and the amount change goes unnoticed.

idemkit stores a fingerprint of the request next to the key. A second call on the same
key with a different fingerprint is rejected (a `PayloadMismatch`, surfaced as HTTP `422`
on the web path) rather than answered with the wrong response. The default fingerprints
the whole request minus known noise, so a real change fails loud. Hand-picking a few
business fields stays available, but it is not the default, because a short list is what
lets a silent collision slip through. The article argues for exactly that default.

Two notes for a money path. The canonical form today is sorted-key JSON. Full RFC
8785 canonicalization, with its exact number rules, is reserved for fingerprint version 2;
every record stores its fingerprint version, so old fingerprints keep working when it
lands. And hold amounts as integer cents or strings, so a float and its rounding never
reach the fingerprint in the first place. The code is in
[`idemkit/core/fingerprint.py`](../idemkit/core/fingerprint.py); the runnable case is
[`examples/method/payload_validation.py`](../examples/method/payload_validation.py).

## 3. Memory: whatever a key remembers is safe to replay

Cache every response, including a decline, and a customer who hits an insufficient-funds
error, adds money, and retries on the same key gets the old decline back.

idemkit caches only success. A non-2xx HTTP response, or a raised exception on the method
and queue surfaces, releases the claim and keeps the fingerprint, so the retry reclaims
the key and runs the handler again. When a specific failure really is final and should
replay, you opt it in with `cacheable_status` (HTTP) or `cache_exceptions` (method). The
runnable case is [`examples/method/error_replay.py`](../examples/method/error_replay.py),
and the rules are in the README under Limitations.

## 4. Boundary: nothing behind the key lies beyond your control

This is the one the article says you cannot fully close. A worker charges the provider,
then dies before idemkit records the result. The record and the provider now disagree,
and no library code running inside your process can settle it, because the truth lives at
the provider. The article puts it plainly: "we kept shrinking that window, but we never
managed to close it."

So idemkit does not claim to close it. It does two smaller things instead.

It gives you the signal. Whenever a side effect may have fired without a recorded result,
the engine emits a distinct decision: `complete_failed`, `lease_reclaimed_loss`,
`lease_lost`, or `ran_unprotected`.
[`idemkit.contrib.reconciliation.reconciliation_handler`](../idemkit/contrib/reconciliation.py)
filters your event stream down to exactly those, so you know which keys to check. A quiet
stream means there is nothing to reconcile.

And it documents the pattern. Pass the idempotency key downstream so the provider dedupes
on the same key, keep a pending record before the call so a sweep can ask the provider
what actually happened, and reconcile the rare disagreement.
[`examples/method/reconciliation.py`](../examples/method/reconciliation.py) is the
runnable version.

This restraint is a design choice, not a missing feature. The moment idempotency grows a
store of pending operations and a poller that chases the provider, it stops being a
decorator and becomes a workflow engine, which is a different tool with a much larger
footprint. idemkit implements the three assumptions that belong in a library, and hands
you a clean signal and a playbook for the fourth.

## The question worth asking

The article ends on a design-review habit: for every write, ask out loud what happens if
this runs twice, and then prove the answer by running it twice, in sequence and in
parallel, so the second run changes nothing. For a single operation, this page is
idemkit's answer. For the part that reaches past your process, the answer is yours to
reconcile, and now you have the signal to do it.
