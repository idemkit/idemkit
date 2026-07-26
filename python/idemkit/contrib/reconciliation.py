"""Turn idemkit's uncertain-completion events into a reconciliation signal.

Most of the time a key completes cleanly and there is nothing to reconcile. A few
decisions mean something else happened: the side effect ran, but idemkit could not
record its result, or ran without protection during a storage outage. In those
cases the record and reality may disagree, and only the downstream system (your
payment provider, your mailer) knows the truth.

idemkit cannot ask the provider for you. It does not know your business ids and it
holds only a hash of the key. What it can do is tell you *which* keys landed in the
uncertain zone, so your own reconciliation can go and check them. That is the
fourth assumption from the design guide (docs/the-four-assumptions.md): the boundary
between your record and a system you do not control.

Wire it like any other event handler::

    from idemkit import MethodConfig, idempotent
    from idemkit.contrib.reconciliation import reconciliation_handler


    def needs_check(event):
        # Correlate back to a business operation via your own pending record,
        # or trust the key you passed downstream, then ask the provider.
        reconcile_queue.put(event.effective_key)


    @idempotent(
        backend=backend,
        config=MethodConfig(
            key_fields=["order_id"],
            event_handlers=(reconciliation_handler(needs_check),),
        ),
    )
    async def charge(*, order_id): ...

The handler fires only for the decisions below. Everything else (a clean run, a
replay, a rejected payload) is left alone, so a quiet queue means nothing to do.
"""

from __future__ import annotations

from idemkit.core.events import EventHandler, IdempotencyEvent
from idemkit.core.state import Decision

#: The decisions where a side effect may have fired without a recorded result, so
#: the record and the downstream system can disagree. Each is emitted at most once
#: per operation; see spec §4.15 and docs/the-four-assumptions.md.
NEEDS_RECONCILIATION: frozenset[Decision] = frozenset(
    {
        # The side effect ran, but writing its result failed. A retry re-runs it.
        Decision.COMPLETE_FAILED,
        # Our lease was reclaimed mid-run; our completion is fenced, but the side
        # effect may already have fired before we lost the key.
        Decision.LEASE_RECLAIMED_LOSS,
        # The lease lapsed and the handler was cancelled part-way.
        Decision.LEASE_LOST,
        # fail_open ran the operation without dedup during a storage outage.
        Decision.RAN_UNPROTECTED,
    }
)

# Decision.CORRUPT_RECORD is left out on purpose. It fires when a stored record cannot be
# deserialized, which is a genuine duplicate-risk case, but a codec or schema change can
# emit it in bulk for records that are only unreadable, not wrong. Reconcile those on
# their own track so a migration does not flood this signal.


def reconciliation_handler(callback: EventHandler) -> EventHandler:
    """Build an event handler that calls ``callback`` only for uncertain completions.

    ``callback`` receives the :class:`~idemkit.IdempotencyEvent` for each decision in
    :data:`NEEDS_RECONCILIATION` and nothing else. Use it to enqueue the key for a
    reconciliation sweep, page an operator, or bump a metric. The event carries the
    hashed ``effective_key`` (safe to log), the ``decision``, and the ``surface``; it
    does not carry your business id, so correlate through your own pending record or
    the key you passed downstream.

    Like every event handler, a raised exception here is caught and suppressed by the
    emitter, so a slow or broken sink never touches the request path.
    """

    def handle(event: IdempotencyEvent) -> None:
        if event.decision in NEEDS_RECONCILIATION:
            callback(event)

    return handle
