"""Close the loop on a money path: dedupe the call, and pass the key to the provider.

idemkit runs your handler once per key and replays the result to duplicates. What it
cannot do is un-charge a provider if your worker dies after the charge but before the
result is recorded. That gap lives at the boundary with a system you do not control,
and the only way to close it is to make the provider dedupe too, then reconcile the
rare cases where the two records disagree.

So this handler does two things the article calls for. It passes the idempotency key
down to the provider, so even a duplicate that slips past idemkit is deduped by the
provider on the same key. And it wires a reconciliation handler that flags the keys
idemkit could not confirm, so a sweep can ask the provider what really happened.

See docs/the-four-assumptions.md for how this maps to the design.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent
from idemkit.contrib.reconciliation import reconciliation_handler

# In production this is a queue or a table your reconciliation job reads. Here it is a
# list so the example stays runnable. On a clean run it stays empty: nothing to do.
needs_reconciliation: list = []


def charge_provider(*, order_id: str, amount: int, idempotency_key: str) -> dict:
    # A real provider (Stripe, Adyen) dedupes on the key you send. Passing it means a
    # duplicate that reaches the provider replays instead of charging a second time.
    return {"order_id": order_id, "amount": amount, "provider_key": idempotency_key}


@idempotent(
    backend=InMemoryBackend(),
    config=MethodConfig(
        key_fields=["order_id"],
        event_handlers=(reconciliation_handler(needs_reconciliation.append),),
    ),
)
async def charge(*, order_id: str, amount: int) -> dict:
    # The order_id is the stable key idemkit dedupes on; send it downstream too.
    return charge_provider(order_id=order_id, amount=amount, idempotency_key=order_id)
