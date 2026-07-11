"""Idempotency for synchronous code, plus a payload-mismatch guard.

idempotent_sync decorates a plain `def` (a Celery task, a threaded worker, a
script). Same options as the async decorator, no await.

It also shows validation_fingerprint: a field that must MATCH on a key hit but is
not part of the key. So if a caller reuses an order_id with a different amount,
idemkit raises PayloadMismatch instead of quietly replaying the wrong result.

Run:  python examples/method/sync.py
"""

from idemkit import InMemoryBackend, PayloadMismatch, idempotent_sync

charged = {"n": 0}


# >>> idemkit plugs in here: idempotent_sync wraps a plain def, no await.
@idempotent_sync(
    backend=InMemoryBackend(),
    key_fields=["order_id"],                                   # the key
    validation_fingerprint=lambda args: str(args["amount"]).encode(),  # must match
    scope=lambda args: "tenant-1",
)
def charge(*, order_id, amount):
    charged["n"] += 1
    return {"order_id": order_id, "amount": amount}


if __name__ == "__main__":
    print("first :", charge(order_id="A1", amount=50))
    print("retry :", charge(order_id="A1", amount=50), "(replayed)")
    print("charges actually made:", charged["n"])  # 1

    # Same key, different amount. The guard catches it instead of replaying.
    try:
        charge(order_id="A1", amount=999)
    except PayloadMismatch:
        print("caught PayloadMismatch: same order_id, different amount")
