"""Reject a reused key whose payload changed (validation_fingerprint).

The key decides WHICH operation this is; validation_fingerprint decides that two
calls with that key must carry the SAME critical fields. If a caller reuses the key
with a different amount, that is a bug (a new operation wearing an old key), so
idemkit raises PayloadMismatch instead of silently replaying the first result.
"""

from idemkit import InMemoryBackend, MethodConfig, PayloadMismatch, idempotent


def do_refund(order_id: str, amount: int) -> dict:
    return {"order_id": order_id, "refunded": amount}


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["order_id"],  # the key: same order_id == same refund
        validation_fingerprint=lambda args: str(args["amount"]).encode(),  # must match on a key hit
    ),
)
async def refund(*, order_id: str, amount: int) -> dict:
    return do_refund(order_id, amount)


async def main() -> None:
    await refund(order_id="A-1", amount=50)
    await refund(order_id="A-1", amount=50)  # same key, same amount -> replayed
    try:
        await refund(order_id="A-1", amount=999)  # same key, DIFFERENT amount
    except PayloadMismatch:
        pass  # refused: a reused key with a changed payload is a caller bug
