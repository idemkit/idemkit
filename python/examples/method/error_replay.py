"""Replay a deterministic failure instead of re-running it.

By default a raised exception releases the claim, so a retry re-runs the handler
(right for a transient blip). List the deterministic ones in cache_exceptions and
idemkit caches the exception and re-raises it on a duplicate, with its original
type, so the side effect is never attempted twice.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent


class InsufficientFunds(Exception):
    """A deterministic business-rule failure: retrying won't change the outcome."""


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["order_id"],
        scope=lambda args: "tenant-1",
        cache_exceptions=(InsufficientFunds,),  # cache + replay these; re-run anything else
    ),
)
async def charge(*, order_id: str, amount: int) -> dict:
    if amount > 100:
        raise InsufficientFunds(f"cannot charge {amount}")
    return {"order_id": order_id, "charged": amount}


# A duplicate of a failed charge re-raises InsufficientFunds WITHOUT charging again;
# a duplicate of a successful charge replays its result. A transient error (any type
# not in cache_exceptions) would release the claim so a retry re-runs.
