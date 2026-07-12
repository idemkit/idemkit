"""A hot key is hit many times in one process; skip the backend round-trip (a niche optimization).

use_local_cache keeps a small in-process LRU of completed results, so a repeat
call in the same worker replays from memory without touching Redis or Postgres.
Off by default; almost nobody needs it.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent


def charge_card(order_id: str) -> dict:
    return {"order_id": order_id}


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["order_id"],
        scope=lambda args: "tenant-1",
        use_local_cache=True,
        local_cache_max_items=1024,
    ),
)
async def charge(*, order_id: str) -> dict:
    return charge_card(order_id)
