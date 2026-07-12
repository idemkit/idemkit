"""How long is a result replayable, and how do you test expiry without real sleeps?

After the window a repeat re-executes instead of replaying; set it above your
longest honest retry gap. Passing a ManualClock lets a test advance time by hand,
which is how you test any time-dependent behavior (lease expiry, TTL) instantly.
"""

from idemkit import InMemoryBackend, ManualClock, MethodConfig, idempotent

clock = ManualClock()


def process_order(order_id: str) -> dict:
    return {"order_id": order_id}


@idempotent(
    # InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
    backend=InMemoryBackend(clock=clock),
    config=MethodConfig(
        key_fields=["order_id"], scope=lambda args: "tenant-1", expires_after_seconds=3600
    ),
)
async def process(*, order_id: str) -> dict:
    return process_order(order_id)
