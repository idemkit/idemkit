"""Test time-dependent behavior without real sleeps.

Lease expiry and completed-TTL depend on the clock. Instead of sleeping in tests,
pass a ManualClock to InMemoryBackend and advance it by hand. That makes crash
recovery and TTL expiry deterministic and instant.

Here a completed result is replayed while it is fresh, then expires, and the next
call re-runs.

Run:  python examples/method/manual_clock.py
"""

import asyncio

from idemkit import InMemoryBackend, ManualClock, idempotent

clock = ManualClock()
backend = InMemoryBackend(clock=clock)
runs = {"n": 0}


# >>> idemkit plugs in here (backend uses a ManualClock so TTL is deterministic).
@idempotent(
    backend=backend,
    key_fields=["order_id"],
    scope=lambda args: "tenant-1",
    expires_after_seconds=3600,   # a result is kept for one hour
)
async def process(*, order_id):
    runs["n"] += 1
    return {"order_id": order_id}


async def main():
    await process(order_id="A1")
    await process(order_id="A1")  # still within the hour, so this replays
    print("runs after two quick calls:", runs["n"])  # 1

    clock.advance(3601)  # jump past expires_after_seconds, no real sleep
    await process(order_id="A1")  # the record expired, so this runs again
    print("runs after TTL expiry:", runs["n"])  # 2
    assert runs["n"] == 2


if __name__ == "__main__":
    asyncio.run(main())
