"""Cache and replay a handler's return value across redeliveries.

By default a queue handler returns nothing, and a redelivery is simply skipped
(idemkit records a "processed" marker). If your handler computes a value you want
back on a redelivery, set cache_result=True and idemkit caches and replays it
without re-running the work.

Run:  python examples/queue/cache_result.py
"""

import asyncio
from types import SimpleNamespace

from idemkit import IdempotentConsumer, InMemoryBackend

# The expensive work we do not want to repeat.
computed = {"n": 0}

# >>> idemkit plugs in here: cache and replay the handler's return value.
consumer = IdempotentConsumer(
    backend=InMemoryBackend(),
    key=lambda msg: msg.message_id,
    visibility_timeout_seconds=30,
    cache_result=True,   # cache what the handler returns
)


@consumer.handle
async def enrich(msg):
    computed["n"] += 1
    return {"id": msg.message_id, "score": 0.97}


async def main():
    message = SimpleNamespace(message_id="job-7")

    first = await consumer.dispatch(message)      # runs the handler
    again = await consumer.dispatch(message)      # redelivery: replays the value

    print("first result :", first.result)
    print("replayed     :", again.result)
    print("computed once:", computed["n"])  # 1
    assert computed["n"] == 1 and first.result == again.result


if __name__ == "__main__":
    asyncio.run(main())
