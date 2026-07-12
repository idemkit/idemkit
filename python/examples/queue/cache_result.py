"""Your queue handler RETURNS a value you need back on a redelivery, not just a skip.

By default a queue handler returns nothing and a redelivery is just skipped; set
cache_result=True to cache the return value and replay it without re-running.
"""

from idemkit import IdempotentConsumer, InMemoryBackend, QueueConfig


def enrich(message_id: str) -> dict:
    return {"id": message_id, "score": 0.97}


consumer = IdempotentConsumer(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=QueueConfig(
        dedup_id=lambda msg: msg.message_id,
        visibility_timeout_seconds=30,
        cache_result=True,
    ),
)


@consumer.handle
async def process(msg) -> dict:
    return enrich(msg.message_id)
