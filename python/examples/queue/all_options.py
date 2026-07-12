"""Reference: every queue option on one QueueConfig object.

The consumer takes a backend (WHERE dedup state lives) and one QueueConfig (HOW a
message is deduped). ``key`` and ``visibility_timeout_seconds`` are the required
wiring; everything else has a default.
"""

from idemkit import IdempotentConsumer, InMemoryBackend, QueueConfig

events: list = []

config = QueueConfig(
    # required wiring
    dedup_id=lambda msg: msg.message_id,  # how to read YOUR broker's dedup id
    visibility_timeout_seconds=30,  # the broker's visibility window
    # queue-specific
    scope=lambda msg: msg.queue,  # isolation namespace (per queue / consumer group)
    max_attempts=5,  # give up after this many failed attempts
    on_exhausted=lambda msg, exc: None,  # called when max_attempts is hit (route to a DLQ)
    receive_count=lambda msg: getattr(msg, "receive_count", None),  # broker's attempt count
    attempt_store=None,  # durable attempt counter when the broker has none
    cache_result=False,  # cache and replay the handler's return value
    result_codec=None,  # how that value is serialized (default: JSON)
    validation_fingerprint=None,  # bytes that must match on a dedup-id hit (else PayloadMismatch)
    # shared
    lease_ttl_seconds=None,  # None = derived from visibility_timeout_seconds
    wait_timeout_seconds=5,
    expires_after_seconds=86_400,
    on_storage_error="fail_closed",
    use_local_cache=False,
    local_cache_max_items=1024,
    event_handlers=(events.append,),
)


def charge_customer(message_id: str) -> None: ...  # your real side effect


# InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
consumer = IdempotentConsumer(backend=InMemoryBackend(), config=config)


@consumer.handle
async def process(msg) -> None:
    charge_customer(msg.message_id)
