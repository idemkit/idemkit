"""Give up on a poison message after max_attempts and hand it to a DLQ.

On a handler failure idemkit releases the claim so the broker can redeliver, but
only up to max_attempts; then it calls on_exhausted (route to a DLQ, alert).
"""

from idemkit import IdempotentConsumer, InMemoryBackend, QueueConfig


def send_to_dlq(msg, exc: BaseException | None) -> None: ...


consumer = IdempotentConsumer(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=QueueConfig(
        dedup_id=lambda msg: msg.message_id,
        visibility_timeout_seconds=30,
        max_attempts=3,
        on_exhausted=send_to_dlq,
    ),
)


@consumer.handle
async def process(msg) -> None:
    raise RuntimeError("handler blew up")
