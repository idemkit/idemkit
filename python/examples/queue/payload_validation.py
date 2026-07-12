"""Catch a producer that reused a message id with a different body.

By default the broker's dedup id is authoritative: two deliveries of the same id
replay. Set validation_fingerprint when a buggy producer might reuse a message id
for a DIFFERENT payload; idemkit then refuses to replay the mismatched body,
routes the message to on_exhausted (a DLQ), and acks it (redelivery won't fix a
body mismatch).
"""

from idemkit import IdempotentConsumer, InMemoryBackend, QueueConfig


def send_to_dlq(msg, exc: BaseException | None) -> None: ...


consumer = IdempotentConsumer(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=QueueConfig(
        dedup_id=lambda msg: msg.message_id,
        visibility_timeout_seconds=30,
        validation_fingerprint=lambda msg: msg.body,  # the bytes that must match on a redelivery
        on_exhausted=send_to_dlq,  # where a mismatched message goes
    ),
)


@consumer.handle
async def process(msg) -> None:
    charge_customer(msg.body)  # runs once per message_id, only for a consistent body


def charge_customer(body: bytes) -> None: ...  # your real side effect
