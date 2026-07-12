"""Process an at-least-once message once, even across redeliveries.

IdempotentConsumer runs the handler once per dedup id and tells you whether to ack
or leave the message for redelivery.

Runs as-is, no infrastructure: InMemoryBackend keeps dedup state in this process. For
production swap in Redis or Postgres (state shared across workers), see ../shared/backends.py:

    from idemkit import RedisBackend
    backend = RedisBackend.from_url("redis://localhost:6379")
"""

from idemkit import IdempotentConsumer, InMemoryBackend, QueueConfig


def charge_customer(
    message_id: str,
) -> None: ...  # your real side effect; should happen once per message


# The backend is WHERE dedup state lives.
backend = InMemoryBackend()
consumer = IdempotentConsumer(
    backend=backend,
    config=QueueConfig(
        dedup_id=lambda msg: msg.message_id,  # how to read YOUR broker's dedup id
        visibility_timeout_seconds=30,  # the lease derives from this, kept shorter
    ),
)


@consumer.handle
async def process(msg) -> None:
    charge_customer(msg.message_id)


# In your poll loop:
#   result = await consumer.dispatch(msg)
#   broker.ack(msg) if result.action is ConsumerAction.ACK else broker.nack(msg)
# A full, runnable loop for any broker (RabbitMQ/NATS/...) is in generic_broker.py;
# SQS and Kafka have ready-made helpers in sqs.py / kafka.py.
