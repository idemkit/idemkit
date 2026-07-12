"""Any broker (RabbitMQ/pika, NATS, a Celery-as-consumer loop): a real, runnable poll loop.

SQS and Kafka have ready-made helpers (see sqs.py, kafka.py). For every other broker
you write the loop yourself, and it is always the same three steps: receive a message,
``dispatch`` it through the consumer, then ack it or leave it for redelivery based on
the returned action. This file wires that against a tiny in-memory FakeBroker so it
runs with no infrastructure; swap FakeBroker for your real client's receive/ack/nack.

Run:  python examples/queue/generic_broker.py
"""

import asyncio

from idemkit import ConsumerAction, IdempotentConsumer, InMemoryBackend, QueueConfig


def charge_customer(message_id: str) -> None:
    # Your real side effect; should happen once per message id, not once per delivery.
    print(f"  charged {message_id}")


class FakeBroker:
    """Stands in for RabbitMQ/NATS/etc.: at-least-once, so it can redeliver."""

    def __init__(self, messages: list) -> None:
        self._queue = list(messages)

    def receive(self):
        return self._queue.pop(0) if self._queue else None

    def ack(self, msg) -> None:
        pass  # the broker drops an acked message

    def nack(self, msg) -> None:
        self._queue.append(msg)  # redeliver later


backend = InMemoryBackend()  # prod: RedisBackend/PostgresBackend, see ../shared/backends.py
consumer = IdempotentConsumer(
    backend=backend,
    config=QueueConfig(
        dedup_id=lambda msg: msg["id"],  # how to read YOUR broker's message id
        visibility_timeout_seconds=30,
    ),
)


@consumer.handle
async def process(msg) -> None:
    charge_customer(msg["id"])


async def run_until_drained(broker: FakeBroker) -> None:
    """The poll loop you drop into your service (here it exits when the queue empties;
    a real one is ``while True``)."""
    while (msg := broker.receive()) is not None:
        result = await consumer.dispatch(msg)
        if result.action is ConsumerAction.ACK:
            broker.ack(msg)  # done (ran now, or replayed a previous run)
        else:  # ConsumerAction.RETRY
            broker.nack(msg)  # transient failure: let the broker redeliver


if __name__ == "__main__":
    # Same id twice = an at-least-once redelivery. The charge prints once.
    broker = FakeBroker([{"id": "order-1"}, {"id": "order-1"}])
    asyncio.run(run_until_drained(broker))
