"""Process an at-least-once message once, even across redeliveries.

Brokers like SQS, Kafka, and RabbitMQ redeliver the same message by design. Your
consumer must not act on it twice. IdempotentConsumer runs the handler once per
dedup id and tells you whether to ack the message or leave it for redelivery.

Here the same message is delivered three times (a normal at-least-once broker),
and the handler runs once.

Run:  python examples/queue/quickstart.py
"""

import asyncio
from types import SimpleNamespace

from idemkit import ConsumerAction, IdempotentConsumer, InMemoryBackend

# The side effect: charge, email, ship. Should happen once per message.
handled = {"n": 0}

# >>> idemkit plugs in here: wrap your handler so it runs once per dedup id.
consumer = IdempotentConsumer(
    backend=InMemoryBackend(),
    key=lambda msg: msg.message_id,   # how to read YOUR broker's dedup id
    visibility_timeout_seconds=30,    # the lease derives from this, kept shorter
)


@consumer.handle
async def process(msg) -> None:
    handled["n"] += 1


async def main():
    # SimpleNamespace is a throwaway stand-in for your broker's message object.
    message = SimpleNamespace(message_id="m-1", body=b"charge:42")

    for delivery in range(1, 4):  # three at-least-once deliveries of one message
        result = await consumer.dispatch(message)
        # In your poll loop you act on result.action:
        #   ACK   -> tell the broker to delete the message
        #   RETRY -> leave it for redelivery
        action = "ack" if result.action is ConsumerAction.ACK else "redeliver"
        print(f"delivery {delivery}: {action}")

    print("handler actually ran:", handled["n"])  # 1
    assert handled["n"] == 1


if __name__ == "__main__":
    asyncio.run(main())
