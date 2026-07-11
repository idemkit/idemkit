"""Give up on a poison message after max_attempts and send it to a DLQ.

When a handler fails, idemkit releases the claim so the broker can redeliver. But
not forever. After max_attempts the consumer stops retrying and calls
on_exhausted, where you route the message to a dead-letter queue or alert.

Here the handler always fails. Watch the attempt count climb, then exhaust.

Run:  python examples/queue/dead_letter.py
"""

import asyncio
from types import SimpleNamespace

from idemkit import ConsumerAction, IdempotentConsumer, InMemoryBackend

# Stand-in for a dead-letter queue.
dead_lettered = []


def send_to_dlq(msg, exc):
    dead_lettered.append(msg.message_id)


# >>> idemkit plugs in here: run once per message; DLQ after max_attempts failures.
consumer = IdempotentConsumer(
    backend=InMemoryBackend(),
    key=lambda msg: msg.message_id,
    visibility_timeout_seconds=30,
    max_attempts=3,             # after 3 failed attempts, stop retrying
    on_exhausted=send_to_dlq,   # and hand the message off here
)


@consumer.handle
async def always_fails(msg) -> None:
    raise RuntimeError("handler blew up")


async def main():
    # SimpleNamespace is a throwaway stand-in for your broker's message object.
    message = SimpleNamespace(message_id="poison-1")

    # Expected: deliveries 1 and 2 fail and get redelivered; delivery 3 hits
    # max_attempts=3, so it is acked and routed to the DLQ (not retried again).
    for delivery in range(1, 6):
        result = await consumer.dispatch(message)
        if result.exhausted:
            state = "ack (DLQ'd)"
        elif result.action is ConsumerAction.ACK:
            state = "ack"
        else:
            state = "redeliver"
        print(f"delivery {delivery}: attempt {result.attempt} -> {state}")

        # A real broker deletes an acked message, so stop redelivering once acked.
        if result.action is ConsumerAction.ACK:
            break

    print("dead-lettered:", dead_lettered)  # ['poison-1'], once
    assert dead_lettered == ["poison-1"]


if __name__ == "__main__":
    asyncio.run(main())
