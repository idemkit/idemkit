"""Kafka with idempotency, wired for you.

idemkit.contrib.kafka presets the Kafka-specific choices:
  - the dedup id is topic:partition:offset (the message key is not unique enough)
  - the redelivery deadline is max.poll.interval.ms, so the lease derives from it
  - group_id is the scope, so two consumer groups dedupe independently

It reads confluent-kafka records (methods) and kafka-python records (attributes).
This file uses fake records so it runs with no Kafka broker.

Run:  python examples/queue/kafka.py
"""

from types import SimpleNamespace

from idemkit import ConsumerAction, InMemoryBackend
from idemkit.contrib.kafka import kafka_consumer

processed = {"n": 0}

# >>> idemkit plugs in here: kafka_consumer dedupes on topic:partition:offset.
consumer = kafka_consumer(
    backend=InMemoryBackend(),
    group_id="billing",             # each consumer group processes every record
    max_poll_interval_seconds=300,  # Kafka's redelivery deadline
)


@consumer.handle
def process(record) -> None:
    processed["n"] += 1


if __name__ == "__main__":
    # kafka-python style: topic/partition/offset are attributes.
    # (confluent-kafka exposes them as methods; idemkit handles both.)
    record = SimpleNamespace(topic="charges", partition=0, offset=101, value=b"charge:42")

    for delivery in (1, 2):  # a rebalance redelivers the same offset
        result = consumer.dispatch_sync(record)
        action = "ack" if result.action is ConsumerAction.ACK else "redeliver"
        print(f"delivery {delivery} (offset 101): {action}")

    print("processed once:", processed["n"])  # 1
    assert processed["n"] == 1
