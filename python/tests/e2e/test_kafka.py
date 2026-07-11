"""End-to-end on real Kafka (redpanda): the same offset is processed once.

Mirrors examples/queue/kafka.py against a real Kafka API. One record is produced,
consumed, then the consumer seeks back to its offset and consumes it again (a
redelivery / rebalance). idemkit dedupes on topic:partition:offset, so the handler
runs once.
"""

from __future__ import annotations

import uuid

import pytest

from idemkit import InMemoryBackend
from idemkit.contrib.kafka import kafka_consumer

pytestmark = pytest.mark.e2e


def test_kafka_redelivery_processed_once(kafka_bootstrap):
    from kafka import KafkaConsumer, KafkaProducer, TopicPartition
    from kafka.admin import KafkaAdminClient, NewTopic

    topic = f"idemkit-e2e-{uuid.uuid4().hex[:8]}"
    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
    admin.close()

    producer = KafkaProducer(bootstrap_servers=kafka_bootstrap)
    producer.send(topic, b"charge:42").get(timeout=10)
    producer.flush()
    producer.close()

    consumer = KafkaConsumer(
        bootstrap_servers=kafka_bootstrap,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=10_000,
        group_id=f"idemkit-e2e-{topic}",
    )
    tp = TopicPartition(topic, 0)
    consumer.assign([tp])
    consumer.seek_to_beginning(tp)

    processed = {"n": 0}
    idem = kafka_consumer(backend=InMemoryBackend(), group_id="billing")

    @idem.handle
    def process(record) -> None:
        processed["n"] += 1

    try:
        # First delivery.
        record = next(consumer)
        r1 = idem.dispatch_sync(record)
        assert r1.action.value == "ack"
        assert processed["n"] == 1

        # Redeliver: seek back to the same offset and consume it again.
        consumer.seek(tp, record.offset)
        record2 = next(consumer)
        assert (record2.topic, record2.partition, record2.offset) == (
            record.topic,
            record.partition,
            record.offset,
        )
        idem.dispatch_sync(record2)          # idemkit replays
        assert processed["n"] == 1           # deduped on topic:partition:offset
    finally:
        consumer.close()
