"""Consume Kafka and process each record once, even across rebalances and redeliveries.

contrib.kafka dedupes on topic:partition:offset, derives the lease from
max.poll.interval.ms, and scopes by group_id. Reads both confluent-kafka and
kafka-python records.
"""

from idemkit import InMemoryBackend
from idemkit.contrib.kafka import kafka_consumer


def process_record(
    value: bytes,
) -> None: ...  # your real side effect; runs once per topic:partition:offset


consumer = kafka_consumer(
    backend=InMemoryBackend(),  # prod: RedisBackend/PostgresBackend, see ../shared/backends.py
    group_id="billing",  # each consumer group processes every record; group_id is the scope
    max_poll_interval_seconds=300,  # Kafka's redelivery deadline
)


@consumer.handle
def process(record) -> None:
    process_record(record.value)
