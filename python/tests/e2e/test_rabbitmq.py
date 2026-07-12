"""End-to-end on real RabbitMQ: a requeued message is processed once.

RabbitMQ has no broker-native dedup id, so we publish with a message_id and key
idemkit on it. This shows idemkit working with a broker that has no contrib helper:
the generic IdempotentConsumer is broker-agnostic. The message is nacked with
requeue to force redelivery, and idemkit replays instead of re-running.
"""

from __future__ import annotations

import time
import uuid

import pytest

from idemkit import ConsumerAction, IdempotentConsumer, InMemoryBackend, QueueConfig

pytestmark = pytest.mark.e2e

MESSAGE_ID = "order-1"


def _get_with_retry(channel, queue):
    for _ in range(30):
        method, props, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is not None:
            return method, props, body
        time.sleep(0.1)
    raise AssertionError("no redelivery from RabbitMQ")


def test_rabbitmq_redelivery_processed_once(rabbitmq_url):
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbitmq_url))
    channel = conn.channel()
    queue = f"idemkit-e2e-{uuid.uuid4().hex[:8]}"
    channel.queue_declare(queue=queue, auto_delete=True)
    channel.basic_publish(
        exchange="",
        routing_key=queue,
        body=b"charge:42",
        properties=pika.BasicProperties(message_id=MESSAGE_ID),
    )

    processed = {"n": 0}
    consumer = IdempotentConsumer(
        backend=InMemoryBackend(),
        config=QueueConfig(
            dedup_id=lambda m: m["message_id"],  # key on the id we set when publishing
            visibility_timeout_seconds=30,
        ),
    )

    @consumer.handle
    def process(m) -> None:
        processed["n"] += 1

    try:
        # First delivery: process, then nack+requeue to force a redelivery.
        method, props, body = _get_with_retry(channel, queue)
        r1 = consumer.dispatch_sync({"message_id": props.message_id, "body": body})
        assert r1.action is ConsumerAction.ACK
        assert processed["n"] == 1
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

        # Redelivery: same message_id -> deduped.
        method2, props2, body2 = _get_with_retry(channel, queue)
        assert props2.message_id == MESSAGE_ID
        consumer.dispatch_sync({"message_id": props2.message_id, "body": body2})
        assert processed["n"] == 1
        channel.basic_ack(delivery_tag=method2.delivery_tag)
    finally:
        conn.close()
