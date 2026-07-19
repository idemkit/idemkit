"""Tests for idemkit.contrib broker glue (SQS / Kafka / MCP).

Sync tests — the contrib helpers drive the queue/method surfaces through the sync
bridge, which must not run inside a running event loop.
"""

from __future__ import annotations

from types import SimpleNamespace

from idemkit import InMemoryBackend, MethodConfig
from idemkit.contrib.kafka import kafka_consumer, kafka_dedup_id
from idemkit.contrib.mcp import mcp_idempotent, read_idempotent_hint, wrap_if_idempotent
from idemkit.contrib.sqs import run_forever, sqs_consumer, sqs_receive_count


def _sqs_message(message_id="m1", receive_count=1, body="{}"):
    return {
        "MessageId": message_id,
        "ReceiptHandle": f"rh-{message_id}-{receive_count}",
        "Body": body,
        "Attributes": {"ApproximateReceiveCount": str(receive_count)},
    }


def test_sqs_receive_count_reads_approximate_receive_count():
    assert sqs_receive_count(_sqs_message(receive_count=3)) == 3
    assert sqs_receive_count({"MessageId": "x"}) is None


def test_sqs_consumer_dedupes_by_message_id():
    counter = {"n": 0}
    consumer = sqs_consumer(backend=InMemoryBackend(), visibility_timeout_seconds=30)

    @consumer.handle
    def process(msg) -> None:
        counter["n"] += 1

    r1 = consumer.dispatch_sync(_sqs_message("dup", receive_count=1))
    r2 = consumer.dispatch_sync(_sqs_message("dup", receive_count=2))
    assert counter["n"] == 1
    assert r1.action.value == "ack" and r2.action.value == "ack"


class _FakeSqs:
    def __init__(self, messages):
        self._batches = [messages]
        self.deleted = []

    def receive_message(self, **kwargs):
        batch = self._batches.pop(0) if self._batches else []
        return {"Messages": batch}

    def delete_message(self, QueueUrl, ReceiptHandle):
        self.deleted.append(ReceiptHandle)


def test_sqs_run_forever_deletes_acked_messages():
    counter = {"n": 0}
    consumer = sqs_consumer(backend=InMemoryBackend(), visibility_timeout_seconds=30)

    @consumer.handle
    def process(msg) -> None:
        counter["n"] += 1

    msg = _sqs_message("only", receive_count=1)
    fake = _FakeSqs([msg])
    iterations = {"n": 0}

    def stop():
        iterations["n"] += 1
        return iterations["n"] > 2

    run_forever(
        consumer,
        sqs_client=fake,
        queue_url="q",
        visibility_timeout=30,
        wait_time_seconds=0,
        stop=stop,
    )
    assert counter["n"] == 1
    assert fake.deleted == [msg["ReceiptHandle"]]


def test_kafka_dedup_id_confluent_style_methods():
    record = SimpleNamespace(topic=lambda: "charges", partition=lambda: 2, offset=lambda: 99)
    assert kafka_dedup_id(record) == "charges:2:99"


def test_kafka_dedup_id_kafka_python_style_attrs():
    record = SimpleNamespace(topic="charges", partition=2, offset=99)
    assert kafka_dedup_id(record) == "charges:2:99"


def test_kafka_consumer_dedupes_by_topic_partition_offset():
    counter = {"n": 0}
    consumer = kafka_consumer(backend=InMemoryBackend(), group_id="billing")

    @consumer.handle
    def process(record) -> None:
        counter["n"] += 1

    record = SimpleNamespace(topic="charges", partition=0, offset=7, value=b"x")
    consumer.dispatch_sync(record)
    consumer.dispatch_sync(record)
    assert counter["n"] == 1


def test_rabbitmq_consumer_dedupes_by_message_id():
    from idemkit.contrib.rabbitmq import RabbitMessage, rabbitmq_consumer

    counter = {"n": 0}
    consumer = rabbitmq_consumer(backend=InMemoryBackend(), lease_seconds=300)

    @consumer.handle
    def process(msg) -> None:
        counter["n"] += 1

    msg = RabbitMessage(message_id="dup", body=b"x", redelivered=False)
    consumer.dispatch_sync(msg)
    consumer.dispatch_sync(RabbitMessage("dup", b"x", redelivered=True))  # redelivery
    assert counter["n"] == 1


def test_rabbitmq_run_forever_acks_and_requeues():
    from idemkit.contrib.rabbitmq import rabbitmq_consumer, run_forever

    consumer = rabbitmq_consumer(backend=InMemoryBackend(), lease_seconds=300)

    @consumer.handle
    def process(msg) -> None:
        pass  # returns None -> ACK

    acked: list[int] = []

    class FakeChannel:
        def __init__(self) -> None:
            self._deliveries = [
                (
                    SimpleNamespace(delivery_tag=1, redelivered=False),
                    SimpleNamespace(message_id="m-1"),
                    b"x",
                ),
            ]

        def basic_qos(self, prefetch_count: int) -> None:
            pass

        def basic_get(self, queue: str, auto_ack: bool):
            return self._deliveries.pop(0) if self._deliveries else (None, None, None)

        def basic_ack(self, delivery_tag: int) -> None:
            acked.append(delivery_tag)

        def basic_nack(self, delivery_tag: int, requeue: bool = True) -> None:
            pass

    channel = FakeChannel()
    calls = {"n": 0}

    def stop() -> bool:
        calls["n"] += 1
        return calls["n"] > 2  # one delivery, then one empty poll, then stop

    run_forever(consumer, channel=channel, queue="charges", stop=stop)
    assert acked == [1]  # the one message was acked


def test_pubsub_callback_dedupes_and_acks():
    from idemkit.contrib.pubsub import pubsub_callback, pubsub_consumer

    counter = {"n": 0}
    consumer = pubsub_consumer(backend=InMemoryBackend(), ack_deadline_seconds=60)

    @consumer.handle
    def process(message) -> None:
        counter["n"] += 1

    outcomes: list[str] = []

    class FakeMessage:
        message_id = "dup"
        data = b"x"

        def ack(self) -> None:
            outcomes.append("ack")

        def nack(self) -> None:
            outcomes.append("nack")

    callback = pubsub_callback(consumer)
    callback(FakeMessage())
    callback(FakeMessage())  # redelivery
    assert counter["n"] == 1
    assert outcomes == ["ack", "ack"]  # both acked (first ran, second replayed)


def test_mcp_idempotent_enforces_dedup_and_sets_hint():
    counter = {"n": 0}
    backend = InMemoryBackend()

    @mcp_idempotent(backend=backend, config=MethodConfig(key_fields=["order_id"]))
    def refund(*, order_id: str) -> dict:
        counter["n"] += 1
        return {"order_id": order_id}

    assert refund.idempotent_hint is True
    r1 = refund(order_id="A1")
    r2 = refund(order_id="A1")
    assert r1 == r2 == {"order_id": "A1"}
    assert counter["n"] == 1


def test_read_idempotent_hint_variants():
    assert read_idempotent_hint({"idempotentHint": True}) is True
    assert read_idempotent_hint({"idempotentHint": False}) is False
    assert read_idempotent_hint({}) is False
    assert read_idempotent_hint(SimpleNamespace(idempotentHint=True)) is True
    assert read_idempotent_hint(lambda: None) is False


def test_wrap_if_idempotent_only_wraps_declared_tools():
    backend = InMemoryBackend()

    def pure_tool(*, q: str) -> str:
        return q.upper()

    same = wrap_if_idempotent(
        pure_tool, annotations={}, backend=backend, config=MethodConfig(key_fields=["q"])
    )
    assert same is pure_tool
    counter = {"n": 0}

    def side_effect_tool(*, order_id: str) -> dict:
        counter["n"] += 1
        return {"id": order_id}

    wrapped = wrap_if_idempotent(
        side_effect_tool,
        annotations={"idempotentHint": True},
        backend=backend,
        config=MethodConfig(key_fields=["order_id"]),
    )
    assert wrapped is not side_effect_tool
    wrapped(order_id="Z")
    wrapped(order_id="Z")
    assert counter["n"] == 1
