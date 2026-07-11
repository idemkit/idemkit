"""End-to-end on real SQS (localstack): a redelivered message is processed once.

Mirrors examples/queue/sqs.py against a real SQS API. A message is received but
not deleted (a consumer crash after the side effect, before ack). After the
visibility timeout it is redelivered with the same MessageId, and idemkit replays
instead of re-running.
"""

from __future__ import annotations

import time
import uuid

import pytest

from idemkit import InMemoryBackend
from idemkit.contrib.sqs import sqs_consumer

pytestmark = pytest.mark.e2e

VISIBILITY = 2  # seconds; short so redelivery happens quickly


def _receive(client, queue_url):
    for _ in range(15):
        resp = client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=1,
            AttributeNames=["ApproximateReceiveCount"],
        )
        if resp.get("Messages"):
            return resp["Messages"][0]
    raise AssertionError("no message received from SQS")


def test_sqs_redelivery_processed_once(sqs_client):
    queue_url = sqs_client.create_queue(
        QueueName=f"idemkit-e2e-{uuid.uuid4().hex[:8]}",
        Attributes={"VisibilityTimeout": str(VISIBILITY)},
    )["QueueUrl"]
    try:
        processed = {"n": 0}
        consumer = sqs_consumer(
            backend=InMemoryBackend(), visibility_timeout_seconds=VISIBILITY
        )

        @consumer.handle
        def process(msg) -> None:
            processed["n"] += 1

        sqs_client.send_message(QueueUrl=queue_url, MessageBody="charge:42")

        # First delivery: process, but do NOT delete (simulate a crash before ack).
        m1 = _receive(sqs_client, queue_url)
        r1 = consumer.dispatch_sync(m1)
        assert r1.action.value == "ack"
        assert processed["n"] == 1

        # Wait past the visibility timeout so SQS redelivers the same MessageId.
        time.sleep(VISIBILITY + 2)
        m2 = _receive(sqs_client, queue_url)
        assert m2["MessageId"] == m1["MessageId"]

        consumer.dispatch_sync(m2)          # idemkit replays
        assert processed["n"] == 1          # NOT re-run
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=m2["ReceiptHandle"])
    finally:
        sqs_client.delete_queue(QueueUrl=queue_url)
