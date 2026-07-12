"""Amazon SQS glue for idemkit's queue surface.

SQS is at-least-once: it redelivers a message whenever a consumer doesn't delete
it within the visibility timeout. :func:`sqs_consumer` presets an
:class:`~idemkit.IdempotentConsumer` for the standard boto3 message shape so the
side effect runs once per SQS ``MessageId`` even across redeliveries, concurrent
consumers, and crashes.

Two SQS-specific defaults it gets right for you:

* **Dedup id = ``MessageId``.** Stable across redeliveries of the same message.
* **Attempt count = ``ApproximateReceiveCount``.** SQS's own receive count, so
  ``max_attempts`` is enforced durably (no in-process counter). Request it when
  you receive: ``AttributeNames=["ApproximateReceiveCount"]`` — :func:`run_forever`
  does this for you.

Ack = *delete the message*. :func:`run_forever` deletes on ``ACK`` and leaves the
message for redelivery on ``RETRY``.

Example::

    import boto3
    from idemkit import RedisBackend
    from idemkit.contrib.sqs import sqs_consumer, run_forever

    sqs = boto3.client("sqs")
    consumer = sqs_consumer(
        backend=RedisBackend.from_url("redis://localhost:6379"),
        visibility_timeout_seconds=30,
        config=QueueConfig(
            max_attempts=5,
            on_exhausted=lambda msg, exc: sqs.send_message(QueueUrl=DLQ, MessageBody=msg["Body"]),
        ),
    )


    @consumer.handle
    def process(msg) -> None:
        charge_customer(msg["Body"])  # runs once per MessageId


    run_forever(consumer, sqs_client=sqs, queue_url=QUEUE_URL, visibility_timeout=30)
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from idemkit.adapters.queue import ConsumerAction, IdempotentConsumer
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.policy import QueueConfig


def sqs_receive_count(message: dict[str, Any]) -> int | None:
    """Read SQS ``ApproximateReceiveCount`` from a boto3 message dict."""
    attrs = message.get("Attributes") or {}
    value = attrs.get("ApproximateReceiveCount")
    return int(value) if value is not None else None


def sqs_consumer(
    *,
    backend: IdempotencyBackend,
    visibility_timeout_seconds: float,
    config: QueueConfig | None = None,
) -> IdempotentConsumer:
    """Build an :class:`~idemkit.IdempotentConsumer` wired for boto3 SQS messages.

    SQS presets the dedup id (``MessageId``) and the attempt count
    (``ApproximateReceiveCount``). Pass a :class:`~idemkit.QueueConfig` for
    behaviour (``max_attempts``, ``on_exhausted``, ``scope`` per queue, ...).
    """
    cfg = config or QueueConfig()
    presets: dict[str, Any] = {
        "dedup_id": lambda m: m["MessageId"],
        "visibility_timeout_seconds": visibility_timeout_seconds,
    }
    if cfg.receive_count is None:
        presets["receive_count"] = sqs_receive_count
    cfg = dataclasses.replace(cfg, **presets)
    return IdempotentConsumer(backend=backend, config=cfg)


def run_forever(
    consumer: IdempotentConsumer,
    *,
    sqs_client: Any,
    queue_url: str,
    visibility_timeout: int,
    wait_time_seconds: int = 20,
    max_messages: int = 10,
    stop: Callable[[], bool] | None = None,
) -> None:
    """Poll SQS and dispatch each message, deleting it on ``ACK``.

    A blocking loop for a synchronous worker (it uses ``dispatch_sync``). Long-polls
    with ``wait_time_seconds`` and requests ``ApproximateReceiveCount`` so attempt
    counting is durable. On ``ACK`` the message is deleted; on ``RETRY`` it is left
    for redelivery (the visibility timeout lapses). Pass ``stop=lambda: flag`` to
    break the loop for graceful shutdown.
    """
    while stop is None or not stop():
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=wait_time_seconds,
            VisibilityTimeout=visibility_timeout,
            AttributeNames=["ApproximateReceiveCount"],
        )
        for message in resp.get("Messages", []):
            result = consumer.dispatch_sync(message)
            if result.action is ConsumerAction.ACK:
                sqs_client.delete_message(
                    QueueUrl=queue_url, ReceiptHandle=message["ReceiptHandle"]
                )
