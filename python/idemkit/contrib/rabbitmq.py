"""RabbitMQ (pika) glue for idemkit's queue surface.

RabbitMQ is at-least-once under manual acks: an unacked delivery is requeued when
the channel or connection drops, and you can requeue explicitly with ``basic_nack``.
So a consumer sees the same message again after a network blip, a crash, or a
transient failure. :func:`rabbitmq_consumer` presets an
:class:`~idemkit.IdempotentConsumer` so the side effect runs once per message even
across those redeliveries.

Two things are different from SQS/Kafka, and worth stating plainly:

* **There is no dedup id built in.** RabbitMQ does not stamp a stable per-message
  id for you. The clean choice is the AMQP ``message_id`` property, which the
  *publisher* must set (a UUID, or your own business id). If your producer doesn't
  set one, dedup has nothing to key on — see :class:`RabbitMessage`.
* **There is no visibility timeout.** An unacked message is not redelivered on a
  timer; it comes back only on nack/requeue or a channel/connection loss. So the
  lease here is just how long idemkit holds the claim before assuming a crash — you
  pick it (``lease_seconds``), it isn't derived from the broker.

The ``redelivered`` flag RabbitMQ sets on a re-delivery is carried through on
:attr:`RabbitMessage.redelivered` if you want to log or branch on it.

Example (pika)::

    import pika
    from idemkit import RedisBackend
    from idemkit.contrib.rabbitmq import rabbitmq_consumer, run_forever

    consumer = rabbitmq_consumer(
        backend=RedisBackend.from_url("redis://localhost:6379"),
        lease_seconds=300,
    )


    @consumer.handle
    def process(msg) -> None:
        charge_customer(msg.body)  # runs once per message_id


    conn = pika.BlockingConnection(pika.ConnectionParameters("localhost"))
    run_forever(consumer, channel=conn.channel(), queue="charges", lease_seconds=300)
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from idemkit.adapters.queue import ConsumerAction, IdempotentConsumer
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.policy import QueueConfig


@dataclasses.dataclass(frozen=True)
class RabbitMessage:
    """One RabbitMQ delivery, in the shape idemkit dedupes on.

    ``message_id`` is the AMQP ``message_id`` property the publisher set; it is the
    dedup id. ``run_forever`` builds this for you from pika's delivery; if you drive
    the consumer yourself, construct it from ``properties.message_id`` and ``body``.
    """

    message_id: str
    body: bytes
    redelivered: bool = False
    delivery_tag: int | None = None


def rabbitmq_dedup_id(message: RabbitMessage) -> str:
    """The message's AMQP ``message_id`` (the dedup id)."""
    return message.message_id


def rabbitmq_consumer(
    *,
    backend: IdempotencyBackend,
    lease_seconds: float = 300.0,
    config: QueueConfig | None = None,
) -> IdempotentConsumer:
    """Build an :class:`~idemkit.IdempotentConsumer` wired for RabbitMQ deliveries.

    Presets the dedup id (the AMQP ``message_id``) and the lease (``lease_seconds``,
    since RabbitMQ has no visibility timeout to derive it from). Pass a
    :class:`~idemkit.QueueConfig` for behaviour (``max_attempts``, ``on_exhausted``,
    ``scope`` per queue, ...).
    """
    cfg = config or QueueConfig()
    cfg = dataclasses.replace(
        cfg,
        dedup_id=rabbitmq_dedup_id,
        visibility_timeout_seconds=lease_seconds,
    )
    return IdempotentConsumer(backend=backend, config=cfg)


def run_forever(
    consumer: IdempotentConsumer,
    *,
    channel: Any,
    queue: str,
    lease_seconds: float = 300.0,
    prefetch: int = 10,
    stop: Callable[[], bool] | None = None,
) -> None:
    """Pull from ``queue`` and dispatch each delivery, acking on ``ACK``.

    A blocking loop for a synchronous worker (it uses ``dispatch_sync`` and pika's
    ``basic_get``). On ``ACK`` the delivery is acked; on ``RETRY`` it is nacked with
    ``requeue=True`` so RabbitMQ redelivers it. The publisher MUST set the AMQP
    ``message_id`` property; a delivery without one raises ``ValueError`` rather than
    silently skipping dedup. Pass ``stop=lambda: flag`` for graceful shutdown.
    """
    channel.basic_qos(prefetch_count=prefetch)
    while stop is None or not stop():
        method, properties, body = channel.basic_get(queue=queue, auto_ack=False)
        if method is None:  # queue empty
            continue
        message_id = getattr(properties, "message_id", None)
        if not message_id:
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            raise ValueError(
                "idemkit: RabbitMQ delivery has no message_id property to dedup on. "
                "Publish with a message_id (a UUID or your business id), or key on a "
                "header via a custom QueueConfig.dedup_id."
            )
        message = RabbitMessage(
            message_id=message_id,
            body=body,
            redelivered=bool(getattr(method, "redelivered", False)),
            delivery_tag=method.delivery_tag,
        )
        result = consumer.dispatch_sync(message)
        if result.action is ConsumerAction.ACK:
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:  # RETRY: leave it for redelivery
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
