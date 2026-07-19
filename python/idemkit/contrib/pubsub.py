"""Google Cloud Pub/Sub glue for idemkit's queue surface.

Pub/Sub is at-least-once: if an ack doesn't reach the server before the ack
deadline (or you nack), the message is redelivered. Even with exactly-once
delivery enabled it only dedups *subscriber-side* redeliveries — a publisher that
retries a lost publish produces a new ``message_id``, so a duplicate can still
reach you. :func:`pubsub_consumer` presets an
:class:`~idemkit.IdempotentConsumer` so the side effect runs once per message.

Two Pub/Sub specifics:

* **Dedup id = ``message_id``.** Stable across subscriber redeliveries of the same
  message. It does *not* cover publisher-side duplicates (those carry different
  ids); if your producer can double-publish, dedup on a business id in the payload
  instead via a custom ``QueueConfig.dedup_id``.
* **Visibility-timeout analogue = the ack deadline.** The lease is derived from it
  (``ack_deadline_seconds``); for slow handlers, extend the deadline with the
  client's lease-management or raise it on the subscription.

Streaming pull runs your callback in a background thread per message. Wrap it with
:func:`pubsub_callback`: it dispatches through idemkit and acks on ``ACK`` / nacks
on ``RETRY``.

Example (google-cloud-pubsub)::

    from google.cloud import pubsub_v1
    from idemkit import RedisBackend
    from idemkit.contrib.pubsub import pubsub_consumer, pubsub_callback

    consumer = pubsub_consumer(
        backend=RedisBackend.from_url("redis://localhost:6379"),
        ack_deadline_seconds=60,
    )


    @consumer.handle
    def process(message) -> None:
        charge_customer(message.data)  # runs once per message_id


    subscriber = pubsub_v1.SubscriberClient()
    subscriber.subscribe(subscription_path, callback=pubsub_callback(consumer))
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable
from typing import Any

from idemkit.adapters.queue import ConsumerAction, IdempotentConsumer
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.policy import QueueConfig


def pubsub_dedup_id(message: Any) -> str:
    """The Pub/Sub ``message_id`` (the dedup id)."""
    return str(message.message_id)


def pubsub_consumer(
    *,
    backend: IdempotencyBackend,
    ack_deadline_seconds: float = 60.0,
    config: QueueConfig | None = None,
) -> IdempotentConsumer:
    """Build an :class:`~idemkit.IdempotentConsumer` wired for Pub/Sub messages.

    Presets the dedup id (``message_id``) and the lease (from the ack deadline).
    Pass a :class:`~idemkit.QueueConfig` for behaviour (``max_attempts``,
    ``on_exhausted``, ``scope`` per subscription, ...).
    """
    cfg = config or QueueConfig()
    cfg = dataclasses.replace(
        cfg,
        dedup_id=pubsub_dedup_id,
        visibility_timeout_seconds=ack_deadline_seconds,
    )
    return IdempotentConsumer(backend=backend, config=cfg)


def pubsub_callback(consumer: IdempotentConsumer) -> Callable[[Any], None]:
    """Wrap ``consumer`` as a streaming-pull callback that acks/nacks the message.

    Returns a function you pass to ``subscriber.subscribe(..., callback=...)``. It
    dispatches the message through idemkit (``dispatch_sync``, since the callback
    runs on the client's thread) and calls ``message.ack()`` on ``ACK`` or
    ``message.nack()`` on ``RETRY`` (redeliver after the ack deadline).
    """

    def _callback(message: Any) -> None:
        result = consumer.dispatch_sync(message)
        if result.action is ConsumerAction.ACK:
            message.ack()
        else:  # RETRY
            message.nack()

    return _callback
