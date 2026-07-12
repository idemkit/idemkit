"""Kafka glue for idemkit's queue surface.

Kafka redelivers when a consumer doesn't make progress within
``max.poll.interval.ms`` (the group coordinator considers it dead and reassigns
its partitions), and on rebalances. :func:`kafka_consumer` presets an
:class:`~idemkit.IdempotentConsumer` so a side effect runs once per record even
across those redeliveries.

Two Kafka-specific choices it gets right for you:

* **Dedup id = ``topic:partition:offset``.** The globally-stable identity of a
  record; the message *key* is not unique enough (many records share a key).
* **Visibility-timeout analogue = ``max.poll.interval.ms``.** Kafka has no
  per-message visibility timeout, but the redelivery deadline is the poll
  interval, so the lease is derived from it (kept shorter, renewed by heartbeat).

Kafka exposes **no native receive count**, so ``max_attempts`` needs a durable
:class:`~idemkit.adapters.queue.AttemptStore` in a multi-consumer deployment
(pass ``attempt_store=``); without one, counting is in-process and idemkit warns.

``group_id`` is required and becomes the scope: two consumer groups read the same
offsets, and each must process every record, so their dedup records must not
collide.

Example (confluent-kafka)::

    from confluent_kafka import Consumer
    from idemkit import RedisBackend
    from idemkit.contrib.kafka import kafka_consumer

    consumer = kafka_consumer(
        backend=RedisBackend.from_url("redis://localhost:6379"),
        group_id="billing",
        max_poll_interval_seconds=300,
    )


    @consumer.handle
    def process(record) -> None:
        charge_customer(record.value())  # runs once per topic:partition:offset


    kc = Consumer({"bootstrap.servers": "...", "group.id": "billing", "enable.auto.commit": False})
    kc.subscribe(["charges"])
    while True:
        record = kc.poll(1.0)
        if record is None or record.error():
            continue
        result = consumer.dispatch_sync(record)
        if result.action.value == "ack":
            kc.commit(record)  # advance the offset only when done
"""

from __future__ import annotations

import dataclasses
from typing import Any

from idemkit.adapters.queue import IdempotentConsumer
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.policy import QueueConfig


def _attr(message: Any, name: str) -> Any:
    """Read ``name`` from a record, whether it's a method (confluent_kafka) or a
    plain attribute (kafka-python)."""
    value = getattr(message, name)
    return value() if callable(value) else value


def kafka_dedup_id(message: Any) -> str:
    """``topic:partition:offset`` for a confluent_kafka or kafka-python record."""
    return f"{_attr(message, 'topic')}:{_attr(message, 'partition')}:{_attr(message, 'offset')}"


def kafka_consumer(
    *,
    backend: IdempotencyBackend,
    group_id: str,
    max_poll_interval_seconds: float = 300.0,
    config: QueueConfig | None = None,
) -> IdempotentConsumer:
    """Build an :class:`~idemkit.IdempotentConsumer` wired for Kafka records.

    Kafka presets the dedup id (``topic:partition:offset``) and the scope
    (``group_id``). Pass a :class:`~idemkit.QueueConfig` for behaviour.
    """
    cfg = config or QueueConfig()
    presets: dict[str, Any] = {
        "dedup_id": kafka_dedup_id,
        "visibility_timeout_seconds": max_poll_interval_seconds,
    }
    if cfg.scope is None:  # group_id is the scope unless the caller set one
        presets["scope"] = lambda _m: group_id
    cfg = dataclasses.replace(cfg, **presets)
    return IdempotentConsumer(backend=backend, config=cfg)
