"""Structured-logging exporter for idemkit's observability events.

A zero-dependency handler that logs one structured line per operation. Route
idemkit's :class:`~idemkit.IdempotencyEvent` to your logger when you don't (yet)
have a metrics pipeline::

    from idemkit import MethodConfig, idempotent
    from idemkit.contrib.logging import logging_handler


    @idempotent(
        backend=backend,
        config=MethodConfig(
            key_fields=["order_id"],
            event_handlers=(logging_handler(),),
        ),
    )
    async def refund(*, order_id): ...

Each record carries the decision, surface, backend, latencies, and the hashed
``effective_key`` (safe to log; never the raw idempotency key). Fields go in the
``extra`` dict so a JSON log formatter picks them up as structured fields.
"""

from __future__ import annotations

import logging

from idemkit.core.events import EventHandler, IdempotencyEvent

_default_logger = logging.getLogger("idemkit.events")


def logging_handler(
    logger: logging.Logger | None = None, *, level: int = logging.INFO
) -> EventHandler:
    """Build an :data:`~idemkit.EventHandler` that logs one line per operation.

    Pass your own ``logger`` (default ``idemkit.events``) and ``level``.
    """
    log = logger or _default_logger

    def handle(event: IdempotencyEvent) -> None:
        log.log(
            level,
            "idemkit %s surface=%s backend=%s latency=%.4fs",
            event.decision.value,
            event.surface,
            event.backend_name,
            event.latency_seconds,
            extra={
                "idempotency_decision": event.decision.value,
                "idempotency_surface": event.surface,
                "idempotency_backend": event.backend_name,
                "idempotency_effective_key": event.effective_key,
                "idempotency_latency_seconds": event.latency_seconds,
                "idempotency_wait_seconds": event.wait_duration_seconds,
            },
        )

    return handle
