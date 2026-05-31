"""Observability event types and emitter per spec §4.15.

Each request emits at least one event carrying the engine's terminal decision,
plus structured fields adopters can route to Prometheus, OpenTelemetry,
structured logs, etc. A request that waits on an in-flight claim emits a
preceding ``idempotency.in_flight_wait`` event as well, so each event *type* is
emitted at most once per request but a waiting request produces two.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass

from idemkit.core.state import Decision

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class IdempotencyEvent:
    """One observability record per request."""

    decision: Decision
    effective_key: str  # already a hash per §4.6; never raw idempotency key
    backend_name: str
    fingerprint_version: int
    latency_seconds: float
    wait_duration_seconds: float = 0.0
    # Which surface produced this event: "http", "queue", or "ai_tool" (§5.8).
    # Carried so one dashboard can cover all three surfaces at once.
    surface: str = "http"

    @property
    def name(self) -> str:
        """Conventional event name: ``idempotency.<decision>``."""
        return f"idempotency.{self.decision.value}"


EventHandler = Callable[[IdempotencyEvent], None]


class EventEmitter:
    """Fan-out emitter. Handler failures MUST NOT propagate to the request path.

    Adopters register handlers via ``IdempotencyConfig.event_handlers``.
    """

    def __init__(self, handlers: list[EventHandler] | None = None) -> None:
        self._handlers: list[EventHandler] = list(handlers or [])

    def add_handler(self, handler: EventHandler) -> None:
        self._handlers.append(handler)

    def emit(self, event: IdempotencyEvent) -> None:
        for handler in self._handlers:
            try:
                handler(event)
            except Exception:
                # Observability is best-effort. A broken handler MUST NOT
                # affect the request path.
                _logger.exception("idemkit event handler raised; suppressed")
