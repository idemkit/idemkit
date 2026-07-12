"""Prometheus exporter for idemkit's observability events.

idemkit emits one :class:`~idemkit.IdempotencyEvent` per operation; you decide
where it goes. This is a ready-made handler that records the standard four metrics
so you get a dashboard without writing the plumbing yourself::

    from idemkit import IdempotencyMiddleware, RedisBackend, HttpConfig
    from idemkit.contrib.prometheus import prometheus_handler

    app.add_middleware(
        IdempotencyMiddleware,
        backend=RedisBackend.from_url("redis://..."),
        config=HttpConfig(event_handlers=(prometheus_handler(),)),
    )

The same handler works on every surface (HTTP, queue, method), so one dashboard
covers all three: filter by the ``surface`` label.

Metrics (namespaced ``idemkit_`` by default):

* ``idemkit_operations_total``: Counter, labels ``decision`` / ``surface`` / ``backend``.
  Derive replay rate, conflict rate, and storage-error rate from ``decision``.
* ``idemkit_latency_seconds``: Histogram, labels ``surface`` / ``backend``.
* ``idemkit_wait_seconds``: Histogram, labels ``surface`` / ``backend`` (only observed
  when a duplicate waited on an in-flight run).

The hashed ``effective_key`` is deliberately NOT a label: it is high-cardinality and
would blow up your metrics store. Requires the ``prometheus`` extra
(``pip install 'idemkit[prometheus]'``).
"""

from __future__ import annotations

from typing import Any

from idemkit.core.events import EventHandler, IdempotencyEvent


def prometheus_handler(registry: Any = None, *, namespace: str = "idemkit") -> EventHandler:
    """Build an :data:`~idemkit.EventHandler` that records idemkit metrics.

    Pass a custom ``registry`` (a ``prometheus_client.CollectorRegistry``) to isolate
    the metrics, e.g. in tests; the default is the global registry. ``namespace``
    prefixes every metric name.
    """
    try:
        from prometheus_client import Counter, Histogram
    except ImportError as e:  # pragma: no cover - exercised via the error message
        raise ImportError(
            "idemkit: install the `prometheus` extra: pip install 'idemkit[prometheus]'"
        ) from e

    kwargs: dict[str, Any] = {"registry": registry} if registry is not None else {}
    operations = Counter(
        f"{namespace}_operations_total",
        "idemkit operations by decision, surface, and backend.",
        ["decision", "surface", "backend"],
        **kwargs,
    )
    latency = Histogram(
        f"{namespace}_latency_seconds",
        "idemkit end-to-end operation latency.",
        ["surface", "backend"],
        **kwargs,
    )
    wait = Histogram(
        f"{namespace}_wait_seconds",
        "Time a duplicate waited on an in-flight run before replaying.",
        ["surface", "backend"],
        **kwargs,
    )

    def handle(event: IdempotencyEvent) -> None:
        operations.labels(event.decision.value, event.surface, event.backend_name).inc()
        latency.labels(event.surface, event.backend_name).observe(event.latency_seconds)
        if event.wait_duration_seconds:
            wait.labels(event.surface, event.backend_name).observe(event.wait_duration_seconds)

    return handle
