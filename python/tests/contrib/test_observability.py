"""The contrib observability handlers record idemkit events."""

from __future__ import annotations

import logging

import pytest

from idemkit import InMemoryBackend, MethodConfig, idempotent
from idemkit.contrib.logging import logging_handler

prometheus = pytest.importorskip("prometheus_client")
from idemkit.contrib.prometheus import prometheus_handler  # noqa: E402


async def test_prometheus_handler_counts_decisions_and_latency() -> None:
    registry = prometheus.CollectorRegistry()
    calls = 0

    @idempotent(
        backend=InMemoryBackend(),
        config=MethodConfig(
            key_fields=["order_id"],
            scope=lambda a: "s",
            event_handlers=(prometheus_handler(registry),),
        ),
    )
    async def charge(*, order_id):
        nonlocal calls
        calls += 1
        return {"ok": True}

    await charge(order_id="o1")  # new -> executes
    await charge(order_id="o1")  # duplicate -> replayed
    assert calls == 1

    # One "new" and one "replayed", both on the ai_tool surface / InMemoryBackend.
    new = registry.get_sample_value(
        "idemkit_operations_total",
        {"decision": "new", "surface": "ai_tool", "backend": "InMemoryBackend"},
    )
    replayed = registry.get_sample_value(
        "idemkit_operations_total",
        {"decision": "replayed", "surface": "ai_tool", "backend": "InMemoryBackend"},
    )
    assert new == 1.0
    assert replayed == 1.0

    latency_count = registry.get_sample_value(
        "idemkit_latency_seconds_count",
        {"surface": "ai_tool", "backend": "InMemoryBackend"},
    )
    assert latency_count == 2.0  # both operations observed a latency


async def test_logging_handler_emits_one_record_per_operation(caplog) -> None:
    logger = logging.getLogger("idemkit.events.test")
    calls = 0

    @idempotent(
        backend=InMemoryBackend(),
        config=MethodConfig(
            key_fields=["order_id"],
            scope=lambda a: "s",
            event_handlers=(logging_handler(logger),),
        ),
    )
    async def charge(*, order_id):
        nonlocal calls
        calls += 1
        return {"ok": True}

    with caplog.at_level(logging.INFO, logger="idemkit.events.test"):
        await charge(order_id="o1")
        await charge(order_id="o1")

    records = [r for r in caplog.records if r.name == "idemkit.events.test"]
    assert len(records) == 2
    decisions = {r.idempotency_decision for r in records}
    assert decisions == {"new", "replayed"}
    # The hashed key is logged; the raw idempotency key never is.
    assert all(hasattr(r, "idempotency_effective_key") for r in records)
