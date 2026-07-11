"""Spec §4.16 storage-error handling tests.

When the backend is unavailable:
- ``fail_closed`` (default): reject with HTTP 503 + Retry-After + Problem Details.
- ``fail_open``: pass through (request runs without idempotency protection).

The ``idempotency.storage_error`` event MUST be emitted in both cases.
"""

from __future__ import annotations

from idemkit.core.config import IdempotencyConfig
from idemkit.core.engine import IdempotencyEngine, NeutralRequest
from idemkit.core.events import IdempotencyEvent
from idemkit.core.state import Decision

from .fakes import FailingBackend


def _engine(*, on_storage_error: str) -> tuple[IdempotencyEngine, list[IdempotencyEvent]]:
    events: list[IdempotencyEvent] = []
    config = IdempotencyConfig(
        scope_mode="single_tenant",
        on_storage_error=on_storage_error,  # type: ignore[arg-type]
        event_handlers=[events.append],
    )
    return IdempotencyEngine(FailingBackend(), config), events


def _req(key: str = "k-storage") -> NeutralRequest:
    return NeutralRequest(
        idempotency_key=key,
        scope="tenant:user",
        method="POST",
        path="/charges",
        query="",
        body=b'{"amount":100}',
        content_type="application/json",
    )


async def test_fail_closed_returns_503_with_retry_after() -> None:
    engine, events = _engine(on_storage_error="fail_closed")
    outcome = await engine.decide(_req())

    assert outcome.kind == "reject"
    assert outcome.response is not None
    assert outcome.response.status == 503
    assert "retry-after" in outcome.response.headers
    assert b"urn:idemkit:storage-error" in outcome.response.body
    assert any(e.decision == Decision.STORAGE_ERROR for e in events)


async def test_fail_open_passes_through() -> None:
    """fail_open: handler runs without idempotency (no effective_key returned)."""
    engine, events = _engine(on_storage_error="fail_open")
    outcome = await engine.decide(_req())

    assert outcome.kind == "pass_through"
    assert outcome.effective_key is None  # no idempotency state to track
    assert outcome.claim_token is None
    assert any(e.decision == Decision.STORAGE_ERROR for e in events)


async def test_storage_error_event_includes_backend_name() -> None:
    engine, events = _engine(on_storage_error="fail_closed")
    await engine.decide(_req())

    storage_events = [e for e in events if e.decision == Decision.STORAGE_ERROR]
    assert len(storage_events) == 1
    assert storage_events[0].backend_name == "FailingBackend"
