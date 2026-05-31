"""Spec §4.15 observability event tests.

Every request emits exactly one event with the correct fields:
effective_key (already hashed), backend_name, fingerprint_version, latency,
plus wait_duration for in-flight wait paths.
"""

from __future__ import annotations

import asyncio

from idemkit.backends.memory import InMemoryBackend
from idemkit.core.config import IdempotencyConfig
from idemkit.core.engine import IdempotencyEngine, NeutralRequest, NeutralResponse
from idemkit.core.events import IdempotencyEvent
from idemkit.core.state import Decision


def _engine(**overrides) -> tuple[IdempotencyEngine, list[IdempotencyEvent]]:
    events: list[IdempotencyEvent] = []
    config = IdempotencyConfig(
        scope_optional=True,
        event_handlers=[events.append],
        **overrides,
    )
    return IdempotencyEngine(InMemoryBackend(), config), events


def _req(key: str = "k-obs", body: bytes = b'{"x":1}') -> NeutralRequest:
    return NeutralRequest(
        idempotency_key=key,
        scope="t:u",
        method="POST",
        path="/x",
        query="",
        body=body,
        content_type="application/json",
    )


async def test_new_event_has_required_fields() -> None:
    engine, events = _engine()
    await engine.decide(_req())

    assert len(events) == 1
    e = events[0]
    assert e.decision == Decision.NEW
    assert e.backend_name == "InMemoryBackend"
    assert e.fingerprint_version == 1
    assert e.latency_seconds >= 0
    assert len(e.effective_key) == 64  # SHA-256 hex = 64 chars
    assert e.name == "idempotency.new"


async def test_replayed_event_emitted_on_second_request() -> None:
    engine, events = _engine()
    first = await engine.decide(_req())
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=200, headers={}, body=b"x"),
    )
    await engine.decide(_req())

    decisions = [e.decision for e in events]
    assert decisions == [Decision.NEW, Decision.REPLAYED]


async def test_wait_event_carries_duration() -> None:
    engine, events = _engine(wait_timeout_seconds=2.0)
    first = await engine.decide(_req())

    async def waiter():
        await engine.decide(_req())

    async def completer():
        await asyncio.sleep(0.05)
        await engine.on_complete(
            first.effective_key,
            first.claim_token,
            NeutralResponse(status=200, headers={}, body=b"x"),
        )

    await asyncio.gather(waiter(), completer())

    replay_events = [e for e in events if e.decision == Decision.REPLAYED]
    assert len(replay_events) == 1
    assert replay_events[0].wait_duration_seconds > 0


async def test_broken_event_handler_does_not_crash_request() -> None:
    """A user-supplied handler that raises MUST NOT propagate to the request path."""
    events: list[IdempotencyEvent] = []

    def broken_handler(event):
        raise RuntimeError("observability blew up")

    config = IdempotencyConfig(
        scope_optional=True,
        event_handlers=[broken_handler, events.append],  # broken first, then collector
    )
    engine = IdempotencyEngine(InMemoryBackend(), config)

    outcome = await engine.decide(_req())
    # Request still succeeded
    assert outcome.kind == "pass_through"
    # Collector still saw the event despite the broken handler
    assert len(events) == 1


async def test_payload_mismatch_event_emitted() -> None:
    engine, events = _engine()
    first = await engine.decide(_req(body=b'{"x":1}'))
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=200, headers={}, body=b"x"),
    )

    await engine.decide(_req(body=b'{"x":99}'))  # different payload

    assert Decision.PAYLOAD_MISMATCH in [e.decision for e in events]


async def test_lease_reclaimed_loss_event_emitted() -> None:
    """Spec §4.1: when our claim was reclaimed mid-handler, completing fails
    and the engine MUST emit ``idempotency.lease_reclaimed_loss``.

    Sequence:
      1. Process A claims with short lease.
      2. Lease expires.
      3. Process B claims (LEASE_RECLAIMED — different claim_token).
      4. Process A's handler finishes; calls on_complete with A's claim_token.
      5. Backend returns False; engine emits lease_reclaimed_loss.
    """
    engine, events = _engine(lease_ttl_seconds=0.1)
    first = await engine.decide(_req())
    assert first.kind == "pass_through"
    assert first.claim_token is not None

    await asyncio.sleep(0.25)  # lease elapses

    # Someone else reclaims
    second = await engine.decide(_req())
    assert second.kind == "pass_through"
    assert second.claim_token != first.claim_token

    # Now the original owner tries to complete with their old token
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=201, headers={}, body=b"original-result"),
    )

    assert Decision.LEASE_RECLAIMED_LOSS in [e.decision for e in events]
