"""End-to-end engine state machine tests against InMemoryBackend."""

from __future__ import annotations

import asyncio

from idemkit.backends.memory import InMemoryBackend
from idemkit.core.config import IdempotencyConfig
from idemkit.core.engine import IdempotencyEngine, NeutralRequest, NeutralResponse
from idemkit.core.events import IdempotencyEvent
from idemkit.core.exceptions import StorageError
from idemkit.core.state import Decision


def _make_engine(**config_overrides) -> tuple[IdempotencyEngine, list[IdempotencyEvent]]:
    """Build an engine wired to InMemoryBackend with optional config tweaks.

    Returns the engine plus a list that captures observability events.
    """
    events: list[IdempotencyEvent] = []
    config = IdempotencyConfig(
        scope_mode="single_tenant",  # tests don't set up auth
        event_handlers=[events.append],
        **config_overrides,
    )
    engine = IdempotencyEngine(InMemoryBackend(), config)
    return engine, events


def _req(
    *,
    key: str | None = "k-1",
    method: str = "POST",
    path: str = "/charges",
    body: bytes = b'{"amount":100}',
    caller: str = "tenant:user",
) -> NeutralRequest:
    return NeutralRequest(
        idempotency_key=key,
        scope=caller,
        method=method,
        path=path,
        query="",
        body=body,
        content_type="application/json",
    )


async def test_new_decision_passes_through() -> None:
    engine, events = _make_engine()
    outcome = await engine.decide(_req())
    assert outcome.kind == "pass_through"
    assert outcome.effective_key is not None
    assert outcome.claim_token is not None
    assert [e.decision for e in events] == [Decision.NEW]


async def test_second_request_after_complete_replays() -> None:
    engine, events = _make_engine()
    first = await engine.decide(_req())
    assert first.kind == "pass_through"

    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(
            status=201,
            headers={"content-type": "application/json"},
            body=b'{"id":"abc"}',
        ),
    )

    second = await engine.decide(_req())
    assert second.kind == "replay"
    assert second.response is not None
    assert second.response.status == 201
    assert second.response.body == b'{"id":"abc"}'
    assert second.response.headers["idempotency-replayed"] == "true"
    decisions = [e.decision for e in events]
    assert Decision.REPLAYED in decisions


async def test_same_key_different_payload_returns_422() -> None:
    engine, events = _make_engine()
    first = await engine.decide(_req(body=b'{"amount":100}'))
    assert first.kind == "pass_through"
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=201, headers={"content-type": "application/json"}, body=b"{}"),
    )

    mismatch = await engine.decide(_req(body=b'{"amount":200}'))
    assert mismatch.kind == "reject"
    assert mismatch.response is not None
    assert mismatch.response.status == 422
    # Problem Details body contains the type URI
    assert b"urn:idemkit:payload-mismatch" in mismatch.response.body
    assert Decision.PAYLOAD_MISMATCH in [e.decision for e in events]


async def test_stripe_compat_returns_409_with_compat_header() -> None:
    engine, _ = _make_engine(compat_mode="stripe")
    first = await engine.decide(_req(body=b'{"amount":100}'))
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=200, headers={"content-type": "application/json"}, body=b"{}"),
    )

    mismatch = await engine.decide(_req(body=b'{"amount":200}'))
    assert mismatch.response.status == 409

    # Replay uses Stripe's deployed spelling
    again = await engine.decide(_req(body=b'{"amount":100}'))
    assert again.kind == "replay"
    assert again.response.headers["idempotent-replayed"] == "true"
    assert "idempotency-replayed" not in again.response.headers


async def test_body_fingerprint_empty_ignores_body() -> None:
    """A body_fingerprint that returns b"" ignores the body — the same key
    replays regardless of body, so a client-side timestamp/nonce that changes
    between retries doesn't trip a 422 payload mismatch."""
    engine, _ = _make_engine(body_fingerprint=lambda body, ct: b"")
    first = await engine.decide(_req(body=b'{"amount":100,"ts":"2026-05-30T10:00:01"}'))
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=200, headers={}, body=b"original"),
    )

    # Same key, DIFFERENT body — still a replay (body excluded from fingerprint).
    second = await engine.decide(_req(body=b'{"amount":100,"ts":"2026-05-30T10:00:02"}'))
    assert second.kind == "replay"
    assert second.response is not None
    assert second.response.body == b"original"


async def test_body_fingerprint_selector_ignores_volatile_fields() -> None:
    """body_fingerprint selects which body fields matter: a retry that only
    differs in a volatile field (ts) replays; a genuinely different selected
    field (amount) is a payload mismatch."""
    import json

    def select(body: bytes, content_type: str | None) -> bytes:
        data = json.loads(body)
        return json.dumps({"amount": data["amount"]}, sort_keys=True).encode()

    engine, _ = _make_engine(body_fingerprint=select)
    first = await engine.decide(_req(body=b'{"amount":100,"ts":"2026-05-30T10:00:01"}'))
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=200, headers={}, body=b"original"),
    )

    # Same amount, different ts -> still a replay (ts not in the fingerprint).
    same = await engine.decide(_req(body=b'{"amount":100,"ts":"2026-05-30T10:00:09"}'))
    assert same.kind == "replay"
    assert same.response is not None and same.response.body == b"original"

    # Different amount -> payload mismatch (the selected field changed).
    diff = await engine.decide(_req(body=b'{"amount":200,"ts":"2026-05-30T10:00:01"}'))
    assert diff.kind == "reject"
    assert diff.response is not None and diff.response.status == 422


async def test_5xx_is_not_cached_by_default() -> None:
    """Spec §4.2: 5xx MUST NOT be cached by default; retry executes again."""
    engine, _ = _make_engine()
    first = await engine.decide(_req())
    await engine.on_complete(
        first.effective_key,
        first.claim_token,
        NeutralResponse(status=500, headers={}, body=b"oops"),
    )

    # Next request must NOT see a cached 500. New claim path.
    second = await engine.decide(_req())
    assert second.kind == "pass_through"  # re-executes


async def test_missing_key_is_pass_through_by_default() -> None:
    engine, _ = _make_engine()
    outcome = await engine.decide(_req(key=None))
    assert outcome.kind == "pass_through"
    assert outcome.effective_key is None  # idempotency does not apply


async def test_missing_key_when_required_returns_400() -> None:
    engine, _ = _make_engine(require_key_for_mutations=True)
    outcome = await engine.decide(_req(key=None))
    assert outcome.kind == "reject"
    assert outcome.response.status == 400
    assert b"urn:idemkit:missing-key" in outcome.response.body


async def test_non_applicable_method_is_pass_through() -> None:
    engine, _ = _make_engine()
    outcome = await engine.decide(_req(method="GET"))
    assert outcome.kind == "pass_through"
    assert outcome.effective_key is None


async def test_in_flight_wait_returns_replay() -> None:
    """Second request observes CLAIMED, waits, gets replay after first completes."""
    engine, events = _make_engine(wait_timeout_seconds=2.0)
    first = await engine.decide(_req())

    async def waiter():
        return await engine.decide(_req())

    async def completer():
        await asyncio.sleep(0.05)
        await engine.on_complete(
            first.effective_key,
            first.claim_token,
            NeutralResponse(
                status=201,
                headers={"content-type": "application/json"},
                body=b'{"x":1}',
            ),
        )

    waited_result, _ = await asyncio.gather(waiter(), completer())
    assert waited_result.kind == "replay"
    assert waited_result.response.body == b'{"x":1}'
    decisions = [e.decision for e in events]
    assert Decision.NEW in decisions
    # The waiter MUST emit in_flight_wait when it starts waiting (§4.15)...
    assert Decision.IN_FLIGHT_WAIT in decisions
    # ...and replayed once the in-flight request completes.
    assert Decision.REPLAYED in decisions


async def test_in_flight_wait_timeout_returns_423() -> None:
    engine, events = _make_engine(wait_timeout_seconds=0.1, lease_ttl_seconds=30.0)
    await engine.decide(_req())  # first claim, never completes

    second = await engine.decide(_req())
    assert second.kind == "reject"
    assert second.response.status == 423
    assert b"urn:idemkit:in-progress" in second.response.body
    assert "retry-after" in second.response.headers
    assert Decision.CONFLICT in [e.decision for e in events]


async def test_release_on_error_allows_retry() -> None:
    engine, _ = _make_engine()
    first = await engine.decide(_req())
    await engine.on_error(first.effective_key, first.claim_token)

    second = await engine.decide(_req())
    assert second.kind == "pass_through"  # got a fresh claim


async def test_missing_identity_in_production_fails_closed() -> None:
    """§4.6: a configured extractor that yields no identity MUST NOT silently
    merge callers onto a shared anonymous bucket. Fail closed instead."""
    events: list[IdempotencyEvent] = []
    config = IdempotencyConfig(
        scope=lambda req: "real-user",  # production mode (not optional)
        event_handlers=[events.append],
    )
    engine = IdempotencyEngine(InMemoryBackend(), config)

    # scope is None on the request → extractor produced nothing
    outcome = await engine.decide(_req(caller=None))  # type: ignore[arg-type]
    assert outcome.kind == "reject"
    assert outcome.response is not None
    assert outcome.response.status == 500
    assert b"urn:idemkit:identity-unavailable" in outcome.response.body


async def test_storage_error_during_wait_respects_policy() -> None:
    """A backend failure mid-wait MUST map to on_storage_error, not a 500."""
    backend = InMemoryBackend()
    config = IdempotencyConfig(
        scope_mode="single_tenant",
        on_storage_error="fail_closed",
    )
    engine = IdempotencyEngine(backend, config)

    # Hold a live claim so the next request enters the wait path.
    await engine.decide(_req())

    async def boom(*args, **kwargs):
        raise StorageError("backend died mid-wait")

    backend.wait_for_completion = boom  # type: ignore[method-assign]

    outcome = await engine.decide(_req())
    assert outcome.kind == "reject"
    assert outcome.response is not None
    assert outcome.response.status == 503
    assert b"urn:idemkit:storage-error" in outcome.response.body
