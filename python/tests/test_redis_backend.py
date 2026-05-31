"""Redis backend tests using fakeredis (in-process, supports Lua + Pub/Sub).

These exercise the same backend contract as test_concurrent_claim.py against
the InMemoryBackend, but executed against the Lua-script-backed implementation.

CAVEAT: fakeredis is NOT real Redis. It re-implements commands (including the
Lua runtime, ``cjson`` and ``TIME``) in-process and does not reproduce real
Redis's single-threaded server-side atomicity. These tests verify the Lua
*logic*; they do NOT prove atomicity under genuine multi-client concurrency.
Run the suite against a real Redis before relying on the concurrency
guarantees (see python/README.md#contributing).
"""

from __future__ import annotations

import asyncio
import os
from collections import Counter

import pytest

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")

from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.core.state import ClaimResultType, State  # noqa: E402

REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture
async def backend():
    # Prefer a real Redis when IDEMKIT_TEST_REDIS_URL is set so the Lua scripts
    # run against a genuine server (fakeredis only approximates them); fall back
    # to fakeredis for a Docker-free default run.
    if REDIS_URL:
        import redis.asyncio as aioredis

        client = aioredis.from_url(REDIS_URL, decode_responses=False)
        await client.flushdb()
    else:
        client = fakeredis_aio.FakeRedis(decode_responses=False)
    b = RedisBackend(client)
    yield b
    await b.aclose()


async def test_new_claim(backend: RedisBackend) -> None:
    result = await backend.claim("k1", "fp", 1, 30.0)
    assert result.result == ClaimResultType.NEW_CLAIMED
    assert result.our_claim_token is not None
    assert result.record.state == State.CLAIMED
    assert result.record.fingerprint == "fp"


async def test_second_claim_returns_already_claimed(backend: RedisBackend) -> None:
    first = await backend.claim("k2", "fp", 1, 30.0)
    second = await backend.claim("k2", "fp", 1, 30.0)
    assert second.result == ClaimResultType.ALREADY_CLAIMED
    assert second.our_claim_token is None
    assert second.record.claim_token == first.our_claim_token


async def test_complete_then_query_returns_completed(backend: RedisBackend) -> None:
    first = await backend.claim("k3", "fp", 1, 30.0)
    ok = await backend.complete(
        "k3", first.our_claim_token, 201,
        {"content-type": "application/json"}, b'{"id":"abc"}', 3600.0,
    )
    assert ok is True

    second = await backend.claim("k3", "fp", 1, 30.0)
    assert second.result == ClaimResultType.ALREADY_COMPLETED
    assert second.record.state == State.COMPLETED
    assert second.record.response_status == 201
    assert second.record.response_body == b'{"id":"abc"}'
    assert second.record.response_headers["content-type"] == "application/json"


async def test_complete_with_wrong_token_rejected(backend: RedisBackend) -> None:
    await backend.claim("k4", "fp", 1, 30.0)
    ok = await backend.complete(
        "k4", "wrong-token", 200, {}, b"x", 3600.0,
    )
    assert ok is False

    # State unchanged
    re = await backend.claim("k4", "fp", 1, 30.0)
    assert re.result == ClaimResultType.ALREADY_CLAIMED
    assert re.record.state == State.CLAIMED


async def test_release_with_correct_token(backend: RedisBackend) -> None:
    first = await backend.claim("k5", "fp", 1, 30.0)
    released = await backend.release("k5", first.our_claim_token)
    assert released is True

    after = await backend.claim("k5", "fp", 1, 30.0)
    assert after.result == ClaimResultType.NEW_CLAIMED


async def test_release_with_wrong_token_fails(backend: RedisBackend) -> None:
    await backend.claim("k6", "fp", 1, 30.0)
    released = await backend.release("k6", "not-our-token")
    assert released is False


async def test_expired_lease_is_reclaimed(backend: RedisBackend) -> None:
    first = await backend.claim("k7", "fp", 1, 0.05)
    await asyncio.sleep(0.1)
    second = await backend.claim("k7", "fp", 1, 30.0)
    assert second.result == ClaimResultType.LEASE_RECLAIMED
    assert second.our_claim_token != first.our_claim_token


async def test_complete_after_reclaim_is_rejected(backend: RedisBackend) -> None:
    """Spec §4.1: a delayed completion from a process whose lease expired and
    was reclaimed by another process MUST be rejected via claim_token check."""
    first = await backend.claim("k8", "fp", 1, 0.05)
    await asyncio.sleep(0.1)
    second = await backend.claim("k8", "fp", 1, 30.0)
    assert second.result == ClaimResultType.LEASE_RECLAIMED

    # Original owner tries to complete; should be rejected
    ok = await backend.complete(
        "k8", first.our_claim_token, 200, {}, b"x", 3600.0,
    )
    assert ok is False

    # State is still our reclaim (CLAIMED with second's token), not COMPLETED
    re = await backend.claim("k8", "fp", 1, 30.0)
    assert re.result == ClaimResultType.ALREADY_CLAIMED
    assert re.record.claim_token == second.our_claim_token


async def test_concurrent_same_key_exactly_one_wins(backend: RedisBackend) -> None:
    """50 concurrent claims, same key, same fingerprint → exactly 1 NEW_CLAIMED."""
    async def attempt():
        return await backend.claim("k9", "fp", 1, 30.0)

    results = await asyncio.gather(*[attempt() for _ in range(50)])
    outcomes = Counter(r.result for r in results)
    assert outcomes[ClaimResultType.NEW_CLAIMED] == 1
    assert outcomes[ClaimResultType.ALREADY_CLAIMED] == 49


async def test_wait_for_completion_returns_completed_record(backend: RedisBackend) -> None:
    first = await backend.claim("k10", "fp", 1, 30.0)

    async def waiter():
        return await backend.wait_for_completion("k10", timeout_seconds=2.0)

    async def completer():
        await asyncio.sleep(0.1)
        await backend.complete(
            "k10", first.our_claim_token, 200, {}, b"hello", 3600.0,
        )

    waited, _ = await asyncio.gather(waiter(), completer())
    assert waited is not None
    assert waited.state == State.COMPLETED
    assert waited.response_body == b"hello"


async def test_wait_for_completion_times_out(backend: RedisBackend) -> None:
    await backend.claim("k11", "fp", 1, 30.0)
    result = await backend.wait_for_completion("k11", timeout_seconds=0.2)
    assert result is None


async def test_corrupt_record_treated_as_absent(backend: RedisBackend) -> None:
    """Spec §4.12: a stored value that fails to deserialize MUST allow re-execution."""
    # Directly inject garbage at the key
    await backend._client.set("k-corrupt", b"\xff\xfegarbage not json")

    # Claim should succeed as NEW (treating corrupt as ABSENT and overwriting)
    result = await backend.claim("k-corrupt", "fp", 1, 30.0)
    assert result.result == ClaimResultType.NEW_CLAIMED
    # ...and the backend signals the recovery so the engine can emit the event.
    assert result.recovered_from_corrupt is True


async def test_corrupt_record_emits_corrupt_record_event(backend: RedisBackend) -> None:
    """Spec §4.12 / §4.15: a corrupt record re-executes AND emits
    idempotency.corrupt_record (this is the conformance.yaml vector)."""
    from idemkit.core.config import IdempotencyConfig
    from idemkit.core.engine import IdempotencyEngine, NeutralRequest
    from idemkit.core.events import IdempotencyEvent
    from idemkit.core.fingerprint import compute_effective_key
    from idemkit.core.state import Decision

    events: list[IdempotencyEvent] = []
    engine = IdempotencyEngine(
        backend,
        IdempotencyConfig(scope_optional=True, event_handlers=[events.append]),
    )
    req = NeutralRequest(
        idempotency_key="K11",
        scope="_idemkit_anonymous",
        method="POST",
        path="/resources",
        query="",
        body=b'{"name": "x"}',
        content_type="application/json",
    )
    eff = compute_effective_key("K11", "_idemkit_anonymous", "POST", "/resources")
    await backend._client.set(eff, b"\xff\xfenot-json")

    outcome = await engine.decide(req)
    assert outcome.kind == "pass_through"  # handler re-executes
    assert Decision.CORRUPT_RECORD in [e.decision for e in events]
