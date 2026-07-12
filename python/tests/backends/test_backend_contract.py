"""Cross-backend contract tests — proof of uniform behavior.

The same behavioral assertions run against every supported backend:
``InMemoryBackend``, ``RedisBackend`` (via fakeredis), and
``PostgresBackend`` (via real PostgreSQL when ``IDEMKIT_TEST_PG_URL`` is set).

If a behavior passes here for all three backends, the spec is satisfied
uniformly — which is the entire promise of the conformance vector file
(``spec/conformance.yaml``) made concrete.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import Counter

import pytest

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")

from idemkit.backends.memory import InMemoryBackend  # noqa: E402
from idemkit.backends.postgres import PostgresBackend, init_pg  # noqa: E402
from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.core.state import ClaimResultType, State  # noqa: E402
from tests._backends import EXTRA_BACKENDS, make_dynamo_backend, make_mongo_backend  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture(scope="module", autouse=True)
async def _pg_schema():
    """Ensure idemkit_records exists if PG is configured."""
    if PG_URL:
        try:
            await init_pg(PG_URL)
        except Exception:
            pass
    return


@pytest.fixture(params=["memory", "redis", "postgres", *EXTRA_BACKENDS])
async def backend(request):
    """Yield a backend instance for every supported implementation."""
    if request.param == "memory":
        yield InMemoryBackend()
    elif request.param == "redis":
        if REDIS_URL:
            import redis.asyncio as aioredis

            client = aioredis.from_url(REDIS_URL, decode_responses=False)
            await client.flushdb()
        else:
            client = fakeredis_aio.FakeRedis(decode_responses=False)
        b = RedisBackend(client)
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "postgres":
        if not PG_URL:
            pytest.skip("set IDEMKIT_TEST_PG_URL to enable PostgreSQL contract tests")
        b = PostgresBackend.from_url(PG_URL, min_size=2, max_size=8)
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "mongo":
        b = make_mongo_backend()
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "dynamodb":
        b = make_dynamo_backend()
        try:
            yield b
        finally:
            await b.aclose()


def _k(suffix: str) -> str:
    """Unique key per test to avoid cross-test pollution on shared PG."""
    return f"contract-{suffix}-{uuid.uuid4().hex}"


# ----- The contract -----


async def test_new_claim(backend) -> None:
    key = _k("new")
    result = await backend.claim(key, "fp", 1, 30.0)
    assert result.result == ClaimResultType.NEW_CLAIMED
    assert result.our_claim_token is not None
    assert result.record.state == State.CLAIMED
    assert result.record.fingerprint == "fp"
    assert result.record.fingerprint_version == 1


async def test_duplicate_claim_returns_already_claimed(backend) -> None:
    key = _k("dup")
    first = await backend.claim(key, "fp", 1, 30.0)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.ALREADY_CLAIMED
    assert second.our_claim_token is None
    assert second.record.claim_token == first.our_claim_token


async def test_complete_returns_true_and_transitions_to_completed(backend) -> None:
    key = _k("complete")
    first = await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.complete(
        key,
        first.our_claim_token,
        201,
        {"content-type": "application/json"},
        b'{"id":"abc"}',
        3600.0,
    )
    assert ok is True

    after = await backend.claim(key, "fp", 1, 30.0)
    assert after.result == ClaimResultType.ALREADY_COMPLETED
    assert after.record.state == State.COMPLETED
    assert after.record.response_status == 201
    assert after.record.response_body == b'{"id":"abc"}'
    assert after.record.response_headers["content-type"] == "application/json"


async def test_complete_with_wrong_token_returns_false(backend) -> None:
    """Spec §4.1: delayed completion from a reclaimed owner MUST be rejected."""
    key = _k("wrong-token")
    await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.complete(
        key,
        "not-the-token",
        200,
        {},
        b"x",
        3600.0,
    )
    assert ok is False

    # State unchanged
    after = await backend.claim(key, "fp", 1, 30.0)
    assert after.result == ClaimResultType.ALREADY_CLAIMED


async def test_release_with_correct_token_clears_claim(backend) -> None:
    key = _k("release")
    first = await backend.claim(key, "fp", 1, 30.0)
    released = await backend.release(key, first.our_claim_token)
    assert released is True

    after = await backend.claim(key, "fp", 1, 30.0)
    assert after.result == ClaimResultType.NEW_CLAIMED


async def test_release_with_wrong_token_returns_false(backend) -> None:
    key = _k("release-wrong")
    await backend.claim(key, "fp", 1, 30.0)
    released = await backend.release(key, "not-the-token")
    assert released is False


async def test_expired_lease_is_reclaimed(backend) -> None:
    """Spec §4.4: an expired lease enables observable reclaim by another caller."""
    key = _k("expired")
    first = await backend.claim(key, "fp", 1, 0.1)
    await asyncio.sleep(0.25)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.LEASE_RECLAIMED
    assert second.our_claim_token is not None
    assert second.our_claim_token != first.our_claim_token


async def test_complete_after_reclaim_is_rejected(backend) -> None:
    """The reclaim-rejection-via-claim-token guarantee, end to end."""
    key = _k("after-reclaim")
    first = await backend.claim(key, "fp", 1, 0.1)
    await asyncio.sleep(0.25)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.LEASE_RECLAIMED

    ok = await backend.complete(
        key,
        first.our_claim_token,
        200,
        {},
        b"x",
        3600.0,
    )
    assert ok is False, "delayed complete from original owner MUST be rejected"

    after = await backend.claim(key, "fp", 1, 30.0)
    assert after.result == ClaimResultType.ALREADY_CLAIMED
    assert after.record.claim_token == second.our_claim_token


async def test_concurrent_claims_exactly_one_wins(backend) -> None:
    """The headline race-safety guarantee, against every backend."""
    key = _k("concurrent")
    parallel = 30

    async def attempt():
        return await backend.claim(key, "fp", 1, 30.0)

    results = await asyncio.gather(*(attempt() for _ in range(parallel)))
    outcomes = Counter(r.result for r in results)
    assert outcomes[ClaimResultType.NEW_CLAIMED] == 1, (
        f"expected exactly 1 NEW_CLAIMED; got {dict(outcomes)}"
    )
    assert outcomes[ClaimResultType.ALREADY_CLAIMED] == parallel - 1


async def test_wait_for_completion_returns_completed_record(backend) -> None:
    """Spec §4.3: subscribe-before-read wait pattern delivers the completed record."""
    key = _k("wait")
    first = await backend.claim(key, "fp", 1, 30.0)

    async def waiter():
        return await backend.wait_for_completion(key, timeout_seconds=2.0)

    async def completer():
        await asyncio.sleep(0.1)
        await backend.complete(
            key,
            first.our_claim_token,
            200,
            {},
            b"hello",
            3600.0,
        )

    waited, _ = await asyncio.gather(waiter(), completer())
    assert waited is not None
    assert waited.state == State.COMPLETED
    assert waited.response_body == b"hello"


async def test_wait_for_completion_times_out(backend) -> None:
    """Wait returns None when the in-flight claim does not complete in time."""
    key = _k("wait-timeout")
    await backend.claim(key, "fp", 1, 30.0)
    result = await backend.wait_for_completion(key, timeout_seconds=0.3)
    assert result is None


async def test_wait_finds_completion_via_poll_when_notification_lost(backend) -> None:
    """Spec §5.5: the poll is the correctness floor. Even with the completion
    notification suppressed (a dropped pub/sub message / failed LISTEN), the
    waiter MUST still find the COMPLETED record via its backed-off re-read,
    rather than hang for the full timeout."""
    name = type(backend).__name__
    if name == "InMemoryBackend":
        backend._signal_waiters_unlocked = lambda key: None  # type: ignore[method-assign]
    elif name == "RedisBackend":
        backend._signal = lambda key: None  # type: ignore[method-assign]
    else:
        pytest.skip("notification suppression isn't cleanly injectable here")

    key = _k("poll")
    first = await backend.claim(key, "fp", 1, 30.0)

    async def completer() -> None:
        await asyncio.sleep(0.1)
        await backend.complete(key, first.our_claim_token, 200, {}, b"polled", 3600.0)

    async def waiter():
        return await backend.wait_for_completion(key, timeout_seconds=2.0)

    waited, _ = await asyncio.gather(waiter(), completer())
    assert waited is not None, "polling must find completion with no notification"
    assert waited.response_body == b"polled"


async def test_renew_extends_a_live_lease(backend) -> None:
    """Spec §5.3.1: renewing a held claim pushes its lease out, so a handler
    that outlives the original lease keeps its claim instead of being reclaimed."""
    key = _k("renew-extends")
    first = await backend.claim(key, "fp", 1, 0.2)  # short lease

    ok = await backend.renew(key, first.our_claim_token, 30.0)  # extend it
    assert ok is True

    # Past the ORIGINAL lease but well within the renewed one.
    await asyncio.sleep(0.4)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.ALREADY_CLAIMED, (
        "renewed lease must still be held; reclaim would mean renew didn't extend"
    )
    assert second.record.claim_token == first.our_claim_token


async def test_renew_with_wrong_token_returns_false(backend) -> None:
    """Fencing: only the lease's owner can renew it (spec §5.3.1)."""
    key = _k("renew-wrong")
    await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.renew(key, "not-the-token", 30.0)
    assert ok is False


async def test_renew_after_completion_returns_false(backend) -> None:
    """A COMPLETED record has no lease to renew."""
    key = _k("renew-completed")
    first = await backend.claim(key, "fp", 1, 30.0)
    await backend.complete(key, first.our_claim_token, 200, {}, b"x", 3600.0)
    ok = await backend.renew(key, first.our_claim_token, 30.0)
    assert ok is False


async def test_renew_after_reclaim_is_fenced(backend) -> None:
    """If our lease lapsed and another caller reclaimed, our renew is rejected —
    the heartbeat's signal to stop (spec §5.3.1)."""
    key = _k("renew-reclaimed")
    first = await backend.claim(key, "fp", 1, 0.1)
    await asyncio.sleep(0.25)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.LEASE_RECLAIMED

    # The original owner's heartbeat fires after the reclaim: must fail.
    ok = await backend.renew(key, first.our_claim_token, 30.0)
    assert ok is False
    # The new owner can still renew.
    ok2 = await backend.renew(key, second.our_claim_token, 30.0)
    assert ok2 is True


async def test_completed_record_expires_on_read_uniformly(backend) -> None:
    """Spec §4.8: a COMPLETED record past its completed_ttl is treated as ABSENT
    on the next claim — on EVERY backend (memory GC, Redis native TTL, Postgres
    lease_until-on-read), not only when an external vacuum runs. This is the
    cross-backend uniformity the conformance promise rests on."""
    key = _k("completed-ttl")
    first = await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.complete(
        key,
        first.our_claim_token,
        200,
        {},
        b"x",
        0.1,  # 100 ms completed_ttl
    )
    assert ok is True

    # Immediately after completion it replays.
    immediate = await backend.claim(key, "fp", 1, 30.0)
    assert immediate.result == ClaimResultType.ALREADY_COMPLETED

    # After the completed_ttl elapses, the record is gone → fresh execution.
    await asyncio.sleep(0.4)
    after = await backend.claim(key, "fp", 1, 30.0)
    assert after.result == ClaimResultType.NEW_CLAIMED
    assert after.our_claim_token is not None


async def test_async_context_manager_yields_self_and_closes(backend) -> None:
    """Every backend supports `async with` (review A-1/A-2): __aenter__ returns
    the backend so it stays usable inside the block, and __aexit__ closes it so a
    script/test doesn't leak the Redis pool / PG listener (the "Event loop is
    closed" teardown traceback)."""
    async with backend as b:
        assert b is backend
        key = _k("ctx-mgr")
        claim = await b.claim(key, "fp", 1, 30.0)
        assert claim.result == ClaimResultType.NEW_CLAIMED
    # aclose is idempotent: the fixture's own teardown will call it again.
