"""Surface-neutral core (``IdempotentCore``) proven against every backend.

This is the Phase 1 deliverable made concrete: the SAME InMemory / Redis /
Postgres backends that drive the HTTP middleware also drive a non-HTTP
``run_once`` loop — a side-effect counter that must fire exactly once per key
under duplicates, concurrency, and a simulated crash. No HTTP types appear here;
the result is opaque ``StoredResult`` bytes.

Mirrors ``test_backend_contract.py``'s cross-backend shape so the core's
correctness is demonstrated uniformly, not assumed.
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
from idemkit.core.events import EventEmitter  # noqa: E402
from idemkit.core.exceptions import StorageError  # noqa: E402
from idemkit.core.runner import (  # noqa: E402
    CoreOutcome,
    IdempotentCore,
    RunStatus,
    StoredResult,
)

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture(scope="module", autouse=True)
async def _pg_schema():
    if PG_URL:
        try:
            await init_pg(PG_URL)
        except Exception:
            pass
    yield


@pytest.fixture(params=["memory", "redis", "postgres"])
async def backend(request):
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


def _core(backend, **overrides) -> IdempotentCore:
    return IdempotentCore(
        backend,
        emitter=EventEmitter(),
        backend_name=type(backend).__name__,
        surface="queue",  # any non-http tag; proves the surface field flows
        **overrides,
    )


def _k(suffix: str) -> str:
    return f"core-{suffix}-{uuid.uuid4().hex}"


# ----- run_once: execute once, replay forever -----


async def test_run_once_executes_then_replays(backend) -> None:
    core = _core(backend)
    key = _k("replay")
    calls = 0

    async def handler() -> StoredResult:
        nonlocal calls
        calls += 1
        return StoredResult(blob=b"result-bytes")

    first = await core.run_once(key, "fp", handler)
    assert first.status is RunStatus.EXECUTED
    assert first.stored is not None and first.stored.blob == b"result-bytes"

    second = await core.run_once(key, "fp", handler)
    assert second.status is RunStatus.REPLAYED
    assert second.stored is not None and second.stored.blob == b"result-bytes"

    assert calls == 1  # the side effect fired exactly once


async def test_side_effect_only_handler_skips_on_replay(backend) -> None:
    """A handler that returns None (no payload) still records completion, so a
    redelivery is skipped rather than re-run — the queue side-effect-only case."""
    core = _core(backend)
    key = _k("sideonly")
    fired = 0

    async def handler() -> None:
        nonlocal fired
        fired += 1

    first = await core.run_once(key, "fp", handler)
    assert first.status is RunStatus.EXECUTED

    second = await core.run_once(key, "fp", handler)
    assert second.status is RunStatus.REPLAYED

    assert fired == 1


async def test_handler_exception_releases_for_retry(backend) -> None:
    """A raised exception releases the claim so a later attempt re-runs once."""
    core = _core(backend)
    key = _k("crash")
    attempts = 0

    async def flaky() -> StoredResult:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient failure")
        return StoredResult(blob=b"ok")

    with pytest.raises(RuntimeError):
        await core.run_once(key, "fp", flaky)

    # The claim was released, so the retry gets a fresh execution, not a conflict.
    retry = await core.run_once(key, "fp", flaky)
    assert retry.status is RunStatus.EXECUTED
    assert retry.stored is not None and retry.stored.blob == b"ok"
    assert attempts == 2


async def test_non_cacheable_result_releases(backend) -> None:
    """When ``is_cacheable`` rejects the result, the claim is released so the
    next call re-runs (the §5.6 'failure / non-cacheable' edge of the machine)."""
    core = _core(backend)
    key = _k("noncacheable")
    calls = 0

    async def handler() -> StoredResult:
        nonlocal calls
        calls += 1
        return StoredResult(blob=b"transient", marker=500)

    first = await core.run_once(key, "fp", handler, is_cacheable=lambda r: False)
    assert first.status is RunStatus.EXECUTED

    second = await core.run_once(key, "fp", handler, is_cacheable=lambda r: False)
    assert second.status is RunStatus.EXECUTED  # not replayed — was not stored
    assert calls == 2


# ----- concurrency: N at once -> exactly one execution -----


async def test_concurrent_run_once_executes_exactly_once(backend) -> None:
    core = _core(backend, wait_timeout_seconds=3.0)
    key = _k("concurrent")
    parallel = 20
    calls = 0

    async def handler() -> StoredResult:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)  # hold the claim so duplicates pile into the wait
        return StoredResult(blob=b"the-one-result")

    results = await asyncio.gather(
        *(core.run_once(key, "fp", handler) for _ in range(parallel))
    )
    statuses = Counter(r.status for r in results)

    assert calls == 1, f"handler must run exactly once; ran {calls}x"
    assert statuses[RunStatus.EXECUTED] == 1
    # Every duplicate either waited and replayed, or (if it timed out) conflicted;
    # none ran the handler a second time.
    assert statuses[RunStatus.EXECUTED] + statuses[RunStatus.REPLAYED] + statuses[
        RunStatus.CONFLICT
    ] == parallel
    for r in results:
        if r.status is RunStatus.REPLAYED:
            assert r.stored is not None and r.stored.blob == b"the-one-result"


# ----- decide/complete/release primitives directly -----


async def test_decide_proceed_then_complete(backend) -> None:
    core = _core(backend)
    key = _k("decide")
    d = await core.decide(key, "fp")
    assert d.outcome is CoreOutcome.PROCEED
    assert d.claim_token is not None

    await core.complete(key, d.claim_token, StoredResult(blob=b"stored", marker=200))

    again = await core.decide(key, "fp")
    assert again.outcome is CoreOutcome.REPLAY
    assert again.stored is not None and again.stored.blob == b"stored"


async def test_decide_mismatch_on_different_fingerprint(backend) -> None:
    core = _core(backend)
    key = _k("mismatch")
    d = await core.decide(key, "fp-A")
    assert d.claim_token is not None
    await core.complete(key, d.claim_token, StoredResult(blob=b"a"))

    other = await core.decide(key, "fp-B")
    assert other.outcome is CoreOutcome.MISMATCH
    assert other.stored is None


# ----- lease renewal / heartbeat (§5.3.1) -----


async def test_heartbeat_renews_lease_during_long_handler(backend) -> None:
    """A handler that runs several lease-lengths long keeps its claim because the
    heartbeat renews it; without renewal the short lease would lapse mid-flight."""
    core = _core(backend, lease_ttl_seconds=0.3)
    key = _k("hb-renew")

    renew_calls = 0
    real_renew = backend.renew

    async def counting_renew(*args, **kwargs):
        nonlocal renew_calls
        renew_calls += 1
        return await real_renew(*args, **kwargs)

    backend.renew = counting_renew  # type: ignore[method-assign]

    async def slow_handler() -> StoredResult:
        await asyncio.sleep(0.75)  # ~2.5x the lease
        return StoredResult(blob=b"slow-result")

    result = await core.run_once(
        key, "fp", slow_handler, heartbeat=True, heartbeat_interval_seconds=0.1
    )
    assert result.status is RunStatus.EXECUTED
    assert result.stored is not None and result.stored.blob == b"slow-result"
    assert renew_calls >= 2, f"expected the heartbeat to renew; got {renew_calls} calls"

    # The result is stored and replays on a duplicate.
    again = await core.run_once(key, "fp", slow_handler, heartbeat=True)
    assert again.status is RunStatus.REPLAYED


async def test_heartbeat_lease_lost_cancels_handler(backend) -> None:
    """When renewal can't be confirmed, the handler is cancelled cooperatively
    and the operation reports LEASE_LOST — it did NOT finish (spec §5.3.1)."""
    core = _core(backend, lease_ttl_seconds=0.3)
    key = _k("hb-lost")
    finished = False

    async def failing_renew(*args, **kwargs):
        return False  # simulate a renewal that can't confirm the lease

    backend.renew = failing_renew  # type: ignore[method-assign]

    async def handler() -> StoredResult:
        nonlocal finished
        await asyncio.sleep(2.0)  # awaits regularly, so cancellation lands here
        finished = True
        return StoredResult(blob=b"should-not-store")

    result = await core.run_once(
        key, "fp", handler, heartbeat=True, heartbeat_interval_seconds=0.1
    )
    assert result.status is RunStatus.LEASE_LOST
    assert result.stored is None
    assert finished is False, "the handler must have been cancelled before finishing"


# ----- local warm-path cache (§5.9) -----


async def test_local_cache_skips_backend_on_warm_replay(backend) -> None:
    """A COMPLETED replay this process already served is returned without another
    backend claim (spec §5.9). An in-progress claim is never cached."""
    claims = 0
    real_claim = backend.claim

    async def counting_claim(*args, **kwargs):
        nonlocal claims
        claims += 1
        return await real_claim(*args, **kwargs)

    backend.claim = counting_claim  # type: ignore[method-assign]
    core = _core(backend, use_local_cache=True)
    key = _k("lru")

    decision = await core.decide(key, "fp")  # claim #1 (in-progress, NOT cached)
    assert decision.claim_token is not None
    await core.complete(key, decision.claim_token, StoredResult(blob=b"v"))

    first = await core.decide(key, "fp")  # claim #2: reads COMPLETED, populates cache
    assert first.outcome is CoreOutcome.REPLAY
    claims_after_first = claims

    second = await core.decide(key, "fp")  # served from the local cache
    assert second.outcome is CoreOutcome.REPLAY
    assert second.stored is not None and second.stored.blob == b"v"
    assert claims == claims_after_first, "warm replay must not hit the backend again"

    # A different fingerprint must NOT be served from cache; it falls through to
    # the backend, which reports the mismatch.
    other = await core.decide(key, "fp-DIFFERENT")
    assert other.outcome is CoreOutcome.MISMATCH
    assert claims == claims_after_first + 1


async def test_response_hook_post_processes_replay(backend) -> None:
    """The response_hook rewrites a result on the way out of a replay (§5.8)."""

    def hook(stored: StoredResult) -> StoredResult:
        return StoredResult(
            blob=stored.blob + b"-hooked", meta=stored.meta, marker=stored.marker
        )

    core = _core(backend, response_hook=hook)
    key = _k("hook")
    decision = await core.decide(key, "fp")
    assert decision.claim_token is not None
    await core.complete(key, decision.claim_token, StoredResult(blob=b"v"))

    replay = await core.decide(key, "fp")
    assert replay.outcome is CoreOutcome.REPLAY
    assert replay.stored is not None and replay.stored.blob == b"v-hooked"


# ----- fail-closed on a completion write failure -----


async def test_complete_write_failure_holds_claim(backend) -> None:
    """If the backend write fails at completion, the claim is HELD (not released),
    so a duplicate sees the in-flight claim and conflicts rather than getting a
    fresh claim and re-running the side effect (§5.4/§5.7 fail closed)."""
    core = _core(backend, wait_timeout_seconds=0.3)
    key = _k("complete-fail")
    decision = await core.decide(key, "fp")
    assert decision.claim_token is not None

    real_complete = backend.complete

    async def boom(*args, **kwargs):
        raise StorageError("simulated write failure at completion")

    backend.complete = boom  # type: ignore[method-assign]
    # complete() swallows the error and does NOT release.
    await core.complete(key, decision.claim_token, StoredResult(blob=b"v"))
    backend.complete = real_complete  # type: ignore[method-assign]

    # The claim is still held → a duplicate conflicts; it does NOT get PROCEED
    # (which would re-run the side effect). The OLD release-on-failure behavior
    # would have returned PROCEED here.
    again = await core.decide(key, "fp")
    assert again.outcome is CoreOutcome.CONFLICT
    assert again.claim_token is None


async def test_local_cache_bounded_by_max_items(backend) -> None:
    """The in-process LRU cache never grows past local_cache_max_items.

    The local cache is populated on a replay (not on first execution), so each key
    is run twice: once to execute, once to replay and cache it.
    """
    core = _core(backend, use_local_cache=True, local_cache_max_items=2)

    for i in range(4):
        key = _k(f"lru-{i}")

        async def handler(v: int = i) -> StoredResult:
            return StoredResult(blob=str(v).encode())

        await core.run_once(key, "fp", handler)   # execute
        await core.run_once(key, "fp", handler)   # replay -> caches locally

    # Four distinct keys cached, cap is 2 → the two oldest were evicted.
    assert len(core._local_cache) == 2
