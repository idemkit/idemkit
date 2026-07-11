"""PostgreSQL backend tests.

These tests require a real PostgreSQL instance. They are skipped if no
``IDEMKIT_TEST_PG_URL`` environment variable is set, OR if the URL is set
but a connection cannot be established.

Local run (with Docker):
    docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=test postgres:16
    IDEMKIT_TEST_PG_URL=postgresql://postgres:test@localhost:5432/postgres \\
        pytest tests/test_postgres_backend.py -v
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections import Counter

import pytest

asyncpg = pytest.importorskip("asyncpg")

from idemkit.backends.postgres import (  # noqa: E402
    PostgresBackend,
    init_pg,
    pg_vacuum,
)
from idemkit.core.state import ClaimResultType, State  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")


async def _can_connect(url: str) -> bool:
    try:
        conn = await asyncpg.connect(url, timeout=2.0)
        await conn.close()
        return True
    except Exception:
        return False


pytestmark = pytest.mark.skipif(
    not PG_URL,
    reason="Set IDEMKIT_TEST_PG_URL to a Postgres URL to run these tests.",
)


@pytest.fixture(scope="module", autouse=True)
async def _setup_schema():
    if PG_URL is None:
        return
    if not await _can_connect(PG_URL):
        pytest.skip(f"cannot connect to {PG_URL}")
    await init_pg(PG_URL)


@pytest.fixture
async def backend():
    if PG_URL is None:
        pytest.skip("no PG URL")
    b = PostgresBackend.from_url(PG_URL, min_size=2, max_size=8)
    yield b
    await b.aclose()


def _k(suffix: str) -> str:
    """Generate a unique effective_key per test to avoid cross-test pollution."""
    return f"test-{uuid.uuid4().hex}-{suffix}"


async def test_new_claim(backend: PostgresBackend) -> None:
    key = _k("new")
    result = await backend.claim(key, "fp", 1, 30.0)
    assert result.result == ClaimResultType.NEW_CLAIMED
    assert result.our_claim_token is not None
    assert result.record.state == State.CLAIMED


async def test_second_claim_returns_already_claimed(backend: PostgresBackend) -> None:
    key = _k("dup")
    first = await backend.claim(key, "fp", 1, 30.0)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.ALREADY_CLAIMED
    assert second.record.claim_token == first.our_claim_token


async def test_complete_then_query(backend: PostgresBackend) -> None:
    key = _k("complete")
    first = await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.complete(
        key, first.our_claim_token, 201,
        {"content-type": "application/json"}, b'{"id":"x"}', 3600.0,
    )
    assert ok is True

    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.ALREADY_COMPLETED
    assert second.record.response_status == 201
    assert second.record.response_body == b'{"id":"x"}'
    assert second.record.response_headers["content-type"] == "application/json"


async def test_complete_with_wrong_token_rejected(backend: PostgresBackend) -> None:
    key = _k("wrong")
    await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.complete(key, "not-the-token", 200, {}, b"x", 3600.0)
    assert ok is False


async def test_release_with_correct_token(backend: PostgresBackend) -> None:
    key = _k("release")
    first = await backend.claim(key, "fp", 1, 30.0)
    released = await backend.release(key, first.our_claim_token)
    assert released is True

    after = await backend.claim(key, "fp", 1, 30.0)
    assert after.result == ClaimResultType.NEW_CLAIMED


async def test_expired_lease_is_reclaimed(backend: PostgresBackend) -> None:
    key = _k("reclaim")
    first = await backend.claim(key, "fp", 1, 0.1)
    await asyncio.sleep(0.2)
    second = await backend.claim(key, "fp", 1, 30.0)
    assert second.result == ClaimResultType.LEASE_RECLAIMED
    assert second.our_claim_token != first.our_claim_token


async def test_complete_after_reclaim_is_rejected(backend: PostgresBackend) -> None:
    key = _k("after-reclaim")
    first = await backend.claim(key, "fp", 1, 0.1)
    await asyncio.sleep(0.2)
    await backend.claim(key, "fp", 1, 30.0)
    ok = await backend.complete(
        key, first.our_claim_token, 200, {}, b"x", 3600.0,
    )
    assert ok is False


async def test_concurrent_same_key_exactly_one_wins(backend: PostgresBackend) -> None:
    key = _k("concurrent")

    async def attempt():
        return await backend.claim(key, "fp", 1, 30.0)

    results = await asyncio.gather(*[attempt() for _ in range(20)])
    outcomes = Counter(r.result for r in results)
    assert outcomes[ClaimResultType.NEW_CLAIMED] == 1
    assert outcomes[ClaimResultType.ALREADY_CLAIMED] == 19


async def test_wait_for_completion_returns_completed_record(backend: PostgresBackend) -> None:
    key = _k("wait")
    first = await backend.claim(key, "fp", 1, 30.0)

    async def waiter():
        return await backend.wait_for_completion(key, timeout_seconds=3.0)

    async def completer():
        await asyncio.sleep(0.2)
        await backend.complete(
            key, first.our_claim_token, 200, {}, b"hello", 3600.0,
        )

    waited, _ = await asyncio.gather(waiter(), completer())
    assert waited is not None
    assert waited.state == State.COMPLETED
    assert waited.response_body == b"hello"


async def test_wait_for_completion_times_out(backend: PostgresBackend) -> None:
    key = _k("wait-timeout")
    await backend.claim(key, "fp", 1, 30.0)
    result = await backend.wait_for_completion(key, timeout_seconds=0.3)
    assert result is None


async def test_corrupt_row_treated_as_absent(backend: PostgresBackend) -> None:
    """Spec §4.12: a row that fails to deserialize MUST be re-claimed fresh
    (recovered_from_corrupt), not turned into a 500. Postgres rows are normally
    structurally valid, so we simulate a deserialization failure once."""
    key = _k("corrupt")
    first = await backend.claim(key, "fp", 1, 30.0)
    # Far-future expiry so the expired-reclaim paths miss it and the claim
    # reaches the existing-row read where deserialization happens.
    await backend.complete(key, first.our_claim_token, 200, {}, b"x", 3600.0)

    real = backend._row_to_record
    calls = {"n": 0}

    def flaky(row):
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("simulated corrupt/drifted row")
        return real(row)

    backend._row_to_record = flaky  # type: ignore[method-assign]
    try:
        result = await backend.claim(key, "fp", 1, 30.0)
    finally:
        backend._row_to_record = real  # type: ignore[method-assign]

    assert result.result == ClaimResultType.NEW_CLAIMED
    assert result.recovered_from_corrupt is True


async def test_pg_vacuum_removes_expired_records(backend: PostgresBackend) -> None:
    """The vacuum CLI removes COMPLETED records past their TTL."""
    key = _k("vacuum")
    first = await backend.claim(key, "fp", 1, 30.0)
    await backend.complete(
        key, first.our_claim_token, 200, {}, b"x", 3600.0,
    )

    # Vacuum with a 0-second TTL deletes anything completed in the past
    assert PG_URL is not None
    n_removed = await pg_vacuum(PG_URL, expires_after_seconds=0.0)
    assert n_removed >= 1  # at least our record (other tests may have left some)

    # Re-claim should be NEW now
    again = await backend.claim(key, "fp", 1, 30.0)
    assert again.result == ClaimResultType.NEW_CLAIMED
