"""Clock-skew scenarios: prove lease ownership follows the storage clock.

idemkit compares a lease against the backend's own clock, never the app server's:
PostgreSQL `NOW()`, Redis `TIME`, or the clock injected into InMemoryBackend. So a
skewed app-server clock cannot cause a wrongful reclaim or a double execution. These
tests skew the app clock hard and confirm the lease is unmoved, then advance the
authoritative clock and confirm it does move (and fences the old owner).
"""

from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Any
from unittest import mock

import pytest

from idemkit import InMemoryBackend, ManualClock
from idemkit.core.state import ClaimResultType

FP = "fp"
FP_VERSION = 1


async def test_inmemory_lease_follows_injected_clock_not_wall_time() -> None:
    clock = ManualClock()
    backend = InMemoryBackend(clock=clock)
    key = "k"

    r1 = await backend.claim(key, FP, FP_VERSION, 10.0)
    assert r1.result is ClaimResultType.NEW_CLAIMED

    # A wild wall-clock jump (a skewed app clock) must not touch the lease: it
    # follows the injected clock, which has not moved.
    with mock.patch("time.time", return_value=1e12):
        r2 = await backend.claim(key, FP, FP_VERSION, 10.0)
    assert r2.result is ClaimResultType.ALREADY_CLAIMED

    # Move the authoritative (injected) clock past the lease: now it reclaims,
    # and the original owner's late completion is fenced out.
    clock.advance(11.0)
    r3 = await backend.claim(key, FP, FP_VERSION, 10.0)
    assert r3.result is ClaimResultType.LEASE_RECLAIMED
    assert r1.our_claim_token is not None
    assert await backend.complete(key, r1.our_claim_token, 200, {}, b"x", 100.0) is False


async def _assert_lease_ignores_client_clock_skew(backend: Any) -> None:
    key = f"skew-{uuid.uuid4().hex}"
    r1 = await backend.claim(key, FP, FP_VERSION, 100.0)
    assert r1.result is ClaimResultType.NEW_CLAIMED

    # Skew this client's clock a million seconds ahead. If the lease used the
    # client clock it would look long expired; it uses the server clock, so it is
    # still live and a re-claim is rejected.
    real = time.time
    with mock.patch("time.time", side_effect=lambda: real() + 1_000_000):
        r2 = await backend.claim(key, FP, FP_VERSION, 100.0)
    assert r2.result is ClaimResultType.ALREADY_CLAIMED, (
        "lease reclaimed under a skewed client clock; it must use the server clock"
    )
    await backend.release(key, r1.our_claim_token)


async def _assert_lease_expires_on_server_clock(backend: Any) -> None:
    key = f"exp-{uuid.uuid4().hex}"
    r1 = await backend.claim(key, FP, FP_VERSION, 0.3)
    assert r1.result is ClaimResultType.NEW_CLAIMED

    await asyncio.sleep(0.5)  # real time; the server clock advances with it

    r2 = await backend.claim(key, FP, FP_VERSION, 5.0)
    assert r2.result is ClaimResultType.LEASE_RECLAIMED
    # The first owner is fenced after the server-clock reclaim.
    assert r1.our_claim_token is not None
    assert await backend.complete(key, r1.our_claim_token, 200, {}, b"x", 100.0) is False
    await backend.release(key, r2.our_claim_token)


_REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")
_PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")


@pytest.mark.skipif(not _REDIS_URL, reason="needs IDEMKIT_TEST_REDIS_URL")
async def test_redis_lease_ignores_client_clock_skew() -> None:
    from idemkit import RedisBackend

    assert _REDIS_URL is not None
    async with RedisBackend.from_url(_REDIS_URL) as backend:
        await _assert_lease_ignores_client_clock_skew(backend)


@pytest.mark.skipif(not _REDIS_URL, reason="needs IDEMKIT_TEST_REDIS_URL")
async def test_redis_lease_expires_on_server_clock() -> None:
    from idemkit import RedisBackend

    assert _REDIS_URL is not None
    async with RedisBackend.from_url(_REDIS_URL) as backend:
        await _assert_lease_expires_on_server_clock(backend)


@pytest.mark.skipif(not _PG_URL, reason="needs IDEMKIT_TEST_PG_URL")
async def test_postgres_lease_ignores_client_clock_skew() -> None:
    from idemkit.backends.postgres import PostgresBackend, init_pg

    assert _PG_URL is not None
    await init_pg(_PG_URL)
    async with PostgresBackend.from_url(_PG_URL) as backend:
        await _assert_lease_ignores_client_clock_skew(backend)


@pytest.mark.skipif(not _PG_URL, reason="needs IDEMKIT_TEST_PG_URL")
async def test_postgres_lease_expires_on_server_clock() -> None:
    from idemkit.backends.postgres import PostgresBackend, init_pg

    assert _PG_URL is not None
    await init_pg(_PG_URL)
    async with PostgresBackend.from_url(_PG_URL) as backend:
        await _assert_lease_expires_on_server_clock(backend)
