"""The standalone conformance suite, run against idemkit's own three backends.

This is the §11.1 artifact validating itself: the same ``BackendConformance``
runner a third-party library would point at its own backend, here pointed at
InMemory, Redis, and PostgreSQL. Every vector must pass on every backend.
"""

from __future__ import annotations

import io
import os
from contextlib import redirect_stdout

import pytest

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")

from idemkit.backends.memory import InMemoryBackend  # noqa: E402
from idemkit.backends.postgres import PostgresBackend, init_pg  # noqa: E402
from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.conformance import BackendConformance  # noqa: E402
from tests._backends import EXTRA_BACKENDS, make_dynamo_backend, make_mongo_backend  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture(scope="module", autouse=True)
async def _pg_schema():
    if PG_URL:
        try:
            await init_pg(PG_URL)
        except Exception:
            pass
    return


@pytest.fixture(params=["memory", "redis", "postgres", *EXTRA_BACKENDS])
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


async def test_backend_passes_conformance(backend) -> None:
    report = await BackendConformance(backend).run()
    assert report.passed, report.report()
    # Sanity: the suite actually ran a meaningful number of vectors.
    assert report.total >= 13


async def test_report_surfaces_failures() -> None:
    """A backend that violates the contract is reported as failing, not crashed —
    so the runner is usable as a real gate."""

    class BrokenBackend(InMemoryBackend):
        async def complete(self, *args, **kwargs) -> bool:
            # Always claim success even on a wrong token: breaks fencing.
            await super().complete(*args, **kwargs)
            return True

    report = await BackendConformance(BrokenBackend()).run()
    assert not report.passed
    failed_ids = {r.vector_id for r in report.failures}
    assert "fencing-wrong-token" in failed_ids


def test_conformance_cli_runs_against_memory() -> None:
    """`idemkit conformance` (no URLs) validates the in-memory backend and exits
    0. Sync test: cli.main() calls asyncio.run() internally."""
    from idemkit.cli import main

    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = main(["conformance"])
    output = buf.getvalue()
    assert rc == 0
    assert "PASSED" in output
    assert "InMemoryBackend" in output
