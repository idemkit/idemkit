"""End-to-end ASGI middleware tests against real backends.

The 12 tests in test_asgi_middleware.py exercise the middleware with
``InMemoryBackend``. These tests re-run the most critical behaviors
against ``RedisBackend`` (via fakeredis) and ``PostgresBackend`` (via
Docker) so we know the full stack works with the production backends —
not just the engine-backend layer in isolation.
"""

from __future__ import annotations

import asyncio
import os
import uuid

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")

from idemkit import IdempotencyMiddleware  # noqa: E402
from idemkit.backends.postgres import PostgresBackend, init_pg  # noqa: E402
from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.core.config import IdempotencyConfig  # noqa: E402

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


@pytest.fixture(params=["redis", "postgres"])
async def backend(request):
    if request.param == "redis":
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
            pytest.skip("set IDEMKIT_TEST_PG_URL to enable PG tests")
        b = PostgresBackend.from_url(PG_URL, min_size=2, max_size=8)
        try:
            yield b
        finally:
            await b.aclose()


def _build_app(backend, handler, **config_kwargs) -> Starlette:
    # Use a per-app-unique tenant so different test instantiations don't collide
    tenant = f"test-{uuid.uuid4().hex[:8]}"
    config = IdempotencyConfig(
        scope=lambda req: tenant,
        **config_kwargs,
    )
    app = Starlette(routes=[Route("/echo", handler, methods=["POST", "GET"])])
    app.add_middleware(IdempotencyMiddleware, backend=backend, config=config)
    return app


@pytest.fixture
def call_counter():
    return {"n": 0}


def _key(suffix: str) -> str:
    return f"asgi-{suffix}-{uuid.uuid4().hex}"


async def test_duplicate_request_replays_against_real_backend(backend, call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse({"call": call_counter["n"], "body": body}, status_code=201)

    app = _build_app(backend, handler)
    key = _key("dup")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={"x": 1}, headers={"Idempotency-Key": key})
        r2 = await client.post("/echo", json={"x": 1}, headers={"Idempotency-Key": key})

    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json() == r2.json()
    assert r2.headers.get("idempotency-replayed") == "true"
    assert call_counter["n"] == 1


async def test_concurrent_duplicates_against_real_backend(backend, call_counter) -> None:
    """The race-condition guarantee end-to-end over HTTP through a real backend."""

    async def handler(request: Request) -> Response:
        await asyncio.sleep(0.1)
        call_counter["n"] += 1
        return JSONResponse({"call": call_counter["n"]}, status_code=200)

    app = _build_app(backend, handler)
    key = _key("concurrent")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        results = await asyncio.gather(
            *[
                client.post("/echo", json={"x": 1}, headers={"Idempotency-Key": key})
                for _ in range(8)
            ]
        )

    assert all(r.status_code == 200 for r in results)
    assert call_counter["n"] == 1, "handler ran more than once under concurrent load"
    # All bodies identical
    assert len({r.text for r in results}) == 1


async def test_payload_mismatch_against_real_backend(backend, call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse(body, status_code=200)

    app = _build_app(backend, handler)
    key = _key("mismatch")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={"amount": 100}, headers={"Idempotency-Key": key})
        r2 = await client.post("/echo", json={"amount": 200}, headers={"Idempotency-Key": key})

    assert r1.status_code == 200
    assert r2.status_code == 422
    assert b"urn:idemkit:payload-mismatch" in r2.content
    assert call_counter["n"] == 1  # second never executed


async def test_5xx_not_cached_against_real_backend(backend, call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"err": "boom"}, status_code=500)

    app = _build_app(backend, handler)
    key = _key("5xx")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": key})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": key})

    assert r1.status_code == 500
    assert r2.status_code == 500
    assert call_counter["n"] == 2  # retry path open


async def test_stripe_compat_uses_stripe_header_against_real_backend(backend, call_counter) -> None:
    """Wire-level Stripe compatibility verified through Redis/PG persistence."""

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"ok": True}, status_code=200)

    app = _build_app(backend, handler, compat_mode="stripe")
    key = _key("stripe")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={}, headers={"Idempotency-Key": key})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": key})

    assert r2.headers.get("idempotent-replayed") == "true"  # Stripe spelling
    assert r2.headers.get("idempotency-replayed") is None
    assert call_counter["n"] == 1
