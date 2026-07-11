"""End-to-end: a whole HTTP endpoint stays idempotent on real Redis and Postgres.

This is the double_charge example (examples/http/double_charge.py) wired to a real
backend. It fires 8 concurrent identical requests and asserts the handler runs
exactly once, the other 7 replay. If a change breaks the documented guarantee,
this fails.
"""

from __future__ import annotations

import asyncio
import uuid

import httpx
import pytest
from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from idemkit import IdempotencyMiddleware, PostgresBackend, RedisBackend
from idemkit.backends.postgres import init_pg

pytestmark = pytest.mark.e2e


def _make_app(backend, counter) -> Starlette:
    async def charge(request):
        await request.body()
        counter["n"] += 1  # the side effect: charge the card
        return JSONResponse({"charge": counter["n"]}, status_code=201)

    app = Starlette(routes=[Route("/charge", charge, methods=["POST"])])
    app.add_middleware(
        IdempotencyMiddleware, backend=backend, scope=lambda req: "customer-1"
    )
    return app


async def _fire_duplicates(app, key, n):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
        return await asyncio.gather(
            *[
                client.post(
                    "/charge", json={"amount": 10}, headers={"Idempotency-Key": key}
                )
                for _ in range(n)
            ]
        )


def _assert_once(counter, results):
    assert counter["n"] == 1  # handler ran exactly once
    charges = {r.json()["charge"] for r in results}
    assert charges == {1}  # everyone got the first result
    replayed = sum(1 for r in results if r.headers.get("idempotency-replayed") == "true")
    assert replayed == len(results) - 1


async def test_http_idempotent_on_real_redis(redis_url):
    counter = {"n": 0}
    async with RedisBackend.from_url(redis_url) as backend:
        app = _make_app(backend, counter)
        results = await _fire_duplicates(app, f"e2e-{uuid.uuid4()}", 8)
    _assert_once(counter, results)


async def test_http_idempotent_on_real_postgres(pg_url):
    await init_pg(pg_url)
    counter = {"n": 0}
    async with PostgresBackend.from_url(pg_url) as backend:
        app = _make_app(backend, counter)
        results = await _fire_duplicates(app, f"e2e-{uuid.uuid4()}", 8)
    _assert_once(counter, results)
