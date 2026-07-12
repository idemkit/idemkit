"""The FastAPI route class dedupes, replays serialized dict responses, and scopes."""

from __future__ import annotations

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("httpx")

import httpx
from fastapi import APIRouter, FastAPI, Request

from idemkit import HttpConfig, InMemoryBackend
from idemkit.contrib.fastapi import idempotent_route

JSON = {"Content-Type": "application/json"}


def _make_app():
    calls = {"n": 0}
    router = APIRouter(
        route_class=idempotent_route(
            backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: req.headers["x-user"])
        )
    )

    @router.post("/charge")
    async def charge(request: Request) -> dict:  # returns a dict, not a Response
        calls["n"] += 1
        return {"charged": True, "n": calls["n"]}

    app = FastAPI()
    app.include_router(router)
    return app, calls


async def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


async def test_dict_return_dedupes_and_replays() -> None:
    """A handler returning a dict (the idiomatic FastAPI shape) is captured after
    FastAPI serializes it, so a duplicate replays the serialized body and the
    handler runs once."""
    app, calls = _make_app()
    headers = {"Idempotency-Key": "k1", "x-user": "u1", **JSON}
    async with await _client(app) as c:
        r1 = await c.post("/charge", headers=headers, content="{}")
        r2 = await c.post("/charge", headers=headers, content="{}")
    assert r1.status_code == 200
    assert r1.json() == {"charged": True, "n": 1}
    assert r2.json() == {"charged": True, "n": 1}  # replayed
    assert r2.headers.get("idempotency-replayed") == "true"
    assert calls["n"] == 1


async def test_scope_isolates_tenants() -> None:
    """scope reads the real Request, so the same key under two users does not
    collide (each runs)."""
    app, calls = _make_app()
    async with await _client(app) as c:
        r1 = await c.post(
            "/charge", headers={"Idempotency-Key": "same", "x-user": "u1", **JSON}, content="{}"
        )
        r2 = await c.post(
            "/charge", headers={"Idempotency-Key": "same", "x-user": "u2", **JSON}, content="{}"
        )
    assert r1.json()["n"] == 1
    assert r2.json()["n"] == 2  # different tenant, same key -> not deduped
    assert calls["n"] == 2


async def test_no_key_passes_through() -> None:
    """A request without an Idempotency-Key is not deduped (runs every time)."""
    app, calls = _make_app()
    async with await _client(app) as c:
        await c.post("/charge", headers={"x-user": "u1", **JSON}, content="{}")
        await c.post("/charge", headers={"x-user": "u1", **JSON}, content="{}")
    assert calls["n"] == 2
