"""Spec §4.10 header allow/deny tests.

These exercise which headers survive into the stored replay vs which are
dropped. Only the replay path is testable end-to-end; the first request
streams the handler's response verbatim.
"""

from __future__ import annotations

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from idemkit import IdempotencyMiddleware, InMemoryBackend
from idemkit.core.config import IdempotencyConfig


def _build_app(handler, **config_kwargs) -> Starlette:
    config = IdempotencyConfig(scope=lambda req: "test-user", **config_kwargs)
    app = Starlette(routes=[Route("/echo", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)
    return app


async def test_authorization_header_not_stored() -> None:
    async def handler(request: Request) -> Response:
        return JSONResponse(
            {"ok": True},
            status_code=200,
            headers={"authorization": "Bearer secret-token"},
        )

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-auth"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-auth"})

    # First response saw the Authorization header (passthrough)
    assert r1.headers.get("authorization") == "Bearer secret-token"
    # Replay strips it
    assert r2.headers.get("authorization") is None
    assert r2.headers["idempotency-replayed"] == "true"


async def test_vary_header_not_stored() -> None:
    """idemkit is not Vary-aware on replay (spec §4.10), so Vary is not persisted."""

    async def handler(request: Request) -> Response:
        return JSONResponse(
            {"ok": True},
            status_code=200,
            headers={"vary": "Accept-Encoding"},
        )

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={}, headers={"Idempotency-Key": "k-vary"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-vary"})

    assert r2.headers.get("vary") is None


async def test_etag_and_location_are_preserved_on_replay() -> None:
    async def handler(request: Request) -> Response:
        return JSONResponse(
            {"ok": True},
            status_code=201,
            headers={
                "etag": '"v1"',
                "location": "/resources/abc",
                "content-language": "en",
            },
        )

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={}, headers={"Idempotency-Key": "k-etag"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-etag"})

    assert r2.headers.get("etag") == '"v1"'
    assert r2.headers.get("location") == "/resources/abc"
    assert r2.headers.get("content-language") == "en"


async def test_custom_allow_list_passes_extra_headers() -> None:
    async def handler(request: Request) -> Response:
        return JSONResponse({"ok": True}, status_code=200, headers={"x-request-id": "req-42"})

    # By default, x-request-id is NOT in allow list → dropped.
    # Custom allow extends it through.
    app = _build_app(handler, header_allow={"content-type", "x-request-id"})
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={}, headers={"Idempotency-Key": "k-custom"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-custom"})

    assert r2.headers.get("x-request-id") == "req-42"
