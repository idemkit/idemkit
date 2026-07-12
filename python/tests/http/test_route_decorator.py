"""Per-route @Idempotency.protect decorator tests (Starlette)."""

from __future__ import annotations

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response, StreamingResponse
from starlette.routing import Route

from idemkit import HttpConfig, Idempotency, InMemoryBackend


def _client(app: Starlette) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_protected_route_deduplicates() -> None:
    calls = {"n": 0}
    idem = Idempotency(
        backend=InMemoryBackend(),
        config=HttpConfig(scope=lambda req: req.headers.get("x-user-id", "anon")),
    )

    @idem.protect
    async def endpoint(request: Request) -> Response:
        calls["n"] += 1
        return JSONResponse({"n": calls["n"]}, status_code=201)

    app = Starlette(routes=[Route("/p", endpoint, methods=["POST"])])
    async with _client(app) as client:
        r1 = await client.post("/p", headers={"Idempotency-Key": "k1"})
        r2 = await client.post("/p", headers={"Idempotency-Key": "k1"})
    assert r1.status_code == 201
    assert r2.status_code == 201
    assert calls["n"] == 1
    assert r1.json() == r2.json()
    assert r2.headers.get("idempotency-replayed") == "true"
    assert "idempotency-replayed" not in {k.lower() for k in r1.headers}


async def test_only_decorated_route_is_protected() -> None:
    """A sibling route without the decorator is untouched (per-route scoping)."""
    protected = {"n": 0}
    plain = {"n": 0}
    idem = Idempotency(backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: "u"))

    @idem.protect
    async def guarded(request: Request) -> Response:
        protected["n"] += 1
        return JSONResponse({"n": protected["n"]})

    async def unguarded(request: Request) -> Response:
        plain["n"] += 1
        return JSONResponse({"n": plain["n"]})

    app = Starlette(
        routes=[
            Route("/guarded", guarded, methods=["POST"]),
            Route("/plain", unguarded, methods=["POST"]),
        ]
    )
    async with _client(app) as client:
        await client.post("/guarded", headers={"Idempotency-Key": "k"})
        await client.post("/guarded", headers={"Idempotency-Key": "k"})
        await client.post("/plain", headers={"Idempotency-Key": "k"})
        await client.post("/plain", headers={"Idempotency-Key": "k"})
    assert protected["n"] == 1
    assert plain["n"] == 2


async def test_decorator_sees_real_request_for_identity() -> None:
    """The extractor receives the real Starlette Request (case-insensitive
    headers, .client, .state all available)."""
    calls = {"n": 0}
    idem = Idempotency(
        backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: req.headers["X-User-Id"])
    )

    @idem.protect
    async def endpoint(request: Request) -> Response:
        calls["n"] += 1
        body = await request.json()
        return JSONResponse({"u": body["u"]})

    app = Starlette(routes=[Route("/p", endpoint, methods=["POST"])])
    async with _client(app) as client:
        ra = await client.post(
            "/p", json={"u": "a"}, headers={"Idempotency-Key": "k", "X-User-Id": "ua"}
        )
        rb = await client.post(
            "/p", json={"u": "b"}, headers={"Idempotency-Key": "k", "X-User-Id": "ub"}
        )
    assert calls["n"] == 2
    assert ra.json() == {"u": "a"}
    assert rb.json() == {"u": "b"}


async def test_decorator_payload_mismatch_raises_typed_exception() -> None:
    """The per-route decorator RAISES a typed exception (§6.3) so app code can
    branch on it — distinct from the middleware, which returns problem+json."""
    from idemkit.core.exceptions import PayloadMismatch

    idem = Idempotency(backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: "u"))

    @idem.protect
    async def endpoint(request: Request) -> Response:
        body = await request.json()
        return JSONResponse(body)

    app = Starlette(routes=[Route("/p", endpoint, methods=["POST"])])
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=True)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/p", json={"a": 1}, headers={"Idempotency-Key": "k"})
        assert r1.status_code == 200
        with pytest.raises(PayloadMismatch):
            await client.post("/p", json={"a": 2}, headers={"Idempotency-Key": "k"})


async def test_decorator_problem_handler_renders_problem_json() -> None:
    """With idempotency_problem_handler installed, the typed exception is rendered
    as the same RFC 9457 problem+json the middleware would return."""
    from idemkit.adapters.route import idempotency_problem_handler
    from idemkit.core.exceptions import IdempotencyError

    idem = Idempotency(backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: "u"))

    @idem.protect
    async def endpoint(request: Request) -> Response:
        body = await request.json()
        return JSONResponse(body)

    app = Starlette(routes=[Route("/p", endpoint, methods=["POST"])])
    app.add_exception_handler(IdempotencyError, idempotency_problem_handler)
    async with _client(app) as client:
        r1 = await client.post("/p", json={"a": 1}, headers={"Idempotency-Key": "k"})
        r2 = await client.post("/p", json={"a": 2}, headers={"Idempotency-Key": "k"})
    assert r1.status_code == 200
    assert r2.status_code == 422
    assert b"urn:idemkit:payload-mismatch" in r2.content
    assert r2.headers["content-type"] == "application/problem+json"


async def test_decorator_streaming_response_bypasses_cache() -> None:
    calls = {"n": 0}
    idem = Idempotency(backend=InMemoryBackend(), config=HttpConfig(scope=lambda req: "u"))

    @idem.protect
    async def endpoint(request: Request) -> Response:
        calls["n"] += 1

        async def gen():
            yield b"a"
            yield b"b"

        return StreamingResponse(gen(), status_code=200)

    app = Starlette(routes=[Route("/p", endpoint, methods=["POST"])])
    async with _client(app) as client:
        r1 = await client.post("/p", headers={"Idempotency-Key": "k"})
        await client.post("/p", headers={"Idempotency-Key": "k"})
    assert r1.content == b"ab"
    assert r1.headers.get("idempotency-replay-unavailable") == "streaming"
    assert calls["n"] == 2


def test_protect_requires_response_return() -> None:
    """When idempotency applies, returning a non-Response raises a clear error."""
    from idemkit.adapters.route import _require_response

    with pytest.raises(TypeError, match="must return a"):
        _require_response({"not": "a response"})
