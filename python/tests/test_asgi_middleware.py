"""End-to-end ASGI middleware tests with Starlette + httpx."""

from __future__ import annotations

import asyncio

import httpx
import pytest
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from idemkit import IdempotencyMiddleware, InMemoryBackend
from idemkit.core.config import IdempotencyConfig


def _build_app(
    handler,
    *,
    backend: InMemoryBackend | None = None,
    scope=None,
    **config_kwargs,
) -> Starlette:
    backend = backend or InMemoryBackend()
    if scope is None:
        def scope(req):
            return "test-tenant:test-user"
    config = IdempotencyConfig(scope=scope, **config_kwargs)
    app = Starlette(routes=[Route("/echo", handler, methods=["POST", "GET"])])
    app.add_middleware(IdempotencyMiddleware, backend=backend, config=config)
    return app


@pytest.fixture
def call_counter():
    return {"n": 0}


async def test_duplicate_request_replays_handler_runs_once(call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse({"call": call_counter["n"], "body": body}, status_code=201)

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={"x": 1}, headers={"Idempotency-Key": "k1"})
        r2 = await client.post("/echo", json={"x": 1}, headers={"Idempotency-Key": "k1"})

    assert r1.status_code == 201
    assert r2.status_code == 201
    assert r1.json() == r2.json()
    assert r2.headers["idempotency-replayed"] == "true"
    assert "idempotency-replayed" not in {k.lower() for k in r1.headers.keys()}
    assert call_counter["n"] == 1


async def test_concurrent_duplicates_handler_runs_once(call_counter) -> None:
    async def handler(request: Request) -> Response:
        await asyncio.sleep(0.1)
        call_counter["n"] += 1
        return JSONResponse({"call": call_counter["n"]}, status_code=200)

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        results = await asyncio.gather(
            *[
                client.post("/echo", json={"x": 1}, headers={"Idempotency-Key": "k-concurrent"})
                for _ in range(10)
            ]
        )

    assert all(r.status_code == 200 for r in results)
    assert call_counter["n"] == 1
    bodies = {r.text for r in results}
    assert len(bodies) == 1


async def test_same_key_different_payload_returns_422(call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse(body, status_code=200)

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={"amount": 100}, headers={"Idempotency-Key": "k-pay"})
        r2 = await client.post("/echo", json={"amount": 200}, headers={"Idempotency-Key": "k-pay"})

    assert r1.status_code == 200
    assert r2.status_code == 422
    assert r2.headers["content-type"].startswith("application/problem+json")
    assert b"urn:idemkit:payload-mismatch" in r2.content
    assert call_counter["n"] == 1


async def test_5xx_response_is_not_cached(call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"err": "boom"}, status_code=500)

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-5xx"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-5xx"})

    assert r1.status_code == 500
    assert r2.status_code == 500
    assert call_counter["n"] == 2


async def test_get_request_passes_through_without_idempotency(call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"call": call_counter["n"]})

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.get("/echo", headers={"Idempotency-Key": "k-get"})
        r2 = await client.get("/echo", headers={"Idempotency-Key": "k-get"})

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert call_counter["n"] == 2


async def test_set_cookie_header_not_replayed() -> None:
    """Spec §4.10: Set-Cookie MUST NOT be stored or replayed."""

    async def handler(request: Request) -> Response:
        resp = JSONResponse({"ok": True}, status_code=201)
        resp.set_cookie("session", "secret-value")
        return resp

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-cookie"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-cookie"})

    assert "set-cookie" in {k.lower() for k in r1.headers.keys()}
    set_cookie_replay = [h for h in r2.headers.keys() if h.lower() == "set-cookie"]
    assert set_cookie_replay == []


async def test_require_key_for_mutations_rejects_missing() -> None:
    async def handler(request: Request) -> Response:
        return JSONResponse({}, status_code=201)

    app = _build_app(handler, require_key_for_mutations=True)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/echo", json={})

    assert r.status_code == 400
    assert b"urn:idemkit:missing-key" in r.content


async def test_handler_exception_releases_claim() -> None:
    state = {"phase": "first"}

    async def handler(request: Request) -> Response:
        if state["phase"] == "first":
            state["phase"] = "second"
            raise RuntimeError("handler boom")
        return JSONResponse({"ok": True}, status_code=200)

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-boom"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-boom"})

    assert r1.status_code == 500
    assert r2.status_code == 200
    assert r2.json() == {"ok": True}


async def test_cross_tenant_isolation_different_callers_dont_collide(call_counter) -> None:
    """Spec §4.6: two callers with the same Idempotency-Key MUST NOT see each other's responses."""

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse({"tenant": body.get("tenant")}, status_code=200)

    app = _build_app(
        handler,
        scope=lambda req: req.headers.get("x-tenant", "anon"),
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r_a = await client.post(
            "/echo",
            json={"tenant": "A"},
            headers={"Idempotency-Key": "shared-key", "X-Tenant": "tenant-A"},
        )
        r_b = await client.post(
            "/echo",
            json={"tenant": "B"},
            headers={"Idempotency-Key": "shared-key", "X-Tenant": "tenant-B"},
        )

    # Both ran (different effective keys despite same Idempotency-Key)
    assert call_counter["n"] == 2
    assert r_a.json() == {"tenant": "A"}
    assert r_b.json() == {"tenant": "B"}
    # Neither sees the other's response replayed
    assert "idempotency-replayed" not in {k.lower() for k in r_a.headers.keys()}
    assert "idempotency-replayed" not in {k.lower() for k in r_b.headers.keys()}


async def test_stripe_compat_uses_stripe_replay_header(call_counter) -> None:
    """Spec §6.3.1: stripe_compat mode emits ``Idempotent-Replayed`` (Stripe's spelling)."""

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"ok": True}, status_code=200)

    app = _build_app(handler, compat_mode="stripe")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={}, headers={"Idempotency-Key": "k-stripe"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-stripe"})

    # Stripe spelling, NOT our default
    assert r2.headers.get("idempotent-replayed") == "true"
    assert r2.headers.get("idempotency-replayed") is None
    assert call_counter["n"] == 1


async def test_stripe_compat_returns_409_on_payload_mismatch(call_counter) -> None:
    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse(body, status_code=200)

    app = _build_app(handler, compat_mode="stripe")
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={"a": 1}, headers={"Idempotency-Key": "k-cs"})
        r2 = await client.post("/echo", json={"a": 2}, headers={"Idempotency-Key": "k-cs"})

    # Stripe-compat: 409 instead of 422
    assert r2.status_code == 409
    assert b"urn:idemkit:payload-mismatch" in r2.content


async def test_multiple_set_cookie_headers_preserved_on_first_response() -> None:
    """Bug fix: ASGI duplicate response headers (e.g., two Set-Cookie) MUST be
    forwarded to the first client verbatim. The capturer's dict view of headers
    is used only for stored-record filtering, never for forwarding."""

    async def handler(request: Request) -> Response:
        resp = JSONResponse({"ok": True}, status_code=201)
        resp.set_cookie("session", "value-a")
        resp.set_cookie("csrf", "value-b")
        return resp

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-multi-cookie"})

    # Both cookies present (httpx exposes them via the response's cookies object)
    set_cookies = r1.headers.get_list("set-cookie")
    cookie_names = {c.split("=", 1)[0].strip() for c in set_cookies}
    assert "session" in cookie_names
    assert "csrf" in cookie_names


async def test_caller_identity_header_lookup_is_case_insensitive(call_counter) -> None:
    """A natural extractor like req.headers["X-User-Id"] (capitalized) MUST work,
    so distinct users stay isolated instead of silently collapsing."""

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.json()
        return JSONResponse({"u": body.get("u")}, status_code=200)

    app = _build_app(handler, scope=lambda req: req.headers["X-User-Id"])
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post(
            "/echo", json={"u": "a"}, headers={"Idempotency-Key": "k", "X-User-Id": "user-a"}
        )
        r2 = await client.post(
            "/echo", json={"u": "b"}, headers={"Idempotency-Key": "k", "X-User-Id": "user-b"}
        )

    assert r1.status_code == 200
    assert r2.status_code == 200
    # Different users, same key → both executed, neither replayed the other's.
    assert call_counter["n"] == 2
    assert r1.json() == {"u": "a"}
    assert r2.json() == {"u": "b"}


async def test_quoted_and_unquoted_key_dedupe_together(call_counter) -> None:
    """Spec §6.1: quoted (RFC 8941) and bare Idempotency-Key values map to the
    same key."""

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"n": call_counter["n"]}, status_code=200)

    app = _build_app(handler)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={"a": 1}, headers={"Idempotency-Key": '"abc"'})
        r2 = await client.post("/echo", json={"a": 1}, headers={"Idempotency-Key": "abc"})

    assert call_counter["n"] == 1  # treated as the same key
    assert r2.headers.get("idempotency-replayed") == "true"
    assert r1.json() == r2.json()


async def test_oversize_key_rejected_as_400() -> None:
    """Spec §6.1: keys > 255 bytes MUST be rejected with 400."""

    async def handler(request: Request) -> Response:
        return JSONResponse({}, status_code=201)

    app = _build_app(handler, require_key_for_mutations=True)
    transport = httpx.ASGITransport(app=app)
    long_key = "x" * 256
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/echo", json={}, headers={"Idempotency-Key": long_key})

    assert r.status_code == 400
    assert b"urn:idemkit:missing-key" in r.content
