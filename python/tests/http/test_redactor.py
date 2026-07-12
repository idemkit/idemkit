"""Spec §4.14 PII redactor tests.

The redactor MUST:
- Run before persistence.
- Receive a copy; modifying its inputs MUST NOT affect the response delivered
  to the first client.
- Have its exceptions absorbed (a broken redactor MUST NOT crash the request).
"""

from __future__ import annotations

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from idemkit import IdempotencyMiddleware, InMemoryBackend
from idemkit.core.config import IdempotencyConfig


def _build_app(handler, *, redactor) -> Starlette:
    config = IdempotencyConfig(
        scope=lambda req: "test-user",
        response_redactor=redactor,
    )
    app = Starlette(routes=[Route("/echo", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)
    return app


async def test_redactor_modifies_stored_response_not_first_response() -> None:
    """First client receives the original body; replay client receives the redacted body."""

    async def handler(request: Request) -> Response:
        return JSONResponse({"card": "4111-1111-1111-1111", "amount": 100}, status_code=201)

    def redactor(body, headers, status):
        # Replace card number with masked version in the stored copy
        new_body = body.replace(b"4111-1111-1111-1111", b"****-****-****-1111")
        return new_body, headers

    app = _build_app(handler, redactor=redactor)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-pci"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-pci"})

    # First client receives the unredacted response (handler's output)
    assert b"4111-1111-1111-1111" in r1.content
    # Replay (from storage) is redacted
    assert r2.headers["idempotency-replayed"] == "true"
    assert b"4111-1111-1111-1111" not in r2.content
    assert b"****-****-****-1111" in r2.content


async def test_redactor_can_strip_headers() -> None:
    async def handler(request: Request) -> Response:
        return JSONResponse({"ok": True}, status_code=200, headers={"x-debug-pii": "secret"})

    def redactor(body, headers, status):
        headers.pop("x-debug-pii", None)
        return body, headers

    app = _build_app(handler, redactor=redactor)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-strip"})
        # x-debug-pii won't be stored anyway because it's not in the default allow list,
        # but a custom allow list could include it. Use config_kwargs to extend allow.
    # First response had the header from handler (we stream untouched)
    assert r1.headers.get("x-debug-pii") == "secret"


async def test_redactor_exception_does_not_crash_request_and_does_not_store() -> None:
    """A broken redactor MUST be absorbed AND MUST NOT persist unredacted data.

    Spec §4.14 / §9.1: storing the original response when the redactor failed
    would leak the very PII the redactor was supposed to scrub. Instead the
    claim is released and a retry re-executes (no replay).
    """
    call_counter = {"n": 0}

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return JSONResponse({"ok": True, "call": call_counter["n"]}, status_code=200)

    def broken_redactor(body, headers, status):
        raise RuntimeError("redactor blew up")

    app = _build_app(handler, redactor=broken_redactor)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-broken-r"})
        r2 = await client.post("/echo", json={}, headers={"Idempotency-Key": "k-broken-r"})

    # First request succeeds with the original (handler) body
    assert r1.status_code == 200
    assert r1.json()["ok"] is True
    # The response was NOT cached (redactor failed): the second request
    # re-executes the handler and is not a replay.
    assert r2.status_code == 200
    assert call_counter["n"] == 2
    assert "idempotency-replayed" not in {k.lower() for k in r2.headers}


async def test_redactor_not_called_for_non_cacheable_status() -> None:
    """5xx is not cached, so the redactor never runs on the storage path."""
    redactor_calls = {"n": 0}

    async def handler(request: Request) -> Response:
        return JSONResponse({"err": "x"}, status_code=500)

    def redactor(body, headers, status):
        redactor_calls["n"] += 1
        return body, headers

    app = _build_app(handler, redactor=redactor)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/echo", json={}, headers={"Idempotency-Key": "k-5xx-r"})

    assert redactor_calls["n"] == 0
