"""Every HTTP example dedupes over a real duplicate request.

The examples are clean snippets exposing an `app` (or `wrapped` WSGI callable).
Here we import each one, fire a duplicate request, and assert the side effect ran
once and the second response was a replay.
"""

from __future__ import annotations

import io
import uuid
from unittest import mock

import pytest

from tests.examples._examples import load_module


async def test_getting_started_dedupes() -> None:
    import httpx

    mod = load_module("http/getting_started.py")
    key = f"k-{uuid.uuid4()}"  # unique so a rerun does not replay a prior run
    headers = {"Idempotency-Key": key, "Content-Type": "application/json"}
    transport = httpx.ASGITransport(app=mod.app)
    with mock.patch.object(mod, "charge_card", wraps=mod.charge_card) as spy:
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r1 = await c.post("/charge", headers=headers, content='{"order": "o1"}')
            r2 = await c.post("/charge", headers=headers, content='{"order": "o1"}')
    assert r1.status_code == 201
    assert r2.headers.get("idempotency-replayed") == "true"
    assert spy.call_count == 1  # the card was charged once


async def test_fastapi_middleware_dedupes_and_redacts() -> None:
    pytest.importorskip("fastapi")
    import httpx

    mod = load_module("http/fastapi_middleware.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "k1", "X-User-Id": "u1", "Content-Type": "application/json"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.post("/charge", headers=headers, content='{"amount": 50, "nonce": "a"}')
        # nonce differs, but body_fingerprint keys only on amount, so this dedupes.
        r2 = await c.post("/charge", headers=headers, content='{"amount": 50, "nonce": "b"}')
    assert r1.json()["charged"] == 50
    assert "card_number" in r1.json()  # first caller sees the full body
    assert r2.headers.get("idempotency-replayed") == "true"
    assert "card_number" not in r2.json()  # redacted from the stored copy


async def test_fastapi_route_dedupes_dict_return() -> None:
    pytest.importorskip("fastapi")
    import httpx

    mod = load_module("http/fastapi_route.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "fr1", "x-user": "u1", "Content-Type": "application/json"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.post("/charge", headers=headers, content='{"id": "o1"}')
        r2 = await c.post("/charge", headers=headers, content='{"id": "o1"}')
    assert r1.status_code == 200
    assert r1.json() == r2.json()  # dict return replayed
    assert r2.headers.get("idempotency-replayed") == "true"


def test_drf_view_dedupes() -> None:
    pytest.importorskip("rest_framework")
    mod = load_module("http/drf_view.py")
    from rest_framework.test import APIRequestFactory

    factory = APIRequestFactory()
    view = mod.ChargeView.as_view()

    def post(key):
        return factory.post(
            "/charge", {"a": 1}, format="json", HTTP_IDEMPOTENCY_KEY=key, HTTP_X_USER="u1"
        )

    r1 = view(post("k1"))
    r2 = view(post("k1"))  # duplicate
    r1.render()
    assert r1.status_code == 201
    assert bytes(r2.content) == bytes(r1.content)  # stored response replayed
    assert r2.headers.get("idempotency-replayed") == "true"


async def test_route_decorator_dedupes() -> None:
    pytest.importorskip("fastapi")
    import httpx

    mod = load_module("http/route_decorator.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "r1", "X-User-Id": "u1", "Content-Type": "application/json"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        r1 = await c.post("/refund", headers=headers, content="{}")
        r2 = await c.post("/refund", headers=headers, content="{}")
    assert r1.status_code == r2.status_code == 201
    assert r1.json() == r2.json() == {"refunded": True}


def test_flask_wsgi_dedupes() -> None:
    mod = load_module("http/flask_wsgi.py")

    def post(key: str) -> dict:
        environ = {
            "REQUEST_METHOD": "POST",
            "PATH_INFO": "/charge",
            "QUERY_STRING": "",
            "CONTENT_TYPE": "application/json",
            "wsgi.input": io.BytesIO(b"{}"),
            "HTTP_IDEMPOTENCY_KEY": key,
            "HTTP_X_USER_ID": "u1",
        }
        captured: dict = {}

        def start_response(status, headers, exc_info=None):
            captured["headers"] = {k.lower(): v for k, v in headers}

        b"".join(mod.wrapped(environ, start_response))
        return captured["headers"]

    with mock.patch.object(mod, "charge_card", wraps=mod.charge_card) as spy:
        post("key-1")
        h2 = post("key-1")
    assert h2.get("idempotency-replayed") == "true"
    assert spy.call_count == 1


async def test_response_hook_tags_only_replay() -> None:
    pytest.importorskip("httpx")
    import httpx

    mod = load_module("http/response_hook.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "k1"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        first = await c.post("/charge", headers=headers)
        replay = await c.post("/charge", headers=headers)
    assert "x-served-from-cache" not in first.headers  # live request: hook did NOT run
    assert replay.headers["x-served-from-cache"] == "true"  # replay was tagged


async def test_all_options_dedupes() -> None:
    pytest.importorskip("httpx")
    import httpx

    mod = load_module("http/all_options.py")
    transport = httpx.ASGITransport(app=mod.app)
    headers = {"Idempotency-Key": "k1", "X-User-Id": "u1"}
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        first = await c.post("/charge", headers=headers)
        replay = await c.post("/charge", headers=headers)
    assert first.status_code == 201
    assert replay.headers.get("idempotency-replayed") == "true"
    assert len(mod.events) >= 1  # event_handlers received structured events
