"""WSGI middleware tests — plain sync tests (the WSGI path uses the sync bridge,
which refuses to run inside a running event loop, so these must NOT be async)."""

from __future__ import annotations

import io
import json

from idemkit import InMemoryBackend, WSGIIdempotencyMiddleware


def _call(mw, *, method="POST", path="/charge", headers=None, body=b"{}"):
    environ = {
        "REQUEST_METHOD": method,
        "PATH_INFO": path,
        "QUERY_STRING": "",
        "CONTENT_TYPE": "application/json",
        "CONTENT_LENGTH": str(len(body)),
        "wsgi.input": io.BytesIO(body),
    }
    for name, value in (headers or {}).items():
        environ["HTTP_" + name.upper().replace("-", "_")] = value
    captured = {}

    def start_response(status, response_headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = response_headers
        return lambda _data: None

    result = mw(environ, start_response)
    out = b"".join(result)
    return int(captured["status"].split(" ", 1)[0]), dict(captured["headers"]), out


def _counting_app(counter):
    def app(environ, start_response):
        counter["n"] += 1
        environ["wsgi.input"].read()  # a real app consumes the body
        start_response("201 Created", [("Content-Type", "application/json")])
        return [json.dumps({"call": counter["n"]}).encode()]

    return app


def test_duplicate_replays_handler_runs_once():
    counter = {"n": 0}
    mw = WSGIIdempotencyMiddleware(
        _counting_app(counter),
        backend=InMemoryBackend(),
        scope=lambda req: "tenant-1",
    )
    h = {"Idempotency-Key": "k1"}
    s1, _, b1 = _call(mw, headers=h, body=b'{"amount": 10}')
    s2, hdr2, b2 = _call(mw, headers=h, body=b'{"amount": 10}')

    assert s1 == 201 and s2 == 201
    assert b1 == b2
    assert hdr2.get("idempotency-replayed") == "true"
    assert counter["n"] == 1


def test_same_key_different_body_returns_422():
    counter = {"n": 0}
    mw = WSGIIdempotencyMiddleware(
        _counting_app(counter),
        backend=InMemoryBackend(),
        scope=lambda req: "tenant-1",
    )
    s1, _, _ = _call(mw, headers={"Idempotency-Key": "k2"}, body=b'{"amount": 10}')
    s2, _, _ = _call(mw, headers={"Idempotency-Key": "k2"}, body=b'{"amount": 999}')

    assert s1 == 201
    assert s2 == 422
    assert counter["n"] == 1


def test_missing_key_required_returns_400():
    counter = {"n": 0}
    mw = WSGIIdempotencyMiddleware(
        _counting_app(counter),
        backend=InMemoryBackend(),
        scope=lambda req: "tenant-1",
        require_key_for_mutations=True,
    )
    s, _, _ = _call(mw, headers={}, body=b"{}")
    assert s == 400
    assert counter["n"] == 0


def test_non_applicable_method_passes_through():
    counter = {"n": 0}
    mw = WSGIIdempotencyMiddleware(
        _counting_app(counter),
        backend=InMemoryBackend(),
        scope=lambda req: "tenant-1",
    )
    # GET is not in applicable_methods → runs every time, no dedup.
    _call(mw, method="GET", headers={"Idempotency-Key": "k3"})
    _call(mw, method="GET", headers={"Idempotency-Key": "k3"})
    assert counter["n"] == 2


def test_scope_from_header_isolates_tenants():
    counter = {"n": 0}
    mw = WSGIIdempotencyMiddleware(
        _counting_app(counter),
        backend=InMemoryBackend(),
        scope=lambda req: req.headers["x-user-id"],
    )
    h1 = {"Idempotency-Key": "same", "X-User-Id": "alice"}
    h2 = {"Idempotency-Key": "same", "X-User-Id": "bob"}
    _call(mw, headers=h1, body=b'{"a": 1}')
    _call(mw, headers=h2, body=b'{"a": 1}')
    # Same key, different tenants → two independent executions.
    assert counter["n"] == 2


def test_oversized_request_bypasses_idempotency():
    counter = {"n": 0}
    mw = WSGIIdempotencyMiddleware(
        _counting_app(counter),
        backend=InMemoryBackend(),
        scope=lambda req: "tenant-1",
        max_request_body_bytes=16,  # tiny cap so a normal body counts as oversized
    )
    big = b'{"payload": "' + b"x" * 100 + b'"}'  # well over 16 bytes
    h = {"Idempotency-Key": "k-big"}
    s1, _, _ = _call(mw, headers=h, body=big)
    s2, _, _ = _call(mw, headers=h, body=big)
    # A body over the cap bypasses idempotency and streams through (the handler
    # runs on BOTH requests) rather than being buffered and hashed.
    assert s1 == 201 and s2 == 201
    assert counter["n"] == 2
