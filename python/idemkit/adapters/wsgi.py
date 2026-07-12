"""WSGI middleware for idemkit — Flask, Django (sync views), Pyramid, Bottle, any
WSGI app.

The idemkit core is async (it awaits the backend), and WSGI is synchronous, so
this middleware drives the same HTTP engine the ASGI middleware uses through the
shared background loop in :mod:`idemkit.core.sync_bridge` — exactly like
``idempotent_sync`` and ``IdempotentConsumer.dispatch_sync``. Behaviour matches
the ASGI middleware: same config, same fingerprinting, same replay/reject wire
format, same size/streaming bypass, same fail-closed scope rule.

Usage (Flask)::

    from flask import Flask
    from idemkit import HttpConfig, WSGIIdempotencyMiddleware, RedisBackend

    app = Flask(__name__)
    app.wsgi_app = WSGIIdempotencyMiddleware(
        app.wsgi_app,
        backend=RedisBackend.from_url("redis://localhost:6379"),
        config=HttpConfig(scope=lambda req: req.headers["x-user-id"]),
    )

Django: wrap ``application`` in ``wsgi.py`` the same way. Extractors (``scope`` /
``key``) receive a lightweight request proxy exposing ``req.headers`` (case
-insensitive), ``req.environ`` (the raw WSGI environ), and ``req.body``.
"""

from __future__ import annotations

import io
import logging
from collections.abc import Callable, Iterable, Iterator
from typing import Any

from idemkit import problem_details
from idemkit.adapters.asgi import _CaseInsensitiveHeaders, _unwrap_sf_string
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.config import resolve_http_config
from idemkit.core.engine import IdempotencyEngine, NeutralRequest, NeutralResponse
from idemkit.core.policy import HttpConfig
from idemkit.core.sync_bridge import register_closable, run_sync

_logger = logging.getLogger(__name__)

_inmemory_warned = False


def _warn_inmemory_backend() -> None:
    global _inmemory_warned
    if _inmemory_warned:
        return
    _inmemory_warned = True
    _logger.warning(
        "idemkit: InMemoryBackend is for development and tests only. Its state "
        "lives in one process, so idempotency is NOT shared across multiple "
        "workers (gunicorn/uWSGI workers, multiple pods) and duplicate requests "
        "can slip through. Use RedisBackend or PostgresBackend in production."
    )


# WSGI types, kept loose to avoid a hard dependency on a typing shim.
WSGIEnviron = dict[str, Any]
StartResponse = Callable[..., Any]
WSGIApp = Callable[[WSGIEnviron, StartResponse], Iterable[bytes]]


class WSGIIdempotencyMiddleware:
    """WSGI middleware applying idempotency per the configured backend.

    Wrap your WSGI application (``app.wsgi_app`` in Flask, ``application`` in a
    Django ``wsgi.py``). Accepts the same keyword arguments as the ASGI
    :class:`~idemkit.IdempotencyMiddleware`, configured with an ``HttpConfig``.
    """

    def __init__(
        self,
        app: WSGIApp,
        *,
        backend: IdempotencyBackend,
        config: HttpConfig | None = None,
    ) -> None:
        resolved = resolve_http_config(config)
        self.app = app
        self.config = resolved
        self.backend = backend
        self.engine = IdempotencyEngine(backend=backend, config=resolved)
        # The Redis/Postgres pool binds to the sync-bridge background loop; track
        # it so it closes cleanly at process exit (no "Event loop is closed").
        register_closable(backend)
        if type(backend).__name__ == "InMemoryBackend":
            _warn_inmemory_backend()

    def __call__(self, environ: WSGIEnviron, start_response: StartResponse) -> Iterable[bytes]:
        method = (environ.get("REQUEST_METHOD") or "GET").upper()
        path = environ.get("PATH_INFO") or "/"
        query = environ.get("QUERY_STRING") or ""
        content_type = environ.get("CONTENT_TYPE")
        headers = _headers_from_environ(environ)

        max_request = self.config.max_request_body_bytes
        stream = environ.get("wsgi.input") or io.BytesIO(b"")
        body, oversized, tail = _read_request_body(stream, max_request)

        if oversized:
            # Too large to buffer for fingerprinting → bypass idempotency. The
            # require_key_for_mutations gate is a separate safety control and MUST
            # still apply (don't let a big body smuggle a mutation past it).
            key = self._extract_key(environ, headers, body)
            if (
                self.config.require_key_for_mutations
                and method in self.config.applicable_methods
                and not key
            ):
                return _send(start_response, _problem(problem_details.missing_key()))
            _logger.warning(
                "idemkit: request body exceeded max_request_body_bytes=%s; "
                "bypassing idempotency and streaming the request through.",
                max_request,
            )
            environ["wsgi.input"] = _PrefixedInput(body, stream, tail)
            return self.app(environ, start_response)

        # Body fully buffered — hand the downstream app a fresh stream over it.
        environ["wsgi.input"] = io.BytesIO(body)
        environ["CONTENT_LENGTH"] = str(len(body))

        idempotency_key = self._extract_key(environ, headers, body)
        caller_scope = self._extract_scope(environ, headers)

        request = NeutralRequest(
            idempotency_key=idempotency_key,
            scope=caller_scope,
            method=method,
            path=path,
            query=query,
            body=body,
            content_type=content_type,
        )

        outcome = run_sync(self.engine.decide(request))

        if outcome.kind in ("replay", "reject"):
            assert outcome.response is not None
            return _send(start_response, outcome.response)

        if outcome.effective_key is None or outcome.claim_token is None:
            # Idempotency does not apply (method not in scope, or no key and not
            # required). Forward as-is.
            return self.app(environ, start_response)

        return self._run_and_capture(
            environ, start_response, outcome.effective_key, outcome.claim_token
        )

    # ----- pass-through with capture -----

    def _run_and_capture(
        self,
        environ: WSGIEnviron,
        start_response: StartResponse,
        effective_key: str,
        claim_token: str,
    ) -> Iterable[bytes]:
        captured: dict[str, Any] = {"status": None, "headers": None}

        def capturing_start_response(
            status: str, response_headers: list[tuple[str, str]], exc_info: Any = None
        ) -> Callable[[bytes], Any]:
            if exc_info is not None:
                try:
                    raise exc_info[1].with_traceback(exc_info[2])
                finally:
                    exc_info = None
            captured["status"] = status
            captured["headers"] = response_headers
            return lambda _data: None

        try:
            result = self.app(environ, capturing_start_response)
        except Exception:
            run_sync(self.engine.on_error(effective_key, claim_token))
            raise

        body_chunks: list[bytes] = []
        size = 0
        max_body = self.config.max_body_bytes
        it: Iterator[bytes] = iter(result)
        try:
            for chunk in it:
                body_chunks.append(chunk)
                size += len(chunk)
                if size > max_body:
                    # Spec §4.9: too big to cache. Stream through uncached with a
                    # bypass header and release the claim so a retry re-executes.
                    _close(result)
                    run_sync(self.engine.on_error(effective_key, claim_token))
                    status, hdrs = _parse_captured(captured)
                    hdrs = [*hdrs, ("Idempotency-Replay-Unavailable", "size-exceeded")]
                    start_response(status, hdrs)
                    return _chain(body_chunks, it, result)
        except Exception:
            _close(result)
            run_sync(self.engine.on_error(effective_key, claim_token))
            raise
        _close(result)

        status, hdrs = _parse_captured(captured)
        status_code = int(status.split(" ", 1)[0])
        body = b"".join(body_chunks)
        run_sync(
            self.engine.on_complete(
                effective_key,
                claim_token,
                NeutralResponse(status=status_code, headers=_headers_to_dict(hdrs), body=body),
            )
        )
        start_response(status, hdrs)
        return [body]

    # ----- extraction -----

    def _extract_key(
        self, environ: WSGIEnviron, headers: dict[str, str], body: bytes
    ) -> str | None:
        if self.config.key is not None:
            try:
                return self.config.key(_WsgiRequestProxy(environ, headers, body))
            except Exception:
                _logger.exception("idemkit: key raised; treating as no key")
                return None
        return _unwrap_sf_string(headers.get("idempotency-key"))

    def _extract_scope(self, environ: WSGIEnviron, headers: dict[str, str]) -> str | None:
        if self.config.scope is None:
            return None  # single-tenant mode
        try:
            value = self.config.scope(_WsgiRequestProxy(environ, headers, b""))
        except Exception:
            # A configured-but-failing extractor fails closed (engine returns 500),
            # never silently shares a namespace (the cross-tenant bug §4.6 prevents).
            _logger.exception(
                "idemkit: scope extractor raised; this request will be refused "
                "(500). The WSGI proxy exposes req.headers / req.environ / req.body."
            )
            return None
        return value or None


class _WsgiRequestProxy:
    """Request-like object handed to user ``scope`` / ``key`` extractors.

    Exposes ``headers`` (case-insensitive), ``environ`` (raw WSGI environ), and
    ``body`` (request bytes; passed to ``key`` only).
    """

    __slots__ = ("body", "environ", "headers")

    def __init__(self, environ: WSGIEnviron, headers: dict[str, str], body: bytes) -> None:
        self.environ = environ
        self.headers = headers
        self.body = body


class _PrefixedInput:
    """A read-only stream that yields an already-read prefix, then the rest.

    Used on the oversized-request bypass so the downstream app still sees the
    whole body while idemkit's buffer stays bounded.
    """

    def __init__(self, prefix: bytes, original: Any, tail: bytes) -> None:
        self._buf = prefix + tail
        self._pos = 0
        self._original = original

    def read(self, size: int = -1) -> bytes:
        if size is None or size < 0:
            rest = self._buf[self._pos :]
            self._pos = len(self._buf)
            try:
                rest += self._original.read()
            except Exception:
                pass
            return rest
        out = self._buf[self._pos : self._pos + size]
        self._pos += len(out)
        if len(out) < size:
            out += self._original.read(size - len(out))
        return out

    def readline(self, *args: Any) -> bytes:
        # Minimal readline over the buffered prefix; falls back to the original.
        nl = self._buf.find(b"\n", self._pos)
        if nl != -1:
            out = self._buf[self._pos : nl + 1]
            self._pos = nl + 1
            return out
        rest = self._buf[self._pos :]
        self._pos = len(self._buf)
        tail: bytes = self._original.readline(*args)
        return rest + tail


def _read_request_body(stream: Any, max_bytes: int | None) -> tuple[bytes, bool, bytes]:
    """Read the request body up to ``max_bytes`` (None = unbounded).

    Returns ``(body, oversized, tail)``. When oversized, ``body`` holds exactly
    ``max_bytes`` and ``tail`` holds the extra byte(s) already read past the cap
    (so no data is lost when chaining to the downstream app).
    """
    if max_bytes is None:
        try:
            data = stream.read()
        except Exception:
            data = b""
        return (data or b""), False, b""

    chunks: list[bytes] = []
    remaining = max_bytes + 1  # one past the cap to detect an oversized body
    while remaining > 0:
        chunk = stream.read(remaining)
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    if len(data) > max_bytes:
        return data[:max_bytes], True, data[max_bytes:]
    return data, False, b""


def _headers_from_environ(environ: WSGIEnviron) -> _CaseInsensitiveHeaders:
    """WSGI environ → case-insensitive lowercase header dict."""
    out: _CaseInsensitiveHeaders = _CaseInsensitiveHeaders()
    for key, value in environ.items():
        if key.startswith("HTTP_"):
            name = key[5:].replace("_", "-").lower()
            out[name] = value
        elif key == "CONTENT_TYPE":
            out["content-type"] = value
        elif key == "CONTENT_LENGTH":
            out["content-length"] = value
    return out


def _headers_to_dict(headers: list[tuple[str, str]]) -> dict[str, str]:
    return {name.lower(): value for name, value in headers}


def _parse_captured(captured: dict[str, Any]) -> tuple[str, list[tuple[str, str]]]:
    status = captured["status"]
    headers = captured["headers"]
    if status is None or headers is None:
        raise RuntimeError("idemkit: the downstream WSGI app did not call start_response.")
    return str(status), list(headers)


def _problem(problem: tuple[int, dict[str, str], bytes]) -> NeutralResponse:
    status, headers, body = problem
    return NeutralResponse(status=status, headers=headers, body=body)


def _send(start_response: StartResponse, response: NeutralResponse) -> list[bytes]:
    status_line = f"{response.status} {_reason(response.status)}"
    header_items = list(response.headers.items())
    start_response(status_line, header_items)
    return [response.body]


def _chain(buffered: list[bytes], rest: Iterator[bytes], closable: Any) -> Iterator[bytes]:
    yield from buffered
    try:
        yield from rest
    finally:
        _close(closable)


def _close(obj: Any) -> None:
    close = getattr(obj, "close", None)
    if callable(close):
        try:
            close()
        except Exception:
            pass


def _reason(status: int) -> str:
    try:
        from http import HTTPStatus

        return HTTPStatus(status).phrase
    except Exception:
        return ""
