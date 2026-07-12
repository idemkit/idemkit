"""ASGI middleware for idemkit, compatible with FastAPI / Starlette / any ASGI 3 app.

Spec coverage: §4.7 (key + scope extraction), §4.4 (release on
exception / disconnect), §4.9 (response size enforcement by streamed bytes),
§4.11 (encoding verbatim replay), §6 (wire format).
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from idemkit import problem_details
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.config import resolve_http_config
from idemkit.core.engine import IdempotencyEngine, NeutralRequest, NeutralResponse
from idemkit.core.policy import HttpConfig

_logger = logging.getLogger(__name__)

_inmemory_warned = False


def _warn_inmemory_backend() -> None:
    """Warn once per process that InMemoryBackend is not production-safe (§7.4)."""
    global _inmemory_warned
    if _inmemory_warned:
        return
    _inmemory_warned = True
    _logger.warning(
        "idemkit: InMemoryBackend is for development and tests only. Its state "
        "lives in one process, so idempotency is NOT shared across multiple "
        "workers/instances (e.g. `uvicorn --workers N`, gunicorn, k8s replicas) "
        "and duplicate requests can slip through. Use RedisBackend or "
        "PostgresBackend in production."
    )


# ASGI types — kept generic to avoid framework deps in core.
ASGIScope = dict[str, Any]
ASGIMessage = dict[str, Any]
ASGIReceive = Callable[[], Awaitable[ASGIMessage]]
ASGISend = Callable[[ASGIMessage], Awaitable[None]]
ASGIApp = Callable[[ASGIScope, ASGIReceive, ASGISend], Awaitable[None]]


class IdempotencyMiddleware:
    """ASGI middleware that applies idempotency per the configured backend.

    Usage::

        from idemkit import IdempotencyMiddleware, HttpConfig, InMemoryBackend

        app.add_middleware(
            IdempotencyMiddleware,
            backend=InMemoryBackend(),
            config=HttpConfig(scope=lambda req: req.state.user.id),
        )

    On the ASGI ``lifespan`` shutdown the middleware closes the backend it was
    given (pool + background listener), so the one-line install does not leak.
    Pass ``manage_backend=False`` if you share the backend elsewhere and close it
    yourself.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        backend: IdempotencyBackend,
        config: HttpConfig | None = None,
        manage_backend: bool = True,
    ) -> None:
        resolved = resolve_http_config(config)
        self.app = app
        self.config = resolved
        self._backend = backend
        self._manage_backend = manage_backend
        self.engine = IdempotencyEngine(backend=backend, config=resolved)
        if type(backend).__name__ == "InMemoryBackend":
            _warn_inmemory_backend()

    async def __call__(self, scope: ASGIScope, receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") == "lifespan" and self._manage_backend:
            # Run the app's own lifespan, then close the backend on shutdown so the
            # pool + subscriber task don't leak. aclose is optional on the backend
            # Protocol, so call it only if present (custom backends may omit it).
            try:
                await self.app(scope, receive, send)
            finally:
                aclose = getattr(self._backend, "aclose", None)
                if aclose is not None:
                    try:
                        await aclose()
                    except Exception:
                        _logger.exception("idemkit: error closing backend on shutdown")
            return

        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return

        # Read and buffer the request body so both fingerprinting and the
        # downstream handler can see it. Per spec §4.5, the body is decoded
        # (Content-Encoding handled by the server upstream of ASGI). The buffer
        # is bounded by ``max_request_body_bytes`` to prevent a large-request
        # memory DoS; an oversized request is streamed through WITHOUT
        # idempotency rather than buffered whole.
        max_request = self.config.max_request_body_bytes
        body_chunks: list[bytes] = []
        body_size = 0
        client_disconnected = False
        oversized_request = False
        trailing_more = False
        more = True
        while more:
            message = await receive()
            mtype = message["type"]
            if mtype == "http.request":
                chunk = message.get("body", b"") or b""
                body_chunks.append(chunk)
                body_size += len(chunk)
                more = message.get("more_body", False)
                if max_request is not None and body_size > max_request:
                    oversized_request = True
                    trailing_more = more
                    break
            elif mtype == "http.disconnect":
                client_disconnected = True
                break
            else:
                # Unknown message; bail out of buffering and let the
                # downstream app see it via a passthrough receive.
                body_chunks.append(b"")
                more = False
        if client_disconnected:
            # Client gave up before the body finished. Nothing to do.
            return

        method = (scope.get("method") or "GET").upper()
        path = scope.get("path") or "/"
        query_bytes: bytes = scope.get("query_string", b"") or b""
        try:
            query = query_bytes.decode("latin1")
        except Exception:
            query = ""

        headers = _read_headers(scope.get("headers") or [])

        if oversized_request:
            # Too large to buffer for fingerprinting, so idempotency is bypassed.
            # The require_key_for_mutations gate is a separate safety control and
            # MUST still apply: don't let an oversized body smuggle a mutation
            # past it.
            prefix = b"".join(body_chunks)
            key = self._extract_idempotency_key(scope, headers, prefix)
            if (
                self.config.require_key_for_mutations
                and method in self.config.applicable_methods
                and not key
            ):
                status, p_headers, p_body = problem_details.missing_key()
                await _send_neutral_response(
                    send,
                    NeutralResponse(status=status, headers=p_headers, body=p_body),
                )
                return
            _logger.warning(
                "idemkit: request body exceeded max_request_body_bytes=%s; "
                "bypassing idempotency and streaming the request through.",
                max_request,
            )
            chained = _build_chained_receive(body_chunks, trailing_more, receive)
            await self.app(scope, chained, send)
            return

        full_body = b"".join(body_chunks)

        idempotency_key = self._extract_idempotency_key(scope, headers, full_body)
        caller_scope = self._extract_caller_identity(scope)

        request = NeutralRequest(
            idempotency_key=idempotency_key,
            scope=caller_scope,
            method=method,
            path=path,
            query=query,
            body=full_body,
            content_type=headers.get("content-type"),
        )

        outcome = await self.engine.decide(request)

        if outcome.kind == "replay":
            assert outcome.response is not None
            await _send_neutral_response(send, outcome.response)
            return

        if outcome.kind == "reject":
            assert outcome.response is not None
            await _send_neutral_response(send, outcome.response)
            return

        # pass_through — replay the buffered body to the downstream app and
        # capture its response so we can call engine.on_complete / on_error.
        receive_replay = _build_receive_replay(full_body)

        if outcome.effective_key is None or outcome.claim_token is None:
            # Idempotency does not apply to this request (e.g., method not in
            # applicable_methods or key absent and not required). Just forward.
            await self.app(scope, receive_replay, send)
            return

        capturer = _ResponseCapturer(
            inner_send=send,
            max_body_bytes=self.config.max_body_bytes,
        )

        try:
            await self.app(scope, receive_replay, capturer.send)
        except Exception:
            await self.engine.on_error(outcome.effective_key, outcome.claim_token)
            raise

        if not capturer.completed:
            # Handler did not produce a complete response (e.g., disconnect).
            await self.engine.on_error(outcome.effective_key, outcome.claim_token)
            return

        if capturer.oversize:
            # Spec §4.9: response exceeded max_body_bytes. Don't cache it —
            # release the claim so a retry can re-execute. Client already
            # received the full response from the streaming pass-through.
            await self.engine.on_error(outcome.effective_key, outcome.claim_token)
            return

        captured = capturer.to_neutral()
        await self.engine.on_complete(outcome.effective_key, outcome.claim_token, captured)

    # ----- extraction hooks -----

    def _extract_idempotency_key(
        self,
        scope: ASGIScope,
        headers: dict[str, str],
        body: bytes,
    ) -> str | None:
        if self.config.key is not None:
            # Custom extractor sees a minimal request-like object via scope.
            request_proxy = _RequestProxy(scope, headers, body)
            try:
                return self.config.key(request_proxy)
            except Exception:
                _logger.exception("idemkit: key raised; treating as no key")
                return None
        return _unwrap_sf_string(headers.get("idempotency-key"))

    def _extract_caller_identity(self, scope: ASGIScope) -> str | None:
        if self.config.scope is None:
            return None  # only reachable in single-tenant mode
        request_proxy = _RequestProxy(scope, _read_headers(scope.get("headers") or []), b"")
        try:
            value = self.config.scope(request_proxy)
        except Exception:
            # The extractor raised — commonly because it reached for something the
            # middleware proxy doesn't have (e.g. `req.state` / `req.client`; the
            # proxy exposes `req.headers` / `req.scope` / `req.body` only). We do
            # NOT bucket the request as anonymous: a configured-but-failing
            # extractor fails closed (the engine returns 500, §4.6), because
            # silently sharing a namespace is the cross-tenant bug we prevent.
            _logger.exception(
                "idemkit: scope extractor raised; this request will be "
                "refused (500). The middleware passes extractors a lightweight "
                "proxy with req.headers / req.scope / req.body — not a Starlette "
                "Request, so req.state / req.client are unavailable here. Read "
                "identity from a header or req.scope, or use the @Idempotency "
                "decorator (which gets the real Request)."
            )
            return None
        return value or None


class _CaseInsensitiveHeaders(dict[str, str]):
    """A header dict with case-insensitive lookups.

    HTTP header names are case-insensitive, and extractors written against a
    Starlette ``Request`` expect ``req.headers.get("X-User-Id")`` to work
    regardless of case. Keys are stored lowercased; lookups lowercase the query.
    """

    def __getitem__(self, key: str) -> str:
        return super().__getitem__(key.lower())

    def get(self, key: str, default: str | None = None) -> str | None:  # type: ignore[override]
        return super().get(key.lower(), default)

    def __contains__(self, key: object) -> bool:
        return super().__contains__(key.lower() if isinstance(key, str) else key)


class _RequestProxy:
    """Minimal request-like object exposed to user-defined extractors.

    Extractors receive this for raw ASGI. It exposes:

    * ``headers`` — case-insensitive ``dict``-like of request headers;
    * ``scope`` — the raw ASGI scope (e.g. ``scope["client"]`` for the peer
      address, ``scope["state"]`` for data set by upstream middleware);
    * ``body`` — the raw request body bytes (passed to ``key`` only).
    """

    __slots__ = ("body", "headers", "scope")

    def __init__(self, scope: ASGIScope, headers: dict[str, str], body: bytes) -> None:
        self.scope = scope
        self.headers = headers
        self.body = body


def _unwrap_sf_string(value: str | None) -> str | None:
    """Accept both quoted and unquoted Idempotency-Key forms (§6.1).

    The IETF draft specifies an RFC 8941 Structured Field String (quoted), but
    most clients send the value bare. Strip the surrounding quotes (and unescape
    ``\\"`` / ``\\\\``) so ``"abc"`` and ``abc`` map to the same key.
    """
    if value is None:
        return None
    v = value.strip()
    if len(v) >= 2 and v[0] == '"' and v[-1] == '"':
        out: list[str] = []
        escaped = False
        for ch in v[1:-1]:
            if escaped:
                out.append(ch)
                escaped = False
            elif ch == "\\":
                escaped = True
            else:
                out.append(ch)
        return "".join(out)
    return value


def _read_headers(raw: list[tuple[bytes, bytes]]) -> dict[str, str]:
    """ASGI headers list → case-insensitive lowercase dict (last wins)."""
    out: _CaseInsensitiveHeaders = _CaseInsensitiveHeaders()
    for name, value in raw:
        try:
            n = name.decode("latin1").lower()
            v = value.decode("latin1")
        except Exception:
            continue
        out[n] = v
    return out


def _build_receive_replay(body: bytes) -> ASGIReceive:
    """Return a receive callable that replays the buffered body once."""
    state = {"sent": False}

    async def receive() -> ASGIMessage:
        if not state["sent"]:
            state["sent"] = True
            return {"type": "http.request", "body": body, "more_body": False}
        # After body is delivered, the downstream app should not call receive
        # again, but if it does (some frameworks), report disconnect.
        return {"type": "http.disconnect"}

    return receive


def _build_chained_receive(
    buffered: list[bytes], more_remaining: bool, original: ASGIReceive
) -> ASGIReceive:
    """Receive callable for the oversized-request bypass path.

    Replays the buffered prefix as one ``http.request`` message, then (if the
    body wasn't fully read) delegates to the original ``receive`` for the
    remaining messages. This bounds idemkit's memory to the cap while letting
    the handler consume the full request.
    """
    state = {"sent": False}

    async def receive() -> ASGIMessage:
        if not state["sent"]:
            state["sent"] = True
            return {
                "type": "http.request",
                "body": b"".join(buffered),
                "more_body": more_remaining,
            }
        if more_remaining:
            return await original()
        return {"type": "http.disconnect"}

    return receive


async def _send_neutral_response(send: ASGISend, response: NeutralResponse) -> None:
    headers_list = [
        (name.encode("latin1"), value.encode("latin1")) for name, value in response.headers.items()
    ]
    await send(
        {
            "type": "http.response.start",
            "status": response.status,
            "headers": headers_list,
        }
    )
    await send(
        {
            "type": "http.response.body",
            "body": response.body,
            "more_body": False,
        }
    )


class _ResponseCapturer:
    """Wraps the outer ASGI ``send`` to capture status, headers, and body.

    Caching decision is made at the first body message, by its ``more_body`` flag:

    * **Single-shot** (``more_body=False``): the whole body is in this one
      message. If it fits in ``max_body_bytes`` it is cached and sent verbatim;
      if not, the start message is sent with an added
      ``Idempotency-Replay-Unavailable: size-exceeded`` header and the body is
      passed through uncached (spec §4.9 / §6.2). Enforcement counts the actual
      bytes in hand, never the ``Content-Length`` header.
    * **Incremental / streaming** (``more_body=True``): buffering would delay
      delivery to the client, so the response is NOT buffered. The start message
      goes out with ``Idempotency-Replay-Unavailable: streaming`` and every chunk
      streams straight through, uncached (spec §4.9 streaming bypass).

    The start message is otherwise forwarded VERBATIM, preserving duplicate
    headers like multiple ``Set-Cookie`` on the path to the first client. A
    separate dict view of the headers is built for storage (where duplicates do
    not survive anyway because the allow/deny filter drops Set-Cookie etc.).
    """

    def __init__(self, inner_send: ASGISend, max_body_bytes: int) -> None:
        self._send = inner_send
        self._max = max_body_bytes
        self.status: int = 0
        self.headers: dict[str, str] = {}
        self._start_message: ASGIMessage | None = None
        self._start_sent: bool = False
        self._first_body_seen: bool = False
        self._body_chunks: list[bytes] = []
        self.oversize: bool = False
        self.completed: bool = False

    @property
    def body(self) -> bytes:
        if self.oversize:
            return b""
        return b"".join(self._body_chunks)

    def to_neutral(self) -> NeutralResponse:
        return NeutralResponse(
            status=self.status,
            headers=dict(self.headers),
            body=self.body,
        )

    async def _flush_start(self, *, bypass_reason: str | None = None) -> None:
        if self._start_sent or self._start_message is None:
            return
        message = self._start_message
        if bypass_reason is not None:
            headers = list(message.get("headers", []))
            headers.append((b"idempotency-replay-unavailable", bypass_reason.encode("latin1")))
            message = {**message, "headers": headers}
        self._start_sent = True
        await self._send(message)

    async def send(self, message: ASGIMessage) -> None:
        mtype = message["type"]
        if mtype == "http.response.start":
            self.status = int(message["status"])
            # Build a dict view for storage (duplicates not significant here
            # because Set-Cookie / WWW-Authenticate are denied by §4.10).
            self.headers = {
                k.decode("latin1").lower(): v.decode("latin1")
                for k, v in message.get("headers", [])
            }
            # Hold the start message just long enough to learn, at the first body
            # message, whether the response is cacheable (the bypass header, if
            # any, has to precede the body).
            self._start_message = message
            return
        if mtype != "http.response.body":
            await self._send(message)
            return

        chunk = message.get("body", b"") or b""
        more = message.get("more_body", False)

        if self.oversize:
            # Already streaming through (size-exceeded or streaming bypass).
            await self._send(message)
            if not more:
                self.completed = True
            return

        if not self._first_body_seen:
            self._first_body_seen = True
            if more:
                # Incremental response: don't buffer (it would stall delivery).
                self.oversize = True
                await self._flush_start(bypass_reason="streaming")
                await self._send(message)
                return
            # Single-shot: the whole body is this chunk. Enforce the cap on the
            # bytes actually in hand (not Content-Length).
            if len(chunk) > self._max:
                self.oversize = True
                await self._flush_start(bypass_reason="size-exceeded")
                await self._send(message)
                self.completed = True
                return
            self._body_chunks.append(chunk)
            await self._flush_start()
            await self._send(message)
            self.completed = True
            return

        # Defensive: a single-shot response shouldn't emit further body messages.
        await self._send(message)
        if not more:
            self.completed = True
