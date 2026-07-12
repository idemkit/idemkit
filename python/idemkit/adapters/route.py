"""Per-route idempotency for Starlette / FastAPI.

Where ``IdempotencyMiddleware`` applies idempotency to a whole app, ``Idempotency``
attaches it to individual endpoints with a decorator. Bind it to a backend and
config once, then decorate the routes you want protected::

    from idemkit import HttpConfig, Idempotency, RedisBackend

    idem = Idempotency(
        backend=RedisBackend.from_url("redis://localhost:6379"),
        config=HttpConfig(scope=lambda req: req.state.user.id),
    )


    @app.post("/charge")
    @idem.protect
    async def charge(request: Request) -> Response:
        ...
        return JSONResponse({"ok": True}, status_code=201)

Two requirements for a decorated endpoint:

1. It must declare a ``request: Request`` parameter (this is how the decorator
   gets the request; it is also what your ``scope`` / ``key``
   receive — the real Starlette ``Request``, so ``request.state`` and
   ``request.client`` work).
2. When idempotency applies, it must return a Starlette/FastAPI ``Response``
   (``JSONResponse``, ``PlainTextResponse``, ...). A handler that returns a dict
   relies on FastAPI's serialization, which happens *after* this decorator runs
   and cannot be captured here. For dict/model returns, use the middleware
   instead, or return a ``Response`` explicitly.
"""

from __future__ import annotations

import functools
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from idemkit.adapters.asgi import _unwrap_sf_string
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.config import resolve_http_config
from idemkit.core.engine import (
    EngineOutcome,
    IdempotencyEngine,
    NeutralRequest,
    NeutralResponse,
)
from idemkit.core.exceptions import (
    ConfigurationError,
    IdempotencyConflict,
    IdempotencyError,
    IdempotencyKeyMissing,
    PayloadMismatch,
    StorageUnavailable,
)
from idemkit.core.policy import HttpConfig

_logger = logging.getLogger(__name__)


class Idempotency:
    """Decorator-based idempotency bound to a backend + config."""

    def __init__(
        self,
        *,
        backend: IdempotencyBackend,
        config: HttpConfig | None = None,
    ) -> None:
        resolved = resolve_http_config(config)
        self.config = resolved
        self.engine = IdempotencyEngine(backend=backend, config=resolved)

    def protect(self, func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
        """Decorate an async endpoint so duplicate requests are deduplicated."""

        @functools.wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            request = _find_request(args, kwargs)
            if request is None:
                raise TypeError(
                    "idemkit: @Idempotency.protect requires the endpoint to "
                    "declare a `request: Request` parameter."
                )

            body = await request.body()
            max_request = self.config.max_request_body_bytes
            if max_request is not None and len(body) > max_request:
                # Too large to fingerprint; bypass idempotency entirely.
                _logger.warning(
                    "idemkit: request body exceeded max_request_body_bytes=%s; "
                    "bypassing idempotency for this request.",
                    max_request,
                )
                return await func(*args, **kwargs)

            neutral = self._neutral_request(request, body)
            outcome = await self.engine.decide(neutral)

            if outcome.kind == "replay":
                assert outcome.response is not None
                return _to_response(outcome.response)

            if outcome.kind == "reject":
                # The per-route decorator raises typed exceptions (§6.3) so your
                # code can catch and branch on them. Register
                # ``idempotency_problem_handler`` to render them as the same
                # problem+json the middleware would return.
                _raise_for_reject(outcome)

            if outcome.effective_key is None or outcome.claim_token is None:
                # Idempotency does not apply (method not in scope, or no key).
                return await func(*args, **kwargs)

            try:
                result = await func(*args, **kwargs)
            except Exception:
                await self.engine.on_error(outcome.effective_key, outcome.claim_token)
                raise

            response = _require_response(result)
            captured, bypass = _capture(response, self.config.max_body_bytes)
            if captured is None:
                # Streaming or oversized response: deliver it, don't cache it.
                if bypass is not None:
                    response.headers["idempotency-replay-unavailable"] = bypass
                await self.engine.on_error(outcome.effective_key, outcome.claim_token)
                return response

            await self.engine.on_complete(outcome.effective_key, outcome.claim_token, captured)
            return response

        return wrapper

    # Allow `@idem` as shorthand for `@idem.protect`.
    __call__ = protect

    def _neutral_request(self, request: Any, body: bytes) -> NeutralRequest:
        if self.config.key is not None:
            try:
                key = self.config.key(request)
            except Exception:
                _logger.exception("idemkit: key raised; treating as no key")
                key = None
        else:
            key = _unwrap_sf_string(request.headers.get("idempotency-key"))

        if self.config.scope is None:
            identity: str | None = None  # only reachable in optional mode
        else:
            try:
                identity = self.config.scope(request) or None
            except Exception:
                _logger.exception("idemkit: scope raised; treating as anonymous")
                identity = None

        return NeutralRequest(
            idempotency_key=key,
            scope=identity,
            method=request.method,
            path=request.url.path,
            query=request.url.query,
            body=body,
            content_type=request.headers.get("content-type"),
        )


def _raise_for_reject(outcome: EngineOutcome) -> None:
    """Map a reject outcome to a typed exception carrying its problem+json (§6.3)."""
    assert outcome.response is not None
    problem = (
        outcome.response.status,
        dict(outcome.response.headers),
        outcome.response.body,
    )
    reason = outcome.reject_reason
    if reason == "payload_mismatch":
        exc: IdempotencyError = PayloadMismatch(
            "Idempotency key was reused with a different payload. Resend the "
            "original payload or use a new key; the stored result is not replayed."
        )
    elif reason == "in_progress":
        exc = IdempotencyConflict(
            "Another request with this idempotency key is in progress and did not "
            "finish within the wait timeout. Retry with bounded backoff."
        )
    elif reason == "missing_key":
        exc = IdempotencyKeyMissing("This endpoint requires a valid Idempotency-Key header.")
    elif reason == "storage_error":
        exc = StorageUnavailable(
            "The idempotency backend is unavailable and the policy is fail_closed. "
            "Retry after a short backoff."
        )
    else:  # identity_unavailable — a server-side configuration problem
        exc = ConfigurationError(
            "The scope extractor produced no identity, so idempotency "
            "was refused to preserve cross-tenant isolation. Fix the extractor."
        )
    exc.problem = problem
    raise exc


def idempotency_problem_handler(request: Any, exc: Exception) -> Any:
    """Starlette/FastAPI exception handler that renders an idemkit typed
    exception as the same RFC 9457 problem+json the middleware would return.

    Install it once so the decorator's typed exceptions still reach the client
    as proper HTTP responses::

        from idemkit.adapters.route import idempotency_problem_handler
        from idemkit.core.exceptions import IdempotencyError

        app.add_exception_handler(IdempotencyError, idempotency_problem_handler)

    Exceptions without a wire mapping (``problem is None``) are re-raised so the
    framework's default handling applies.
    """
    from starlette.responses import Response

    problem = getattr(exc, "problem", None)
    if problem is None:
        raise exc
    status, headers, body = problem
    return Response(content=body, status_code=status, headers=dict(headers))


def _find_request(args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any | None:
    from starlette.requests import Request

    for value in (*kwargs.values(), *args):
        if isinstance(value, Request):
            return value
    return None


def _require_response(result: Any) -> Any:
    from starlette.responses import Response

    if isinstance(result, Response):
        return result
    raise TypeError(
        "idemkit: an idempotency-protected endpoint must return a "
        "Starlette/FastAPI Response (e.g. JSONResponse(...)), not "
        f"{type(result).__name__}. Returning a dict/model relies on FastAPI "
        "serialization that runs after this decorator and can't be captured. "
        "Return a Response here, or use IdempotencyMiddleware app-wide."
    )


def _capture(response: Any, max_body_bytes: int) -> tuple[NeutralResponse | None, str | None]:
    body = getattr(response, "body", None)
    if body is None:
        # No materialized body (e.g. StreamingResponse) → bypass cache.
        return None, "streaming"
    if len(body) > max_body_bytes:
        return None, "size-exceeded"
    headers = dict(response.headers.items())
    return (
        NeutralResponse(status=response.status_code, headers=headers, body=bytes(body)),
        None,
    )


def _to_response(neutral: NeutralResponse) -> Any:
    from starlette.responses import Response

    return Response(
        content=neutral.body,
        status_code=neutral.status,
        headers=dict(neutral.headers),
    )
