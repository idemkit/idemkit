"""FastAPI-native idempotency via a custom route class.

The app-wide :class:`~idemkit.IdempotencyMiddleware` already works with FastAPI, but
a route class fits FastAPI better in two ways:

* your handler can return a ``dict`` / Pydantic model (FastAPI serializes it, then
  idemkit captures the serialized bytes for replay), so you don't rewrite endpoints
  to return ``JSONResponse``;
* your ``scope`` / ``key`` callables get the real ``Request``, so ``request.state``
  works (the middleware only exposes a lightweight proxy).

Use it per-router or app-wide::

    from fastapi import APIRouter
    from idemkit import RedisBackend, HttpConfig
    from idemkit.contrib.fastapi import idempotent_route

    backend = RedisBackend.from_url("redis://localhost:6379")
    router = APIRouter(
        route_class=idempotent_route(
            backend=backend, config=HttpConfig(scope=lambda req: req.state.user_id)
        )
    )


    @router.post("/charge")
    async def charge(order: Order) -> dict:
        return {"charged": True}  # a dict is fine; idemkit captures the serialized body

For the whole app: ``app.router.route_class = idempotent_route(backend=backend, config=config)``.
Non-applicable methods (GET by default) and keyless requests pass straight through,
so the class is safe to apply broadly. Requires FastAPI.
"""

from __future__ import annotations

from typing import Any

from idemkit.backends.base import IdempotencyBackend
from idemkit.core.config import resolve_http_config
from idemkit.core.engine import IdempotencyEngine, NeutralRequest, NeutralResponse
from idemkit.core.policy import HttpConfig


def idempotent_route(
    *,
    backend: IdempotencyBackend,
    config: HttpConfig | None = None,
) -> type:
    """Build a FastAPI ``APIRoute`` subclass that applies idempotency.

    Pass it as ``route_class=`` to an ``APIRouter`` (or assign
    ``app.router.route_class``). One engine is shared by every route on the router.
    """
    try:
        from fastapi import Request
        from fastapi.routing import APIRoute
    except ImportError as e:  # pragma: no cover - exercised via the error message
        raise ImportError("idemkit: install FastAPI: pip install fastapi") from e

    from idemkit.adapters.asgi import _unwrap_sf_string

    resolved = resolve_http_config(config)
    engine = IdempotencyEngine(backend=backend, config=resolved)

    def _extract_key(request: Request) -> str | None:
        if resolved.key is not None:
            try:
                return resolved.key(request)
            except Exception:
                return None
        return _unwrap_sf_string(request.headers.get("idempotency-key"))

    def _extract_scope(request: Request) -> str | None:
        if resolved.scope is None:
            return None
        try:
            value = resolved.scope(request)
        except Exception:
            # A configured-but-failing extractor fails closed: the engine sees no
            # identity and rejects per scope_mode (never buckets as anonymous).
            return None
        return value or None

    class IdempotentAPIRoute(APIRoute):
        def get_route_handler(self) -> Any:
            original = super().get_route_handler()

            async def handler(request: Request) -> Any:
                body = await request.body()  # cached, so the handler still parses it
                neutral = NeutralRequest(
                    idempotency_key=_extract_key(request),
                    scope=_extract_scope(request),
                    method=request.method,
                    path=request.url.path,
                    query=request.url.query,
                    body=body,
                    content_type=request.headers.get("content-type"),
                )
                outcome = await engine.decide(neutral)
                if outcome.kind in ("replay", "reject"):
                    assert outcome.response is not None
                    return _to_response(outcome.response)
                if outcome.effective_key is None or outcome.claim_token is None:
                    return await original(request)  # idempotency does not apply
                try:
                    response = await original(request)
                except Exception:
                    await engine.on_error(outcome.effective_key, outcome.claim_token)
                    raise
                captured = _capture(response)
                if captured is None:
                    # A streaming/file response has no buffered body to cache;
                    # release so a retry re-runs (the client already got the bytes).
                    await engine.on_error(outcome.effective_key, outcome.claim_token)
                    return response
                # on_complete enforces max_body_bytes and cacheable_status: an
                # oversized or non-cacheable response is released, not stored.
                await engine.on_complete(outcome.effective_key, outcome.claim_token, captured)
                return response

            return handler

    return IdempotentAPIRoute


def _to_response(neutral: NeutralResponse) -> Any:
    from starlette.responses import Response

    return Response(content=neutral.body, status_code=neutral.status, headers=neutral.headers)


def _capture(response: Any) -> NeutralResponse | None:
    body = getattr(response, "body", None)
    if not isinstance(body, (bytes, bytearray)):
        return None  # streaming / file response: no buffered body to store
    return NeutralResponse(
        status=response.status_code,
        headers=dict(response.headers),
        body=bytes(body),
    )
