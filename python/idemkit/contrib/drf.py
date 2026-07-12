"""Django REST Framework idempotency, as a view mixin.

The WSGI middleware protects a whole Django app, but a DRF mixin lets you protect
*specific* views and read DRF's request (``request.user`` for ``scope``, ``request.data``
in the view). :func:`idempotent_view` returns a mixin class you put BEFORE the DRF
base class:

    from rest_framework.views import APIView
    from rest_framework.response import Response
    from idemkit import RedisBackend, HttpConfig
    from idemkit.contrib.drf import idempotent_view

    backend = RedisBackend.from_url("redis://localhost:6379")
    Idempotent = idempotent_view(
        backend=backend, config=HttpConfig(scope=lambda req: str(req.user.pk))
    )

    class ChargeView(Idempotent, APIView):
        def post(self, request):
            return Response({"charged": True}, status=201)

It claims the key after DRF authentication runs (so ``request.user`` is set), replays
the stored response on a duplicate, and releases the claim if the view raises. Works
on ``APIView`` and ``ViewSet`` (both share the ``initial`` / ``handle_exception`` /
``finalize_response`` lifecycle). Requires Django + DRF; this module imports neither
until a response is built.
"""

from __future__ import annotations

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
from idemkit.core.exceptions import ConfigurationError
from idemkit.core.policy import HttpConfig
from idemkit.core.sync_bridge import register_closable, run_sync


class _ShortCircuit(Exception):
    """Internal: raised from ``initial`` to return a replay/reject response without
    running the view. Caught in the mixin's ``handle_exception``."""

    def __init__(self, response: NeutralResponse) -> None:
        self.neutral = response


def idempotent_view(
    *,
    backend: IdempotencyBackend,
    config: HttpConfig | None = None,
) -> type:
    """Build a DRF view mixin that applies idempotency.

    Put the returned class BEFORE your DRF base (``class V(idempotent_view(...), APIView)``).
    If it is placed AFTER the base, the mixin raises at class-definition time rather
    than silently doing nothing. One engine is shared by every view using this mixin.
    """
    resolved = resolve_http_config(config)
    engine = IdempotencyEngine(backend=backend, config=resolved)
    # The Redis/Postgres pool binds to the sync-bridge loop; close it at exit.
    register_closable(backend)

    def _extract_key(request: Any) -> str | None:
        if resolved.key is not None:
            try:
                return resolved.key(request)
            except Exception:
                return None
        return _unwrap_sf_string(request.headers.get("Idempotency-Key"))

    def _extract_scope(request: Any) -> str | None:
        if resolved.scope is None:
            return None
        try:
            value = resolved.scope(request)
        except Exception:
            # A configured-but-failing extractor fails closed (engine rejects per
            # scope_mode); it never silently shares one namespace across tenants.
            return None
        return value or None

    def _neutral(request: Any) -> NeutralRequest:
        try:
            body = request.body  # cached, so the view's request.data still parses
        except Exception:
            body = b""
        return NeutralRequest(
            idempotency_key=_extract_key(request),
            scope=_extract_scope(request),
            method=request.method,
            path=request.path,
            query=request.META.get("QUERY_STRING", "") if hasattr(request, "META") else "",
            body=bytes(body) if isinstance(body, (bytes, bytearray)) else b"",
            content_type=getattr(request, "content_type", None),
        )

    class IdempotencyMixin:
        _idem_outcome: EngineOutcome | None = None
        _idemkit_drf_mixin = True

        def __init_subclass__(cls, **kwargs: Any) -> None:
            super().__init_subclass__(**kwargs)
            # Silent no-op guard: if a DRF base that defines `initial` precedes this
            # mixin in the MRO, our overrides never run and idempotency does nothing.
            # Fail at class-definition time instead of shipping unprotected.
            mro = cls.__mro__
            mixin_pos = next(
                (i for i, c in enumerate(mro) if c.__dict__.get("_idemkit_drf_mixin")), None
            )
            base_pos = next(
                (
                    i
                    for i, c in enumerate(mro)
                    if "initial" in c.__dict__ and not c.__dict__.get("_idemkit_drf_mixin")
                ),
                None,
            )
            if mixin_pos is not None and base_pos is not None and mixin_pos > base_pos:
                raise ConfigurationError(
                    f"idemkit: {cls.__name__} lists idempotent_view(...) AFTER its DRF base "
                    f"({mro[base_pos].__name__}), so the mixin's hooks are shadowed and "
                    "idempotency would silently do nothing. Put the mixin FIRST: "
                    "class V(idempotent_view(...), APIView)."
                )

        def initial(self, request: Any, *args: Any, **kwargs: Any) -> None:
            # Run DRF auth/permissions/throttling first, so scope can read request.user.
            super().initial(request, *args, **kwargs)  # type: ignore[misc]
            outcome = run_sync(engine.decide(_neutral(request)))
            self._idem_outcome = outcome
            if outcome.kind in ("replay", "reject"):
                assert outcome.response is not None
                raise _ShortCircuit(outcome.response)

        def handle_exception(self, exc: Any) -> Any:
            if isinstance(exc, _ShortCircuit):
                return _to_django_response(exc.neutral)
            # The view raised: release the claim so a retry re-runs (fail-closed).
            outcome = getattr(self, "_idem_outcome", None)
            if outcome is not None and outcome.kind == "pass_through" and outcome.claim_token:
                run_sync(engine.on_error(outcome.effective_key, outcome.claim_token))
                self._idem_outcome = None
            return super().handle_exception(exc)  # type: ignore[misc]

        def finalize_response(self, request: Any, response: Any, *args: Any, **kwargs: Any) -> Any:
            response = super().finalize_response(request, response, *args, **kwargs)  # type: ignore[misc]
            outcome = getattr(self, "_idem_outcome", None)
            if (
                outcome is not None
                and outcome.kind == "pass_through"
                and outcome.effective_key
                and outcome.claim_token
            ):
                response.render()  # DRF renders lazily; force it to capture the bytes
                captured = NeutralResponse(
                    status=response.status_code,
                    headers=dict(response.items()),
                    body=bytes(response.content),
                )
                run_sync(engine.on_complete(outcome.effective_key, outcome.claim_token, captured))
                self._idem_outcome = None
            return response

    return IdempotencyMixin


def _to_django_response(neutral: NeutralResponse) -> Any:
    from django.http import HttpResponse

    response = HttpResponse(content=neutral.body, status=neutral.status)
    for name, value in neutral.headers.items():
        response[name] = value
    return response
