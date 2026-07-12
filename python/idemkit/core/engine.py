"""HTTP surface adapter over the surface-neutral core (spec §6, Appendix A §4).

The engine is the *HTTP* user of ``IdempotentCore``. It owns everything that is
specific to HTTP — method scope, key validity (400s), caller-identity scoping
and its fail-closed rule, Problem Details, the replay header, response size and
redaction — and delegates the distributed-correctness state machine (claim →
in-flight wait → fenced complete/release + observability) to the core.

The adapter contract is unchanged: ``decide`` returns an ``EngineOutcome`` that
tells the ASGI/route adapter whether to run the handler, replay a stored
response, or reject; after the handler runs the adapter calls ``on_complete`` or
``on_error``. This inverted shape (decide → run → callback) is required because
the ASGI middleware structurally wraps the handler; it is why HTTP uses
``decide``/``complete`` rather than the core's ``run_once`` convenience.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Literal

from idemkit import problem_details
from idemkit.backends.base import IdempotencyBackend
from idemkit.core.config import IdempotencyConfig
from idemkit.core.events import EventEmitter
from idemkit.core.fingerprint import (
    FINGERPRINT_VERSION,
    compute_effective_key,
    compute_fingerprint,
)
from idemkit.core.runner import CoreOutcome, IdempotentCore, StoredResult

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class NeutralRequest:
    """Framework-neutral request handed to the engine by an adapter."""

    idempotency_key: str | None
    scope: str | None
    method: str
    path: str
    query: str
    body: bytes
    content_type: str | None


@dataclass(frozen=True)
class NeutralResponse:
    """Framework-neutral response (either captured from handler or built by engine)."""

    status: int
    headers: dict[str, str] = field(default_factory=dict)
    body: bytes = b""


@dataclass(frozen=True)
class EngineOutcome:
    """What the engine tells the adapter to do."""

    kind: Literal["pass_through", "replay", "reject"]
    response: NeutralResponse | None = None
    # For pass_through with idempotency applied, the adapter needs these to
    # call on_complete / on_error after the handler runs.
    effective_key: str | None = None
    claim_token: str | None = None
    # On a reject, which case it was — so the per-route decorator can raise the
    # matching typed exception (§6.3). The app-wide middleware ignores this and
    # uses ``response`` (the problem+json), keeping Appendix A unchanged.
    reject_reason: (
        Literal[
            "missing_key",
            "payload_mismatch",
            "in_progress",
            "storage_error",
            "identity_unavailable",
        ]
        | None
    ) = None


_ANON_IDENTITY = "_idemkit_anonymous"


class IdempotencyEngine:
    """The HTTP state machine. One engine per middleware instance."""

    def __init__(self, backend: IdempotencyBackend, config: IdempotencyConfig) -> None:
        self.backend = backend
        self.config = config
        self.backend_name = type(backend).__name__
        self.emitter = EventEmitter(config.event_handlers)
        self.core = IdempotentCore(
            backend,
            emitter=self.emitter,
            backend_name=self.backend_name,
            surface="http",
            on_storage_error=config.on_storage_error,
            lease_ttl_seconds=config.lease_ttl_seconds,
            wait_timeout_seconds=config.wait_timeout_seconds,
            expires_after_seconds=config.expires_after_seconds,
            fingerprint_version=FINGERPRINT_VERSION,
            use_local_cache=config.use_local_cache,
            local_cache_max_items=config.local_cache_max_items,
        )

    # ----- Decision phase: called before handler runs -----

    async def decide(self, request: NeutralRequest) -> EngineOutcome:
        # Method scope (§4.2)
        if request.method.upper() not in self.config.applicable_methods:
            return EngineOutcome(kind="pass_through")

        # Key presence / validity (§6.1)
        key = request.idempotency_key
        if key is None or len(key) == 0:
            if self.config.require_key_for_mutations:
                return EngineOutcome(
                    kind="reject",
                    response=_problem_to_neutral(problem_details.missing_key()),
                    reject_reason="missing_key",
                )
            return EngineOutcome(kind="pass_through")

        key_bytes = key.encode("utf-8")
        if len(key_bytes) > 255:
            return EngineOutcome(
                kind="reject",
                response=_problem_to_neutral(problem_details.key_too_long()),
                reject_reason="missing_key",
            )

        # Caller identity (§4.6 cross-tenant scoping).
        scope = request.scope
        if not scope:
            if self.config.scope is None:
                # No extractor configured at all: single-tenant mode. The loud
                # warning was emitted at config construction; here we just use
                # the shared namespace deliberately.
                scope = _ANON_IDENTITY
            else:
                # An extractor IS configured but produced no identity for this
                # request (returned None/empty or raised). Silently bucketing an
                # authenticated app's request into the shared namespace is the
                # cross-tenant leak §4.6 prevents, so fail closed.
                _logger.error(
                    "idemkit: scope extractor produced no identity for a "
                    "request; refusing it to preserve cross-tenant isolation (§4.6). "
                    "Fix the extractor so it always returns a stable, non-empty value."
                )
                return EngineOutcome(
                    kind="reject",
                    response=_problem_to_neutral(problem_details.identity_unavailable()),
                    reject_reason="identity_unavailable",
                )

        effective_key = compute_effective_key(key, scope, request.method, request.path)
        if self.config.body_fingerprint is not None:
            # Selector: fingerprint exactly the bytes it returns, opaque
            # (content_type=None), so the caller fully controls what matters.
            fp_body = self.config.body_fingerprint(request.body, request.content_type)
            fp_content_type: str | None = None
        else:
            fp_body, fp_content_type = request.body, request.content_type
        fingerprint = compute_fingerprint(
            request.method,
            request.path,
            request.query,
            fp_body,
            fp_content_type,
        )

        # Hand off to the surface-neutral state machine.
        decision = await self.core.decide(effective_key, fingerprint)

        if decision.outcome is CoreOutcome.PROCEED:
            return EngineOutcome(
                kind="pass_through",
                effective_key=effective_key,
                claim_token=decision.claim_token,
            )

        if decision.outcome is CoreOutcome.REPLAY:
            assert decision.stored is not None
            return EngineOutcome(
                kind="replay",
                response=self._build_replay_response(decision.stored),
            )

        if decision.outcome is CoreOutcome.MISMATCH:
            return EngineOutcome(
                kind="reject",
                response=_problem_to_neutral(
                    problem_details.payload_mismatch(self.config.is_stripe_compat)
                ),
                reject_reason="payload_mismatch",
            )

        if decision.outcome is CoreOutcome.CONFLICT:
            retry_after = math.ceil(self.config.lease_ttl_seconds)
            return EngineOutcome(
                kind="reject",
                response=_problem_to_neutral(
                    problem_details.in_progress(self.config.is_stripe_compat, retry_after)
                ),
                reject_reason="in_progress",
            )

        if decision.outcome is CoreOutcome.STORAGE_REFUSE:
            return EngineOutcome(
                kind="reject",
                response=_problem_to_neutral(problem_details.storage_error()),
                reject_reason="storage_error",
            )

        # STORAGE_PASS: storage is down and fail_open — run the handler this once
        # without idempotency protection.
        return EngineOutcome(kind="pass_through")

    # ----- Completion phase: called after handler runs -----

    async def on_complete(
        self,
        effective_key: str,
        claim_token: str,
        response: NeutralResponse,
    ) -> None:
        """Adapter hook: handler finished with a response.

        Cacheable → store; non-cacheable → release.
        """
        if response.status not in self.config.cacheable_status:
            await self.core.release(effective_key, claim_token)
            return

        if len(response.body) > self.config.max_body_bytes:
            # Response exceeds size cap; do not store. Adapter has already
            # sent the response to the client with the bypass header.
            await self.core.release(effective_key, claim_token)
            return

        redacted = self._apply_redactor(response.status, response.body, response.headers)
        if redacted is None:
            # Redactor failed. Persisting the unredacted response could leak the
            # very data the redactor was configured to scrub (§4.14, §9.1 PCI).
            # Release instead so a retry re-executes — never store unredacted.
            await self.core.release(effective_key, claim_token)
            return
        body, headers = redacted
        headers = _filter_headers(headers, self.config)

        await self.core.complete(
            effective_key,
            claim_token,
            StoredResult(blob=body, meta=headers, marker=response.status),
        )

    async def on_error(self, effective_key: str, claim_token: str) -> None:
        """Adapter hook: handler raised or client disconnected.

        Spec §4.4: release the claim. (Handler cancellation policy:
        idemkit does NOT cancel the coroutine.)
        """
        await self.core.release(effective_key, claim_token)

    # ----- internals -----

    def _apply_redactor(
        self, status: int, body: bytes, headers: dict[str, str]
    ) -> tuple[bytes, dict[str, str]] | None:
        """Run the redactor on a copy. Returns the redacted ``(body, headers)``,
        or ``None`` to signal "do not store" when the redactor raises.
        """
        if self.config.response_redactor is None:
            return body, headers
        try:
            new_body, new_headers = self.config.response_redactor(body, dict(headers), status)
            return new_body, new_headers
        except Exception:
            # Redactor errors MUST NOT crash the request path AND MUST NOT result
            # in unredacted data being persisted. Signal "do not store"; the
            # caller releases the claim so a retry re-executes (§4.14).
            _logger.exception(
                "idemkit: response_redactor raised; response will NOT be cached "
                "to avoid persisting unredacted data (§4.14)."
            )
            return None

    def _build_replay_response(self, stored: StoredResult) -> NeutralResponse:
        headers = dict(stored.meta)
        if self.config.is_stripe_compat:
            headers["idempotent-replayed"] = "true"
        else:
            headers["idempotency-replayed"] = "true"
        body = stored.blob
        if self.config.response_hook is not None:
            try:
                body, headers = self.config.response_hook(body, dict(headers), stored.marker)
            except Exception:
                # A hook must never break a replay: log and serve the response
                # unmodified (still with the replayed marker header).
                _logger.exception(
                    "idemkit: response_hook raised; replaying the response unmodified."
                )
        return NeutralResponse(
            status=stored.marker,
            headers=headers,
            body=body,
        )


def _problem_to_neutral(problem: tuple[int, dict[str, str], bytes]) -> NeutralResponse:
    status, headers, body = problem
    return NeutralResponse(status=status, headers=headers, body=body)


def _filter_headers(headers: dict[str, str], config: IdempotencyConfig) -> dict[str, str]:
    """Apply header allow / deny lists per spec §4.10."""
    allow = config.effective_header_allow()
    deny = config.effective_header_deny()
    out: dict[str, str] = {}
    for name, value in headers.items():
        n = name.lower()
        if n in deny:
            continue
        if n in allow:
            out[n] = value
    return out
