"""Public configuration for idemkit middleware, per spec §4.2 / §4.6 / §4.8 / §4.16."""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Literal

from idemkit.core.events import EventHandler
from idemkit.core.exceptions import ConfigurationError

_logger = logging.getLogger(__name__)


# Type aliases for the pluggable hooks (§4.7, §4.14).
# Adapters pass framework-specific request types here.
CallerIdentityExtractor = Callable[[Any], str]
KeyExtractor = Callable[[Any], "str | None"]
ResponseRedactor = Callable[[bytes, dict[str, str], int], "tuple[bytes, dict[str, str]]"]
# (replay body, replay headers, status) -> the (body, headers) to send to the
# DUPLICATE. Runs ONLY when replaying a stored response, so you can add a header,
# tweak Cache-Control, and so on. Never runs on the first (live) request.
ResponseHook = Callable[[bytes, dict[str, str], int], "tuple[bytes, dict[str, str]]"]
# (raw request body, content-type) -> the bytes to fingerprint. Lets you select
# which body fields matter (drop volatile timestamps/nonces). Returns the
# canonical bytes idemkit hashes; return b"" to ignore the body entirely (§4.5).
BodyFingerprint = Callable[[bytes, "str | None"], bytes]


DEFAULT_HEADER_ALLOW: frozenset[str] = frozenset(
    {
        "content-type",
        "content-encoding",
        "content-language",
        "content-disposition",
        "location",
        "etag",
        "last-modified",
        "link",
        "cache-control",
    }
)

DEFAULT_HEADER_DENY: frozenset[str] = frozenset(
    {
        # Privacy / auth sensitive
        "set-cookie",
        "authorization",
        "www-authenticate",
        # Vary (idemkit is not Vary-aware on replay)
        "vary",
        # Hop-by-hop per RFC 7230 §6.1
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailers",
        "transfer-encoding",
        "upgrade",
    }
)


@dataclass
class IdempotencyConfig:
    """Internal settings container for the HTTP middleware/decorator.

    Built from the flat keyword arguments passed to ``IdempotencyMiddleware`` /
    ``Idempotency``; users configure those surfaces with plain keywords and do not
    construct this object directly. Holds the core policy (TTLs, lease, storage-error
    policy, local cache, observability) plus the HTTP-specific options.
    """

    # ── Core policy ──
    lease_ttl_seconds: float = 30.0
    wait_timeout_seconds: float = 10.0
    expires_after_seconds: float = 86_400.0
    on_storage_error: Literal["fail_closed", "fail_open"] = "fail_closed"
    use_local_cache: bool = False
    local_cache_max_items: int = 1024
    event_handlers: list[EventHandler] = field(default_factory=list)

    # ── HTTP-specific ──

    # Methods on which idempotency applies (§4.2 method scope)
    applicable_methods: set[str] = field(default_factory=lambda: {"POST", "PATCH"})

    # Response body size cap (§4.9)
    max_body_bytes: int = 1024 * 1024  # 1 MiB

    # Request body buffering cap. The middleware buffers the request body in
    # memory to fingerprint it; this bounds that buffer to prevent a
    # large-request memory DoS. A request whose body exceeds this is streamed
    # to the handler WITHOUT idempotency (it is not deduplicated). Set to
    # None to disable the cap (unbounded buffering — not recommended).
    max_request_body_bytes: int | None = 1024 * 1024  # 1 MiB

    # Cacheable status policy (§4.2): 5xx never cached by default
    cacheable_status: set[int] = field(default_factory=lambda: {200, 201, 202})

    # Wire-compatibility mode (§6.3.1).
    #   "default" — 422 on payload mismatch, 423 on in-flight timeout, and the
    #               Idempotency-Replayed header.
    #   "stripe"  — 409 for both, and the Idempotent-Replayed header (Stripe's
    #               deployed spelling), for drop-in compatibility with clients
    #               written against Stripe's idempotency format.
    compat_mode: Literal["default", "stripe"] = "default"

    # Required-key enforcement (§6.1)
    require_key_for_mutations: bool = False

    # Request-body fingerprint (§4.5). The body is part of the FINGERPRINT (not the
    # idempotency key): reusing a key with a different body is detected and rejected
    # (422/409), so the wrong stored response is never replayed.
    #   None (default) — fingerprint the whole body.
    #   a callback ``body_fingerprint(raw_body, content_type) -> bytes`` —
    #     fingerprint only what it returns, so you keep the fields that define the
    #     operation and ignore volatile ones (a client timestamp, a nonce). Return
    #     canonical bytes (e.g. ``json.dumps({...}, sort_keys=True).encode()``), or
    #     ``b""`` to ignore the body entirely (dedupe on key + caller + method + path).
    body_fingerprint: BodyFingerprint | None = None

    # Pluggable extraction (§4.7) and cross-tenant scoping (§4.6).
    # With no `scope`, idemkit runs in SINGLE-TENANT mode (all callers share one
    # namespace). scope_mode picks what happens then:
    #   "warn"          (default) run single-tenant and log a loud warning;
    #   "single_tenant" run single-tenant, no warning (you acknowledged it);
    #   "strict"        raise ConfigurationError if no scope (CI/production gate).
    scope: CallerIdentityExtractor | None = None
    scope_mode: Literal["warn", "single_tenant", "strict"] = "warn"
    key: KeyExtractor | None = None

    # PII redaction (§4.14)
    response_redactor: ResponseRedactor | None = None

    # Replay response hook: modify the response served to a DUPLICATE (runs only
    # on replay, never on the first request).
    response_hook: ResponseHook | None = None

    # Header allow/deny lists (§4.10) — None falls back to module defaults
    header_allow: set[str] | None = None
    header_deny: set[str] | None = None

    def __post_init__(self) -> None:
        # Normalize methods
        self.applicable_methods = {m.upper() for m in self.applicable_methods}

        if self.compat_mode not in ("default", "stripe"):
            raise ConfigurationError(
                f"idemkit: compat_mode must be 'default' or 'stripe', got {self.compat_mode!r}."
            )

        if self.scope_mode not in ("warn", "single_tenant", "strict"):
            raise ConfigurationError(
                f"idemkit: scope_mode must be 'warn', 'single_tenant', or "
                f"'strict', got {self.scope_mode!r}."
            )

        # Cross-tenant scoping (§4.6). Default: single-tenant + loud warning.
        if self.scope is None:
            if self.scope_mode == "strict":
                raise ConfigurationError(
                    "idemkit: scope_mode='strict' requires a `scope` extractor. "
                    "Provide one, or use scope_mode='single_tenant' to run "
                    "single-tenant on purpose. See spec §4.6."
                )
            if self.scope_mode == "warn":
                _logger.warning(
                    "idemkit: running single-tenant — no `scope` is set, so all "
                    "callers share one idempotency namespace. That's fine for local "
                    "dev and single-tenant services; if you have more than one user "
                    "or tenant, set `scope` so they don't collide. Silence this with "
                    "scope_mode='single_tenant', or make it a hard error with "
                    "scope_mode='strict'. See spec §4.6."
                )

        # Lease sanity warning (§4.8)
        if self.lease_ttl_seconds < 5.0:
            _logger.warning(
                "idemkit: lease_ttl_seconds=%.2f is very low; if your handler p99 "
                "exceeds this, claims will expire mid-handler and adverse reclaim "
                "races may occur. See spec §4.8.",
                self.lease_ttl_seconds,
            )

    @property
    def is_stripe_compat(self) -> bool:
        """True when the Stripe wire-compatibility mode is active (§6.3.1)."""
        return self.compat_mode == "stripe"

    def effective_header_allow(self) -> frozenset[str]:
        if self.header_allow:
            return frozenset(h.lower() for h in self.header_allow)
        return DEFAULT_HEADER_ALLOW

    def effective_header_deny(self) -> frozenset[str]:
        if self.header_deny:
            return frozenset(h.lower() for h in self.header_deny)
        return DEFAULT_HEADER_DENY


def resolve_http_config(config: Any) -> IdempotencyConfig:
    """Build the HTTP config from an ``HttpConfig`` (flat, widened with the HTTP
    defaults) or the internal ``IdempotencyConfig``.
    """
    from idemkit.core.policy import HttpConfig

    if config is None:
        return IdempotencyConfig()

    if isinstance(config, HttpConfig):
        # A flat HttpConfig. lease/wait default to None ("use the HTTP default");
        # other None fields fall back to the IdempotencyConfig defaults (not passed).
        data: dict[str, Any] = {
            "lease_ttl_seconds": (
                config.lease_ttl_seconds if config.lease_ttl_seconds is not None else 30.0
            ),
            "wait_timeout_seconds": (
                config.wait_timeout_seconds if config.wait_timeout_seconds is not None else 10.0
            ),
            "expires_after_seconds": config.expires_after_seconds,
            "on_storage_error": config.on_storage_error,
            "use_local_cache": config.use_local_cache,
            "local_cache_max_items": config.local_cache_max_items,
            "event_handlers": list(config.event_handlers),
            "scope": config.scope,
            "scope_mode": config.scope_mode,
            "key": config.key,
            "require_key_for_mutations": config.require_key_for_mutations,
            "body_fingerprint": config.body_fingerprint,
            "response_redactor": config.response_redactor,
            "response_hook": config.response_hook,
            "compat_mode": config.compat_mode,
        }
        for f in (
            "applicable_methods",
            "header_allow",
            "header_deny",
            "cacheable_status",
            "max_body_bytes",
            "max_request_body_bytes",
        ):
            v = getattr(config, f)
            if v is not None:
                data[f] = v
        return IdempotencyConfig(**data)

    if not isinstance(config, IdempotencyConfig):
        raise TypeError(
            "idemkit: config= must be an HttpConfig or IdempotencyConfig, "
            f"got {type(config).__name__}."
        )
    return config
