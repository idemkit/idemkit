"""One flat config object per surface: ``HttpConfig`` / ``QueueConfig`` / ``MethodConfig``.

Each surface is configured with exactly one of these objects, passed as ``config=``.
There are no loose keyword overrides: the config is the one way in. To reuse settings
across surfaces or call sites, write a small factory that returns the config you want
(there is no separate shared object to learn).

The datastore is configured separately, on the backend (``PostgresBackend(table=...)``,
``RedisBackend.from_url(url, namespace=...)``): the backend is WHERE dedup state
lives; the config is HOW a duplicate is judged and replayed.

Surface-option fields are typed loosely here (this module cannot import the HTTP
config's aliases without a cycle); the adapters validate them.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from idemkit.core.events import EventHandler


class _Unset:
    """Sentinel for a keyword that was not passed, so a config can supply it."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<use default>"


# Typed as Any so a signature can default a real-typed keyword to it without
# upsetting type checkers, while the runtime can still detect "not passed".
UNSET: Any = _Unset()


# The options shared by every surface (lease/wait default to None = "use the
# surface's own default"). They are inlined flat into each surface config below.
@dataclass(frozen=True)
class MethodConfig:
    """Config for ``@idempotent`` / ``idempotent_sync`` (flat: all options on one
    object). Pass ``config=MethodConfig(...)``."""

    # method-specific
    key_fields: list[str] | None = None
    scope: Callable[[Any], Any] | None = None
    validation_fingerprint: Callable[[Any], bytes] | None = None
    normalize_args: Callable[[Any], Any] | None = None
    result_codec: Any = "json"
    strict_keys: bool = True
    # When True, refuse to derive the key from all arguments: the call must name its
    # key via key_fields, normalize_args, or a per-call idempotency_key (else raises
    # IdempotencyKeyMissing). The enforcing analogue of Powertools raise_on_no_idempotency_key.
    require_key: bool = False
    # Exception types that are DETERMINISTIC: instead of releasing the claim (retry
    # re-runs), cache the exception and re-raise it on a duplicate. Default: cache
    # nothing (every raise is treated as a transient crash and re-run).
    cache_exceptions: tuple[type[BaseException], ...] = ()
    version: str = "1"
    # shared
    lease_ttl_seconds: float | None = None
    wait_timeout_seconds: float | None = None
    expires_after_seconds: float = 86_400.0
    on_storage_error: Literal["fail_closed", "fail_open"] = "fail_closed"
    use_local_cache: bool = False
    local_cache_max_items: int = 1024
    event_handlers: tuple[EventHandler, ...] = ()


@dataclass(frozen=True)
class QueueConfig:
    """Config for ``IdempotentConsumer`` (flat). ``dedup_id`` (how to read the broker's
    dedup id from a message) and ``visibility_timeout_seconds`` (the broker's window)
    are required for a direct consumer; the ``contrib`` helpers (sqs/kafka) preset
    ``dedup_id`` for you."""

    # required wiring
    dedup_id: Callable[[Any], str] | None = None
    visibility_timeout_seconds: float | None = None
    # queue-specific
    scope: Callable[[Any], Any] | None = None
    max_attempts: int = 5
    on_exhausted: Callable[[Any, Any], Any] | None = None
    receive_count: Callable[[Any], Any] | None = None
    attempt_store: Any | None = None
    cache_result: bool = False
    result_codec: Any | None = None
    # A field that must match on a dedup-id hit but isn't part of the key: catches a
    # producer that reused a message id with a different body (raises PayloadMismatch).
    validation_fingerprint: Callable[[Any], bytes] | None = None
    # shared
    lease_ttl_seconds: float | None = None
    wait_timeout_seconds: float | None = None
    expires_after_seconds: float = 86_400.0
    on_storage_error: Literal["fail_closed", "fail_open"] = "fail_closed"
    use_local_cache: bool = False
    local_cache_max_items: int = 1024
    event_handlers: tuple[EventHandler, ...] = ()


@dataclass(frozen=True)
class HttpConfig:
    """Config for the HTTP middleware / route decorator (flat). ``None`` fields fall
    back to the HTTP defaults."""

    # http-specific
    scope: Callable[[Any], str] | None = None
    scope_mode: Literal["warn", "single_tenant", "strict"] = "warn"
    key: Callable[[Any], str | None] | None = None
    require_key_for_mutations: bool = False
    applicable_methods: set[str] | None = None
    body_fingerprint: Callable[[bytes, Any], bytes] | None = None
    response_redactor: Any | None = None
    response_hook: Any | None = None
    header_allow: set[str] | None = None
    header_deny: set[str] | None = None
    cacheable_status: set[int] | None = None
    max_body_bytes: int | None = None
    max_request_body_bytes: int | None = None
    compat_mode: Literal["default", "stripe"] = "default"
    # shared
    lease_ttl_seconds: float | None = None
    wait_timeout_seconds: float | None = None
    expires_after_seconds: float = 86_400.0
    on_storage_error: Literal["fail_closed", "fail_open"] = "fail_closed"
    use_local_cache: bool = False
    local_cache_max_items: int = 1024
    event_handlers: tuple[EventHandler, ...] = ()


def pick(explicit: Any, config: Any, field_name: str, default: Any) -> Any:
    """Resolve one value: an explicit keyword wins, then the config field, then the
    default. A config field of ``None`` means "inherit the default". ``config`` is a
    flat surface config or ``None``."""
    if explicit is not UNSET:
        return explicit
    if config is not None:
        value = getattr(config, field_name)
        if value is not None:
            return value
    return default
