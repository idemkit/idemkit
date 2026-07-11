"""Reusable core-policy config shared by the queue and method surfaces.

The queue consumer and the ``@idempotent`` decorator take a handful of core-policy
keywords (lease, wait, retention, storage-error policy, local cache, events). When
you have many of them, repeating those keywords everywhere is noise. Build one
:class:`IdempotencyPolicy` and pass it as ``config=`` to every consumer/decorator;
a keyword passed directly still overrides the policy for that one call.

(The HTTP middleware has its own richer :class:`~idemkit.core.config.IdempotencyConfig`,
which already serves the same reuse purpose there.)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from idemkit.core.events import EventHandler


class _Unset:
    """Sentinel for a keyword that was not passed, so a policy can supply it."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<use default>"


# Typed as Any so a signature can default a real-typed keyword to it without
# upsetting type checkers, while the runtime can still detect "not passed".
UNSET: Any = _Unset()


@dataclass(frozen=True)
class IdempotencyPolicy:
    """Core-policy settings, bundled so you set them once and reuse them.

    ``lease_ttl_seconds`` and ``wait_timeout_seconds`` default to ``None``, which
    means "use each surface's own default"; set them to pin a value everywhere.
    The rest carry the same defaults as the individual keywords.
    """

    lease_ttl_seconds: float | None = None
    wait_timeout_seconds: float | None = None
    expires_after_seconds: float = 86_400.0
    on_storage_error: Literal["fail_closed", "fail_open"] = "fail_closed"
    use_local_cache: bool = False
    local_cache_max_items: int = 1024
    event_handlers: tuple[EventHandler, ...] = ()


def pick(explicit: Any, config: IdempotencyPolicy | None, field: str, default: Any) -> Any:
    """Resolve one core-policy value: an explicit keyword wins, then the policy,
    then the surface default. A policy field of ``None`` means "inherit the
    surface default" (used by lease/wait so a policy need not pin them)."""
    if explicit is not UNSET:
        return explicit
    if config is not None:
        value = getattr(config, field)
        if value is not None:
            return value
    return default
