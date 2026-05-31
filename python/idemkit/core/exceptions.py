"""Public exception types for idemkit."""

from __future__ import annotations


class IdempotencyError(Exception):
    """Base for all idemkit exceptions."""

    #: When raised from a decorator path, the RFC 9457 problem this maps to,
    #: as ``(status, headers, body)`` — so a framework exception handler can
    #: render the same problem+json the middleware would have returned. ``None``
    #: when the exception has no HTTP wire mapping (e.g. raised by the AI tool
    #: surface). Set by the per-route decorator; see ``adapters/route.py``.
    problem: tuple[int, dict[str, str], bytes] | None = None


class ConfigurationError(IdempotencyError):
    """Raised when ``IdempotencyConfig`` is incorrectly configured.

    Most commonly: ``scope`` is missing in production mode.
    See spec §4.6.
    """


class StorageError(IdempotencyError):
    """Raised by backend implementations when storage is unavailable.

    The middleware catches this and maps it to ``on_storage_error`` policy
    (spec §4.16): either ``fail_closed`` (503) or ``fail_open``.
    """


# ----- typed exceptions for the decorator paths (spec §6.3) -----
#
# The app-wide ASGI middleware sits above the handler and returns problem+json
# directly; it never raises into user code. The per-route and tool-call
# DECORATORS wrap the handler, so they raise these typed exceptions instead, and
# user code can catch and branch on them. Messages are written to be actionable
# (a hashed key + what to do), never a bare hash or a KeyError.


class IdempotencyConflict(IdempotencyError):
    """Another executor holds a live claim for this operation and it did not
    complete within the wait timeout (spec §6.3, the 423/409 case).

    Retry with bounded, capped exponential backoff — do NOT loop forever. Once
    the holder's lease expires the next attempt reclaims and runs.
    """


class PayloadMismatch(IdempotencyError):
    """The idempotency key was reused with a different payload (spec §6.3, the
    422/409 case). Do not retry as-is; resend the original payload or use a new
    key. The stored result is never replayed for a mismatched payload."""


class IdempotencyKeyMissing(IdempotencyError):
    """A required idempotency key was absent (spec §6.3, the 400 case)."""


class StorageUnavailable(IdempotencyError):
    """Storage was unreachable and the policy is ``fail_closed`` (spec §6.3, the
    503 case). Retry after a short backoff."""
