"""Serialize and rebuild a cached handler exception (deterministic-error replay).

By default idemkit treats every raised exception as a crash: it releases the claim
so a retry re-runs the handler. But some failures are *deterministic* (a validation
error, a business-rule rejection) and re-running only re-fails. When the caller
declares such types via ``cache_exceptions=(...)``, idemkit stores the exception and
replays it: a duplicate re-raises the same error instead of running the side effect
again.

Only the type path and message are stored, never a pickled object (unsafe, and it
rarely round-trips). On replay the original type is reinstantiated with the message
when the type is *already imported* and constructible from a single string; else a
:class:`~idemkit.core.exceptions.ReplayedError` carrying the original type name and
message is raised, so a duplicate never silently succeeds.
"""

from __future__ import annotations

import json
import sys
from typing import Any

from idemkit.core.exceptions import ReplayedError
from idemkit.core.runner import StoredResult

# Recorded in StoredResult.meta so a replay can tell a cached exception apart from
# a cached return value (spec §5.4 uses meta for exactly this kind of codec tag).
_EXC_FLAG = "idemkit_exc"


def encode_exception(exc: BaseException) -> StoredResult:
    """Serialize ``exc`` into a StoredResult flagged as a cached exception."""
    payload = {
        "type": f"{type(exc).__module__}:{type(exc).__qualname__}",
        "message": str(exc),
    }
    return StoredResult(blob=json.dumps(payload).encode("utf-8"), meta={_EXC_FLAG: "1"})


def is_cached_exception(stored: StoredResult | None) -> bool:
    """True if ``stored`` holds a cached exception rather than a return value."""
    return stored is not None and stored.meta.get(_EXC_FLAG) == "1"


def rebuild_exception(stored: StoredResult) -> BaseException:
    """Reconstruct the exception from a cached-exception StoredResult.

    Re-raises the original type when it is already imported and constructible from
    a single message; otherwise returns a :class:`ReplayedError` with the original
    type path and message.
    """
    try:
        payload = json.loads(stored.blob.decode("utf-8"))
        exc_type = str(payload["type"])
        message = str(payload["message"])
    except Exception:
        return ReplayedError("unknown", "cached exception could not be decoded")

    module_name, _, qualname = exc_type.partition(":")
    cls = _lookup_type(module_name, qualname)
    if cls is not None and isinstance(cls, type) and issubclass(cls, BaseException):
        try:
            return cls(message)
        except Exception:
            # Constructor needs more than a message; fall back rather than guess.
            pass
    return ReplayedError(exc_type, message)


def _lookup_type(module_name: str, qualname: str) -> Any:
    """Resolve ``module:qualname`` WITHOUT importing new modules.

    Only already-loaded modules (plus builtins) are consulted, so replaying a
    cached exception can never trigger an import driven by stored data.
    """
    module = sys.modules.get(module_name)
    if module is None and module_name == "builtins":
        import builtins

        module = builtins
    if module is None:
        return None
    obj: Any = module
    for part in qualname.split("."):
        obj = getattr(obj, part, None)
        if obj is None:
            return None
    return obj
