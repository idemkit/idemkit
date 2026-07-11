"""idemkit — correct, framework-agnostic HTTP idempotency middleware.

The behavioral contract is documented in ``spec/README.md`` at the project root.
"""

from idemkit._version import __version__
from idemkit.adapters.asgi import IdempotencyMiddleware
from idemkit.backends.memory import InMemoryBackend, ManualClock
from idemkit.core.exceptions import (
    ConfigurationError,
    IdempotencyConflict,
    IdempotencyError,
    IdempotencyKeyMissing,
    PayloadMismatch,
    StorageError,
    StorageUnavailable,
)
from idemkit.core.policy import IdempotencyPolicy
from idemkit.core.state import Decision

# Every surface is configured with plain keyword arguments — there is no public
# config object to construct. (`IdempotencyConfig` remains internally as the HTTP
# middleware's settings container; it is intentionally not exported here.)

__all__ = [
    "ConfigurationError",
    "ConsumerAction",
    "ConsumerResult",
    "Decision",
    "Idempotency",
    "IdempotencyConflict",
    "IdempotencyError",
    "IdempotencyKeyMissing",
    "IdempotencyMiddleware",
    "IdempotencyPolicy",
    "IdempotentConsumer",
    "InMemoryBackend",
    "ManualClock",
    "PayloadMismatch",
    "PostgresBackend",
    "RedisBackend",
    "StorageError",
    "StorageUnavailable",
    "WSGIIdempotencyMiddleware",
    "__version__",
    "idempotency_problem_handler",
    "idempotent",
    "idempotent_sync",
]


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    """Lazy imports for components with optional deps."""
    if name == "RedisBackend":
        from idemkit.backends.redis import RedisBackend
        return RedisBackend
    if name == "PostgresBackend":
        from idemkit.backends.postgres import PostgresBackend
        return PostgresBackend
    if name == "Idempotency":
        from idemkit.adapters.route import Idempotency
        return Idempotency
    if name == "idempotency_problem_handler":
        from idemkit.adapters.route import idempotency_problem_handler
        return idempotency_problem_handler
    if name == "WSGIIdempotencyMiddleware":
        from idemkit.adapters.wsgi import WSGIIdempotencyMiddleware
        return WSGIIdempotencyMiddleware
    if name in ("IdempotentConsumer", "ConsumerAction", "ConsumerResult"):
        from idemkit.adapters import queue
        return getattr(queue, name)
    if name in ("idempotent", "idempotent_sync"):
        from idemkit.adapters import ai
        return getattr(ai, name)
    raise AttributeError(f"module 'idemkit' has no attribute {name!r}")
