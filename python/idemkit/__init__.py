"""idemkit — correct, framework-agnostic idempotency for HTTP, queues, and function calls.

The behavioral contract is documented in ``spec/README.md`` at the project root.
"""

from idemkit._version import __version__
from idemkit.adapters.asgi import IdempotencyMiddleware
from idemkit.backends.memory import InMemoryBackend, ManualClock
from idemkit.core.events import EventHandler, IdempotencyEvent
from idemkit.core.exceptions import (
    ConfigurationError,
    IdempotencyConflict,
    IdempotencyError,
    IdempotencyKeyMissing,
    PayloadMismatch,
    ReplayedError,
    StorageError,
    StorageUnavailable,
)
from idemkit.core.policy import HttpConfig, MethodConfig, QueueConfig
from idemkit.core.state import Decision

# Each surface is configured with one config object passed as `config=`:
# HttpConfig / QueueConfig / MethodConfig. The datastore is configured on the
# backend (table / namespace), not here.

__all__ = [
    "ConfigurationError",
    "ConsumerAction",
    "ConsumerResult",
    "Decision",
    "EventHandler",
    "HttpConfig",
    "Idempotency",
    "IdempotencyConflict",
    "IdempotencyError",
    "IdempotencyEvent",
    "IdempotencyKeyMissing",
    "IdempotencyMiddleware",
    "IdempotentConsumer",
    "InMemoryBackend",
    "ManualClock",
    "MethodConfig",
    "PayloadMismatch",
    "PostgresBackend",
    "QueueConfig",
    "RedisBackend",
    "ReplayedError",
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
