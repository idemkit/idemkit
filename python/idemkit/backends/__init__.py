"""idemkit backend implementations.

Production-ready in v0.1: ``InMemoryBackend`` (dev/test), ``RedisBackend``,
``PostgresBackend``. Custom backends implement the ``IdempotencyBackend``
Protocol from ``idemkit.backends.base``.
"""

from idemkit.backends.base import IdempotencyBackend
from idemkit.backends.memory import InMemoryBackend, MemoryBackendFull

__all__ = [
    "IdempotencyBackend",
    "InMemoryBackend",
    "MemoryBackendFull",
    "PostgresBackend",
    "RedisBackend",
]


def __getattr__(name: str):  # type: ignore[no-untyped-def]
    """Lazy imports — Redis/Postgres deps are optional."""
    if name == "RedisBackend":
        from idemkit.backends.redis import RedisBackend

        return RedisBackend
    if name == "PostgresBackend":
        from idemkit.backends.postgres import PostgresBackend

        return PostgresBackend
    raise AttributeError(f"module 'idemkit.backends' has no attribute {name!r}")
