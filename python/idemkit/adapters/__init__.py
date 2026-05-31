"""HTTP framework adapters for idemkit.

v0.1 ships ASGI middleware (FastAPI / Starlette / any ASGI 3 app).
"""

from idemkit.adapters.asgi import IdempotencyMiddleware

__all__ = ["IdempotencyMiddleware"]
