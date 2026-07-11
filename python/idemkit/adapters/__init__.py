"""HTTP framework adapters for idemkit.

Ships ASGI middleware (FastAPI / Starlette / any ASGI 3 app) and WSGI middleware
(Flask / Django sync / any WSGI app).
"""

from idemkit.adapters.asgi import IdempotencyMiddleware
from idemkit.adapters.wsgi import WSGIIdempotencyMiddleware

__all__ = ["IdempotencyMiddleware", "WSGIIdempotencyMiddleware"]
