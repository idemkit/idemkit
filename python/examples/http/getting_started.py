"""Stop a double charge: a retry with the same Idempotency-Key replays the first response.

Runs as-is, no infrastructure: InMemoryBackend keeps dedup state in this process. For
production swap in Redis or Postgres (state shared across workers), see ../shared/backends.py:

    from idemkit import RedisBackend
    backend = RedisBackend.from_url("redis://localhost:6379")
"""

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from idemkit import HttpConfig, IdempotencyMiddleware, InMemoryBackend


def charge_card(order: str) -> int:
    # Your real side effect. Should happen once per Idempotency-Key.
    return 4242


async def charge(request):
    body = await request.json()
    return JSONResponse({"charge_id": charge_card(body["order"])}, status_code=201)


app = Starlette(routes=[Route("/charge", charge, methods=["POST"])])

# The backend is WHERE dedup state lives.
app.add_middleware(
    IdempotencyMiddleware,
    backend=InMemoryBackend(),
    config=HttpConfig(scope=lambda req: "customer-42"),
)
