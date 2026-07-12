"""Stop a double charge: a retry with the same Idempotency-Key replays the first response.

Runs as-is with `pip install "idemkit[asgi]" fastapi`: InMemoryBackend keeps dedup
state in this process. For production swap in Redis or Postgres (state shared across
workers), see ../shared/backends.py:

    from idemkit import RedisBackend
    backend = RedisBackend.from_url("redis://localhost:6379")
"""

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from idemkit import HttpConfig, IdempotencyMiddleware, InMemoryBackend


def charge_card(order: str) -> int:
    # Your real side effect. Should happen once per Idempotency-Key.
    return 4242


app = FastAPI()


@app.post("/charge")
async def charge(request: Request) -> JSONResponse:
    body = await request.json()
    return JSONResponse({"charge_id": charge_card(body["order"])}, status_code=201)


# The backend is WHERE dedup state lives.
app.add_middleware(
    IdempotencyMiddleware,
    backend=InMemoryBackend(),
    # scope isolates one tenant's keys from another's: read it from the request
    # (an auth header here), never a constant. Omit scope for a single-tenant service.
    config=HttpConfig(scope=lambda req: req.headers.get("x-customer-id", "anonymous")),
)
