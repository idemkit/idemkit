"""You want just ONE route idempotent (e.g. a webhook) and to catch typed exceptions in code.

Unlike the app-wide middleware it hands your handler the real Starlette Request
(so req.state works) and raises typed exceptions you can catch. Your handler must
return a Response. For FastAPI routes that return a dict use fastapi_route.py; for
the whole app use fastapi_middleware.py.
"""

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse

from idemkit import (
    HttpConfig,
    Idempotency,
    IdempotencyError,
    InMemoryBackend,
    idempotency_problem_handler,
)

app = FastAPI()
idem = Idempotency(
    # InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
    backend=InMemoryBackend(),
    config=HttpConfig(scope=lambda req: req.headers["x-user-id"]),
)


@app.post("/refund")
@idem.protect
async def refund(request: Request) -> JSONResponse:
    return JSONResponse({"refunded": True}, status_code=201)


app.add_exception_handler(IdempotencyError, idempotency_problem_handler)
