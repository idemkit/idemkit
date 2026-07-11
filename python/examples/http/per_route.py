"""Protect one route instead of the whole app.

The app-wide middleware sits above every handler. When you want idempotency on
just a few routes, or you want to catch the conflict in your own code, use the
Idempotency decorator. Unlike the middleware it hands your extractors the real
Starlette Request (so req.state works) and raises typed exceptions.

Run:

    pip install -e ".[asgi]" fastapi uvicorn      # from python/
    uvicorn examples.http.per_route:app --reload

    curl -X POST localhost:8000/refund -H "Idempotency-Key: r1" \\
         -H "X-User-Id: u1" -H "Content-Type: application/json" -d '{}'
    # run it again: replayed, and the handler does not run twice
"""

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import JSONResponse

from idemkit import (
    Idempotency,
    IdempotencyError,
    InMemoryBackend,
    idempotency_problem_handler,
)

app = FastAPI()

# >>> idemkit plugs in here: bind the decorator once, then apply @idem.protect per route.
idem = Idempotency(
    backend=InMemoryBackend(),
    scope=lambda req: req.headers["x-user-id"],
)


@app.post("/refund")
@idem.protect
async def refund(request: Request) -> JSONResponse:
    # Two rules for a decorated route:
    #   1. declare a `request: Request` parameter (the decorator needs the real one)
    #   2. return a Response object, not a dict
    return JSONResponse({"refunded": True}, status_code=201)


# Optional: render the decorator's typed exceptions as problem+json responses
# (the same wire format the middleware returns) instead of a bare 500.
app.add_exception_handler(IdempotencyError, idempotency_problem_handler)
