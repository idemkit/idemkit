"""The 30-second pitch: a flaky client retries a charge 5 times.

Without idemkit the customer is billed 5 times. With it, once. The other four
retries replay the first response. This fires 5 identical requests at the same
endpoint, first with no protection, then with the middleware turned on.

Run (no infrastructure, in-memory backend):

    pip install -e ".[asgi]" httpx        # from python/
    python examples/http/double_charge.py

Point it at a real Redis to prove it holds across separate workers, not just in
one process:

    IDEMKIT_DEMO_REDIS_URL=redis://localhost:6379 python examples/http/double_charge.py
"""

import asyncio
import logging
import os
import uuid

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from idemkit import IdempotencyMiddleware, InMemoryBackend

# The demo uses the in-memory backend for zero setup. Quiet idemkit's (correct)
# "InMemoryBackend is dev-only" notice so the output stays clean. A real app
# should NOT silence it.
logging.getLogger("idemkit").setLevel(logging.ERROR)

# The side effect we want to happen exactly once. Every call appends here.
CHARGES: list[str] = []


async def charge(request: Request) -> JSONResponse:
    body = await request.json()
    CHARGES.append(body["order"])  # the dangerous part: a real charge
    return JSONResponse({"charge_number": len(CHARGES)}, status_code=201)


def build_app(backend=None) -> Starlette:
    app = Starlette(routes=[Route("/charge", charge, methods=["POST"])])
    if backend is not None:
        # >>> idemkit plugs in here: one middleware line. scope isolates tenants
        # (here everyone is the same customer).
        app.add_middleware(
            IdempotencyMiddleware, backend=backend, scope=lambda req: "customer-42"
        )
    return app


async def fire_five(app: Starlette, idempotency_key: str) -> None:
    """Send 5 identical requests at once, all carrying the SAME key (a client
    retrying the same request on a flaky network)."""
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo") as client:
        await asyncio.gather(
            *[
                client.post(
                    "/charge",
                    json={"order": "ord-1001"},
                    headers={"Idempotency-Key": idempotency_key},
                )
                for _ in range(5)
            ]
        )


def make_backend():
    url = os.environ.get("IDEMKIT_DEMO_REDIS_URL")
    if url:
        from idemkit import RedisBackend

        return RedisBackend.from_url(url), f"RedisBackend ({url})"
    return InMemoryBackend(), "InMemoryBackend (in-process)"


async def main() -> None:
    backend, backend_name = make_backend()
    key = f"retry-of-the-same-request-{uuid.uuid4()}"  # one key, reused by all 5
    print(f"backend: {backend_name}\n")
    try:
        # 1. No middleware: every retry runs the handler.
        CHARGES.clear()
        await fire_five(build_app(), key)
        n = len(CHARGES)
        print("WITHOUT idemkit:")
        print(f"  5 identical retries  ->  {n} charges  X   (customer billed {n}x)\n")

        # 2. With middleware: the first request runs, the rest replay it.
        CHARGES.clear()
        await fire_five(build_app(backend), key)
        print("WITH idemkit:")
        print(f"  5 identical retries  ->  {len(CHARGES)} charge   OK  (other 4 replayed)")
    finally:
        aclose = getattr(backend, "aclose", None)
        if aclose is not None:
            await aclose()


if __name__ == "__main__":
    asyncio.run(main())
