"""idemkit — see it in action in 30 seconds.

Fires 5 simultaneous identical "charge" requests (a client retrying on a flaky
network) at the SAME endpoint, twice:

  1. without idemkit  -> the customer is charged 5 times
  2. with idemkit     -> the customer is charged once; the other 4 replay

Run it (zero infrastructure, in-memory backend):

    pip install -e "python/[asgi]" httpx    # not on PyPI yet; install from source
    python demo/double_charge.py

Point it at a real Redis to prove the same under multiple workers:

    IDEMKIT_DEMO_REDIS_URL=redis://localhost:6379 python demo/double_charge.py
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

# This demo deliberately uses the in-memory backend for zero setup, so quiet
# idemkit's (correct, but here irrelevant) "InMemoryBackend is dev-only" notice
# to keep the 30-second output clean. A real app should NOT silence it.
logging.getLogger("idemkit").setLevel(logging.ERROR)

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from idemkit import IdempotencyConfig, IdempotencyMiddleware, InMemoryBackend

CHARGES: list[str] = []


async def charge(request: Request) -> JSONResponse:
    # The dangerous part: a real side effect. Every call charges the card.
    body = await request.json()
    CHARGES.append(body["order"])
    return JSONResponse({"order": body["order"], "charge_number": len(CHARGES)}, status_code=201)


def _make_backend():
    url = os.environ.get("IDEMKIT_DEMO_REDIS_URL")
    if url:
        from idemkit import RedisBackend

        return RedisBackend.from_url(url), f"RedisBackend ({url})"
    return InMemoryBackend(), "InMemoryBackend (in-process)"


def _app(backend=None) -> Starlette:
    app = Starlette(routes=[Route("/charge", charge, methods=["POST"])])
    if backend is not None:
        app.add_middleware(
            IdempotencyMiddleware,
            backend=backend,
            config=IdempotencyConfig(scope=lambda req: "customer-42"),
        )
    return app


async def _fire_five(app: Starlette, key: str) -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://demo") as client:
        await asyncio.gather(
            *[
                client.post(
                    "/charge",
                    json={"order": "ord-1001"},
                    headers={"Idempotency-Key": key},
                )
                for _ in range(5)
            ]
        )


async def main() -> None:
    backend, backend_name = _make_backend()
    key = f"retry-of-the-same-request-{uuid.uuid4()}"  # one key, reused by all 5 retries
    print(f"backend: {backend_name}\n")
    try:
        CHARGES.clear()
        await _fire_five(_app(), key)
        n = len(CHARGES)
        print("WITHOUT idemkit:")
        print(f"  5 identical retries  ->  {n} charges  X   (customer billed {n}x)\n")

        CHARGES.clear()
        await _fire_five(_app(backend), key)
        print("WITH idemkit:")
        print(f"  5 identical retries  ->  {len(CHARGES)} charge   OK  (other 4 replayed)")
    finally:
        aclose = getattr(backend, "aclose", None)
        if aclose is not None:
            await aclose()


if __name__ == "__main__":
    asyncio.run(main())
