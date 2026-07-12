"""You want only SOME FastAPI routes idempotent, and handlers that return a dict (not a Response).

Unlike the app-wide middleware, the route class lets your handler return a dict or
Pydantic model (idemkit captures the serialized body), and your scope/key callables
get the real Request (so request.state works). Set it per-router with route_class=,
or app-wide with app.router.route_class = idempotent_route(...).

For the whole app use fastapi_middleware.py; for one route where you want to catch
typed exceptions use route_decorator.py.
"""

from fastapi import APIRouter, FastAPI

from idemkit import HttpConfig, InMemoryBackend
from idemkit.contrib.fastapi import idempotent_route


def charge_card(order: str) -> int:
    return 4242  # your real side effect; runs once per Idempotency-Key


# dev only; prod backend (Redis/Postgres) in ../shared/backends.py
router = APIRouter(
    route_class=idempotent_route(
        backend=InMemoryBackend(),
        config=HttpConfig(scope=lambda req: req.headers.get("x-user", "anon")),
    )
)


@router.post("/charge")
async def charge(order: dict) -> dict:
    return {"charge_id": charge_card(order["id"])}  # a dict; no JSONResponse needed


app = FastAPI()
app.include_router(router)
