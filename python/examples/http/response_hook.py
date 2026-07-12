"""You want the REPLAYED response to differ from the original (tag it, tweak a cache header).

Tag or tweak the cached response (add a header, adjust the body) without touching
the first, live response.
"""

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from idemkit import HttpConfig, IdempotencyMiddleware, InMemoryBackend


def tag_replay(body: bytes, headers: dict, status: int):
    headers["x-served-from-cache"] = "true"
    return (body, headers)


async def charge(request):
    return JSONResponse({"charged": True}, status_code=201)


app = Starlette(routes=[Route("/charge", charge, methods=["POST"])])
app.add_middleware(
    IdempotencyMiddleware,
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=HttpConfig(scope=lambda req: "tenant-1", response_hook=tag_replay),
)
