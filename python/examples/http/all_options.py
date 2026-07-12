"""Reference: every HTTP option on one HttpConfig object (you rarely set past scope).

You configure the middleware with one HttpConfig passed as ``config=``; the quick
case just omits the fields you don't need (see getting_started.py). To reuse settings
across surfaces, write a factory that returns the config.
"""

from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from idemkit import HttpConfig, IdempotencyMiddleware, InMemoryBackend

events: list = []

config = HttpConfig(
    # http-specific
    scope=lambda req: req.headers.get("x-user-id", "anon"),  # isolate tenants (the common one)
    scope_mode="warn",  # "warn" | "single_tenant" (silent) | "strict" (error in CI)
    require_key_for_mutations=False,  # True = reject a POST/PATCH with no key (400)
    applicable_methods={"POST", "PATCH"},  # which methods get idempotency
    # These three are identity placeholders; replace the body with your own logic:
    body_fingerprint=lambda body, ct: body,  # fingerprint only the fields that define the op
    response_redactor=lambda body, headers, status: (body, headers),  # scrub PII before caching
    response_hook=lambda body, headers, status: (body, headers),  # tweak the REPLAYED response
    cacheable_status={200, 201, 202},  # which statuses are stored for replay (5xx never is)
    max_body_bytes=1024 * 1024,  # cap on the cached response (1 MiB)
    max_request_body_bytes=1024 * 1024,  # cap on the buffered request body
    compat_mode="default",  # "stripe" returns 409 instead of 422/423
    # shared
    lease_ttl_seconds=30,  # how long one run may hold the key
    wait_timeout_seconds=10,  # how long a duplicate waits for an in-flight run
    expires_after_seconds=86_400,  # how long the result is kept for replay
    on_storage_error="fail_closed",  # backend down: reject (safe) vs "fail_open"
    use_local_cache=False,  # in-process replay cache (rarely needed)
    local_cache_max_items=1024,
    event_handlers=(events.append,),  # one structured event per request (you export it)
)


async def charge(request):
    return JSONResponse({"charged": True}, status_code=201)


app = Starlette(routes=[Route("/charge", charge, methods=["POST"])])
# InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)
