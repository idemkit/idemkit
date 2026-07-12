"""Dedupe inbound provider webhooks on the PROVIDER's event id (not an Idempotency-Key).

Webhook senders (Stripe, GitHub, ...) deliver at-least-once and retry aggressively, and
they do NOT send an ``Idempotency-Key`` header — the stable id lives in a provider header
or in the body. So you tell idemkit where to read it with ``key=``:

  - GitHub: the id is the ``X-GitHub-Delivery`` header (the clean case).
  - Stripe: the id is ``event["id"]`` (e.g. "evt_1Abc...") in the JSON body. idemkit awaits
    the body before calling ``key``, so a route-decorator handler can read it there.

Two details that are specific to webhooks:
  - **Scope is per provider, not per user** — a webhook has no authenticated user.
  - **Size the replay window to the provider's retry horizon.** Stripe retries for up to
    3 days, so keep keys ~7 days (``expires_after_seconds``); a shorter window would let a
    late retry re-run the handler.

Runs with ``pip install "idemkit[asgi]" fastapi``.
"""

import json

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

# Stripe retries for up to 3 days; keep keys a bit longer so no retry outlives the record.
STRIPE_RETRY_WINDOW = 7 * 24 * 60 * 60

app = FastAPI()
# InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
backend = InMemoryBackend()


# --- GitHub: the event id is a header (simplest) ---
github = Idempotency(
    backend=backend,
    config=HttpConfig(
        key=lambda req: req.headers.get("x-github-delivery"),  # the delivery id
        scope=lambda req: "github",  # per provider, not per user
        expires_after_seconds=STRIPE_RETRY_WINDOW,
    ),
)


@app.post("/webhooks/github")
@github.protect
async def github_webhook(request: Request) -> JSONResponse:
    # Verify the signature first in real code; then handle the event exactly once.
    return JSONResponse({"ok": True})


# --- Stripe: the event id is in the JSON body ---
def stripe_event_id(req: Request) -> str | None:
    # idemkit awaits request.body() before calling key(), so Starlette has it cached here.
    body = getattr(req, "_body", b"") or b"{}"
    return json.loads(body).get("id")


stripe = Idempotency(
    backend=backend,
    config=HttpConfig(
        key=stripe_event_id,
        scope=lambda req: "stripe",
        expires_after_seconds=STRIPE_RETRY_WINDOW,
    ),
)


@app.post("/webhooks/stripe")
@stripe.protect
async def stripe_webhook(request: Request) -> JSONResponse:
    return JSONResponse({"received": True})


# Render idemkit's typed exceptions as application/problem+json.
app.add_exception_handler(IdempotencyError, idempotency_problem_handler)
