"""HTTP idempotency for FastAPI / Starlette / any ASGI app.

Add one middleware and a retry with the same Idempotency-Key replays the first
response. This file also shows the three options you reach for most:

  - scope:            isolate tenants so two users never collide on one key
  - body_fingerprint: ignore a volatile field so an honest retry is not a 422
  - response_redactor: keep a secret out of the stored copy

Run it, then POST the same request twice:

    pip install -e ".[asgi]" fastapi uvicorn      # from python/
    uvicorn examples.http.fastapi_app:app --reload

    curl -X POST localhost:8000/charge -H "Idempotency-Key: k1" \\
         -H "X-User-Id: u1" -H "Content-Type: application/json" \\
         -d '{"amount": 50, "nonce": "anything"}'
    # run it again: identical response, plus header  idempotency-replayed: true
"""

import json

from fastapi import FastAPI

from idemkit import IdempotencyMiddleware, InMemoryBackend

app = FastAPI()


def fingerprint_operation(body: bytes, content_type):
    # By default the whole body is fingerprinted, so a changed "nonce" would look
    # like a different request and return 422. Fingerprint only the fields that
    # define the operation, and the volatile nonce no longer matters.
    data = json.loads(body or b"{}")
    return json.dumps({"amount": data.get("amount")}, sort_keys=True).encode()


def redact_secret(body: bytes, headers, status):
    # The first caller still gets the full response. This only scrubs the COPY
    # idemkit stores for replay, so the card number is never cached.
    data = json.loads(body or b"{}")
    data.pop("card_number", None)
    return json.dumps(data).encode(), headers


# >>> idemkit plugs in here: one middleware, dedupes on the Idempotency-Key header.
app.add_middleware(
    IdempotencyMiddleware,
    backend=InMemoryBackend(),  # swap for RedisBackend/PostgresBackend in production
    scope=lambda req: req.headers["x-user-id"],  # read identity from a header
    body_fingerprint=fingerprint_operation,
    response_redactor=redact_secret,
)


@app.post("/charge")
async def charge(payload: dict):
    # Runs once per (Idempotency-Key, user, method, path). Retries replay it.
    # The redactor above strips card_number from the STORED copy; the first
    # caller still gets it in full.
    return {"charged": payload.get("amount"), "card_number": "4242424242424242"}
