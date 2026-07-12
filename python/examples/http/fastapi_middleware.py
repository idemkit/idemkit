"""Make a WHOLE FastAPI app idempotent, with tenant isolation, a volatile-field
filter, and PII kept out of the stored copy (scope + body_fingerprint + redactor).

Three ways to wire HTTP idempotency, pick one:
  - whole app          -> IdempotencyMiddleware (this file)
  - some routes        -> fastapi_route.py (a route class; return a dict)
  - one route + typed  -> route_decorator.py (typed exceptions you can catch)
"""

import json

from fastapi import FastAPI

from idemkit import HttpConfig, IdempotencyMiddleware, InMemoryBackend

app = FastAPI()


def fingerprint_operation(body: bytes, content_type):
    # Key only on the fields that define the operation, so a changed nonce still dedupes.
    data = json.loads(body or b"{}")
    return json.dumps({"amount": data.get("amount")}, sort_keys=True).encode()


def redact_secret(body: bytes, headers, status):
    # Scrub the card number from the COPY idemkit stores; the first caller still gets it.
    data = json.loads(body or b"{}")
    data.pop("card_number", None)
    return json.dumps(data).encode(), headers


app.add_middleware(
    IdempotencyMiddleware,
    backend=InMemoryBackend(),  # prod: RedisBackend/PostgresBackend, see ../shared/backends.py
    config=HttpConfig(
        scope=lambda req: req.headers.get("x-user-id", "anonymous"),
        body_fingerprint=fingerprint_operation,
        response_redactor=redact_secret,
    ),
)


@app.post("/charge")
async def charge(payload: dict):
    return {"charged": payload.get("amount"), "card_number": "4242424242424242"}
