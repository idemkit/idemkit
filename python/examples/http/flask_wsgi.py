"""HTTP idempotency for a WSGI app (Flask, Django sync, any WSGI).

idemkit is async inside, but WSGI is synchronous, so the WSGI middleware runs the
core on a shared background loop for you. Wrap your app once and retries with the
same Idempotency-Key replay the first response.

This file drives a tiny raw WSGI app directly, so it runs standalone with no web
server. In a real Flask app you would write:

    app.wsgi_app = WSGIIdempotencyMiddleware(app.wsgi_app, backend=..., scope=...)

Run:  python examples/http/flask_wsgi.py
"""

import io

from idemkit import InMemoryBackend, WSGIIdempotencyMiddleware

# The side effect: every real run charges the card once.
runs = {"n": 0}


def app(environ, start_response):
    """A plain WSGI handler. In Flask this is your view function."""
    runs["n"] += 1
    start_response("201 Created", [("Content-Type", "application/json")])
    return [b'{"charged": true}']


# >>> idemkit plugs in here: wrap the WSGI app once. scope reads the tenant id from
# a header (req exposes req.headers, req.environ, req.body).
wrapped = WSGIIdempotencyMiddleware(
    app,
    backend=InMemoryBackend(),
    scope=lambda req: req.headers["x-user-id"],
)


def post_charge(idempotency_key):
    """Send one POST /charge through the middleware and return (status, body)."""
    environ = {
        "REQUEST_METHOD": "POST",
        "PATH_INFO": "/charge",
        "QUERY_STRING": "",
        "CONTENT_TYPE": "application/json",
        "wsgi.input": io.BytesIO(b"{}"),
        "HTTP_IDEMPOTENCY_KEY": idempotency_key,
        "HTTP_X_USER_ID": "u1",
    }
    captured = {}

    def start_response(status, headers, exc_info=None):
        captured["status"] = status
        captured["headers"] = dict(headers)

    body = b"".join(wrapped(environ, start_response))
    return captured["status"], captured["headers"], body


if __name__ == "__main__":
    # First request runs the handler.
    status1, _, body1 = post_charge("key-1")
    # Retry with the same key replays the stored response; the handler is skipped.
    status2, headers2, body2 = post_charge("key-1")

    print("first :", status1, body1)
    replayed = "replayed" if headers2.get("idempotency-replayed") else ""
    print("retry :", status2, body2, replayed)
    print("handler actually ran:", runs["n"])  # 1
    assert runs["n"] == 1
