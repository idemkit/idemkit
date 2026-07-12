"""Your app is synchronous (Flask, Django sync, any WSGI): make it idempotent by wrapping it once.

idemkit is async inside; the WSGI middleware runs the core on a background loop
for you. In Flask: app.wsgi_app = WSGIIdempotencyMiddleware(app.wsgi_app, ...).
"""

from idemkit import HttpConfig, InMemoryBackend, WSGIIdempotencyMiddleware


def charge_card() -> None: ...


def app(environ, start_response):
    charge_card()
    start_response("201 Created", [("Content-Type", "application/json")])
    return [b'{"charged": true}']


wrapped = WSGIIdempotencyMiddleware(
    # InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
    app,
    backend=InMemoryBackend(),
    config=HttpConfig(scope=lambda req: req.headers["x-user-id"]),
)
