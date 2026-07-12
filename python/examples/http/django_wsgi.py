"""Django (sync): make the whole site idempotent by wrapping its WSGI application once.

Django is synchronous under WSGI; idemkit is async inside and the WSGI middleware
runs the core on a background loop for you. Wire it in your project's ``wsgi.py``,
wrapping the object Django hands you:

    # myproject/wsgi.py
    import os
    from django.core.wsgi import get_wsgi_application
    from idemkit import HttpConfig, WSGIIdempotencyMiddleware
    from idemkit.backends.postgres import PostgresBackend

    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
    application = get_wsgi_application()
    application = WSGIIdempotencyMiddleware(
        application,
        backend=PostgresBackend.from_url("postgresql://localhost/app"),
        config=HttpConfig(scope=lambda req: req.headers.get("x-user-id", "anonymous")),
    )

That wraps every view. It acts only on POST/PATCH by default (the methods where a
retry duplicates); everything else passes through. To dedupe a single DRF view
instead of the whole site, use ``contrib.drf.idempotent_view`` (see drf_view.py).

Below is a runnable stand-in: ``application`` is a plain WSGI callable playing the
role of ``get_wsgi_application()``, so this file runs without a configured Django.
"""

from idemkit import HttpConfig, InMemoryBackend, WSGIIdempotencyMiddleware


def charge_card() -> None: ...


def application(environ, start_response):
    # Stands in for Django's get_wsgi_application(); your real views live here.
    charge_card()
    start_response("201 Created", [("Content-Type", "application/json")])
    return [b'{"charged": true}']


# In a real project this is the last line of wsgi.py. InMemoryBackend is dev only;
# prod backend (Redis/Postgres): ../shared/backends.py
application = WSGIIdempotencyMiddleware(
    application,
    backend=InMemoryBackend(),
    config=HttpConfig(scope=lambda req: req.headers.get("x-user-id", "anonymous")),
)
