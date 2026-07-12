"""Move from the dev InMemoryBackend to a real Redis or Postgres for production.

Every other example uses InMemoryBackend so it runs with no setup, but that
backend is dev-only (one process). In production you build a RedisBackend or
PostgresBackend and pass it to the exact same surfaces:

    app.add_middleware(IdempotencyMiddleware, backend=backend)        # HTTP
    IdempotentConsumer(backend=backend, config=QueueConfig(...))      # queue
    @idempotent(backend=backend, config=MethodConfig(...))            # method

The backend is the ONLY thing that changes between dev and prod. Redis/Postgres
hold a pool and a listener; close them on shutdown (ASGI lifespan) or use the
backend as an async context manager.
"""

from idemkit import InMemoryBackend


def in_memory():
    # Dev, tests, prototypes. State is NOT shared across workers or restarts.
    return InMemoryBackend()


def redis():
    from idemkit import RedisBackend

    # Most production deployments. Redis 6+ and Redis Cluster.
    return RedisBackend.from_url(
        "redis://localhost:6379/0",
        # namespace prefixes every key, so idemkit can share a Redis with other
        # data or run two isolated instances on one server:
        namespace="idemkit",
    )
    # TLS and auth: from_url forwards extra kwargs to the redis client, so a
    # rediss:// URL and client options just work, e.g.
    #     RedisBackend.from_url("rediss://cache:6380", password="...", ssl_cert_reqs="required")


def postgres():
    from idemkit.backends.postgres import PostgresBackend

    # When you already run Postgres. Create the table ONCE before first use:
    #     idemkit init-pg postgresql://user:pass@localhost/db --table idempotency_keys
    # then drop expired rows on a cron (daily is fine):
    #     idemkit pg-vacuum postgresql://user:pass@localhost/db --table idempotency_keys
    return PostgresBackend.from_url(
        "postgresql://user:pass@localhost:5432/db",
        table="idempotency_keys",  # default is "idemkit_records"
    )
