"""Move from the dev InMemoryBackend to a real store (Redis, Postgres, MongoDB, or
DynamoDB) for production.

Every other example uses InMemoryBackend so it runs with no setup, but that
backend is dev-only (one process). In production you build one of the real
backends and pass it to the exact same surfaces:

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


def mongo():
    from idemkit.backends.mongo import MongoBackend

    # When you already run MongoDB (4.2+). Leases use the server clock ($$NOW); a
    # TTL index self-reaps expired docs, so there is nothing to vacuum.
    return MongoBackend.from_url(
        "mongodb://localhost:27017",
        database="idemkit",
        collection="idemkit_records",  # the default
    )


def dynamodb():
    from idemkit.backends.dynamodb import DynamoBackend

    # When you already run on DynamoDB (managed, nothing to operate). The table is
    # created on first use (on-demand billing, TTL on `ttl`). Credentials/region come
    # from the environment like any boto3 app; endpoint_url is only for DynamoDB
    # Local / LocalStack in tests.
    # NOTE: DynamoDB leases use the CLIENT clock (see the backend's docstring).
    # In production, pre-create the table and pass create_table=False.
    # Timeouts are flat kwargs (default only if omitted), like the other backends;
    # advanced callers can pass a full botocore config=Config(...) instead.
    return DynamoBackend(
        table="idempotency_keys",
        region_name="us-east-1",
        connect_timeout=5,
        read_timeout=10,
        max_retries=3,
    )
