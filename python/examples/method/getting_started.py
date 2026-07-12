"""Run a function once per set of arguments (dedupe on the arguments).

Runs as-is, no infrastructure: InMemoryBackend keeps dedup state in this process. For
production swap in Postgres or Redis (state shared across workers), see ../shared/backends.py:

    from idemkit import PostgresBackend
    backend = PostgresBackend.from_url("postgresql://localhost/app", table="idempotency_keys")
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent


def create_invoice(customer: str, plan: str) -> dict:
    return {"invoice_id": "inv_123", "customer": customer, "plan": plan}


backend = InMemoryBackend()


@idempotent(
    backend=backend,
    config=MethodConfig(
        key_fields=["customer", "plan", "period"], scope=lambda args: args["customer"]
    ),
)
async def charge_subscription(*, customer: str, plan: str, period: str) -> dict:
    return create_invoice(customer, plan)
