"""Run a function once per set of arguments (dedupe on the arguments).

Runs as-is, no infrastructure: InMemoryBackend keeps dedup state in this process. For
production swap in Postgres or Redis (state shared across workers), see ../shared/backends.py:

    from idemkit import PostgresBackend
    backend = PostgresBackend.from_url("postgresql://localhost/app", table="idempotency_keys")
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent


def create_invoice(customer: str, plan: str, period: str) -> dict:
    return {"invoice_id": "inv_123", "customer": customer, "plan": plan, "period": period}


backend = InMemoryBackend()


@idempotent(
    backend=backend,
    # Dedupe on the arguments: one invoice per (customer, plan, period). Call it
    # twice with the same three and the body runs once; the second call replays.
    config=MethodConfig(key_fields=["customer", "plan", "period"]),
)
async def charge_subscription(*, customer: str, plan: str, period: str) -> dict:
    return create_invoice(customer, plan, period)
