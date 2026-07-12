"""See what idemkit is doing in production (metrics and logs) without hand-rolling it.

Every operation emits one IdempotencyEvent. idemkit.contrib ships two EventHandlers
you pass in `event_handlers=`, on any surface (HTTP / queue / method), so one
dashboard covers all three:

  - prometheus_handler(): idemkit_operations_total{decision,surface,backend} + latency
    and wait histograms. Needs the `prometheus` extra: pip install "idemkit[prometheus]".
  - logging_handler(): one structured line per op (decision, latency, hashed key). No deps.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent
from idemkit.contrib.logging import logging_handler
from idemkit.contrib.prometheus import prometheus_handler


def charge_customer(order_id: str) -> dict:
    return {"charged": order_id}


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in backends.py
    config=MethodConfig(
        key_fields=["order_id"],
        event_handlers=(
            prometheus_handler(),  # metrics: replay/conflict/storage-error rates off `decision`
            logging_handler(),  # one structured log line per operation
        ),
    ),
)
async def charge(*, order_id: str) -> dict:
    return charge_customer(order_id)
