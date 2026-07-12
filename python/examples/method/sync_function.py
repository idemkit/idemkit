"""Your code is synchronous (a Celery task, a thread, a script): dedupe with no event loop.

This is also the Celery answer. A task body is a plain sync function, so stack
`idempotent_sync` UNDER `@app.task` (it must be the INNER decorator, so `@app.task`
registers the deduped function):

    @celery_app.task
    @idempotent_sync(backend=backend, config=MethodConfig(key_fields=["order_id"]))
    def refund(order_id): ...

Dedupe on the arguments, not the Celery task id (a resubmission gets a new id but
the same arguments). Below runs standalone, no broker.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent_sync


def issue_refund(order_id: str, amount: int) -> dict:
    return {"order_id": order_id, "amount": amount}


@idempotent_sync(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["order_id"],
        validation_fingerprint=lambda args: str(args["amount"]).encode(),
        scope=lambda args: "tenant-1",
    ),
)
def refund(*, order_id: str, amount: int) -> dict:
    return issue_refund(order_id, amount)
