"""Reference: every method / AI-tool option on one MethodConfig object.

You configure the decorator with one MethodConfig passed as ``config=``; the quick
case just omits the fields you don't need (see getting_started.py). To reuse settings
across surfaces, write a factory that returns the config.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent

events: list = []

config = MethodConfig(
    # method-specific
    key_fields=["order_id"],  # the arguments that make two calls "the same"
    scope=lambda args: "tenant-1",  # isolation namespace (per user / per conversation)
    version="1",  # bump to invalidate every cached result (behavior changed)
    validation_fingerprint=lambda args: str(args["amount"]).encode(),  # must-match, not in key
    result_codec="json",  # "json" | "dataclass" | "pydantic" | a custom codec | "pickle"
    strict_keys=True,  # warn when a volatile-looking field lands in the key
    require_key=False,  # True: refuse to derive the key from all args (raise instead)
    cache_exceptions=(),  # e.g. (ValueError,): cache + replay these, don't re-run
    # shared (same names on every surface)
    lease_ttl_seconds=60,  # how long one run may hold the key
    wait_timeout_seconds=10,  # how long a duplicate waits for an in-flight run
    expires_after_seconds=86_400,  # how long the result is kept for replay
    on_storage_error="fail_closed",  # backend down: reject (safe) vs "fail_open"
    use_local_cache=False,  # in-process replay cache (rarely needed)
    local_cache_max_items=1024,
    event_handlers=(events.append,),  # one structured event per call (you export it)
)


def issue_refund(order_id: str, amount: int) -> dict:
    return {"order_id": order_id, "amount": amount}


# InMemoryBackend is dev only; prod backend (Redis/Postgres): ../shared/backends.py
@idempotent(backend=InMemoryBackend(), config=config)
async def refund(*, order_id: str, amount: int) -> dict:
    return issue_refund(order_id, amount)
