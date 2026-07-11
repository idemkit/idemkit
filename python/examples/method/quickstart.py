"""Run a function once per set of arguments.

Some duplicates are a plain function call, and the caller has no idempotency key
to give you (a job that overlaps, an internal call, an LLM tool call). @idempotent
dedupes on the arguments you name in key_fields. Call the function twice with the
same values and the body runs once; the second call replays the first result.

Run:  python examples/method/quickstart.py
"""

import asyncio

from idemkit import InMemoryBackend, idempotent

# The side effect: booking a flight. Should happen once per route+date.
runs = {"n": 0}


# >>> idemkit plugs in here: dedupe on the named arguments.
@idempotent(
    backend=InMemoryBackend(),
    key_fields=["customer", "plan", "period"],  # these define "the same call"
    # scope isolates callers, so two users with the same args never collide.
    # Optional: with no scope, idemkit runs single-tenant and warns once.
    scope=lambda args: "billing-run-2026-07",   # a constant here (one billing run)
)
async def charge_subscription(*, customer, plan, period):
    runs["n"] += 1
    return {"invoice": "inv_123", "customer": customer, "plan": plan}


async def main():
    first = await charge_subscription(customer="acme", plan="pro", period="2026-07")
    # Same arguments, so this replays the first result instead of charging again.
    again = await charge_subscription(customer="acme", plan="pro", period="2026-07")

    print("first call :", first)
    print("second call:", again, "(replayed)")
    print("charges actually made:", runs["n"])  # 1
    assert runs["n"] == 1


if __name__ == "__main__":
    asyncio.run(main())
