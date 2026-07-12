"""Stop an LLM agent from doing the same tool call twice.

An agent mints a fresh tool_call_id every turn, so dedupe on the ARGUMENTS, not
the id. The same shape works with OpenAI function calling and Anthropic tool use:
parse the arguments, call this function, feed the result back.
"""

from idemkit import InMemoryBackend, MethodConfig, idempotent


def book_flight_seat(origin: str, destination: str, date: str) -> dict:
    return {"pnr": "XYZ123", "route": f"{origin}->{destination}", "date": date}


@idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["origin", "destination", "date"], scope=lambda args: "conversation-42"
    ),
)
async def book_flight(*, origin: str, destination: str, date: str) -> dict:
    return book_flight_seat(origin, destination, date)
