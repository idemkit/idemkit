"""Stop an LLM agent from doing the same tool call twice.

An agent re-emits the same tool call on a retry or a re-plan, and it mints a fresh
tool_call_id every turn. If you dedupe on that id, a retry looks new and the side
effect runs again. Dedupe on the ARGUMENTS instead, and it runs once.

This uses a fake model (no API key needed) that re-plans the same booking on turn
two. The same shape works with OpenAI function calling and Anthropic tool use:
parse the tool arguments, call the decorated function, feed the result back. The
tool_call_id only links the result to the call.

Run:  python examples/method/agent_loop.py
"""

import asyncio
import json

from idemkit import InMemoryBackend, idempotent

backend = InMemoryBackend()
booked = {"n": 0}


# >>> idemkit plugs in here: dedupe on the tool arguments, never the call id.
@idempotent(
    backend=backend,
    key_fields=["origin", "destination", "date"],  # dedupe on args, NOT the call id
    scope=lambda args: "conversation-42",           # per-conversation scope
    # Best for agents: an AMBIENT (zero-arg) scope reads a contextvar, so the
    # session id never has to be a tool parameter the model fills in:
    #     import contextvars
    #     session = contextvars.ContextVar("session")
    #     scope=lambda: session.get()   # set session.set(id) at the start of each turn
)
async def book_flight(*, origin, destination, date):
    booked["n"] += 1
    return {"pnr": "XYZ123", "route": f"{origin}->{destination}"}


TOOLS = {"book_flight": book_flight}


def fake_model_turn(turn):
    """Both turns emit the SAME booking, with a DIFFERENT tool_call_id each time
    (exactly what a real model does on a re-plan)."""
    args = {"origin": "SFO", "destination": "JFK", "date": "2026-08-01"}
    return {"id": f"call_{turn}", "name": "book_flight", "arguments": json.dumps(args)}


async def main():
    for turn in (1, 2):  # turn 2 is a re-plan of the same booking
        call = fake_model_turn(turn)
        args = json.loads(call["arguments"])
        result = await TOOLS[call["name"]](**args)  # deduped on args, runs once
        print(f"turn {turn}  id={call['id']}  ->  {result}")

    print("flights actually booked:", booked["n"])  # 1
    assert booked["n"] == 1


if __name__ == "__main__":
    asyncio.run(main())
