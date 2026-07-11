"""Enforce idempotency on MCP tool calls.

An MCP tool can advertise idempotentHint, but nothing enforces it: an agent can
still call a side-effectful tool twice. idemkit.contrib.mcp turns the hint into
real dedup.

Two ways to use it:
  1. mcp_idempotent    decorate a tool directly (and stamp the hint)
  2. wrap_if_idempotent auto-wrap only the tools that already declare the hint

This uses plain functions (no mcp package needed). With FastMCP you would stack
the decorator under @mcp.tool().

Run:  python examples/method/mcp.py
"""

from idemkit import InMemoryBackend
from idemkit.contrib.mcp import mcp_idempotent, read_idempotent_hint, wrap_if_idempotent

backend = InMemoryBackend()
refunds = {"n": 0}


# In FastMCP:  @mcp.tool()  goes above this decorator.
# order_id is the stable identity here, so strict_keys=False silences the
# "volatile-looking field" warning; scope isolates per agent session.
# >>> idemkit plugs in here: enforce dedup on an MCP tool and stamp idempotentHint.
@mcp_idempotent(
    backend=backend,
    key_fields=["order_id", "amount"],
    scope=lambda args: "agent-session",
    strict_keys=False,
)
def refund(*, order_id: str, amount: int) -> dict:
    refunds["n"] += 1
    return {"order_id": order_id, "refunded": amount}


def demo_direct_decorator():
    refund(order_id="A123", amount=50)
    refund(order_id="A123", amount=50)  # agent re-plan: replays, no second refund
    print("tool advertises idempotentHint:", read_idempotent_hint(refund))  # True
    print("refunds actually issued:", refunds["n"])  # 1
    assert refunds["n"] == 1


def demo_auto_wrap_from_hint():
    """A tool registry carries MCP annotations. wrap_if_idempotent adds dedup only
    to tools that already declare the hint, and leaves the rest untouched."""
    sends = {"n": 0}

    def send_email(*, to: str, template: str) -> str:
        sends["n"] += 1
        return f"sent {template} to {to}"

    tool = wrap_if_idempotent(
        send_email,
        annotations={"idempotentHint": True},   # the tool declares itself idempotent
        backend=backend,
        key_fields=["to", "template"],
        scope=lambda args: "agent-session",
    )
    tool(to="a@x.com", template="receipt")
    tool(to="a@x.com", template="receipt")
    print("emails actually sent:", sends["n"])  # 1
    assert sends["n"] == 1


if __name__ == "__main__":
    demo_direct_decorator()
    demo_auto_wrap_from_hint()
