"""Enforce idempotency on an MCP tool (make the advisory idempotentHint real).

idemkit.contrib.mcp stamps the hint and enforces dedup. In FastMCP, stack
@mcp.tool() above @mcp_idempotent. (wrap_if_idempotent auto-wraps only the tools
that already declare the hint.)
"""

from idemkit import InMemoryBackend, MethodConfig
from idemkit.contrib.mcp import mcp_idempotent


def do_refund(order_id: str, amount: int) -> dict:
    return {"order_id": order_id, "refunded": amount}


@mcp_idempotent(
    backend=InMemoryBackend(),  # dev only; prod backend (Redis/Postgres) in ../shared/backends.py
    config=MethodConfig(
        key_fields=["order_id", "amount"], scope=lambda args: "agent-session", strict_keys=False
    ),
)
def refund(*, order_id: str, amount: int) -> dict:
    return do_refund(order_id, amount)
