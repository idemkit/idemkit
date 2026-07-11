"""Enforce idempotency on MCP / LLM tool calls.

The Model Context Protocol lets a tool advertise ``idempotentHint: true`` in its
annotations — but that is advisory metadata for the model/client, and **nothing
enforces it**. An agent that retries, re-plans, or runs parallel branches can
still call a side-effectful tool twice.

idemkit's method surface (:func:`idemkit.idempotent`) *is* the enforcement layer.
This module is a thin bridge:

* :func:`mcp_idempotent` — decorate a tool so duplicate calls with the same
  arguments replay the first result instead of re-running, and stamp the
  ``idempotentHint`` so the tool truthfully advertises what it now guarantees.
* :func:`wrap_if_idempotent` — given a handler and its MCP annotations, enforce
  dedup automatically *when the tool already declares* ``idempotentHint: true``.
* :func:`read_idempotent_hint` — read the hint off a function or an annotations
  object/dict.

No dependency on the ``mcp`` package: these operate on plain functions and an
annotations mapping, so they work with the official Python SDK (FastMCP), a hand
-rolled server, or an Anthropic / OpenAI tool-dispatch loop.

Example — a FastMCP tool that really is safe to call twice::

    from mcp.server.fastmcp import FastMCP
    from idemkit import RedisBackend
    from idemkit.contrib.mcp import mcp_idempotent

    mcp = FastMCP("payments")
    backend = RedisBackend.from_url("redis://localhost:6379")

    @mcp.tool()
    @mcp_idempotent(backend=backend, key_fields=["order_id", "amount"])
    async def refund(order_id: str, amount: int) -> dict:
        return await payments.refund(order_id, amount)   # charged once per (order_id, amount)

The agent re-emitting ``refund("A123", 50)`` on a re-plan replays the first
result instead of issuing a second refund.
"""

from __future__ import annotations

import inspect
from collections.abc import Callable, Mapping
from typing import Any, cast

from idemkit.adapters.ai import CallerIdentity, idempotent, idempotent_sync
from idemkit.backends.base import IdempotencyBackend

HINT_KEY = "idempotentHint"


def mcp_idempotent(
    *,
    backend: IdempotencyBackend,
    key_fields: list[str] | None = None,
    scope: CallerIdentity | None = None,
    version: str = "1",
    **kwargs: Any,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Enforce idempotency on an MCP tool and mark it as idempotent.

    Works on ``async def`` and plain ``def`` tools alike (it picks the matching
    idemkit decorator). Dedups on ``key_fields`` — the argument fields that define
    "the same call"; volatile per-turn ids (``tool_call_id``) are never listed, so
    a retry with a fresh id still dedupes. Extra keywords pass through to
    :func:`idemkit.idempotent` (``result_codec``, ``lease_ttl_seconds``, ...).

    The returned function carries ``idempotent_hint = True`` so a server can
    surface ``idempotentHint`` in the tool's advertised annotations.
    """

    def decorate(fn: Callable[..., Any]) -> Callable[..., Any]:
        deco = idempotent if inspect.iscoroutinefunction(fn) else idempotent_sync
        wrapped: Any = deco(
            backend=backend,
            key_fields=key_fields,
            scope=scope,
            version=version,
            **kwargs,
        )(fn)
        wrapped.idempotent_hint = True
        return cast("Callable[..., Any]", wrapped)

    return decorate


def read_idempotent_hint(source: Any) -> bool:
    """Return whether ``source`` declares ``idempotentHint``.

    ``source`` may be a function (checks the ``idempotent_hint`` attribute set by
    :func:`mcp_idempotent`), a mapping (checks the ``idempotentHint`` key), or any
    object exposing an ``idempotentHint`` attribute (e.g. an MCP tool annotations
    object).
    """
    if isinstance(source, Mapping):
        return bool(source.get(HINT_KEY, False))
    if hasattr(source, "idempotent_hint"):
        return bool(source.idempotent_hint)
    if hasattr(source, HINT_KEY):
        return bool(getattr(source, HINT_KEY))
    return False


def wrap_if_idempotent(
    fn: Callable[..., Any],
    *,
    annotations: Any,
    backend: IdempotencyBackend,
    key_fields: list[str] | None = None,
    scope: CallerIdentity | None = None,
    version: str = "1",
    **kwargs: Any,
) -> Callable[..., Any]:
    """Enforce dedup on ``fn`` only if its MCP ``annotations`` declare the hint.

    Use this to auto-apply idempotency across a registry of tools: a tool that
    already advertises ``idempotentHint: true`` gets real enforcement; a tool that
    doesn't is returned unchanged (wrapping a pure tool only wastes storage, so
    idemkit never guesses — the declaration is the caller's, §8.2).
    """
    if not read_idempotent_hint(annotations):
        return fn
    return mcp_idempotent(
        backend=backend,
        key_fields=key_fields,
        scope=scope,
        version=version,
        **kwargs,
    )(fn)
