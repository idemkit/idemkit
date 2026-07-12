"""Tests for the synchronous bridge that drives the async core from sync code."""

from __future__ import annotations

import asyncio

import pytest

from idemkit.core.sync_bridge import run_sync


def test_run_sync_executes_coroutine_and_returns_result() -> None:
    async def work() -> int:
        await asyncio.sleep(0)
        return 42

    assert run_sync(work()) == 42


def test_run_sync_propagates_exceptions() -> None:
    async def boom() -> None:
        raise ValueError("kaboom")

    with pytest.raises(ValueError, match="kaboom"):
        run_sync(boom())


async def test_run_sync_refuses_inside_running_loop() -> None:
    """Called from async code (a running loop), it would block the caller's loop —
    so it raises and tells you to use the async API instead."""

    async def work() -> int:
        return 1

    with pytest.raises(RuntimeError, match="running event loop"):
        run_sync(work())
