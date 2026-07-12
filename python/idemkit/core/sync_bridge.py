"""Drive idemkit's async core from synchronous callers.

The whole library is async (the core awaits the backend), but a large slice of
Python side-effect code is synchronous — Celery tasks, Flask/Django views, plain
threaded SQS/Kafka workers. The synchronous facades (``idempotent_sync`` and
``IdempotentConsumer.dispatch_sync``) run that code on a single, shared background
event loop kept alive in a daemon thread.

Why one persistent loop rather than ``asyncio.run`` per call: a backend's
connection pool (Redis, PostgreSQL) binds to the loop it is first used on. Spinning
up a fresh loop per call would rebind — or orphan — the pool every time. A single
long-lived loop keeps the pool valid across calls, exactly as an async program
would.
"""

from __future__ import annotations

import asyncio
import atexit
import threading
from collections.abc import Coroutine
from typing import Any, TypeVar

_T = TypeVar("_T")


class _BackgroundLoop:
    """A daemon thread running a dedicated asyncio loop."""

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run, name="idemkit-sync-loop", daemon=True)
        self._thread.start()

    def _run(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    def run(self, coro: Coroutine[Any, Any, _T]) -> _T:
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)
        return future.result()

    def close(self) -> None:
        if not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
            self._loop.close()


_lock = threading.Lock()
_instance: _BackgroundLoop | None = None
_closables: list[Any] = []


def register_closable(obj: Any) -> None:
    """Track a backend used through the sync facade so it can be closed cleanly
    on the background loop at process exit.

    Without this, a Redis/Postgres pool bound to the background loop is GC'd after
    the loop is gone, printing a spurious ``RuntimeError: Event loop is closed`` at
    shutdown. Closing it on its own loop first avoids that. Only objects exposing
    ``aclose`` are tracked; duplicates (by identity) are ignored.
    """
    if hasattr(obj, "aclose") and all(obj is not c for c in _closables):
        _closables.append(obj)


def run_sync(coro: Coroutine[Any, Any, _T]) -> _T:
    """Run an idemkit core coroutine to completion from a synchronous caller.

    Lazily starts the shared background loop on first use. Refuses to run from
    inside an already-running event loop (that would block the caller's loop): in
    async code, use the async API (``idempotent`` / ``dispatch``) directly.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        pass  # good: no running loop, we're in synchronous code
    else:
        coro.close()
        raise RuntimeError(
            "idemkit: the synchronous API (idempotent_sync / dispatch_sync) "
            "was called from inside a running event loop, which would block it. "
            "Use the async API (idempotent / dispatch / await) instead."
        )

    global _instance
    if _instance is None:
        with _lock:
            if _instance is None:
                _instance = _BackgroundLoop()
    return _instance.run(coro)


async def _aclose_all() -> None:
    for obj in _closables:
        try:
            await obj.aclose()
        except Exception:
            pass


@atexit.register
def _shutdown() -> None:
    global _instance
    if _instance is not None:
        # Close tracked backends ON the background loop (while it's still alive),
        # then stop the loop — avoids the GC-after-loop-closed teardown noise.
        try:
            _instance.run(_aclose_all())
        except Exception:
            pass
        _instance.close()
        _instance = None
    _closables.clear()
