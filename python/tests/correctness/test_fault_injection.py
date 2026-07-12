"""Fault injection: prove the core stays correct when storage misbehaves.

`FaultInjectingBackend` wraps a real backend and randomly raises a transient
`StorageError` before selected operations reach it, exactly like a network blip a
caller can safely retry. We then run concurrent work through it and check the
guarantees hold:

- with only claim faults, the side effect still runs exactly once;
- with faults on every operation (including complete, where a lost completion is
  at-least-once by design), a caller never sees a *wrong* result.

Faults are seeded, so a failure reproduces.
"""

from __future__ import annotations

import asyncio
import random
from typing import Any

import pytest

from idemkit import InMemoryBackend, MethodConfig, idempotent
from idemkit.core.exceptions import IdempotencyError, StorageError


class FaultInjectingBackend:
    """A backend wrapper that injects transient failures into chosen operations.

    A fault is raised BEFORE the inner op runs, so it did not apply: this models
    a transient blip the caller retries, not a half-applied write.
    """

    def __init__(
        self, inner: Any, *, fail_ops: set[str], rng: random.Random, fail_prob: float = 0.3
    ) -> None:
        self._inner = inner
        self._fail_ops = fail_ops
        self._rng = rng
        self._fail_prob = fail_prob

    def _maybe_fail(self, op: str) -> None:
        if op in self._fail_ops and self._rng.random() < self._fail_prob:
            raise StorageError(f"injected transient fault in {op}")

    async def claim(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_fail("claim")
        return await self._inner.claim(*args, **kwargs)

    async def complete(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_fail("complete")
        return await self._inner.complete(*args, **kwargs)

    async def release(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_fail("release")
        return await self._inner.release(*args, **kwargs)

    async def renew(self, *args: Any, **kwargs: Any) -> Any:
        self._maybe_fail("renew")
        return await self._inner.renew(*args, **kwargs)

    async def wait_for_completion(self, *args: Any, **kwargs: Any) -> Any:
        return await self._inner.wait_for_completion(*args, **kwargs)

    async def aclose(self) -> None:
        aclose = getattr(self._inner, "aclose", None)
        if aclose is not None:
            await aclose()


async def _fire_concurrent(op: Any, *, workers: int, retries: int) -> list[Any]:

    async def worker() -> Any:
        for _ in range(retries):
            try:
                return await op(x=7)
            except IdempotencyError:
                await asyncio.sleep(0.003)
        return None

    return await asyncio.gather(*[worker() for _ in range(workers)])


@pytest.mark.parametrize("seed", range(25))
async def test_exactly_once_under_transient_claim_faults(seed: int) -> None:
    rng = random.Random(seed)
    backend = FaultInjectingBackend(InMemoryBackend(), fail_ops={"claim"}, rng=rng, fail_prob=0.35)
    runs = {"n": 0}

    @idempotent(
        backend=backend,
        config=MethodConfig(key_fields=["x"], scope=lambda a: "s", wait_timeout_seconds=3.0),
    )
    async def op(*, x: int) -> dict[str, int]:
        runs["n"] += 1
        await asyncio.sleep(0.01)
        return {"v": x}

    results = await _fire_concurrent(op, workers=8, retries=25)
    assert runs["n"] == 1, f"handler ran {runs['n']} times under claim faults"
    got = [r for r in results if r is not None]
    assert got, "no worker got through despite the faults being transient"
    assert all(r == {"v": 7} for r in got)


@pytest.mark.parametrize("seed", range(25))
async def test_never_a_wrong_result_under_faults_on_every_op(seed: int) -> None:
    rng = random.Random(seed)
    backend = FaultInjectingBackend(
        InMemoryBackend(),
        fail_ops={"claim", "complete", "release", "renew"},
        rng=rng,
        fail_prob=0.3,
    )
    runs = {"n": 0}

    @idempotent(
        backend=backend,
        config=MethodConfig(
            key_fields=["x"], scope=lambda a: "s", lease_ttl_seconds=0.05, wait_timeout_seconds=0.3
        ),
    )
    async def op(*, x: int) -> dict[str, int]:
        runs["n"] += 1
        await asyncio.sleep(0.005)
        return {"v": x}

    results = await _fire_concurrent(op, workers=8, retries=60)
    got = [r for r in results if r is not None]
    assert all(r == {"v": 7} for r in got), f"a wrong result slipped through: {got}"
    assert runs["n"] >= 1
