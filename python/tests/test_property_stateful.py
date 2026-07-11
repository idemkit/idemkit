"""Property-based (Hypothesis) verification of the idempotency state machine.

Hypothesis generates random sequences of claim / complete / release / renew /
advance-clock operations against a real backend and a small reference model, and
asserts they agree at every step, plus a replay-stability invariant after each
step. This adversarially explores orderings and timings a hand-written test would
not, and shrinks any divergence to a minimal repro. It proves the fencing + lease
+ completed-TTL state machine matches its spec.

The keyspace is tiny on purpose, so claims collide and leases get reclaimed often.

- The **memory** machine always runs and controls a ManualClock, so it exercises
  lease/TTL expiry and reclaim deterministically.
- The **Redis / Postgres** machines run when `IDEMKIT_TEST_REDIS_URL` /
  `IDEMKIT_TEST_PG_URL` are set. They don't advance a clock (real backends use the
  server clock), and use long leases so nothing expires mid-test, so they verify
  the fencing + state transitions of the real Lua / SQL under random orderings.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any

from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, precondition, rule

from idemkit.backends.memory import InMemoryBackend, ManualClock
from idemkit.core.state import ClaimResultType

FP = "fp"
FP_VERSION = 1
GARBAGE_TOKEN = "stale-token-that-was-never-valid"

_ABSENT: dict[str, Any] = {"state": "ABSENT", "token": None, "lease_until": None, "result": None}


class _BaseStateMachine(RuleBasedStateMachine):
    """Drive a backend and a reference model in lockstep. Subclasses pick the
    backend; ``controllable_clock`` gates the clock-advance rule."""

    controllable_clock: bool = True

    def __init__(self) -> None:
        super().__init__()
        self.loop = asyncio.new_event_loop()
        prefix = self._key_prefix()
        self.keys = [f"{prefix}-k{i}" for i in range(3)]
        self.clock, self.backend = self._setup()
        self.model: dict[str, dict[str, Any]] = {k: dict(_ABSENT) for k in self.keys}

    # ----- subclass hooks -----
    def _key_prefix(self) -> str:
        raise NotImplementedError

    def _setup(self) -> tuple[Any, Any]:
        raise NotImplementedError

    # ----- helpers -----
    def _run(self, coro: Any) -> Any:
        return self.loop.run_until_complete(coro)

    def _ttl(self, ttl: float) -> float:
        # Real backends can't advance the clock, so keep leases far past the (fast)
        # test so nothing expires mid-run and the model never predicts a reclaim.
        return ttl if self.controllable_clock else ttl + 3600.0

    def _gc(self, key: str, now: float) -> None:
        # Matches the backend: an expired COMPLETED record is dropped; an expired
        # CLAIMED record is kept (claim() reclaims it, preserving fencing).
        m = self.model[key]
        if m["state"] == "COMPLETED" and m["lease_until"] < now:
            self.model[key] = dict(_ABSENT)

    # ----- rules -----
    @rule(idx=st.integers(0, 2), ttl=st.floats(min_value=1, max_value=1000))
    def claim(self, idx: int, ttl: float) -> None:
        key = self.keys[idx]
        ttl = self._ttl(ttl)
        now = self.clock()
        self._gc(key, now)
        m = self.model[key]

        if m["state"] == "ABSENT":
            expected = ClaimResultType.NEW_CLAIMED
        elif m["state"] == "COMPLETED":
            expected = ClaimResultType.ALREADY_COMPLETED
        elif m["lease_until"] < now:
            expected = ClaimResultType.LEASE_RECLAIMED
        else:
            expected = ClaimResultType.ALREADY_CLAIMED

        result = self._run(self.backend.claim(key, FP, FP_VERSION, ttl))
        assert result.result == expected, (
            f"claim({key}): expected {expected}, got {result.result}; model={m}, now={now}"
        )
        if expected in (ClaimResultType.NEW_CLAIMED, ClaimResultType.LEASE_RECLAIMED):
            self.model[key] = {
                "state": "CLAIMED",
                "token": result.our_claim_token,
                "lease_until": now + ttl,
                "result": None,
            }

    @rule(idx=st.integers(0, 2), valid=st.booleans(), ttl=st.floats(1, 1000))
    def complete(self, idx: int, valid: bool, ttl: float) -> None:
        key = self.keys[idx]
        ttl = self._ttl(ttl)
        now = self.clock()
        m = self.model[key]
        token = m["token"] if (valid and m["token"] is not None) else GARBAGE_TOKEN
        expected = m["state"] == "CLAIMED" and token == m["token"]
        body = b"result:" + key.encode()

        ok = self._run(self.backend.complete(key, token, 200, {}, body, ttl))
        assert ok == expected, (
            f"complete({key}, valid={valid}): expected {expected}, got {ok}; model={m}"
        )
        if expected:
            self.model[key] = {
                "state": "COMPLETED",
                "token": m["token"],
                "lease_until": now + ttl,
                "result": body,
            }

    @rule(idx=st.integers(0, 2), valid=st.booleans())
    def release(self, idx: int, valid: bool) -> None:
        key = self.keys[idx]
        m = self.model[key]
        token = m["token"] if (valid and m["token"] is not None) else GARBAGE_TOKEN
        expected = m["state"] == "CLAIMED" and token == m["token"]

        ok = self._run(self.backend.release(key, token))
        assert ok == expected, (
            f"release({key}, valid={valid}): expected {expected}, got {ok}; model={m}"
        )
        if expected:
            self.model[key] = dict(_ABSENT)

    @rule(idx=st.integers(0, 2), valid=st.booleans(), ttl=st.floats(1, 1000))
    def renew(self, idx: int, valid: bool, ttl: float) -> None:
        key = self.keys[idx]
        ttl = self._ttl(ttl)
        now = self.clock()
        m = self.model[key]
        token = m["token"] if (valid and m["token"] is not None) else GARBAGE_TOKEN
        expected = m["state"] == "CLAIMED" and token == m["token"]

        ok = self._run(self.backend.renew(key, token, ttl))
        assert ok == expected, (
            f"renew({key}, valid={valid}): expected {expected}, got {ok}; model={m}"
        )
        if expected:
            m["lease_until"] = now + ttl

    @precondition(lambda self: self.controllable_clock)
    @rule(dt=st.floats(min_value=0.1, max_value=2000))
    def advance_clock(self, dt: float) -> None:
        self.clock.advance(dt)

    @invariant()
    def completed_result_replays_unchanged(self) -> None:
        # A live COMPLETED record must replay its exact stored result: the "no
        # wrong / lost result" guarantee under every interleaving above.
        now = self.clock()
        for key, m in self.model.items():
            if m["state"] == "COMPLETED" and m["lease_until"] >= now:
                result = self._run(self.backend.claim(key, FP, FP_VERSION, self._ttl(10.0)))
                assert result.result == ClaimResultType.ALREADY_COMPLETED
                assert result.record is not None
                assert result.record.response_body == m["result"], (
                    f"replayed result diverged for {key}: "
                    f"got {result.record.response_body!r}, model {m['result']!r}"
                )

    def teardown(self) -> None:
        try:
            self._run(self.backend.aclose())
        except Exception:
            pass
        self.loop.close()


class MemoryStateMachine(_BaseStateMachine):
    controllable_clock = True

    def _key_prefix(self) -> str:
        return "mem"

    def _setup(self) -> tuple[Any, Any]:
        clock = ManualClock()
        return clock, InMemoryBackend(clock=clock)


MemoryStateMachine.TestCase.settings = settings(
    max_examples=300, stateful_step_count=50, deadline=None
)
TestMemoryStateMachine = MemoryStateMachine.TestCase


_REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")
if _REDIS_URL:
    import time

    class RedisStateMachine(_BaseStateMachine):
        controllable_clock = False

        def _key_prefix(self) -> str:
            return f"prop-{uuid.uuid4().hex[:8]}"

        def _setup(self) -> tuple[Any, Any]:
            from idemkit import RedisBackend

            return time.monotonic, RedisBackend.from_url(_REDIS_URL)

    RedisStateMachine.TestCase.settings = settings(
        max_examples=30, stateful_step_count=25, deadline=None
    )
    TestRedisStateMachine = RedisStateMachine.TestCase


_PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
if _PG_URL:
    import time

    class PostgresStateMachine(_BaseStateMachine):
        controllable_clock = False

        def _key_prefix(self) -> str:
            return f"prop-{uuid.uuid4().hex[:8]}"

        def _setup(self) -> tuple[Any, Any]:
            from idemkit.backends.postgres import PostgresBackend, init_pg

            self.loop.run_until_complete(init_pg(_PG_URL))
            return time.monotonic, PostgresBackend.from_url(_PG_URL, min_size=1, max_size=4)

    PostgresStateMachine.TestCase.settings = settings(
        max_examples=20, stateful_step_count=20, deadline=None
    )
    TestPostgresStateMachine = PostgresStateMachine.TestCase
