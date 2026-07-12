"""In-memory backend per spec §4.1 / §4.13.

Single-process only — NOT distributed. Intended for development and tests.
``IdempotencyMiddleware`` logs a warning if this backend is used outside a
test environment.

Storage model: per-effective-key ``asyncio.Lock`` (here approximated by a
single coarse lock since asyncio is single-threaded; the coarse lock is
correct and sufficient for in-process use) plus a dict keyed by effective
key. Per-key ``asyncio.Event`` instances signal completion to waiters
(spec §4.3 subscribe-before-read).

Per spec §4.13, eviction is per-key TTL only — no LRU. When ``max_size`` is
reached, new claims are rejected with ``MemoryBackendFull``, which the
middleware maps to a ``storage_error`` event (spec §4.16). This is the
explicit design choice: any LRU that can evict ``CLAIMED`` records risks
duplicate execution under load.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from collections.abc import Callable

from idemkit.core.state import (
    ClaimResult,
    ClaimResultType,
    State,
    StoredRecord,
)

_logger = logging.getLogger(__name__)

# In-flight wait poll schedule (spec §5.5): re-read at a backed-off cadence so a
# lost completion notification can't hang a waiter for the full timeout.
_POLL_INITIAL_SECONDS = 0.05
_POLL_CAP_SECONDS = 1.0


class MemoryBackendFull(RuntimeError):
    """Raised when ``InMemoryBackend`` is at ``max_size`` and rejects a new claim."""


class ManualClock:
    """An advanceable clock for deterministic lease/TTL tests (spec §9.4).

    Pass to ``InMemoryBackend(clock=ManualClock())`` and call ``advance(seconds)``
    to expire leases and completed-TTLs without real sleeps::

        clock = ManualClock()
        backend = InMemoryBackend(clock=clock)
        ...  # claim + complete
        clock.advance(86_401)  # past expires_after_seconds
        ...  # next claim now sees the record as expired
    """

    def __init__(self, start: float = 0.0) -> None:
        self._t = start

    def __call__(self) -> float:
        return self._t

    def advance(self, seconds: float) -> None:
        self._t += seconds


class InMemoryBackend:
    """Reference in-memory implementation of the backend Protocol."""

    def __init__(
        self,
        *,
        max_size: int = 10_000,
        default_expires_after_seconds: float = 86_400.0,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._max_size = max_size
        self._default_completed_ttl = default_expires_after_seconds
        self._records: dict[str, StoredRecord] = {}
        self._waiters: dict[str, asyncio.Event] = {}
        self._lock = asyncio.Lock()
        # The storage clock that drives lease expiry and TTLs. Inject a ManualClock
        # in tests to advance time deterministically instead of sleeping (spec §9.4).
        # (The in-flight wait timeout still uses real time, so it always elapses.)
        self._clock: Callable[[], float] = clock or time.monotonic

    async def aclose(self) -> None:
        """No-op: the in-memory backend holds no external resources. Provided so
        it is a drop-in for the Redis/Postgres backends in ``async with`` and
        ``aclose()`` teardown code."""
        return

    async def __aenter__(self) -> InMemoryBackend:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        async with self._lock:
            self._gc_expired_unlocked()
            existing = self._records.get(effective_key)
            now = self._clock()

            if existing is None:
                if len(self._records) >= self._max_size:
                    _logger.warning(
                        "idemkit.InMemoryBackend at max_size=%d; rejecting claim. "
                        "Increase max_size or switch to a distributed backend.",
                        self._max_size,
                    )
                    raise MemoryBackendFull(f"InMemoryBackend at max_size={self._max_size}")
                return self._fresh_claim(
                    effective_key, fingerprint, fingerprint_version, lease_ttl_seconds, now
                )

            if existing.state == State.COMPLETED:
                # Expired completed records are dropped by _gc_expired_unlocked above
                # so reaching here means a valid completed record exists.
                return ClaimResult(
                    result=ClaimResultType.ALREADY_COMPLETED,
                    record=existing,
                )

            # state == CLAIMED
            if existing.lease_until < now:
                # Lease expired; reclaim atomically. Wake any pre-existing waiter
                # that was watching the old claim — they will re-read state and
                # see the new record (different claim_token) and behave correctly.
                old_event = self._waiters.pop(effective_key, None)
                if old_event is not None:
                    old_event.set()
                result = self._fresh_claim(
                    effective_key,
                    fingerprint,
                    fingerprint_version,
                    lease_ttl_seconds,
                    now,
                )
                return ClaimResult(
                    result=ClaimResultType.LEASE_RECLAIMED,
                    record=result.record,
                    our_claim_token=result.our_claim_token,
                )

            return ClaimResult(
                result=ClaimResultType.ALREADY_CLAIMED,
                record=existing,
            )

    def _fresh_claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
        now: float,
    ) -> ClaimResult:
        """Write a brand-new CLAIMED record. Caller holds self._lock."""
        claim_token = secrets.token_hex(16)  # 128 bits, spec §4.1
        record = StoredRecord(
            effective_key=effective_key,
            state=State.CLAIMED,
            fingerprint=fingerprint,
            fingerprint_version=fingerprint_version,
            claim_token=claim_token,
            claimed_at=now,
            lease_until=now + lease_ttl_seconds,
        )
        self._records[effective_key] = record
        return ClaimResult(
            result=ClaimResultType.NEW_CLAIMED,
            record=record,
            our_claim_token=claim_token,
        )

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        expires_after_seconds: float,
    ) -> bool:
        async with self._lock:
            existing = self._records.get(effective_key)
            if (
                existing is None
                or existing.state != State.CLAIMED
                or existing.claim_token != claim_token
            ):
                return False

            now = self._clock()
            self._records[effective_key] = StoredRecord(
                effective_key=existing.effective_key,
                state=State.COMPLETED,
                fingerprint=existing.fingerprint,
                fingerprint_version=existing.fingerprint_version,
                claim_token=existing.claim_token,
                claimed_at=existing.claimed_at,
                lease_until=now + expires_after_seconds,  # spec §4.8 TTL from completed_at
                completed_at=now,
                response_status=response_status,
                response_headers=dict(response_headers),
                response_body=response_body,
            )
            self._signal_waiters_unlocked(effective_key)
            return True

    async def release(self, effective_key: str, claim_token: str) -> bool:
        async with self._lock:
            existing = self._records.get(effective_key)
            if (
                existing is None
                or existing.state != State.CLAIMED
                or existing.claim_token != claim_token
            ):
                return False
            del self._records[effective_key]
            self._signal_waiters_unlocked(effective_key)
            return True

    async def renew(
        self,
        effective_key: str,
        claim_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        """Push out the lease of a still-held claim (spec §5.3.1)."""
        async with self._lock:
            existing = self._records.get(effective_key)
            if (
                existing is None
                or existing.state != State.CLAIMED
                or existing.claim_token != claim_token
            ):
                return False
            now = self._clock()
            # Replace with a copy carrying the new lease deadline. We do not
            # require the old lease to still be live: this op and claim() both
            # hold self._lock, so they serialize — if a reclaim had already
            # happened, claim_token would differ and we'd have returned False.
            self._records[effective_key] = StoredRecord(
                effective_key=existing.effective_key,
                state=existing.state,
                fingerprint=existing.fingerprint,
                fingerprint_version=existing.fingerprint_version,
                claim_token=existing.claim_token,
                claimed_at=existing.claimed_at,
                lease_until=now + lease_ttl_seconds,
            )
            return True

    async def wait_for_completion(
        self,
        effective_key: str,
        timeout_seconds: float,
    ) -> StoredRecord | None:
        # Subscribe BEFORE re-reading state, per spec §4.3, then wait on a
        # NOTIFICATION-OR-POLL schedule (§5.5): the event is the latency
        # optimization, the bounded backed-off re-read is the correctness floor.
        # An in-process Event can't really be "lost", but keeping the same loop
        # shape as the distributed backends is what makes the contract uniform.
        async with self._lock:
            event = self._waiters.setdefault(effective_key, asyncio.Event())

        deadline = time.monotonic() + timeout_seconds
        poll = _POLL_INITIAL_SECONDS
        while True:
            async with self._lock:
                existing = self._records.get(effective_key)
                if existing is not None and existing.state == State.COMPLETED:
                    return existing
                if existing is None:
                    # Released/reclaimed; the caller should retry the claim.
                    return None
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            event.clear()
            try:
                await asyncio.wait_for(event.wait(), timeout=min(poll, remaining))
            except asyncio.TimeoutError:
                pass
            poll = min(poll * 2, _POLL_CAP_SECONDS)

    def _signal_waiters_unlocked(self, effective_key: str) -> None:
        """Wake everyone waiting on this key. Caller holds self._lock."""
        event = self._waiters.pop(effective_key, None)
        if event is not None:
            event.set()

    def _gc_expired_unlocked(self) -> None:
        """Lazy GC: drop expired CLAIMED and COMPLETED records.

        Caller holds self._lock. Runs on every claim attempt; cheap because
        it's a single dict scan keyed on the small live set.
        """
        now = self._clock()
        expired: list[str] = []
        for key, record in self._records.items():
            if record.state == State.CLAIMED and record.lease_until < now:
                # CLAIMED lease expired — eligible for reclaim, but until a
                # new claim comes in we keep the record (claim() handles it).
                # We don't proactively delete here because doing so would lose
                # the claim_token used to reject a delayed completion from
                # the original owner. The reclaim path in claim() handles
                # the swap.
                continue
            if record.state == State.COMPLETED and record.lease_until < now:
                expired.append(key)
        for key in expired:
            del self._records[key]
            self._waiters.pop(key, None)
