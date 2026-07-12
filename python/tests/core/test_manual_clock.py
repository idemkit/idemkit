"""ManualClock — deterministic lease/TTL tests without real sleeps (spec §9.4)."""

from __future__ import annotations

from idemkit import InMemoryBackend, ManualClock
from idemkit.core.state import ClaimResultType


async def test_manual_clock_expires_completed_ttl_deterministically() -> None:
    clock = ManualClock()
    backend = InMemoryBackend(clock=clock)

    first = await backend.claim("k", "fp", 1, 30.0)
    assert first.result == ClaimResultType.NEW_CLAIMED
    await backend.complete("k", first.our_claim_token, 200, {}, b"body", 100.0)

    # Before the TTL elapses, the record replays.
    assert (await backend.claim("k", "fp", 1, 30.0)).result == ClaimResultType.ALREADY_COMPLETED

    # Advance past completed_ttl — no real sleep — and it re-claims fresh.
    clock.advance(101)
    assert (await backend.claim("k", "fp", 1, 30.0)).result == ClaimResultType.NEW_CLAIMED


async def test_manual_clock_expires_lease_for_crash_recovery() -> None:
    clock = ManualClock()
    backend = InMemoryBackend(clock=clock)

    # A claim whose owner "crashes" (never completes/releases).
    await backend.claim("k", "fp", 1, 30.0)
    # A redelivery before the lease lapses is blocked.
    assert (await backend.claim("k", "fp", 1, 30.0)).result == ClaimResultType.ALREADY_CLAIMED
    # Advance past the lease — the redelivery reclaims it.
    clock.advance(31)
    assert (await backend.claim("k", "fp", 1, 30.0)).result == ClaimResultType.LEASE_RECLAIMED
