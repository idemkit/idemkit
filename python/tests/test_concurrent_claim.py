"""The race-condition contract test for spec §4.1.

If this test does not pass, idemkit has no reason to exist. Every other
feature is built on the foundation of: N concurrent claims with the same
effective_key result in exactly one successful claim.
"""

from __future__ import annotations

import asyncio
from collections import Counter

from idemkit.backends.memory import InMemoryBackend
from idemkit.core.state import ClaimResultType, State


async def test_concurrent_same_key_exactly_one_new_claim() -> None:
    """50 concurrent claims, same effective_key, same fingerprint: exactly one NEW_CLAIMED."""
    backend = InMemoryBackend()
    effective_key = "eff-key-shared"
    fingerprint = "fp-1"
    lease_ttl = 30.0
    parallel = 50

    async def attempt() -> object:
        return await backend.claim(
            effective_key=effective_key,
            fingerprint=fingerprint,
            fingerprint_version=1,
            lease_ttl_seconds=lease_ttl,
        )

    results = await asyncio.gather(*(attempt() for _ in range(parallel)))
    outcomes = Counter(r.result for r in results)

    assert outcomes[ClaimResultType.NEW_CLAIMED] == 1, (
        f"expected exactly 1 NEW_CLAIMED out of {parallel}; got {dict(outcomes)}"
    )
    assert outcomes[ClaimResultType.ALREADY_CLAIMED] == parallel - 1

    winners = [r for r in results if r.result == ClaimResultType.NEW_CLAIMED]
    losers = [r for r in results if r.result == ClaimResultType.ALREADY_CLAIMED]

    assert winners[0].our_claim_token is not None
    assert all(loser.our_claim_token is None for loser in losers)
    # Losers observe the same CLAIMED record (with the winner's claim_token)
    winning_token = winners[0].our_claim_token
    assert all(loser.record.claim_token == winning_token for loser in losers)


async def test_complete_with_correct_token_transitions_to_completed() -> None:
    """Claim → complete with the same token → record is COMPLETED with the response."""
    backend = InMemoryBackend()
    result = await backend.claim(
        effective_key="k1",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert result.result == ClaimResultType.NEW_CLAIMED
    assert result.our_claim_token is not None

    ok = await backend.complete(
        effective_key="k1",
        claim_token=result.our_claim_token,
        response_status=201,
        response_headers={"Content-Type": "application/json"},
        response_body=b'{"id": "abc"}',
        completed_ttl_seconds=3600.0,
    )
    assert ok is True

    # Next claim should observe ALREADY_COMPLETED with the stored response
    next_result = await backend.claim(
        effective_key="k1",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert next_result.result == ClaimResultType.ALREADY_COMPLETED
    assert next_result.record.state == State.COMPLETED
    assert next_result.record.response_status == 201
    assert next_result.record.response_body == b'{"id": "abc"}'
    assert next_result.record.response_headers["Content-Type"] == "application/json"


async def test_complete_with_wrong_token_is_rejected() -> None:
    """Spec §4.1: complete with a non-matching claim_token MUST NOT transition.

    This is the safety net for delayed completions from an owner whose lease
    expired and was reclaimed by another process.
    """
    backend = InMemoryBackend()
    result = await backend.claim(
        effective_key="k2",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert result.result == ClaimResultType.NEW_CLAIMED

    ok = await backend.complete(
        effective_key="k2",
        claim_token="not-the-real-token",
        response_status=200,
        response_headers={},
        response_body=b"x",
        completed_ttl_seconds=3600.0,
    )
    assert ok is False, "complete with wrong claim_token must return False"

    # Original claim still CLAIMED, not corrupted
    next_result = await backend.claim(
        effective_key="k2",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert next_result.result == ClaimResultType.ALREADY_CLAIMED
    assert next_result.record.state == State.CLAIMED


async def test_release_with_correct_token_clears_claim() -> None:
    """Release allows the next request to claim as NEW."""
    backend = InMemoryBackend()
    result = await backend.claim(
        effective_key="k3",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert result.our_claim_token is not None

    released = await backend.release("k3", result.our_claim_token)
    assert released is True

    after = await backend.claim(
        effective_key="k3",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert after.result == ClaimResultType.NEW_CLAIMED


async def test_expired_lease_is_reclaimed() -> None:
    """Spec §4.4: an expired lease is reclaimable; outcome is LEASE_RECLAIMED.

    A new claim_token is generated; the original owner's delayed complete will
    be rejected (covered by test_complete_with_wrong_token_is_rejected).
    """
    backend = InMemoryBackend()
    first = await backend.claim(
        effective_key="k4",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=0.05,  # 50ms lease
    )
    assert first.result == ClaimResultType.NEW_CLAIMED
    original_token = first.our_claim_token

    await asyncio.sleep(0.1)  # lease elapses

    second = await backend.claim(
        effective_key="k4",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert second.result == ClaimResultType.LEASE_RECLAIMED
    assert second.our_claim_token is not None
    assert second.our_claim_token != original_token


async def test_wait_for_completion_returns_completed_record() -> None:
    """Spec §4.3: a waiter is signalled when the in-flight claim completes."""
    backend = InMemoryBackend()
    first = await backend.claim(
        effective_key="k5",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert first.our_claim_token is not None

    async def waiter() -> object:
        return await backend.wait_for_completion("k5", timeout_seconds=2.0)

    async def completer() -> None:
        await asyncio.sleep(0.05)
        await backend.complete(
            effective_key="k5",
            claim_token=first.our_claim_token,
            response_status=200,
            response_headers={},
            response_body=b"hello",
            completed_ttl_seconds=3600.0,
        )

    waited, _ = await asyncio.gather(waiter(), completer())
    assert waited is not None
    assert waited.state == State.COMPLETED
    assert waited.response_body == b"hello"
