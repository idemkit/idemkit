"""Spec §4.13 in-memory eviction safety tests.

The in-memory backend has no LRU — at ``max_size`` it raises
``MemoryBackendFull``, which the engine routes via the ``storage_error``
path. This prevents the class of bugs where an LRU evicts a ``CLAIMED``
record and causes duplicate execution.
"""

from __future__ import annotations

import asyncio

import pytest

from idemkit.backends.memory import InMemoryBackend, MemoryBackendFull


async def test_at_max_size_rejects_new_claims() -> None:
    backend = InMemoryBackend(max_size=3)

    # Fill it up
    for i in range(3):
        result = await backend.claim(
            effective_key=f"k-{i}",
            fingerprint=f"fp-{i}",
            fingerprint_version=1,
            lease_ttl_seconds=30.0,
        )
        assert result.our_claim_token is not None

    # 4th claim is rejected
    with pytest.raises(MemoryBackendFull):
        await backend.claim(
            effective_key="k-4",
            fingerprint="fp-4",
            fingerprint_version=1,
            lease_ttl_seconds=30.0,
        )


async def test_max_size_does_not_block_already_existing_keys() -> None:
    """Existing keys (replays / repeats) work even at capacity."""
    backend = InMemoryBackend(max_size=2)

    first = await backend.claim(
        effective_key="k-A",
        fingerprint="fp-A",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    await backend.claim(
        effective_key="k-B",
        fingerprint="fp-B",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )

    # A duplicate claim on existing k-A succeeds (returns ALREADY_CLAIMED,
    # not MemoryBackendFull)
    duplicate = await backend.claim(
        effective_key="k-A",
        fingerprint="fp-A",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert duplicate.our_claim_token is None  # we did not get a new claim
    assert duplicate.record.claim_token == first.our_claim_token


async def test_claim_succeeds_after_record_expires() -> None:
    """Once a COMPLETED record expires (TTL elapsed), space is freed."""
    backend = InMemoryBackend(max_size=2, default_completed_ttl_seconds=30.0)

    # Fill with one CLAIMED + one COMPLETED whose TTL is short
    await backend.claim(
        effective_key="k-A",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    b = await backend.claim(
        effective_key="k-B",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    await backend.complete(
        effective_key="k-B",
        claim_token=b.our_claim_token,
        response_status=200,
        response_headers={},
        response_body=b"x",
        completed_ttl_seconds=0.05,  # 50 ms
    )

    # Now full at 2/2
    with pytest.raises(MemoryBackendFull):
        await backend.claim(
            effective_key="k-C",
            fingerprint="fp",
            fingerprint_version=1,
            lease_ttl_seconds=30.0,
        )

    # Wait for k-B's COMPLETED TTL to expire
    await asyncio.sleep(0.15)

    # Now claiming k-C succeeds (gc cleared k-B)
    c = await backend.claim(
        effective_key="k-C",
        fingerprint="fp",
        fingerprint_version=1,
        lease_ttl_seconds=30.0,
    )
    assert c.our_claim_token is not None
