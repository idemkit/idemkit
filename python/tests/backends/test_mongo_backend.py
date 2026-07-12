"""MongoDB-specific behavior (the Protocol itself is covered by the cross-backend
contract test). Runs only when ``IDEMKIT_TEST_MONGO_URL`` is set."""

from __future__ import annotations

import uuid

import pytest

pytest.importorskip("pymongo")

from tests._backends import MONGO_URL

pytestmark = pytest.mark.skipif(not MONGO_URL, reason="needs IDEMKIT_TEST_MONGO_URL")


def _k(suffix: str) -> str:
    return f"mongo-{suffix}-{uuid.uuid4().hex}"


async def test_custom_collection_isolates_records() -> None:
    """Two collections on one database share no keys (like a Postgres custom table)."""
    from idemkit.backends.mongo import MongoBackend
    from idemkit.core.state import ClaimResultType

    a = MongoBackend.from_url(MONGO_URL, database="idemkit_iso", collection="coll_a")
    b = MongoBackend.from_url(MONGO_URL, database="idemkit_iso", collection="coll_b")
    key = _k("iso")
    try:
        ra = await a.claim(key, "fp", 1, 60.0)
        rb = await b.claim(key, "fp", 1, 60.0)
        # Same effective_key, different collections: both are NEW (fully isolated).
        assert ra.result is ClaimResultType.NEW_CLAIMED
        assert rb.result is ClaimResultType.NEW_CLAIMED
    finally:
        await a.aclose()
        await b.aclose()


async def test_corrupt_record_treated_as_absent() -> None:
    """Spec §4.12: a live but unreadable doc is re-claimed fresh, not turned into a 500."""
    from datetime import datetime, timedelta, timezone

    from idemkit.backends.mongo import MongoBackend
    from idemkit.core.state import ClaimResultType

    b = MongoBackend.from_url(MONGO_URL, database="idemkit_corrupt")
    key = _k("corrupt")
    try:
        coll = await b._ensure_collection()
        # A doc that looks live (lease far in the future, so it is not takeable) but is
        # missing the fields _doc_to_record needs, so parsing it fails.
        far_future = datetime.now(timezone.utc) + timedelta(hours=1)
        await coll.insert_one({"_id": key, "state": "CLAIMED", "lease_until": far_future})
        result = await b.claim(key, "fp", 1, 30.0)
        assert result.result is ClaimResultType.NEW_CLAIMED
        assert result.recovered_from_corrupt is True
    finally:
        await b.aclose()


async def test_ttl_index_is_created() -> None:
    """A TTL index on expire_at (lease + grace) is created so Mongo self-reaps."""
    from idemkit.backends.mongo import MongoBackend

    b = MongoBackend.from_url(MONGO_URL, database="idemkit_ttl")
    try:
        coll = await b._ensure_collection()
        info = await coll.index_information()
        assert any(
            "expireAfterSeconds" in spec and "expire_at" in str(spec.get("key"))
            for spec in info.values()
        ), f"no TTL index on expire_at: {info}"
    finally:
        await b.aclose()
