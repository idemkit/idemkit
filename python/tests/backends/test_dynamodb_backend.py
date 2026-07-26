"""DynamoDB-specific behavior (the Protocol itself is covered by the cross-backend
contract test). Runs only when ``IDEMKIT_TEST_DYNAMODB_ENDPOINT`` is set."""

from __future__ import annotations

import uuid

import pytest

pytest.importorskip("aioboto3")

from tests._backends import DYNAMODB_ENDPOINT

pytestmark = pytest.mark.skipif(
    not DYNAMODB_ENDPOINT, reason="needs IDEMKIT_TEST_DYNAMODB_ENDPOINT"
)


def _dynamo(table: str, **kwargs):
    from idemkit.backends.dynamodb import DynamoBackend

    return DynamoBackend(
        table=table,
        endpoint_url=DYNAMODB_ENDPOINT,
        region_name="us-east-1",
        aws_access_key_id="test",
        aws_secret_access_key="test",
        **kwargs,
    )


def test_from_url_matches_direct_construction() -> None:
    """`from_url` exists for parity with the other backends and maps the URL to the
    endpoint, without connecting (lazy)."""
    from idemkit.backends.dynamodb import DynamoBackend

    backend = DynamoBackend.from_url(DYNAMODB_ENDPOINT, table="idempotency_keys")
    assert backend._endpoint_url == DYNAMODB_ENDPOINT
    assert backend._table_name == "idempotency_keys"
    assert backend._create_table is True


def _k(suffix: str) -> str:
    return f"dynamo-{suffix}-{uuid.uuid4().hex}"


async def test_flat_timeout_kwargs_are_applied() -> None:
    """connect_timeout / read_timeout / max_retries build a botocore Config and the
    backend still works (proves the flat knobs flow through to the resource)."""
    from idemkit.core.state import ClaimResultType

    b = _dynamo("idemkit_timeouts", connect_timeout=2, read_timeout=3, max_retries=5)
    try:
        result = await b.claim(_k("to"), "fp", 1, 30.0)
        assert result.result is ClaimResultType.NEW_CLAIMED
    finally:
        await b.aclose()


async def test_corrupt_record_treated_as_absent() -> None:
    """Spec §4.12: a live but unreadable item is re-claimed fresh, not turned into a 500."""
    from decimal import Decimal

    from idemkit.core.state import ClaimResultType

    b = _dynamo("idemkit_corrupt")
    key = _k("corrupt")
    try:
        table = await b._ensure_table()
        # Looks live (lease far in the future, so the claim condition fails) but is
        # missing the fields _item_to_record needs, so parsing it fails.
        far_future = Decimal(str(1e18))
        await table.put_item(
            Item={"effective_key": key, "state": "CLAIMED", "lease_until": far_future}
        )
        result = await b.claim(key, "fp", 1, 30.0)
        assert result.result is ClaimResultType.NEW_CLAIMED
        assert result.recovered_from_corrupt is True
    finally:
        await b.aclose()


async def test_table_auto_created_and_custom_table_isolates() -> None:
    """The table is created on first use, and two tables share no keys."""
    from idemkit.core.state import ClaimResultType

    a = _dynamo("idemkit_iso_a")
    b = _dynamo("idemkit_iso_b")
    key = _k("iso")
    try:
        ra = await a.claim(key, "fp", 1, 60.0)  # also proves auto table creation
        rb = await b.claim(key, "fp", 1, 60.0)
        # Same effective_key, different tables: both are NEW (fully isolated).
        assert ra.result is ClaimResultType.NEW_CLAIMED
        assert rb.result is ClaimResultType.NEW_CLAIMED
    finally:
        await a.aclose()
        await b.aclose()
