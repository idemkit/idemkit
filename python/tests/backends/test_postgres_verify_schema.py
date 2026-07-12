"""Tests for ``PostgresBackend.verify_schema()`` — spec §7.2.

When the table is missing, the user MUST get a clear ConfigurationError
pointing to ``idemkit init-pg``, not an obscure asyncpg.UndefinedTableError.
"""

from __future__ import annotations

import os

import pytest

asyncpg = pytest.importorskip("asyncpg")

from idemkit.backends.postgres import PostgresBackend  # noqa: E402
from idemkit.core.exceptions import ConfigurationError  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")

pytestmark = pytest.mark.skipif(
    not PG_URL, reason="set IDEMKIT_TEST_PG_URL to enable verify_schema tests"
)


async def test_verify_schema_raises_clear_error_when_table_missing() -> None:
    """If idemkit_records does not exist, raise ConfigurationError naming the CLI."""
    assert PG_URL is not None
    # Drop the table to simulate a fresh / un-initialized DB.
    conn = await asyncpg.connect(PG_URL)
    try:
        await conn.execute("DROP TABLE IF EXISTS idemkit_records CASCADE")
    finally:
        await conn.close()

    # from_url is lazy (§7.1): the schema is verified on first use, so the
    # ConfigurationError surfaces on the first backend operation.
    backend = PostgresBackend.from_url(PG_URL, min_size=1, max_size=2)
    with pytest.raises(ConfigurationError) as exc:
        await backend.claim("vs-missing", "fp", 1, 30.0)
    await backend.aclose()

    assert "idemkit init-pg" in str(exc.value)
    assert "idemkit_records" in str(exc.value)


async def test_verify_schema_succeeds_when_table_present() -> None:
    """After init-pg, verify_schema is a no-op on first use."""
    from idemkit.backends.postgres import init_pg

    assert PG_URL is not None
    await init_pg(PG_URL)
    backend = PostgresBackend.from_url(PG_URL, min_size=1, max_size=2)
    # First op opens the pool and verifies the schema without raising.
    await backend.claim("vs-present", "fp", 1, 30.0)
    await backend.aclose()
