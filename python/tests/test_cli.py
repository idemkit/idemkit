"""CLI integration tests for ``idemkit init-pg`` and ``idemkit pg-vacuum``.

These are SYNC tests because ``idemkit.cli.main()`` internally calls
``asyncio.run()`` — which can only run when there is no currently-running
event loop. The async helpers below use ``asyncio.run`` to talk to PG for
setup/verification.
"""

from __future__ import annotations

import asyncio
import io
import os
import uuid
from contextlib import redirect_stdout

import pytest

asyncpg = pytest.importorskip("asyncpg")

from idemkit.cli import main  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")

pytestmark = pytest.mark.skipif(
    not PG_URL, reason="set IDEMKIT_TEST_PG_URL to enable CLI tests"
)


def _run_cli(args: list[str]) -> tuple[int, str]:
    """Invoke ``idemkit.cli.main`` and capture exit code + stdout."""
    buf = io.StringIO()
    with redirect_stdout(buf):
        rc = main(args)
    return rc, buf.getvalue()


async def _drop_table() -> None:
    conn = await asyncpg.connect(PG_URL)
    try:
        await conn.execute("DROP TABLE IF EXISTS idemkit_records CASCADE")
    finally:
        await conn.close()


async def _table_exists() -> bool:
    conn = await asyncpg.connect(PG_URL)
    try:
        result = await conn.fetchval(
            "SELECT to_regclass('public.idemkit_records')"
        )
        return result == "idemkit_records"
    finally:
        await conn.close()


async def _insert_expired_completed(key: str) -> None:
    conn = await asyncpg.connect(PG_URL)
    try:
        await conn.execute(
            """
            INSERT INTO idemkit_records (
                effective_key, state, fingerprint, fingerprint_version,
                claim_token, claimed_at, lease_until,
                completed_at, response_status, response_headers, response_body
            ) VALUES (
                $1, 'COMPLETED', $2, 1, $3,
                NOW() - INTERVAL '2 hours',
                NOW() - INTERVAL '90 minutes',
                NOW() - INTERVAL '90 minutes',
                200, '{}'::jsonb, $4
            )
            """,
            key, "fp", "token-xyz", b"body",
        )
    finally:
        await conn.close()


async def _row_exists(key: str) -> bool:
    conn = await asyncpg.connect(PG_URL)
    try:
        row = await conn.fetchrow(
            "SELECT effective_key FROM idemkit_records WHERE effective_key = $1",
            key,
        )
        return row is not None
    finally:
        await conn.close()


# ----- Tests (intentionally sync — see module docstring) -----


def test_init_pg_creates_schema() -> None:
    """`idemkit init-pg` creates the idemkit_records table from scratch."""
    asyncio.run(_drop_table())
    rc, output = _run_cli(["init-pg", PG_URL])
    assert rc == 0
    assert "initialized" in output.lower()
    assert asyncio.run(_table_exists())


def test_init_pg_is_idempotent() -> None:
    """Running init-pg twice does not error (CREATE IF NOT EXISTS everywhere)."""
    rc1, _ = _run_cli(["init-pg", PG_URL])
    rc2, _ = _run_cli(["init-pg", PG_URL])
    assert rc1 == 0
    assert rc2 == 0


def test_pg_vacuum_removes_expired_records() -> None:
    """`idemkit pg-vacuum` deletes COMPLETED records past TTL."""
    _run_cli(["init-pg", PG_URL])
    key = f"vacuum-cli-{uuid.uuid4().hex}"
    asyncio.run(_insert_expired_completed(key))

    rc, output = _run_cli(["pg-vacuum", PG_URL, "--completed-ttl-seconds", "60"])
    assert rc == 0
    assert "vacuumed" in output.lower()
    assert not asyncio.run(_row_exists(key))


def test_help_exits_zero() -> None:
    """`idemkit --help` exits cleanly (argparse SystemExit on --help)."""
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0


def test_unknown_command_returns_nonzero() -> None:
    """argparse raises SystemExit for unknown subcommands."""
    with pytest.raises(SystemExit) as exc:
        main(["nonexistent-command"])
    assert exc.value.code != 0
