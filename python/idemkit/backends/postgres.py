"""PostgreSQL backend per spec §4.1 / §4.3 / §7.2.

Storage: one row per effective_key in ``idemkit_records``.

Atomic claim via ``INSERT ... ON CONFLICT DO NOTHING``; lease reclaim via
a conditional ``UPDATE ... WHERE state = 'CLAIMED' AND lease_until < NOW()``.
All time comparisons use PostgreSQL's ``NOW()`` — never the app's clock.

In-flight wait uses one dedicated ``LISTEN idemkit_completions`` connection
per ``PostgresBackend`` instance; ``pg_notify`` payloads carry the
effective_key and are demultiplexed in-process to per-key ``asyncio.Event``
waiters. Per-request LISTEN is forbidden — it exhausts the connection pool.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import secrets
import time
from typing import Any

from idemkit.core.exceptions import ConfigurationError, StorageError
from idemkit.core.state import ClaimResult, ClaimResultType, State, StoredRecord

_logger = logging.getLogger(__name__)

NOTIFY_CHANNEL = "idemkit_completions"

# In-flight wait poll schedule (spec §5.5): the bounded backed-off re-read that
# keeps a dropped LISTEN/NOTIFY notification from hanging a waiter to timeout.
_POLL_INITIAL_SECONDS = 0.05
_POLL_CAP_SECONDS = 1.0

SCHEMA_VERSION = 1

DEFAULT_TABLE = "idemkit_records"


def _validate_table(table: str) -> str:
    """Guard the table name (it is interpolated into SQL, so it cannot be bound).

    Accepts an unqualified identifier only, kept short enough that the derived
    index names fit Postgres's 63-char limit.
    """
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", table) or len(table) > 46:
        raise ConfigurationError(
            f"idemkit: invalid Postgres table name {table!r}. Use an unqualified "
            "identifier: letters, digits, and underscores; start with a letter or "
            "underscore; at most 46 characters."
        )
    return table


def _schema_sql(table: str) -> str:
    return f"""
CREATE TABLE IF NOT EXISTS {table} (
    effective_key       TEXT PRIMARY KEY,
    state               TEXT NOT NULL CHECK (state IN ('CLAIMED', 'COMPLETED')),
    fingerprint         TEXT NOT NULL,
    fingerprint_version INTEGER NOT NULL,
    claim_token         TEXT NOT NULL,
    claimed_at          TIMESTAMPTZ NOT NULL,
    lease_until         TIMESTAMPTZ NOT NULL,
    completed_at        TIMESTAMPTZ,
    response_status     INTEGER,
    response_headers    JSONB,
    response_body       BYTEA,
    schema_version      INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS {table}_lease_until_idx
    ON {table} (lease_until)
    WHERE state = 'CLAIMED';

CREATE INDEX IF NOT EXISTS {table}_completed_at_idx
    ON {table} (completed_at)
    WHERE state = 'COMPLETED';
"""


class PostgresBackend:
    """Production-grade PostgreSQL backend."""

    def __init__(
        self,
        pool: Any = None,
        *,
        table: str = DEFAULT_TABLE,
        _url: str | None = None,
        _pool_kwargs: dict[str, Any] | None = None,
        _verify_schema: bool = True,
    ) -> None:
        """Construct with an existing asyncpg pool, or (preferred) via
        ``PostgresBackend.from_url(...)`` for lazy initialization (§7.1).

        ``table`` is the records table name (default ``idemkit_records``).
        Create it with ``idemkit init-pg <url> --table <name>`` before use.
        """
        self._table = _validate_table(table)
        self._pool = pool
        self._url = _url
        self._pool_kwargs = _pool_kwargs or {}
        self._verify_schema_on_init = _verify_schema
        self._pool_lock = asyncio.Lock()
        self._waiters: dict[str, set[asyncio.Event]] = {}
        self._listen_conn: Any | None = None
        self._listen_lock = asyncio.Lock()

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        table: str = DEFAULT_TABLE,
        min_size: int = 4,
        max_size: int = 20,
        verify_schema: bool = True,
        **kwargs: Any,
    ) -> PostgresBackend:
        """Construct from a postgres:// URL with **lazy** initialization (§7.1).

        This is synchronous and does not connect: the connection pool is opened
        on the first backend operation, on the serving event loop. This makes it
        safe to wire into ``app.add_middleware(...)`` at construction time and
        keeps startup cheap (nothing connects until the first request).

        ``verify_schema=True`` (default) checks, on first use, that the
        ``idemkit_records`` table exists and ``schema_version`` matches what this
        library expects, raising ``ConfigurationError`` otherwise with the exact
        CLI command to run.

        A default ``command_timeout`` caps every query so a hung-but-connected
        server fails fast (fail-closed) instead of blocking a request; override it
        via a ``command_timeout=`` keyword. Note that under pool exhaustion an
        ``acquire`` still waits for a free connection; size ``max_size`` for your
        concurrency.
        """
        kwargs.setdefault("command_timeout", 10.0)
        return cls(
            pool=None,
            table=table,
            _url=url,
            _pool_kwargs={"min_size": min_size, "max_size": max_size, **kwargs},
            _verify_schema=verify_schema,
        )

    async def _ensure_pool(self) -> None:
        """Open the pool on first use (lazy init, §7.1)."""
        if self._pool is not None:
            return
        async with self._pool_lock:
            if self._pool is not None:
                return
            if self._url is None:
                raise StorageError("postgres: no pool and no URL configured")
            try:
                import asyncpg
            except ImportError as e:
                raise ImportError(
                    "idemkit: install the `postgres` extra: pip install 'idemkit[postgres]'"
                ) from e
            pool = await asyncpg.create_pool(self._url, **self._pool_kwargs)
            self._pool = pool
            if self._verify_schema_on_init:
                try:
                    await self.verify_schema()
                except Exception:
                    # Tear the pool down so a later call can retry init cleanly
                    # (and so the schema error surfaces again, not a None pool).
                    await pool.close()
                    self._pool = None
                    raise

    async def aclose(self) -> None:
        """Tear down the listener and close the pool."""
        if self._pool is None:
            return
        async with self._listen_lock:
            if self._listen_conn is not None:
                try:
                    await self._listen_conn.remove_listener(NOTIFY_CHANNEL, self._on_notification)
                except Exception:
                    pass
                try:
                    await self._pool.release(self._listen_conn)
                except Exception:
                    pass
                self._listen_conn = None
        await self._pool.close()

    async def __aenter__(self) -> PostgresBackend:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    async def verify_schema(self) -> None:
        """Check the ``idemkit_records`` table exists with expected version.

        Raises ``ConfigurationError`` with the CLI command to run if the
        table is missing or its ``schema_version`` doesn't match.
        """
        import asyncpg

        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(f"SELECT schema_version FROM {self._table} LIMIT 1")
        except asyncpg.UndefinedTableError as e:
            raise ConfigurationError(
                f"idemkit: PostgreSQL table '{self._table}' does not exist. "
                f"Run `idemkit init-pg <database-url> --table {self._table}` to create it. "
                "See spec §7.2 for the schema."
            ) from e

        # Empty table is fine: SCHEMA_VERSION is the column default. A row
        # with mismatched schema_version means drift since this library
        # version was deployed.
        if row is not None and row["schema_version"] != SCHEMA_VERSION:
            raise ConfigurationError(
                f"idemkit: PostgreSQL schema_version mismatch "
                f"(table={row['schema_version']}, library={SCHEMA_VERSION}). "
                f"Run `idemkit init-pg <database-url>` after reviewing the schema."
            )

    # ----- spec §4.1 backend Protocol -----

    async def claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        claim_token = secrets.token_hex(16)
        lease_ms = int(lease_ttl_seconds * 1000)

        await self._ensure_pool()
        try:
            async with self._pool.acquire() as conn:
                # 1. Try fresh INSERT
                row = await conn.fetchrow(
                    f"""
                    INSERT INTO {self._table}
                        (effective_key, state, fingerprint, fingerprint_version,
                         claim_token, claimed_at, lease_until)
                    VALUES ($1, 'CLAIMED', $2, $3, $4, NOW(),
                            NOW() + ($5 || ' milliseconds')::INTERVAL)
                    ON CONFLICT (effective_key) DO NOTHING
                    RETURNING *
                    """,
                    effective_key,
                    fingerprint,
                    fingerprint_version,
                    claim_token,
                    str(lease_ms),
                )
                if row is not None:
                    return ClaimResult(
                        ClaimResultType.NEW_CLAIMED,
                        self._row_to_record(row),
                        claim_token,
                    )

                # 2. Reclaim an expired CLAIMED lease (§4.4) → LEASE_RECLAIMED.
                reclaim = await conn.fetchrow(
                    f"""
                    UPDATE {self._table}
                    SET state = 'CLAIMED',
                        fingerprint = $2,
                        fingerprint_version = $3,
                        claim_token = $4,
                        claimed_at = NOW(),
                        lease_until = NOW() + ($5 || ' milliseconds')::INTERVAL,
                        completed_at = NULL,
                        response_status = NULL,
                        response_headers = NULL,
                        response_body = NULL
                    WHERE effective_key = $1
                      AND state = 'CLAIMED'
                      AND lease_until < NOW()
                    RETURNING *
                    """,
                    effective_key,
                    fingerprint,
                    fingerprint_version,
                    claim_token,
                    str(lease_ms),
                )
                if reclaim is not None:
                    return ClaimResult(
                        ClaimResultType.LEASE_RECLAIMED,
                        self._row_to_record(reclaim),
                        claim_token,
                    )

                # 3. An expired COMPLETED record (lease_until = completed
                #    expiry, in the past) is treated as ABSENT and re-claimed
                #    fresh → NEW_CLAIMED, matching memory/redis where the record
                #    has simply TTL'd away (§4.8). This enforces completed_ttl
                #    on read instead of relying solely on pg-vacuum.
                fresh = await conn.fetchrow(
                    f"""
                    UPDATE {self._table}
                    SET state = 'CLAIMED',
                        fingerprint = $2,
                        fingerprint_version = $3,
                        claim_token = $4,
                        claimed_at = NOW(),
                        lease_until = NOW() + ($5 || ' milliseconds')::INTERVAL,
                        completed_at = NULL,
                        response_status = NULL,
                        response_headers = NULL,
                        response_body = NULL
                    WHERE effective_key = $1
                      AND state = 'COMPLETED'
                      AND lease_until < NOW()
                    RETURNING *
                    """,
                    effective_key,
                    fingerprint,
                    fingerprint_version,
                    claim_token,
                    str(lease_ms),
                )
                if fresh is not None:
                    return ClaimResult(
                        ClaimResultType.NEW_CLAIMED,
                        self._row_to_record(fresh),
                        claim_token,
                    )

                # 4. Read the live existing record (must exist if all above failed).
                existing = await conn.fetchrow(
                    f"SELECT * FROM {self._table} WHERE effective_key = $1",
                    effective_key,
                )
                if existing is None:
                    # Lost the race against a concurrent delete; signal storage error
                    raise StorageError("postgres: record disappeared during claim; retry")
                try:
                    record = self._row_to_record(existing)
                except Exception:
                    # Row can't be deserialized (schema drift, tampering, a
                    # future encryption scheme). §4.12: treat as ABSENT and
                    # re-claim fresh rather than 500. Overwrite the bad row.
                    corrupt = await conn.fetchrow(
                        f"""
                        UPDATE {self._table}
                        SET state = 'CLAIMED',
                            fingerprint = $2,
                            fingerprint_version = $3,
                            claim_token = $4,
                            claimed_at = NOW(),
                            lease_until = NOW() + ($5 || ' milliseconds')::INTERVAL,
                            completed_at = NULL,
                            response_status = NULL,
                            response_headers = NULL,
                            response_body = NULL
                        WHERE effective_key = $1
                        RETURNING *
                        """,
                        effective_key,
                        fingerprint,
                        fingerprint_version,
                        claim_token,
                        str(lease_ms),
                    )
                    if corrupt is None:
                        raise StorageError(
                            "postgres: corrupt record vanished during reclaim"
                        ) from None
                    return ClaimResult(
                        ClaimResultType.NEW_CLAIMED,
                        self._row_to_record(corrupt),
                        claim_token,
                        recovered_from_corrupt=True,
                    )
                if record.state == State.COMPLETED:
                    return ClaimResult(ClaimResultType.ALREADY_COMPLETED, record)
                return ClaimResult(ClaimResultType.ALREADY_CLAIMED, record)
        except StorageError:
            raise
        except Exception as e:
            raise StorageError(f"postgres claim failed: {e}") from e

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        expires_after_seconds: float,
    ) -> bool:
        completed_ms = int(expires_after_seconds * 1000)
        await self._ensure_pool()
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                # lease_until is repurposed as the COMPLETED record's expiry
                # (completed_at + completed_ttl). The claim path treats any
                # record past lease_until as ABSENT, so completed records
                # expire on read uniformly with the other backends (§4.8),
                # not only when pg-vacuum runs.
                row = await conn.fetchrow(
                    f"""
                        UPDATE {self._table}
                        SET state = 'COMPLETED',
                            completed_at = NOW(),
                            lease_until = NOW() + ($6 || ' milliseconds')::INTERVAL,
                            response_status = $2,
                            response_headers = $3::jsonb,
                            response_body = $4
                        WHERE effective_key = $1
                          AND state = 'CLAIMED'
                          AND claim_token = $5
                        RETURNING effective_key
                        """,
                    effective_key,
                    response_status,
                    json.dumps(response_headers),
                    response_body,
                    claim_token,
                    str(completed_ms),
                )
                if row is None:
                    return False
                await conn.execute(
                    "SELECT pg_notify($1, $2)",
                    NOTIFY_CHANNEL,
                    effective_key,
                )
                return True
        except Exception as e:
            raise StorageError(f"postgres complete failed: {e}") from e

    async def release(self, effective_key: str, claim_token: str) -> bool:
        await self._ensure_pool()
        try:
            async with self._pool.acquire() as conn, conn.transaction():
                row = await conn.fetchrow(
                    f"""
                        DELETE FROM {self._table}
                        WHERE effective_key = $1
                          AND state = 'CLAIMED'
                          AND claim_token = $2
                        RETURNING effective_key
                        """,
                    effective_key,
                    claim_token,
                )
                if row is None:
                    return False
                await conn.execute(
                    "SELECT pg_notify($1, $2)",
                    NOTIFY_CHANNEL,
                    effective_key,
                )
                return True
        except Exception as e:
            raise StorageError(f"postgres release failed: {e}") from e

    async def renew(
        self,
        effective_key: str,
        claim_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        """Extend a live claim's lease using the storage clock (spec §5.3.1)."""
        lease_ms = int(lease_ttl_seconds * 1000)
        await self._ensure_pool()
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"""
                    UPDATE {self._table}
                    SET lease_until = NOW() + ($3 || ' milliseconds')::INTERVAL
                    WHERE effective_key = $1
                      AND state = 'CLAIMED'
                      AND claim_token = $2
                    RETURNING effective_key
                    """,
                    effective_key,
                    claim_token,
                    str(lease_ms),
                )
                return row is not None
        except Exception as e:
            raise StorageError(f"postgres renew failed: {e}") from e

    async def wait_for_completion(
        self,
        effective_key: str,
        timeout_seconds: float,
    ) -> StoredRecord | None:
        await self._ensure_pool()
        await self._ensure_listener()

        event = asyncio.Event()
        self._waiters.setdefault(effective_key, set()).add(event)

        # Subscribe-then-read (spec §4.3), then wait on a NOTIFICATION-OR-POLL
        # schedule (spec §5.5). LISTEN/NOTIFY is at-most-once — a notification is
        # lost if the dedicated LISTEN connection drops (server restart, failover)
        # before it re-subscribes. The notification is the latency optimization;
        # the bounded backed-off SELECT is the correctness floor.
        deadline = time.monotonic() + timeout_seconds
        poll = _POLL_INITIAL_SECONDS
        try:
            while True:
                record = await self._get_record(effective_key)
                if record is None:
                    return None
                if record.state == State.COMPLETED:
                    return record
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return None
                event.clear()
                try:
                    await asyncio.wait_for(event.wait(), timeout=min(poll, remaining))
                except asyncio.TimeoutError:
                    pass
                poll = min(poll * 2, _POLL_CAP_SECONDS)
        finally:
            waiters = self._waiters.get(effective_key)
            if waiters is not None:
                waiters.discard(event)
                if not waiters:
                    self._waiters.pop(effective_key, None)

    # ----- internals -----

    async def _get_record(self, effective_key: str) -> StoredRecord | None:
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    f"SELECT * FROM {self._table} WHERE effective_key = $1",
                    effective_key,
                )
        except Exception as e:
            raise StorageError(f"postgres select failed: {e}") from e
        if row is None:
            return None
        try:
            return self._row_to_record(row)
        except Exception:
            # Corrupt row (§4.12): treat as ABSENT
            return None

    def _row_to_record(self, row: Any) -> StoredRecord:
        response_headers_raw = row["response_headers"]
        if isinstance(response_headers_raw, str):
            response_headers = json.loads(response_headers_raw)
        elif response_headers_raw is None:
            response_headers = {}
        else:
            response_headers = dict(response_headers_raw)
        response_body = row["response_body"]
        if response_body is None:
            response_body = b""
        else:
            response_body = bytes(response_body)
        return StoredRecord(
            effective_key=row["effective_key"],
            state=State(row["state"]),
            fingerprint=row["fingerprint"],
            fingerprint_version=row["fingerprint_version"],
            claim_token=row["claim_token"],
            claimed_at=row["claimed_at"].timestamp(),
            lease_until=row["lease_until"].timestamp(),
            completed_at=(
                row["completed_at"].timestamp() if row["completed_at"] is not None else None
            ),
            response_status=row["response_status"],
            response_headers=response_headers,
            response_body=response_body,
        )

    async def _ensure_listener(self) -> None:
        if self._listen_conn is not None and not self._listen_conn.is_closed():
            return
        async with self._listen_lock:
            if self._listen_conn is not None and not self._listen_conn.is_closed():
                return
            # A prior LISTEN connection may have died (a failover, an idle drop).
            # Drop the stale handle so we re-establish NOTIFY, instead of leaving
            # waiters on the poll floor for the rest of the process lifetime.
            if self._listen_conn is not None:
                try:
                    await self._pool.release(self._listen_conn)
                except Exception:
                    pass
                self._listen_conn = None
            conn = await self._pool.acquire()
            try:
                await conn.add_listener(NOTIFY_CHANNEL, self._on_notification)
            except Exception:
                await self._pool.release(conn)
                raise
            # Re-establish NOTIFY automatically on the next waiter if this
            # connection is later terminated by the server.
            conn.add_termination_listener(self._on_listener_terminated)
            self._listen_conn = conn

    def _on_listener_terminated(self, connection: Any) -> None:
        # The LISTEN connection was closed (server failover / idle timeout). Null
        # our handle so the next wait_for_completion rebuilds the listener rather
        # than polling forever. The pool discards the dead connection itself.
        if self._listen_conn is connection:
            self._listen_conn = None

    def _on_notification(
        self,
        connection: Any,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        waiters = self._waiters.get(payload)
        if not waiters:
            return
        for event in list(waiters):
            event.set()


# ----- CLI-callable helpers -----


async def init_pg(database_url: str, *, table: str = DEFAULT_TABLE) -> None:
    """Create the records table and indexes (default ``idemkit_records``). Idempotent."""
    _validate_table(table)
    try:
        import asyncpg
    except ImportError as e:
        raise ImportError(
            "idemkit: install the `postgres` extra: pip install 'idemkit[postgres]'"
        ) from e
    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(_schema_sql(table))
    finally:
        await conn.close()


async def pg_vacuum(
    database_url: str,
    *,
    table: str = DEFAULT_TABLE,
    expires_after_seconds: float = 86_400.0,
    lease_grace_seconds: float = 60.0,
) -> int:
    """Delete expired records. Returns the number of rows removed."""
    _validate_table(table)
    try:
        import asyncpg
    except ImportError as e:
        raise ImportError(
            "idemkit: install the `postgres` extra: pip install 'idemkit[postgres]'"
        ) from e
    conn = await asyncpg.connect(database_url)
    try:
        result = await conn.execute(
            f"""
            DELETE FROM {table}
            WHERE (state = 'COMPLETED'
                   AND completed_at + ($1 || ' seconds')::INTERVAL < NOW())
               OR (state = 'CLAIMED'
                   AND lease_until + ($2 || ' seconds')::INTERVAL < NOW())
            """,
            str(int(expires_after_seconds)),
            str(int(lease_grace_seconds)),
        )
        # result is like "DELETE n"
        try:
            return int(result.split()[-1])
        except (ValueError, IndexError):
            return 0
    finally:
        await conn.close()
