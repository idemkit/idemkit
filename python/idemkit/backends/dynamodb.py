"""Amazon DynamoDB backend for idemkit (spec §4.1).

One item per ``effective_key`` (the partition key), the same single-record layout as
the Postgres and Mongo backends. Correctness rests on DynamoDB's conditional writes:
``claim`` is an ``UpdateItem`` with ``ConditionExpression`` (create if absent, or
reclaim if the lease expired) and ``ReturnValues=ALL_OLD`` so it can tell a fresh
claim from a lease reclaim; ``complete`` / ``release`` / ``renew`` are conditional on
``claim_token`` so a reclaimed executor is fenced. ``lease_until`` doubles as the
COMPLETED record's expiry, so an expired record is treated as ABSENT on read.

**One difference from the other backends:** DynamoDB has no server clock in a
condition expression, so lease decisions use the CLIENT clock.
With NTP-synced hosts this is fine, but unlike Redis/Postgres/Mongo it does not give
the storage-clock guarantee against a badly skewed app clock (spec §5.7). A DynamoDB
TTL attribute reaps expired items (best-effort, up to ~48h), so correctness relies on
expiry-on-read, not on TTL timeliness.

Requires the ``dynamodb`` extra: ``pip install 'idemkit[dynamodb]'``.
"""

from __future__ import annotations

import secrets
import time
from typing import Any

from idemkit.core.exceptions import StorageError
from idemkit.core.state import ClaimResult, ClaimResultType, State, StoredRecord

DEFAULT_TABLE = "idemkit_records"
PK = "effective_key"

_POLL_INITIAL_SECONDS = 0.05
_POLL_CAP_SECONDS = 1.0
_CLAIM_RETRIES = 3


class DynamoBackend:
    """Amazon DynamoDB idempotency backend."""

    def __init__(
        self,
        *,
        table: str = DEFAULT_TABLE,
        endpoint_url: str | None = None,
        region_name: str = "us-east-1",
        create_table: bool = True,
        lease_grace_seconds: float = 60.0,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        max_retries: int | None = None,
        **client_kwargs: Any,
    ) -> None:
        """Configure the backend (lazy: no connection until first use).

        ``endpoint_url`` targets DynamoDB Local / LocalStack for tests. ``create_table``
        makes the table (on-demand billing, TTL on ``ttl``) on first use if it does not
        exist; set it to ``False`` in production and provision the table out of band so
        the runtime role needs no ``CreateTable`` permission.

        ``lease_grace_seconds`` pads the DynamoDB ``ttl`` attribute past the logical
        expiry (which is enforced on read via ``lease_until``). The ``ttl`` attribute
        has one-second granularity, so without a pad a just-lapsed claim could be
        TTL-reaped before the next attempt reclaims it, turning a ``lease_reclaimed``
        into a ``new`` — the same grace Redis and Mongo apply.

        ``connect_timeout`` (default 5s), ``read_timeout`` (default 10s), and
        ``max_retries`` (default 3) fail fast like the other backends instead of
        hanging on botocore's 60s default; each default is applied only if you do not
        pass it. Extra keyword args go to the aioboto3 resource (e.g. credentials);
        advanced callers can pass a full ``config=Config(...)`` (botocore) that
        overrides the three timeout/retry knobs.
        """
        self._table_name = table
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._create_table = create_table
        self._grace_seconds = lease_grace_seconds
        self._connect_timeout = connect_timeout
        self._read_timeout = read_timeout
        self._max_retries = max_retries
        self._client_kwargs = client_kwargs
        self._cm: Any = None
        self._resource: Any = None
        self._table: Any = None
        import asyncio

        self._init_lock = asyncio.Lock()

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        table: str = DEFAULT_TABLE,
        region_name: str = "us-east-1",
        create_table: bool = True,
        lease_grace_seconds: float = 60.0,
        **client_kwargs: Any,
    ) -> DynamoBackend:
        """Construct from a DynamoDB endpoint URL, for parity with the other backends.

        ``url`` is the DynamoDB endpoint, e.g. ``http://localhost:8000`` for DynamoDB
        Local or LocalStack. On real AWS you usually need no endpoint: build
        ``DynamoBackend(region_name=...)`` directly and let botocore resolve it. Extra
        keyword arguments go to the aioboto3 resource (e.g. credentials).
        """
        return cls(
            table=table,
            endpoint_url=url,
            region_name=region_name,
            create_table=create_table,
            lease_grace_seconds=lease_grace_seconds,
            **client_kwargs,
        )

    async def _ensure_table(self) -> Any:
        if self._table is not None:
            return self._table
        async with self._init_lock:
            if self._table is not None:
                return self._table
            try:
                import aioboto3
            except ImportError as e:
                raise ImportError(
                    "idemkit: install the `dynamodb` extra: pip install 'idemkit[dynamodb]'"
                ) from e
            from botocore.config import Config

            session = aioboto3.Session()
            # Fail fast (like Redis/Postgres) instead of hanging on the botocore
            # default 60s timeouts; retry transient throttling with backoff. Each
            # flat knob applies its default only if the caller did not set it; a full
            # ``config=`` (advanced) overrides all three.
            client_kwargs = dict(self._client_kwargs)
            config = client_kwargs.pop("config", None)
            if config is None:
                config = Config(
                    connect_timeout=self._connect_timeout
                    if self._connect_timeout is not None
                    else 5,
                    read_timeout=self._read_timeout if self._read_timeout is not None else 10,
                    retries={
                        "max_attempts": self._max_retries if self._max_retries is not None else 3,
                        "mode": "standard",
                    },
                )
            self._cm = session.resource(
                "dynamodb",
                endpoint_url=self._endpoint_url,
                region_name=self._region_name,
                config=config,
                **client_kwargs,
            )
            self._resource = await self._cm.__aenter__()
            try:
                if self._create_table:
                    await self._ensure_table_exists()
                self._table = await self._resource.Table(self._table_name)
            except Exception:
                # Don't leak the aiohttp session if setup fails; a later call retries.
                await self._cm.__aexit__(None, None, None)
                self._cm = self._resource = self._table = None
                raise
            return self._table

    async def _ensure_table_exists(self) -> None:
        try:
            table = await self._resource.create_table(
                TableName=self._table_name,
                KeySchema=[{"AttributeName": PK, "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": PK, "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            await table.wait_until_exists()
            client = self._resource.meta.client
            try:
                await client.update_time_to_live(
                    TableName=self._table_name,
                    TimeToLiveSpecification={"Enabled": True, "AttributeName": "ttl"},
                )
            except Exception:
                pass  # TTL best-effort; expiry-on-read is what enforces correctness
        except Exception as e:
            # Already exists (ResourceInUseException) is fine; anything else is fatal.
            if "ResourceInUse" not in type(e).__name__ and "ResourceInUse" not in str(e):
                raise

    async def aclose(self) -> None:
        """Close the DynamoDB resource."""
        if self._cm is not None:
            await self._cm.__aexit__(None, None, None)
            self._cm = self._resource = self._table = None

    async def __aenter__(self) -> DynamoBackend:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.aclose()

    # ----- spec §4.1 backend Protocol -----

    async def claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        table = await self._ensure_table()
        from botocore.exceptions import ClientError

        # The condition can fail because a live record is held (return ALREADY_*) or
        # because a concurrent release deleted it in the same instant. In the second
        # case the record is gone, so retry the claim a bounded number of times.
        for _attempt in range(_CLAIM_RETRIES):
            token = secrets.token_hex(16)
            now = time.time()
            lease_until = now + lease_ttl_seconds
            try:
                resp = await table.update_item(
                    Key={PK: effective_key},
                    UpdateExpression=(
                        "SET #s = :claimed, fingerprint = :fp, fingerprint_version = :fpv, "
                        "claim_token = :tok, claimed_at = :now, lease_until = :lu, #ttl = :ttl "
                        "REMOVE completed_at, response_status, response_headers, response_body"
                    ),
                    ConditionExpression="attribute_not_exists(#pk) OR lease_until < :now",
                    ExpressionAttributeNames={"#s": "state", "#pk": PK, "#ttl": "ttl"},
                    ExpressionAttributeValues={
                        ":claimed": "CLAIMED",
                        ":fp": fingerprint,
                        ":fpv": fingerprint_version,
                        ":tok": token,
                        ":now": _num(now),
                        ":lu": _num(lease_until),
                        ":ttl": int(lease_until + self._grace_seconds),
                    },
                    ReturnValues="ALL_OLD",
                )
            except ClientError as e:
                if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                    already = await self._already(
                        table, effective_key, fingerprint, fingerprint_version, lease_ttl_seconds
                    )
                    if already is not None:
                        return already
                    continue  # record vanished under us; retry the claim
                raise StorageError(f"dynamodb claim failed: {e}") from e
            except Exception as e:
                raise StorageError(f"dynamodb claim failed: {e}") from e

            record = self._record(
                effective_key,
                State.CLAIMED,
                fingerprint,
                fingerprint_version,
                token,
                now,
                lease_until,
            )
            old = resp.get("Attributes")
            if old and old.get("state") == "CLAIMED":
                return ClaimResult(ClaimResultType.LEASE_RECLAIMED, record, token)
            return ClaimResult(ClaimResultType.NEW_CLAIMED, record, token)

        raise StorageError("dynamodb: claim contended repeatedly; retry")

    async def _already(
        self,
        table: Any,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult | None:
        """Read the record that made the conditional claim fail. Returns ``None`` if
        it vanished (caller retries the claim), or force-reclaims a corrupt one."""
        try:
            resp = await table.get_item(Key={PK: effective_key}, ConsistentRead=True)
        except Exception as e:
            raise StorageError(f"dynamodb read failed: {e}") from e
        item = resp.get("Item")
        if item is None:
            return None  # released between our failed claim and this read; retry
        try:
            record = self._item_to_record(item)
        except Exception:
            # §4.12: an unreadable record is treated as ABSENT and re-claimed fresh.
            return await self._force_claim(
                table, effective_key, fingerprint, fingerprint_version, lease_ttl_seconds
            )
        if record.state == State.COMPLETED:
            return ClaimResult(ClaimResultType.ALREADY_COMPLETED, record)
        return ClaimResult(ClaimResultType.ALREADY_CLAIMED, record)

    async def _force_claim(
        self,
        table: Any,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        """Overwrite a corrupt record with a fresh claim, unconditionally (§4.12)."""
        token = secrets.token_hex(16)
        now = time.time()
        lease_until = now + lease_ttl_seconds
        try:
            await table.update_item(
                Key={PK: effective_key},
                UpdateExpression=(
                    "SET #s = :claimed, fingerprint = :fp, fingerprint_version = :fpv, "
                    "claim_token = :tok, claimed_at = :now, lease_until = :lu, #ttl = :ttl "
                    "REMOVE completed_at, response_status, response_headers, response_body"
                ),
                ExpressionAttributeNames={"#s": "state", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":claimed": "CLAIMED",
                    ":fp": fingerprint,
                    ":fpv": fingerprint_version,
                    ":tok": token,
                    ":now": _num(now),
                    ":lu": _num(lease_until),
                    ":ttl": int(lease_until + self._grace_seconds),
                },
            )
        except Exception as e:
            raise StorageError(f"dynamodb corrupt-record reclaim failed: {e}") from e
        record = self._record(
            effective_key, State.CLAIMED, fingerprint, fingerprint_version, token, now, lease_until
        )
        return ClaimResult(ClaimResultType.NEW_CLAIMED, record, token, recovered_from_corrupt=True)

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        expires_after_seconds: float,
    ) -> bool:
        table = await self._ensure_table()
        now = time.time()
        expiry = now + expires_after_seconds
        from botocore.exceptions import ClientError

        try:
            await table.update_item(
                Key={PK: effective_key},
                UpdateExpression=(
                    "SET #s = :completed, completed_at = :now, lease_until = :exp, #ttl = :ttl, "
                    "response_status = :st, response_headers = :hd, response_body = :bd"
                ),
                ConditionExpression="#s = :claimed AND claim_token = :tok",
                ExpressionAttributeNames={"#s": "state", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":completed": "COMPLETED",
                    ":claimed": "CLAIMED",
                    ":tok": claim_token,
                    ":now": _num(now),
                    ":exp": _num(expiry),
                    ":ttl": int(expiry + self._grace_seconds),
                    ":st": response_status,
                    ":hd": response_headers,
                    ":bd": response_body,
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise StorageError(f"dynamodb complete failed: {e}") from e
        except Exception as e:
            raise StorageError(f"dynamodb complete failed: {e}") from e

    async def release(self, effective_key: str, claim_token: str) -> bool:
        table = await self._ensure_table()
        from botocore.exceptions import ClientError

        try:
            await table.delete_item(
                Key={PK: effective_key},
                ConditionExpression="#s = :claimed AND claim_token = :tok",
                ExpressionAttributeNames={"#s": "state"},
                ExpressionAttributeValues={":claimed": "CLAIMED", ":tok": claim_token},
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise StorageError(f"dynamodb release failed: {e}") from e
        except Exception as e:
            raise StorageError(f"dynamodb release failed: {e}") from e

    async def renew(
        self,
        effective_key: str,
        claim_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        table = await self._ensure_table()
        now = time.time()
        lease_until = now + lease_ttl_seconds
        from botocore.exceptions import ClientError

        try:
            await table.update_item(
                Key={PK: effective_key},
                UpdateExpression="SET lease_until = :lu, #ttl = :ttl",
                ConditionExpression="#s = :claimed AND claim_token = :tok",
                ExpressionAttributeNames={"#s": "state", "#ttl": "ttl"},
                ExpressionAttributeValues={
                    ":claimed": "CLAIMED",
                    ":tok": claim_token,
                    ":lu": _num(lease_until),
                    ":ttl": int(lease_until + self._grace_seconds),
                },
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                return False
            raise StorageError(f"dynamodb renew failed: {e}") from e
        except Exception as e:
            raise StorageError(f"dynamodb renew failed: {e}") from e

    async def wait_for_completion(
        self,
        effective_key: str,
        timeout_seconds: float,
    ) -> StoredRecord | None:
        import asyncio

        deadline = time.monotonic() + timeout_seconds
        poll = _POLL_INITIAL_SECONDS
        while True:
            record = await self._get_record(effective_key)
            if record is None:
                return None
            if record.state == State.COMPLETED:
                return record
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return None
            await asyncio.sleep(min(poll, remaining))
            poll = min(poll * 2, _POLL_CAP_SECONDS)

    # ----- internals -----

    async def _get_record(self, effective_key: str) -> StoredRecord | None:
        table = await self._ensure_table()
        try:
            resp = await table.get_item(Key={PK: effective_key}, ConsistentRead=True)
        except Exception as e:
            raise StorageError(f"dynamodb read failed: {e}") from e
        item = resp.get("Item")
        if item is None:
            return None
        try:
            return self._item_to_record(item)
        except Exception:
            return None

    def _record(
        self,
        effective_key: str,
        state: State,
        fingerprint: str,
        fingerprint_version: int,
        token: str,
        claimed_at: float,
        lease_until: float,
    ) -> StoredRecord:
        return StoredRecord(
            effective_key=effective_key,
            state=state,
            fingerprint=fingerprint,
            fingerprint_version=fingerprint_version,
            claim_token=token,
            claimed_at=claimed_at,
            lease_until=lease_until,
        )

    def _item_to_record(self, item: dict[str, Any]) -> StoredRecord:
        body = item.get("response_body") or b""
        if hasattr(body, "value"):  # botocore Binary
            body = body.value
        return StoredRecord(
            effective_key=item[PK],
            state=State(item["state"]),
            fingerprint=item["fingerprint"],
            fingerprint_version=int(item["fingerprint_version"]),
            claim_token=item["claim_token"],
            claimed_at=float(item["claimed_at"]),
            lease_until=float(item["lease_until"]),
            completed_at=float(item["completed_at"])
            if item.get("completed_at") is not None
            else None,
            response_status=int(item["response_status"])
            if item.get("response_status") is not None
            else None,
            response_headers={
                str(k): str(v) for k, v in (item.get("response_headers") or {}).items()
            },
            response_body=bytes(body),
        )


def _num(value: float) -> Any:
    """DynamoDB numbers must be Decimal (the resource layer rejects float)."""
    from decimal import Decimal

    return Decimal(str(value))
