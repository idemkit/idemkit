"""MongoDB backend for idemkit (spec §4.1).

One document per ``effective_key`` (``_id``), the same single-record layout as the
Postgres backend. Correctness rests on MongoDB's per-document atomicity plus the
server clock: every claim/complete/renew is an aggregation-pipeline update that uses
``$$NOW`` (the server's time), so a skewed app clock cannot cause a wrongful reclaim
(spec §5.7). ``lease_until`` doubles as the COMPLETED record's expiry, so an expired
record is treated as ABSENT on read, exactly like the other backends.

A TTL index on ``expire_at`` (the lease plus a grace period) lets MongoDB reap
expired docs on its own, so (like Redis) there is nothing to vacuum. The grace keeps
a just-lapsed claim readable long enough to be reclaimed as ``lease_reclaimed``
rather than vanishing. In-flight waiters poll (there is no push channel); that is the
correctness floor every backend already falls back to.

Requires the ``mongo`` extra: ``pip install 'idemkit[mongo]'``.
"""

from __future__ import annotations

import secrets
import time
from datetime import datetime, timezone
from typing import Any

from idemkit.core.exceptions import StorageError
from idemkit.core.state import ClaimResult, ClaimResultType, State, StoredRecord

DEFAULT_COLLECTION = "idemkit_records"

# In-flight waiters poll on a backed-off schedule (spec §5.5); the bounded poll is
# the correctness floor. MongoDB change streams need a replica set, so we do not
# rely on a push notification here.
_POLL_INITIAL_SECONDS = 0.05
_POLL_CAP_SECONDS = 1.0


def _epoch(value: Any) -> float:
    """A BSON Date (or None) to epoch seconds."""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return float(value.timestamp())
    raise TypeError(f"expected a datetime, got {type(value).__name__}")


class MongoBackend:
    """MongoDB idempotency backend."""

    def __init__(
        self,
        collection: Any = None,
        *,
        lease_grace_seconds: float = 60.0,
        _url: str | None = None,
        _database: str | None = None,
        _collection_name: str = DEFAULT_COLLECTION,
        _client_kwargs: dict[str, Any] | None = None,
    ) -> None:
        """Construct with an existing (async) pymongo collection, or via
        :meth:`from_url` for lazy initialization.

        ``lease_grace_seconds`` keeps an expired record readable past its lease before
        the TTL index reaps it, so a just-lapsed claim is still visible to be reclaimed
        (reported as ``lease_reclaimed``) rather than vanishing (reported as ``new``).
        """
        self._collection = collection
        self._grace_ms = int(lease_grace_seconds * 1000)
        self._url = _url
        self._database = _database
        self._collection_name = _collection_name
        self._client_kwargs = _client_kwargs or {}
        self._client: Any = None
        import asyncio

        self._init_lock = asyncio.Lock()

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        database: str = "idemkit",
        collection: str = DEFAULT_COLLECTION,
        lease_grace_seconds: float = 60.0,
        **kwargs: Any,
    ) -> MongoBackend:
        """Construct from a ``mongodb://`` URL with lazy initialization.

        The client connects and the TTL index is created on first use, so this is
        safe to wire in at construction time. A default ``timeoutMS`` caps ops so a
        hung server fails fast (fail-closed) rather than blocking; override it.
        """
        kwargs.setdefault("timeoutMS", 10_000)
        return cls(
            None,
            lease_grace_seconds=lease_grace_seconds,
            _url=url,
            _database=database,
            _collection_name=collection,
            _client_kwargs=kwargs,
        )

    async def _ensure_collection(self) -> Any:
        if self._collection is not None:
            return self._collection
        async with self._init_lock:
            if self._collection is not None:
                return self._collection
            try:
                from pymongo import AsyncMongoClient
            except ImportError as e:
                raise ImportError(
                    "idemkit: install the `mongo` extra: pip install 'idemkit[mongo]'"
                ) from e
            if self._url is None:
                raise StorageError("mongo: no collection and no URL configured")
            self._client = AsyncMongoClient(self._url, **self._client_kwargs)
            coll = self._client[self._database][self._collection_name]
            # Reap expired docs automatically (a claim past lease_until + grace), so
            # there is no vacuum step. Correctness never depends on it (expiry is
            # enforced on read); this only bounds disk. The TTL is on expire_at, not
            # lease_until, so the grace window keeps a lapsed claim reclaimable.
            try:
                await coll.create_index("expire_at", expireAfterSeconds=0)
            except Exception:
                pass  # index may already exist with different options; not fatal
            self._collection = coll
            return coll

    async def aclose(self) -> None:
        """Close the MongoDB client."""
        if self._client is not None:
            await self._client.close()
            self._client = None

    async def __aenter__(self) -> MongoBackend:
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
        coll = await self._ensure_collection()
        token = secrets.token_hex(16)
        lease_ms = int(lease_ttl_seconds * 1000)
        # Take the key if it is new OR its lease has expired (by the SERVER clock),
        # else leave the live record untouched. One atomic pipeline update.
        pipeline = [
            {
                "$set": {
                    "__take": {
                        "$or": [
                            {"$eq": [{"$type": "$state"}, "missing"]},
                            {"$lt": ["$lease_until", "$$NOW"]},
                        ]
                    }
                }
            },
            {
                "$set": {
                    # Capture the pre-claim state (all RHS see the stage's input doc)
                    # so one atomic op can tell a fresh claim from a lease reclaim,
                    # with no second read to race against.
                    "__prev_state": "$state",
                    "state": {"$cond": ["$__take", "CLAIMED", "$state"]},
                    "fingerprint": {"$cond": ["$__take", fingerprint, "$fingerprint"]},
                    "fingerprint_version": {
                        "$cond": ["$__take", fingerprint_version, "$fingerprint_version"]
                    },
                    "claim_token": {"$cond": ["$__take", token, "$claim_token"]},
                    "claimed_at": {"$cond": ["$__take", "$$NOW", "$claimed_at"]},
                    "lease_until": {
                        "$cond": ["$__take", {"$add": ["$$NOW", lease_ms]}, "$lease_until"]
                    },
                    "expire_at": {
                        "$cond": [
                            "$__take",
                            {"$add": ["$$NOW", lease_ms + self._grace_ms]},
                            "$expire_at",
                        ]
                    },
                    "completed_at": {"$cond": ["$__take", None, "$completed_at"]},
                    "response_status": {"$cond": ["$__take", None, "$response_status"]},
                    "response_headers": {"$cond": ["$__take", {}, "$response_headers"]},
                    "response_body": {"$cond": ["$__take", b"", "$response_body"]},
                }
            },
            {"$unset": "__take"},
        ]
        try:
            from pymongo import ReturnDocument

            after = await coll.find_one_and_update(
                {"_id": effective_key},
                pipeline,
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
        except Exception as e:
            raise StorageError(f"mongo claim failed: {e}") from e

        # upsert + return-AFTER always yields the doc, so there is no disappeared-record
        # window to fail closed on.
        took = after.get("claim_token") == token
        if took:
            try:
                record = self._doc_to_record(after)
            except Exception as e:
                raise StorageError(f"mongo claim produced an unreadable record: {e}") from e
            if after.get("__prev_state") == "CLAIMED":
                return ClaimResult(ClaimResultType.LEASE_RECLAIMED, record, token)
            return ClaimResult(ClaimResultType.NEW_CLAIMED, record, token)

        # We did not take it: a live record is held by someone else.
        try:
            record = self._doc_to_record(after)
        except Exception:
            # §4.12: an unreadable record is treated as ABSENT and re-claimed fresh.
            return await self._force_claim(
                coll, effective_key, fingerprint, fingerprint_version, lease_ms
            )
        if record.state == State.COMPLETED:
            return ClaimResult(ClaimResultType.ALREADY_COMPLETED, record)
        return ClaimResult(ClaimResultType.ALREADY_CLAIMED, record)

    async def _force_claim(
        self,
        coll: Any,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ms: int,
    ) -> ClaimResult:
        """Overwrite a corrupt record with a fresh claim (§4.12)."""
        token = secrets.token_hex(16)
        pipeline = [
            {
                "$set": {
                    "state": "CLAIMED",
                    "fingerprint": fingerprint,
                    "fingerprint_version": fingerprint_version,
                    "claim_token": token,
                    "claimed_at": "$$NOW",
                    "lease_until": {"$add": ["$$NOW", lease_ms]},
                    "expire_at": {"$add": ["$$NOW", lease_ms + self._grace_ms]},
                    "completed_at": None,
                    "response_status": None,
                    "response_headers": {},
                    "response_body": b"",
                }
            }
        ]
        try:
            from pymongo import ReturnDocument

            after = await coll.find_one_and_update(
                {"_id": effective_key}, pipeline, upsert=True, return_document=ReturnDocument.AFTER
            )
        except Exception as e:
            raise StorageError(f"mongo corrupt-record reclaim failed: {e}") from e
        return ClaimResult(
            ClaimResultType.NEW_CLAIMED,
            self._doc_to_record(after),
            token,
            recovered_from_corrupt=True,
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
        coll = await self._ensure_collection()
        completed_ms = int(expires_after_seconds * 1000)
        # lease_until is repurposed as the COMPLETED record's expiry, so the claim
        # path expires it on read like every other backend (§4.8).
        pipeline = [
            {
                "$set": {
                    "state": "COMPLETED",
                    "completed_at": "$$NOW",
                    "lease_until": {"$add": ["$$NOW", completed_ms]},
                    "expire_at": {"$add": ["$$NOW", completed_ms + self._grace_ms]},
                    "response_status": response_status,
                    "response_headers": response_headers,
                    "response_body": response_body,
                }
            }
        ]
        try:
            result = await coll.update_one(
                {"_id": effective_key, "state": "CLAIMED", "claim_token": claim_token}, pipeline
            )
        except Exception as e:
            raise StorageError(f"mongo complete failed: {e}") from e
        return bool(result.matched_count == 1)

    async def release(self, effective_key: str, claim_token: str) -> bool:
        coll = await self._ensure_collection()
        try:
            result = await coll.delete_one(
                {"_id": effective_key, "state": "CLAIMED", "claim_token": claim_token}
            )
        except Exception as e:
            raise StorageError(f"mongo release failed: {e}") from e
        return bool(result.deleted_count == 1)

    async def renew(
        self,
        effective_key: str,
        claim_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        coll = await self._ensure_collection()
        lease_ms = int(lease_ttl_seconds * 1000)
        pipeline = [
            {
                "$set": {
                    "lease_until": {"$add": ["$$NOW", lease_ms]},
                    "expire_at": {"$add": ["$$NOW", lease_ms + self._grace_ms]},
                }
            }
        ]
        try:
            result = await coll.update_one(
                {"_id": effective_key, "state": "CLAIMED", "claim_token": claim_token}, pipeline
            )
        except Exception as e:
            raise StorageError(f"mongo renew failed: {e}") from e
        return bool(result.matched_count == 1)

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
        coll = await self._ensure_collection()
        try:
            doc = await coll.find_one({"_id": effective_key})
        except Exception as e:
            raise StorageError(f"mongo read failed: {e}") from e
        if doc is None:
            return None
        try:
            return self._doc_to_record(doc)
        except Exception:
            return None

    def _doc_to_record(self, doc: dict[str, Any]) -> StoredRecord:
        body = doc.get("response_body") or b""
        return StoredRecord(
            effective_key=doc["_id"],
            state=State(doc["state"]),
            fingerprint=doc["fingerprint"],
            fingerprint_version=int(doc["fingerprint_version"]),
            claim_token=doc["claim_token"],
            claimed_at=_epoch(doc["claimed_at"]),
            lease_until=_epoch(doc["lease_until"]),
            completed_at=_epoch(doc["completed_at"]) if doc.get("completed_at") else None,
            response_status=doc.get("response_status"),
            response_headers=dict(doc.get("response_headers") or {}),
            response_body=bytes(body),
        )
