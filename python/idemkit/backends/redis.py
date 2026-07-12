"""Redis backend per spec §4.1 / §4.3 / §7.3.

Storage layout: one Redis string per effective_key, value is JSON-encoded
``StoredRecord``. The response body is base64-encoded inside the JSON to
keep the value safe for transport through Lua's string handling.

State transitions are atomic via Lua scripts that check ``claim_token``
under Redis's single-threaded execution. All scripts use Redis's ``TIME``
command for lease comparisons — never the app's clock (spec §4.4).

Lease vs key TTL: the Redis key TTL is set to ``lease_ttl + lease_grace``
so expired-lease records remain observable for the grace period and can
be explicitly *reclaimed* (emitting ``lease_reclaimed`` rather than ``new``
for observability), rather than silently disappearing as Redis-native
expiry. After the grace period, the key auto-deletes.

In-flight wait uses Pub/Sub on a single shared channel
``idemkit:completions`` with the effective_key as the payload. One
subscriber task per backend instance demultiplexes notifications to
per-key ``asyncio.Event`` waiters.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import secrets
import time
from typing import Any

from idemkit.core.exceptions import StorageError
from idemkit.core.state import ClaimResult, ClaimResultType, State, StoredRecord

_logger = logging.getLogger(__name__)

NOTIFY_CHANNEL = "idemkit:completions"

# In-flight wait poll schedule (spec §5.5): the bounded backed-off re-read that
# keeps a dropped Pub/Sub notification from hanging a waiter for the full timeout.
_POLL_INITIAL_SECONDS = 0.05
_POLL_CAP_SECONDS = 1.0


# Lua script: atomic claim, or detect existing record + reclaim if lease expired.
# Returns: {outcome_string, record_json}
# outcome ∈ {NEW_CLAIMED, LEASE_RECLAIMED, ALREADY_COMPLETED, ALREADY_CLAIMED}
_LUA_CLAIM = """
local key = KEYS[1]
local claim_token = ARGV[1]
local fingerprint = ARGV[2]
local fingerprint_version = tonumber(ARGV[3])
local lease_ttl_ms = tonumber(ARGV[4])
local grace_ms = tonumber(ARGV[5])

local now_t = redis.call('TIME')
local now_ms = tonumber(now_t[1]) * 1000 + math.floor(tonumber(now_t[2]) / 1000)
local key_ttl_ms = lease_ttl_ms + grace_ms

local function make_fresh_record()
  local record = {
    state = 'CLAIMED',
    fingerprint = fingerprint,
    fingerprint_version = fingerprint_version,
    claim_token = claim_token,
    claimed_at_ms = now_ms,
    lease_until_ms = now_ms + lease_ttl_ms
  }
  return cjson.encode(record)
end

local existing = redis.call('GET', key)
if not existing then
  local new_json = make_fresh_record()
  redis.call('SET', key, new_json, 'PX', key_ttl_ms)
  return {'NEW_CLAIMED', new_json}
end

local ok, rec = pcall(cjson.decode, existing)
if not ok or type(rec) ~= 'table' then
  -- Corrupt record (§4.12): overwrite with fresh claim, signalling recovery
  -- so the engine can emit idempotency.corrupt_record.
  local new_json = make_fresh_record()
  redis.call('SET', key, new_json, 'PX', key_ttl_ms)
  return {'NEW_CLAIMED_FROM_CORRUPT', new_json}
end

if rec.state == 'COMPLETED' then
  return {'ALREADY_COMPLETED', existing}
end

-- CLAIMED: check the embedded lease_until_ms against Redis's TIME (§4.4).
if tonumber(rec.lease_until_ms) < now_ms then
  local new_json = make_fresh_record()
  redis.call('SET', key, new_json, 'PX', key_ttl_ms)
  return {'LEASE_RECLAIMED', new_json}
end

return {'ALREADY_CLAIMED', existing}
"""


# Lua script: conditional complete (CLAIMED + matching token only).
# Returns 1 on success, 0 if claim was reclaimed by another process.
_LUA_COMPLETE = """
local key = KEYS[1]
local claim_token = ARGV[1]
local response_status = tonumber(ARGV[2])
local response_headers_json = ARGV[3]
local response_body_b64 = ARGV[4]
local completed_ttl_ms = tonumber(ARGV[5])
local notify_channel = ARGV[6]

local existing = redis.call('GET', key)
if not existing then return 0 end

local ok, rec = pcall(cjson.decode, existing)
if not ok or type(rec) ~= 'table' then return 0 end

if rec.state ~= 'CLAIMED' or rec.claim_token ~= claim_token then
  return 0
end

local now_t = redis.call('TIME')
local now_ms = tonumber(now_t[1]) * 1000 + math.floor(tonumber(now_t[2]) / 1000)

rec.state = 'COMPLETED'
rec.completed_at_ms = now_ms
rec.response_status = response_status
rec.response_headers = cjson.decode(response_headers_json)
rec.response_body_b64 = response_body_b64

redis.call('SET', key, cjson.encode(rec), 'PX', completed_ttl_ms)
redis.call('PUBLISH', notify_channel, key)
return 1
"""


# Lua script: conditional lease renewal (CLAIMED + matching token only), §5.3.1.
# Extends the embedded lease_until and the Redis key TTL. Returns 1 on success,
# 0 if the record is gone, completed, or reclaimed by another executor.
_LUA_RENEW = """
local key = KEYS[1]
local claim_token = ARGV[1]
local lease_ttl_ms = tonumber(ARGV[2])
local grace_ms = tonumber(ARGV[3])

local existing = redis.call('GET', key)
if not existing then return 0 end

local ok, rec = pcall(cjson.decode, existing)
if not ok or type(rec) ~= 'table' then return 0 end

if rec.state ~= 'CLAIMED' or rec.claim_token ~= claim_token then
  return 0
end

local now_t = redis.call('TIME')
local now_ms = tonumber(now_t[1]) * 1000 + math.floor(tonumber(now_t[2]) / 1000)

rec.lease_until_ms = now_ms + lease_ttl_ms
redis.call('SET', key, cjson.encode(rec), 'PX', lease_ttl_ms + grace_ms)
return 1
"""


# Lua script: conditional release.
# Returns 1 on success, 0 if claim was reclaimed.
_LUA_RELEASE = """
local key = KEYS[1]
local claim_token = ARGV[1]
local notify_channel = ARGV[2]

local existing = redis.call('GET', key)
if not existing then return 0 end

local ok, rec = pcall(cjson.decode, existing)
if not ok or type(rec) ~= 'table' then return 0 end

if rec.state ~= 'CLAIMED' or rec.claim_token ~= claim_token then
  return 0
end

redis.call('DEL', key)
redis.call('PUBLISH', notify_channel, key)
return 1
"""


class RedisBackend:
    """Production-grade Redis backend.

    Supports Redis 6+ (Lua + Pub/Sub) and Redis Cluster (single-key
    operations need no hash tag; Pub/Sub broadcasts cluster-wide).
    """

    def __init__(
        self,
        client: Any,
        *,
        lease_grace_seconds: float = 60.0,
        namespace: str = "",
    ) -> None:
        """Construct with an existing async Redis client.

        ``client`` should be a ``redis.asyncio.Redis`` instance (or
        protocol-compatible, e.g. ``fakeredis.aioredis.FakeRedis``).

        ``lease_grace_seconds`` extends the Redis key TTL past the lease so
        expired-lease records can be observably *reclaimed* (emitting
        ``lease_reclaimed`` rather than ``new``) rather than vanishing as
        native Redis expiry. After lease + grace, the key auto-deletes.

        ``namespace`` prefixes every Redis key and the Pub/Sub channel (as
        ``"{namespace}:{key}"``), so idemkit can share a Redis with other data
        or run two isolated instances on one server. Default is empty (no
        prefix).
        """
        self._client = client
        self._lease_grace_seconds = lease_grace_seconds
        self._namespace = namespace
        self._channel = f"{namespace}:{NOTIFY_CHANNEL}" if namespace else NOTIFY_CHANNEL
        self._sha_claim: str | None = None
        self._sha_complete: str | None = None
        self._sha_release: str | None = None
        self._sha_renew: str | None = None
        self._waiters: dict[str, set[asyncio.Event]] = {}
        self._subscriber_task: asyncio.Task[None] | None = None
        self._subscriber_lock = asyncio.Lock()
        self._subscriber_ready = asyncio.Event()

    @classmethod
    def from_url(
        cls,
        url: str,
        *,
        lease_grace_seconds: float = 60.0,
        namespace: str = "",
        **kwargs: Any,
    ) -> RedisBackend:
        """Construct from a redis:// URL.

        Lazy: the connection pool is opened on first call, not now. Extra
        keyword arguments are forwarded to the redis client (e.g. ``password``,
        ``ssl_cert_reqs`` for a ``rediss://`` URL).

        A default ``socket_connect_timeout`` is applied so an unreachable server
        fails fast (fail-closed) instead of hanging a request; override it, or add
        ``socket_timeout`` for a command-level cap. Note ``socket_timeout`` also
        governs the Pub/Sub subscriber's idle reads, so pair it with
        ``health_check_interval`` if you set it.
        """
        try:
            import redis.asyncio as aioredis
        except ImportError as e:
            raise ImportError(
                "idemkit: install the `redis` extra: pip install 'idemkit[redis]'"
            ) from e
        kwargs.setdefault("socket_connect_timeout", 5.0)
        client = aioredis.from_url(url, decode_responses=False, **kwargs)
        return cls(client, lease_grace_seconds=lease_grace_seconds, namespace=namespace)

    async def aclose(self) -> None:
        """Tear down the subscriber and close the client."""
        if self._subscriber_task is not None:
            self._subscriber_task.cancel()
            try:
                await self._subscriber_task
            except (asyncio.CancelledError, Exception):
                pass
            self._subscriber_task = None
        await self._client.aclose()

    async def __aenter__(self) -> RedisBackend:
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
        claim_token = secrets.token_hex(16)
        lease_ms = int(lease_ttl_seconds * 1000)
        grace_ms = int(self._lease_grace_seconds * 1000)
        try:
            outcome_b, record_json_b = await self._eval(
                _LUA_CLAIM,
                "_sha_claim",
                [self._key(effective_key)],
                [claim_token, fingerprint, fingerprint_version, lease_ms, grace_ms],
            )
        except Exception as e:
            raise StorageError(f"redis claim failed: {e}") from e

        outcome = _decode(outcome_b)
        record_json = _decode(record_json_b)
        record = self._record_from_json(effective_key, record_json)

        if outcome == "NEW_CLAIMED":
            return ClaimResult(ClaimResultType.NEW_CLAIMED, record, claim_token)
        if outcome == "NEW_CLAIMED_FROM_CORRUPT":
            return ClaimResult(
                ClaimResultType.NEW_CLAIMED,
                record,
                claim_token,
                recovered_from_corrupt=True,
            )
        if outcome == "LEASE_RECLAIMED":
            return ClaimResult(ClaimResultType.LEASE_RECLAIMED, record, claim_token)
        if outcome == "ALREADY_COMPLETED":
            return ClaimResult(ClaimResultType.ALREADY_COMPLETED, record)
        if outcome == "ALREADY_CLAIMED":
            return ClaimResult(ClaimResultType.ALREADY_CLAIMED, record)
        raise StorageError(f"redis claim unexpected outcome: {outcome!r}")

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        expires_after_seconds: float,
    ) -> bool:
        body_b64 = base64.b64encode(response_body).decode("ascii")
        try:
            result = await self._eval(
                _LUA_COMPLETE,
                "_sha_complete",
                [self._key(effective_key)],
                [
                    claim_token,
                    response_status,
                    json.dumps(response_headers),
                    body_b64,
                    int(expires_after_seconds * 1000),
                    self._channel,
                ],
            )
        except Exception as e:
            raise StorageError(f"redis complete failed: {e}") from e
        return bool(int(result))

    async def release(self, effective_key: str, claim_token: str) -> bool:
        try:
            result = await self._eval(
                _LUA_RELEASE,
                "_sha_release",
                [self._key(effective_key)],
                [claim_token, self._channel],
            )
        except Exception as e:
            raise StorageError(f"redis release failed: {e}") from e
        return bool(int(result))

    async def renew(
        self,
        effective_key: str,
        claim_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        lease_ms = int(lease_ttl_seconds * 1000)
        grace_ms = int(self._lease_grace_seconds * 1000)
        try:
            result = await self._eval(
                _LUA_RENEW,
                "_sha_renew",
                [self._key(effective_key)],
                [claim_token, lease_ms, grace_ms],
            )
        except Exception as e:
            raise StorageError(f"redis renew failed: {e}") from e
        return bool(int(result))

    async def wait_for_completion(
        self,
        effective_key: str,
        timeout_seconds: float,
    ) -> StoredRecord | None:
        await self._ensure_subscriber()

        # Waiters are keyed by the STORAGE key (namespace-prefixed) so a Pub/Sub
        # payload, which carries that same storage key, matches the right waiters.
        storage_key = self._key(effective_key)
        event = asyncio.Event()
        self._waiters.setdefault(storage_key, set()).add(event)

        # Subscribe-then-read (spec §4.3), then wait on a NOTIFICATION-OR-POLL
        # schedule (spec §5.5). Redis Pub/Sub is at-most-once — a notification
        # can be dropped on a connection blip, failover, or reshard, and the
        # subscriber loop can die. The Pub/Sub event is the latency optimization;
        # the bounded backed-off GET is the correctness floor, so a lost
        # notification costs extra latency, never a hang to the full timeout.
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
            waiters = self._waiters.get(storage_key)
            if waiters is not None:
                waiters.discard(event)
                if not waiters:
                    self._waiters.pop(storage_key, None)

    # ----- internals -----

    def _key(self, effective_key: str) -> str:
        """The Redis key for a logical effective_key (namespace-prefixed)."""
        return f"{self._namespace}:{effective_key}" if self._namespace else effective_key

    async def _eval(
        self,
        script: str,
        sha_attr: str,
        keys: list[str],
        args: list[Any],
    ) -> Any:
        """Execute a Lua script via EVALSHA with NOSCRIPT → EVAL fallback (§7.3)."""
        sha: str | None = getattr(self, sha_attr)
        if sha is None:
            sha = await self._client.script_load(script)
            setattr(self, sha_attr, sha)
        try:
            return await self._client.evalsha(sha, len(keys), *keys, *args)
        except Exception as e:
            # redis-py raises NoScriptError on NOSCRIPT (script cache lost after
            # restart / failover / cluster reshard). Reload and retry once (§7.3).
            if type(e).__name__ == "NoScriptError" or "NOSCRIPT" in str(e):
                sha = await self._client.script_load(script)
                setattr(self, sha_attr, sha)
                return await self._client.evalsha(sha, len(keys), *keys, *args)
            raise

    async def _get_record(self, effective_key: str) -> StoredRecord | None:
        try:
            raw = await self._client.get(self._key(effective_key))
        except Exception as e:
            raise StorageError(f"redis get failed: {e}") from e
        if raw is None:
            return None
        try:
            return self._record_from_json(effective_key, _decode(raw))
        except Exception:
            # Corrupt record (§4.12): treat as ABSENT
            return None

    def _record_from_json(self, effective_key: str, record_json: str) -> StoredRecord:
        data = json.loads(record_json)
        response_body_b64 = data.get("response_body_b64", "") or ""
        response_body = base64.b64decode(response_body_b64) if response_body_b64 else b""
        return StoredRecord(
            effective_key=effective_key,
            state=State(data["state"]),
            fingerprint=data["fingerprint"],
            fingerprint_version=int(data["fingerprint_version"]),
            claim_token=data["claim_token"],
            claimed_at=float(data["claimed_at_ms"]) / 1000.0,
            lease_until=float(data["lease_until_ms"]) / 1000.0,
            completed_at=(
                float(data["completed_at_ms"]) / 1000.0
                if data.get("completed_at_ms") is not None
                else None
            ),
            response_status=data.get("response_status"),
            response_headers=data.get("response_headers") or {},
            response_body=response_body,
        )

    async def _ensure_subscriber(self) -> None:
        if self._subscriber_task is not None and not self._subscriber_task.done():
            return
        async with self._subscriber_lock:
            if self._subscriber_task is not None and not self._subscriber_task.done():
                return
            self._subscriber_ready = asyncio.Event()
            self._subscriber_task = asyncio.create_task(self._subscribe_loop())
            try:
                await asyncio.wait_for(self._subscriber_ready.wait(), timeout=2.0)
            except asyncio.TimeoutError:
                _logger.warning("idemkit: Redis subscriber did not become ready in 2s")

    async def _subscribe_loop(self) -> None:
        try:
            pubsub = self._client.pubsub()
            await pubsub.subscribe(self._channel)
            self._subscriber_ready.set()
            try:
                async for message in pubsub.listen():
                    if message is None:
                        continue
                    mtype = message.get("type")
                    if mtype != "message":
                        continue
                    payload = message.get("data")
                    if isinstance(payload, bytes):
                        payload = payload.decode("utf-8", errors="ignore")
                    if not isinstance(payload, str):
                        continue
                    self._signal(payload)
            finally:
                try:
                    await pubsub.unsubscribe(self._channel)
                except Exception:
                    pass
                try:
                    await pubsub.aclose()
                except Exception:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            _logger.exception("idemkit: Redis subscriber loop crashed")

    def _signal(self, storage_key: str) -> None:
        waiters = self._waiters.get(storage_key)
        if not waiters:
            return
        for event in list(waiters):
            event.set()


def _decode(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    return str(value)
