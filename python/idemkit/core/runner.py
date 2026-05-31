"""Surface-neutral idempotent-execution core, per spec §5.

This is the one correct core that HTTP, queue consumers, and AI tool calls all
sit on top of. It owns the distributed-correctness mechanism and nothing about
any particular transport:

    key → atomic claim → in-flight wait → run once → store result → replay

The HTTP adapter (``core.engine.IdempotencyEngine``) is a thin user of this:
it maps a request to an effective key + fingerprint, calls ``decide``, runs the
handler, then calls ``complete`` / ``release``. Queue and AI adapters reuse the
exact same ``IdempotentCore`` with a different result shape and policy.

What lives here (surface-neutral):
  * the claim → branch → wait state machine (spec §5.2, §5.3, §5.5);
  * the cacheable vs reclaim decision plumbing (the *policy* is the caller's);
  * fencing on the ``claim_token`` (spec §5.3);
  * one observability event per terminal decision, tagged with the surface
    (spec §5.8) so a single dashboard covers all three surfaces;
  * the storage-error policy fork (``fail_closed`` / ``fail_open``, spec §5.7).

What does NOT live here: status codes, Problem Details, headers, fingerprint
construction, serializers. Those are per-surface and belong in the adapters.
"""

from __future__ import annotations

import asyncio
import enum
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Literal, TypeVar

from idemkit.backends.base import IdempotencyBackend
from idemkit.core.events import EventEmitter, IdempotencyEvent
from idemkit.core.exceptions import StorageError
from idemkit.core.state import ClaimResultType, Decision, StoredRecord

_logger = logging.getLogger(__name__)

_H = TypeVar("_H")  # a handler's return type, for the heartbeat helper


@dataclass(frozen=True)
class StoredResult:
    """The opaque result a completed operation stores and replays (spec §5.4).

    The backend persists these in its HTTP-shaped columns — ``response_body``,
    ``response_headers``, ``response_status`` — which Appendix A fixes and we do
    not change. Each surface repurposes the three carriers:

    * ``blob`` — HTTP response body; elsewhere the serialized return value (or
      empty for a side-effect-only queue handler, where a completed record just
      means "skip this redelivery").
    * ``meta`` — HTTP response headers; elsewhere a place for a codec to record
      what it serialized (e.g. ``{"codec": "json"}``).
    * ``marker`` — HTTP status code; elsewhere a small sentinel (200 = "a result
      is present"), kept so the existing backend columns stay populated.
    """

    blob: bytes
    meta: dict[str, str] = field(default_factory=dict)
    marker: int = 200


class CoreOutcome(enum.Enum):
    """What ``IdempotentCore.decide`` tells a surface adapter to do."""

    PROCEED = "proceed"
    """Claim acquired. Run the handler, then call ``complete`` or ``release``."""

    REPLAY = "replay"
    """A completed record exists (or finished while we waited). Replay it."""

    MISMATCH = "mismatch"
    """A record exists under this key but its fingerprint disagreed. The surface
    must refuse rather than replay the wrong result (HTTP 422/409)."""

    CONFLICT = "conflict"
    """Another executor holds a live claim and it did not complete within the
    wait timeout (HTTP 423/409)."""

    STORAGE_REFUSE = "storage_refuse"
    """The backend errored and ``on_storage_error=fail_closed``. The surface must
    refuse the operation (HTTP 503)."""

    STORAGE_PASS = "storage_pass"
    """The backend errored and ``on_storage_error=fail_open``. The surface should
    run the handler this once WITHOUT idempotency protection."""


@dataclass(frozen=True)
class CoreDecision:
    """Result of ``decide``: the outcome plus whatever the caller needs next."""

    outcome: CoreOutcome
    claim_token: str | None = None
    stored: StoredResult | None = None
    wait_duration_seconds: float = 0.0


class IdempotentCore:
    """The surface-neutral state machine. One instance per adapter.

    Holds only the knobs the mechanism needs; everything HTTP-/queue-/AI-shaped
    stays in the adapter. The same ``EventEmitter`` instance is shared with the
    adapter so all events (whoever emits them) reach the user's handlers.
    """

    def __init__(
        self,
        backend: IdempotencyBackend,
        *,
        emitter: EventEmitter,
        backend_name: str,
        surface: str = "http",
        on_storage_error: Literal["fail_closed", "fail_open"] = "fail_closed",
        lease_ttl_seconds: float = 30.0,
        wait_timeout_seconds: float = 10.0,
        completed_ttl_seconds: float = 86_400.0,
        fingerprint_version: int = 1,
        use_local_cache: bool = False,
        local_cache_max_items: int = 1024,
        response_hook: Callable[[StoredResult], StoredResult] | None = None,
    ) -> None:
        self.backend = backend
        self.emitter = emitter
        self.backend_name = backend_name
        self.surface = surface
        self.on_storage_error = on_storage_error
        self.lease_ttl_seconds = lease_ttl_seconds
        self.wait_timeout_seconds = wait_timeout_seconds
        self.completed_ttl_seconds = completed_ttl_seconds
        self.fingerprint_version = fingerprint_version
        self.response_hook = response_hook
        self.use_local_cache = use_local_cache
        self.local_cache_max_items = local_cache_max_items
        # effective_key -> (fingerprint, stored_result, expires_at_monotonic).
        # Only COMPLETED results land here (spec §5.9): never an in-progress
        # claim, whose state is volatile and owned elsewhere.
        self._local_cache: OrderedDict[str, tuple[str, StoredResult, float]] = (
            OrderedDict()
        )

    # ----- decision phase -----

    async def decide(self, effective_key: str, fingerprint: str) -> CoreDecision:
        """Claim the key and decide what the caller should do (spec §5.2-§5.5).

        Emits exactly one terminal event (plus a preceding ``in_flight_wait`` on
        the wait path). The caller never has to emit anything itself.
        """
        started = time.monotonic()

        # Warm-path short-circuit (spec §5.9): a COMPLETED result this process
        # already saw, with a matching fingerprint, replays without a backend
        # round-trip. Correctness still rests on the backend — a stale or absent
        # local entry only ever costs a read it could have skipped, never a wrong
        # hit (a fingerprint mismatch falls through to the backend, which then
        # reports the mismatch).
        if self.use_local_cache:
            cached = self._local_get(effective_key, fingerprint)
            if cached is not None:
                self._emit(Decision.REPLAYED, effective_key, started)
                return CoreDecision(CoreOutcome.REPLAY, stored=self._replay(cached))

        try:
            claim = await self.backend.claim(
                effective_key=effective_key,
                fingerprint=fingerprint,
                fingerprint_version=self.fingerprint_version,
                lease_ttl_seconds=self.lease_ttl_seconds,
            )
        except StorageError:
            return self._on_storage_failure(effective_key, started)
        except Exception:
            # An unexpected backend failure (including a ConfigurationError
            # surfaced lazily on first use, e.g. a missing Postgres schema). Log
            # it so the cause isn't lost behind a silent storage decision.
            _logger.exception(
                "idemkit: backend claim failed; applying on_storage_error=%s",
                self.on_storage_error,
            )
            return self._on_storage_failure(effective_key, started)

        if claim.result == ClaimResultType.NEW_CLAIMED:
            decision = (
                Decision.CORRUPT_RECORD
                if claim.recovered_from_corrupt
                else Decision.NEW
            )
            self._emit(decision, effective_key, started)
            return CoreDecision(CoreOutcome.PROCEED, claim_token=claim.our_claim_token)

        if claim.result == ClaimResultType.LEASE_RECLAIMED:
            self._emit(Decision.LEASE_RECLAIMED, effective_key, started)
            return CoreDecision(CoreOutcome.PROCEED, claim_token=claim.our_claim_token)

        # ALREADY_CLAIMED / ALREADY_COMPLETED — the fingerprint must agree before
        # we ever consider replaying (spec §5.1: a key hit with a different
        # payload is a mismatch, never a wrong replay).
        if (
            claim.record.fingerprint != fingerprint
            or claim.record.fingerprint_version != self.fingerprint_version
        ):
            self._emit(Decision.PAYLOAD_MISMATCH, effective_key, started)
            return CoreDecision(CoreOutcome.MISMATCH)

        if claim.result == ClaimResultType.ALREADY_COMPLETED:
            self._emit(Decision.REPLAYED, effective_key, started)
            stored = _stored_from_record(claim.record)
            self._local_put(effective_key, fingerprint, stored)
            return CoreDecision(CoreOutcome.REPLAY, stored=self._replay(stored))

        # ALREADY_CLAIMED: someone else is mid-flight. Wait for them (spec §5.5).
        self._emit(Decision.IN_FLIGHT_WAIT, effective_key, started)
        wait_started = time.monotonic()
        try:
            record = await self.backend.wait_for_completion(
                effective_key=effective_key,
                timeout_seconds=self.wait_timeout_seconds,
            )
        except Exception:
            _logger.exception(
                "idemkit: backend wait_for_completion failed; applying "
                "on_storage_error=%s",
                self.on_storage_error,
            )
            return self._on_storage_failure(effective_key, started)
        wait_duration = time.monotonic() - wait_started

        if record is not None:
            self._emit(
                Decision.REPLAYED, effective_key, started, wait_duration=wait_duration
            )
            stored = _stored_from_record(record)
            self._local_put(effective_key, fingerprint, stored)
            return CoreDecision(
                CoreOutcome.REPLAY,
                stored=self._replay(stored),
                wait_duration_seconds=wait_duration,
            )

        # Timed out, or the in-flight claim was released without completing.
        self._emit(
            Decision.CONFLICT, effective_key, started, wait_duration=wait_duration
        )
        return CoreDecision(
            CoreOutcome.CONFLICT, wait_duration_seconds=wait_duration
        )

    # ----- completion phase -----

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        result: StoredResult,
        completed_ttl_seconds: float | None = None,
    ) -> None:
        """Store the handler's result against our claim (spec §5.2 complete).

        The caller decides cacheability and any per-surface transform (redaction,
        size caps) BEFORE calling this. Here we only do the conditional, fenced
        write and surface a lost race as ``lease_reclaimed_loss``.
        """
        ttl = (
            completed_ttl_seconds
            if completed_ttl_seconds is not None
            else self.completed_ttl_seconds
        )
        # Drop any stale local entry; the next replay re-reads and re-caches with
        # the fresh result (we don't have the fingerprint here to repopulate).
        self._local_invalidate(effective_key)
        try:
            ok = await self.backend.complete(
                effective_key=effective_key,
                claim_token=claim_token,
                response_status=result.marker,
                response_headers=result.meta,
                response_body=result.blob,
                completed_ttl_seconds=ttl,
            )
        except Exception:
            # The backend died trying to persist the result, after the handler
            # already produced its side effect. Do NOT release: releasing would
            # delete the CLAIMED record and let an immediate retry re-run the
            # side effect. Leave the claim so the lease is the real safety net —
            # duplicates see in-flight until it lapses, then one reclaims and
            # re-runs (fail closed, §5.7). We could not cache the result; that is
            # the honest cost of a storage failure at completion time.
            _logger.warning(
                "idemkit: backend.complete failed; the result was not cached. "
                "Holding the claim so the lease (not an immediate retry) governs "
                "re-execution."
            )
            return

        if not ok:
            # Our lease was reclaimed mid-handler and someone else now owns the
            # key; our completion is fenced out and discarded (spec §5.3).
            self._emit_simple(Decision.LEASE_RECLAIMED_LOSS, effective_key)

    async def release(self, effective_key: str, claim_token: str) -> None:
        """Release our claim so a retry can re-run (spec §5.6). Best-effort:
        if the backend is unreachable, lease expiry reclaims the key anyway."""
        self._local_invalidate(effective_key)  # §5.9: invalidate on release
        try:
            await self.backend.release(effective_key, claim_token)
        except Exception:
            pass

    # ----- run-once convenience (direct callers: queue / AI) -----

    async def run_once(
        self,
        effective_key: str,
        fingerprint: str,
        handler: Callable[[], Awaitable[Any]],
        *,
        encode: Callable[[Any], StoredResult | None] | None = None,
        is_cacheable: Callable[[StoredResult | None], bool] = lambda _r: True,
        heartbeat: bool = False,
        heartbeat_interval_seconds: float | None = None,
    ) -> RunOnceResult:
        """Run ``handler`` at most once for ``effective_key``, replaying the
        stored result on any duplicate.

        This is the building block the queue and AI adapters compose with their
        own attempt-counting / ack logic. The HTTP middleware does NOT use it —
        it needs the inverted ``decide`` / ``complete`` shape because it
        structurally wraps the ASGI handler.

        With ``encode=None`` the handler returns a ``StoredResult`` to cache (or
        ``None`` for the side-effect-only case). With an ``encode`` callable the
        handler returns its raw value and ``encode`` serializes it *after* the
        protected run — so a serialization failure does NOT release the claim
        (fail closed, §5.4); it returns ``ENCODE_FAILED`` and leaves the lease to
        govern re-execution, rather than immediately re-running the side effect.

        A handler that *raises* releases the claim so the operation can be
        retried (that's the crash path, distinct from an encode failure).

        ``heartbeat=True`` keeps the lease alive while the handler runs (spec
        §5.3.1), so the lease can be short (fast crash recovery) without a slow
        handler losing its claim. If a renewal fails the handler is cancelled
        cooperatively and the result is ``LEASE_LOST`` — read the honest limits
        on ``_run_under_heartbeat``.
        """
        decision = await self.decide(effective_key, fingerprint)

        if decision.outcome is CoreOutcome.REPLAY:
            return RunOnceResult(RunStatus.REPLAYED, decision.stored)
        if decision.outcome is CoreOutcome.MISMATCH:
            return RunOnceResult(RunStatus.MISMATCH, None)
        if decision.outcome is CoreOutcome.CONFLICT:
            return RunOnceResult(RunStatus.CONFLICT, None)
        if decision.outcome is CoreOutcome.STORAGE_REFUSE:
            raise StorageError(
                "idemkit: storage unavailable and on_storage_error=fail_closed"
            )

        # PROCEED (claim held) or STORAGE_PASS (run unprotected, this once).
        unprotected = decision.outcome is CoreOutcome.STORAGE_PASS
        token = decision.claim_token

        if heartbeat and token is not None and not unprotected:
            interval = heartbeat_interval_seconds or _default_heartbeat_interval(
                self.lease_ttl_seconds
            )
            try:
                value, lease_lost = await self._run_under_heartbeat(
                    effective_key, token, handler, interval
                )
            except BaseException:
                await self.release(effective_key, token)
                raise
            if lease_lost:
                # We no longer hold the claim — the lease lapsed and another
                # executor may already own the key. Do not complete (we'd be
                # fenced anyway) and do not release (it isn't ours to release).
                return RunOnceResult(RunStatus.LEASE_LOST, None)
            return await self._finalize(
                effective_key, token, value, encode, is_cacheable
            )

        try:
            value = await handler()
        except BaseException:
            if token is not None and not unprotected:
                await self.release(effective_key, token)
            raise

        if unprotected or token is None:
            status = RunStatus.UNPROTECTED if unprotected else RunStatus.EXECUTED
            # No claim to store against; still surface the (best-effort) encoded
            # value so the caller can read it uniformly.
            stored = _safe_encode(value, encode)
            return RunOnceResult(status, stored)

        return await self._finalize(effective_key, token, value, encode, is_cacheable)

    async def _finalize(
        self,
        effective_key: str,
        token: str,
        value: Any,
        encode: Callable[[Any], StoredResult | None] | None,
        is_cacheable: Callable[[StoredResult | None], bool],
    ) -> RunOnceResult:
        """Serialize the handler's value and store or release the claim.

        The side effect has already happened and we still hold the claim. A
        serialization failure here is fail-closed: we keep the claim (no release)
        so a retry can't immediately re-run the side effect (§5.4)."""
        if encode is None:
            stored: StoredResult | None = value
        else:
            try:
                stored = encode(value)
            except Exception:
                _logger.error(
                    "idemkit: could not serialize the result; NOT caching and NOT "
                    "releasing the claim, so a retry won't immediately re-run the "
                    "side effect. Fix the codec or the return value (fail closed, "
                    "§5.4)."
                )
                return RunOnceResult(RunStatus.ENCODE_FAILED, None)

        if is_cacheable(stored):
            await self.complete(
                effective_key,
                token,
                stored if stored is not None else StoredResult(b""),
            )
        else:
            await self.release(effective_key, token)
        return RunOnceResult(RunStatus.EXECUTED, stored)

    async def execute_with_heartbeat(
        self,
        effective_key: str,
        claim_token: str,
        handler: Callable[[], Awaitable[_H]],
        *,
        interval_seconds: float | None = None,
    ) -> tuple[_H | None, bool]:
        """Run ``handler`` while renewing the lease (spec §5.3.1), returning
        ``(result, lease_lost)``.

        Public entry point for adapters (the AI tool decorator) that drive their
        own claim/complete flow but still want lease renewal. ``run_once`` uses
        the same machinery internally.
        """
        interval = interval_seconds or _default_heartbeat_interval(
            self.lease_ttl_seconds
        )
        return await self._run_under_heartbeat(
            effective_key, claim_token, handler, interval
        )

    async def _run_under_heartbeat(
        self,
        effective_key: str,
        claim_token: str,
        handler: Callable[[], Awaitable[_H]],
        interval_seconds: float,
    ) -> tuple[_H | None, bool]:
        """Run ``handler`` while renewing the lease in the background (§5.3.1).

        Returns ``(result, lease_lost)``. ``lease_lost`` is True when a renewal
        could not be confirmed (a storage blip, or the lease was reclaimed): the
        handler is then cancelled cooperatively so a well-behaved handler can
        stop before a second executor proceeds.

        Honest limits (do not oversell this): asyncio cancellation only lands at
        an ``await``. A handler that awaits regularly observes the
        ``CancelledError`` and can stop in time — for those, cancellation closes
        the partition window. A handler blocked in a synchronous call (a sync DB
        driver, ``requests``, a CPU-bound loop) will NOT see it until it next
        awaits, possibly after the lease already lapsed; there this degrades to
        the §5.7 partition hazard, and a money path still needs downstream
        idempotency. The ``interval`` is sized below the lease so a failed renew
        is detected with margin.
        """
        lease_lost = False
        handler_task: asyncio.Task[_H] = asyncio.ensure_future(handler())

        async def heartbeat() -> None:
            nonlocal lease_lost
            while True:
                await asyncio.sleep(interval_seconds)
                try:
                    ok = await self.backend.renew(
                        effective_key, claim_token, self.lease_ttl_seconds
                    )
                except Exception:
                    _logger.warning(
                        "idemkit: lease renewal errored; cancelling handler so a "
                        "second executor doesn't race it (§5.3.1)."
                    )
                    ok = False
                if not ok:
                    lease_lost = True
                    handler_task.cancel()
                    return

        beat_task: asyncio.Task[None] = asyncio.ensure_future(heartbeat())
        try:
            result = await handler_task
            return result, lease_lost
        except asyncio.CancelledError:
            if lease_lost:
                # We cancelled the handler ourselves because the lease was lost.
                return None, True
            raise  # a genuine external cancellation; let it propagate.
        finally:
            beat_task.cancel()
            if not handler_task.done():
                handler_task.cancel()
            await asyncio.gather(beat_task, handler_task, return_exceptions=True)

    # ----- internals -----

    def _on_storage_failure(self, effective_key: str, started: float) -> CoreDecision:
        self._emit(Decision.STORAGE_ERROR, effective_key, started)
        if self.on_storage_error == "fail_closed":
            return CoreDecision(CoreOutcome.STORAGE_REFUSE)
        return CoreDecision(CoreOutcome.STORAGE_PASS)

    def _replay(self, stored: StoredResult) -> StoredResult:
        """Apply the response_hook to a result about to be replayed (spec §5.8)."""
        if self.response_hook is None:
            return stored
        return self.response_hook(stored)

    def _local_get(
        self, effective_key: str, fingerprint: str
    ) -> StoredResult | None:
        entry = self._local_cache.get(effective_key)
        if entry is None:
            return None
        cached_fingerprint, stored, expires_at = entry
        if time.monotonic() >= expires_at:
            # TTL-checked on read: a stale entry is dropped, not served.
            self._local_cache.pop(effective_key, None)
            return None
        if cached_fingerprint != fingerprint:
            # Same key, different payload — let the backend report the mismatch.
            return None
        self._local_cache.move_to_end(effective_key)  # LRU bump
        return stored

    def _local_put(
        self, effective_key: str, fingerprint: str, stored: StoredResult
    ) -> None:
        if not self.use_local_cache:
            return
        existing = self._local_cache.get(effective_key)
        if existing is not None and existing[0] == fingerprint:
            # Already cached for this fingerprint. Bump LRU but do NOT extend the
            # expiry: the entry's lifetime is anchored near the original
            # completion, not refreshed on every replay. Refreshing it would let
            # a hot key's local entry outlive the backend record and serve a
            # stale result after the backend expired and re-claimed (§5.9).
            self._local_cache.move_to_end(effective_key)
            return
        expires_at = time.monotonic() + self.completed_ttl_seconds
        self._local_cache[effective_key] = (fingerprint, stored, expires_at)
        self._local_cache.move_to_end(effective_key)
        while len(self._local_cache) > self.local_cache_max_items:
            self._local_cache.popitem(last=False)  # evict least-recently-used

    def _local_invalidate(self, effective_key: str) -> None:
        if self._local_cache:
            self._local_cache.pop(effective_key, None)

    def _emit(
        self,
        decision: Decision,
        effective_key: str,
        started: float,
        wait_duration: float = 0.0,
    ) -> None:
        self.emitter.emit(
            IdempotencyEvent(
                decision=decision,
                effective_key=effective_key,
                backend_name=self.backend_name,
                fingerprint_version=self.fingerprint_version,
                latency_seconds=time.monotonic() - started,
                wait_duration_seconds=wait_duration,
                surface=self.surface,
            )
        )

    def _emit_simple(self, decision: Decision, effective_key: str) -> None:
        self.emitter.emit(
            IdempotencyEvent(
                decision=decision,
                effective_key=effective_key,
                backend_name=self.backend_name,
                fingerprint_version=self.fingerprint_version,
                latency_seconds=0.0,
                surface=self.surface,
            )
        )


class RunStatus(enum.Enum):
    """Terminal status of a ``run_once`` call."""

    EXECUTED = "executed"
    """The handler ran and its result was stored (or released if non-cacheable)."""

    REPLAYED = "replayed"
    """A duplicate; the stored result was returned without running the handler."""

    MISMATCH = "mismatch"
    """Key reused with a different fingerprint; the handler did NOT run."""

    CONFLICT = "conflict"
    """Another executor held the claim and didn't finish in time; did NOT run."""

    UNPROTECTED = "unprotected"
    """Storage was down with fail_open; the handler ran without dedup this once."""

    LEASE_LOST = "lease_lost"
    """The lease could not be renewed mid-handler and the handler was cancelled
    (spec §5.3.1). The operation is unfinished from this executor's side; a
    retry (or the broker's redelivery) will run it again."""

    ENCODE_FAILED = "encode_failed"
    """The handler ran (its side effect happened) but its result could not be
    serialized. Fail closed (§5.4): the claim is held, not released, so a retry
    doesn't immediately re-run the side effect. The result was not cached."""


@dataclass(frozen=True)
class RunOnceResult:
    """What ``run_once`` returns: the status plus the stored/replayed payload."""

    status: RunStatus
    stored: StoredResult | None


def _safe_encode(
    value: Any, encode: Callable[[Any], StoredResult | None] | None
) -> StoredResult | None:
    """Best-effort encode for the unprotected path (no claim to fail closed on)."""
    if encode is None:
        return value  # type: ignore[no-any-return]
    try:
        return encode(value)
    except Exception:
        return None


def _default_heartbeat_interval(lease_ttl_seconds: float) -> float:
    """A renewal cadence with margin below the lease (spec §5.3.1).

    A third of the lease means roughly three chances to renew before it lapses,
    so one dropped renewal doesn't immediately cost the claim. Floored so a
    pathologically small lease still yields a positive sleep.
    """
    return max(lease_ttl_seconds / 3.0, 0.01)


def _stored_from_record(record: StoredRecord) -> StoredResult:
    """Project a backend record's completion fields into a ``StoredResult``."""
    return StoredResult(
        blob=record.response_body,
        meta=dict(record.response_headers),
        marker=record.response_status if record.response_status is not None else 200,
    )
