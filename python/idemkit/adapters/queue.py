"""Queue consumer idempotency, per spec §7.

``IdempotentConsumer`` wraps a message handler so its side effect runs once per
broker dedup id even under at-least-once delivery, concurrent consumers, and
consumer crashes. It is broker-agnostic: you tell it how to read the dedup id,
the scope, and (ideally) the broker's native receive count; it tells you whether
to ack the message or leave it for redelivery.

What this closes that ad-hoc dedup does not (spec §7.2):

* **Lease vs visibility-timeout coupling.** The lease is derived from, and kept
  strictly shorter than, the broker's visibility timeout, and the heartbeat
  (spec §5.3.1) renews it so a slow handler keeps its claim without ever letting
  the lease approach the visibility window. A lease >= visibility is rejected at
  construction.
* **Crash-safe claim.** A consumer that dies after claiming but before acking
  does not wedge the message: the storage-clock lease lapses and the redelivery
  is reclaimed and processed once; the dead consumer's late completion is fenced.
* **Side-effect-only vs result-bearing.** A handler that returns ``None`` records
  a "processed" marker (replay = skip). A handler that returns a value caches it
  and replays it on redelivery.
* **Poison / DLQ boundary.** Idempotency releases the claim on failure so the
  broker can redeliver — but bounded. After ``max_attempts`` the consumer stops
  retrying and calls ``on_exhausted`` (route to a DLQ, alert, ...).
* **Attempt counting that survives release** (spec §7.2 #6). The claim record is
  gone after a release, so attempts are counted from the broker's receive count
  when available, else a separate attempt store.

You write: how to read the dedup id and visibility timeout, and your handler.
idemkit guarantees the side effect runs once per dedup id and keeps the lease
under the visibility timeout. You decide ``max_attempts`` and ``on_exhausted``.
"""

from __future__ import annotations

import asyncio
import enum
import hashlib
import inspect
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol

from idemkit.backends.base import IdempotencyBackend
from idemkit.core.codecs import JsonResultCodec, ResultCodec, SideEffectCodec
from idemkit.core.events import EventEmitter
from idemkit.core.exceptions import ConfigurationError, PayloadMismatch, StorageError
from idemkit.core.fingerprint import compose_key
from idemkit.core.policy import QueueConfig
from idemkit.core.runner import IdempotentCore, RunStatus, StoredResult
from idemkit.core.sync_bridge import register_closable, run_sync

_logger = logging.getLogger(__name__)

# A queue message's identity is its dedup id (carried in the effective key), so
# there is no payload fingerprint to compare — a fixed value means the core's
# fingerprint check never trips for the queue surface (spec §5.1: the broker's
# dedup id IS the "same operation" declaration).
QUEUE_FINGERPRINT = "idemkit-queue-v1"

# By default the lease is half the visibility timeout: comfortably shorter (so a
# redelivery can't race a still-running handler) while leaving room for the
# heartbeat to renew it. Override with an explicit ``lease_ttl_seconds``.
_DEFAULT_LEASE_FRACTION = 0.5


def _default_scope(_m: Any) -> tuple[()]:
    """Default queue scope: no scope (a single shared namespace)."""
    return ()


class ConsumerAction(enum.Enum):
    """What the broker glue should do with the message after ``dispatch``."""

    ACK = "ack"
    """Remove it from the broker. Success, a deduplicated skip, or DLQ'd."""

    RETRY = "retry"
    """Leave it for redelivery. A transient failure (within ``max_attempts``), a
    sibling consumer still processing it, or a lost lease."""


@dataclass(frozen=True)
class ConsumerResult:
    """Outcome of dispatching one message."""

    action: ConsumerAction
    status: RunStatus | None
    attempt: int
    result: Any = None
    exhausted: bool = False
    error: BaseException | None = None


class AttemptStore(Protocol):
    """Durable per-dedup-id attempt counter (spec §7.2 #6 fallback).

    Used only when the broker does not expose a receive count. Implementations
    increment atomically per delivery and expire entries after the redelivery
    window. The claim record cannot hold this count because it is deleted on
    release between redeliveries.
    """

    async def incr(self, dedup_id: str, ttl_seconds: float) -> int:
        """Increment and return the attempt number for ``dedup_id`` (1-based)."""
        ...


class InMemoryAttemptStore:
    """Single-process attempt counter.

    Honest limitation: this lives in one process, so it does NOT count attempts
    across multiple consumer processes or survive a restart. In a multi-consumer
    deployment, prefer the broker's native receive count (``receive_count=``) or
    a durable ``AttemptStore``. idemkit falls back to this only when neither is
    given, and warns once.
    """

    def __init__(self) -> None:
        import asyncio

        self._counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def incr(self, dedup_id: str, ttl_seconds: float) -> int:
        async with self._lock:
            self._counts[dedup_id] = self._counts.get(dedup_id, 0) + 1
            return self._counts[dedup_id]


class IdempotentConsumer:
    """Idempotent queue-consumer wrapper (spec §7.3)."""

    def __init__(
        self,
        *,
        backend: IdempotencyBackend,
        config: QueueConfig,
        handler: Callable[[Any], Any] | None = None,
    ) -> None:
        # backend is WHERE state lives; everything else is on the QueueConfig.
        c = config
        if c.dedup_id is None:
            raise ConfigurationError(
                "idemkit: QueueConfig.dedup_id is required — tell idemkit how to read "
                "the broker's dedup id from a message (e.g. dedup_id=lambda m: m.message_id). "
                "The contrib sqs/kafka helpers preset it for you."
            )
        if c.visibility_timeout_seconds is None:
            raise ConfigurationError(
                "idemkit: QueueConfig.visibility_timeout_seconds is required (the "
                "broker's visibility window; the lease is derived from it)."
            )
        dedup_id = c.dedup_id
        visibility_timeout_seconds = c.visibility_timeout_seconds
        scope = c.scope if c.scope is not None else _default_scope
        max_attempts = c.max_attempts
        on_exhausted = c.on_exhausted
        receive_count = c.receive_count
        attempt_store = c.attempt_store
        cache_result = c.cache_result
        result_codec = c.result_codec
        validation_fingerprint = c.validation_fingerprint
        lease_ttl_seconds = c.lease_ttl_seconds
        expires_after_seconds = c.expires_after_seconds
        wait_timeout_seconds = c.wait_timeout_seconds if c.wait_timeout_seconds is not None else 5.0
        on_storage_error = c.on_storage_error
        use_local_cache = c.use_local_cache
        local_cache_max_items = c.local_cache_max_items
        event_handlers = list(c.event_handlers)

        # Lease vs visibility timeout (§7.2 #1): the lease MUST be shorter than
        # the broker's visibility timeout, or a redelivery can race a handler
        # that is still legitimately running.
        if lease_ttl_seconds is None:
            lease_ttl_seconds = visibility_timeout_seconds * _DEFAULT_LEASE_FRACTION
        if lease_ttl_seconds >= visibility_timeout_seconds:
            raise ConfigurationError(
                f"idemkit: lease_ttl_seconds ({lease_ttl_seconds}) must be shorter "
                f"than visibility_timeout_seconds ({visibility_timeout_seconds}). "
                "A lease at or beyond the visibility timeout lets the broker "
                "redeliver while the first consumer is still running, so both "
                "execute (spec §7.2 #1). Leave lease_ttl_seconds unset to derive "
                "a safe default."
            )

        self.dedup_id = dedup_id
        self.scope = scope
        self.visibility_timeout_seconds = visibility_timeout_seconds
        self.lease_ttl_seconds = lease_ttl_seconds
        self.max_attempts = max_attempts
        self.on_exhausted = on_exhausted
        self.receive_count = receive_count
        self.expires_after_seconds = expires_after_seconds
        self._cache_result = cache_result
        self._validation_fingerprint = validation_fingerprint
        self._backend = backend
        self._handler: Callable[[Any], Any] | None = None
        if handler is not None:
            self._set_handler(handler)
        self._warned_inprocess_attempts = False

        # A fallback attempt counter is ALWAYS available, so max_attempts stays
        # enforced even when a configured receive_count returns None for some
        # delivery (a broker that didn't populate the count). The in-process
        # default is warned at first use; prefer a durable store or the broker's
        # receive count in a multi-consumer deployment (§7.2 #6).
        self._attempt_store: AttemptStore = attempt_store or InMemoryAttemptStore()

        if result_codec is not None:
            self._codec: ResultCodec[Any, Any] = result_codec
        elif cache_result:
            self._codec = JsonResultCodec()
        else:
            self._codec = SideEffectCodec()

        self.core = IdempotentCore(
            backend,
            emitter=EventEmitter(event_handlers or []),
            backend_name=type(backend).__name__,
            surface="queue",
            on_storage_error=on_storage_error,
            lease_ttl_seconds=lease_ttl_seconds,
            wait_timeout_seconds=wait_timeout_seconds,
            expires_after_seconds=expires_after_seconds,
            use_local_cache=use_local_cache,
            local_cache_max_items=local_cache_max_items,
        )

    def handle(self, fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
        """Register the message handler. Usable as ``@consumer.handle``.

        The handler may be ``async def`` (awaited) or a plain ``def`` (run on a
        worker thread so a blocking call doesn't stall the event loop). Drive an
        async handler with ``await dispatch(...)``; a synchronous poll loop calls
        ``dispatch_sync(...)``.
        """
        self._set_handler(fn)
        return fn

    def _set_handler(self, fn: Callable[[Any], Any]) -> None:
        self._handler = fn
        self._handler_is_async = inspect.iscoroutinefunction(fn)

    def effective_key(self, message: Any) -> str:
        """The effective key for a message — exposed for tests that need to set
        up a record by hand (spec §9.4 testability)."""
        dedup_id = self.dedup_id(message)
        return self._effective_key(message, dedup_id)

    def _fingerprint(self, message: Any) -> str:
        """Payload fingerprint compared on a dedup-id hit (spec §5.1).

        With no ``validation_fingerprint`` it is a constant, so two deliveries of
        the same dedup id always replay: the broker's id is authoritative. With one
        set, a redelivery whose fingerprint bytes differ is a ``PayloadMismatch`` —
        this catches a producer that reused a message id with a different body."""
        if self._validation_fingerprint is None:
            return QUEUE_FINGERPRINT
        return hashlib.sha256(self._validation_fingerprint(message)).hexdigest()

    async def dispatch(self, message: Any) -> ConsumerResult:
        """Process one delivery: dedup, run the handler at most once, and report
        whether to ack or leave it for redelivery."""
        if self._handler is None:
            raise RuntimeError(
                "idemkit: no handler registered. Use @consumer.handle or pass "
                "handler= to IdempotentConsumer."
            )

        dedup_id = self.dedup_id(message)
        effective_key = self._effective_key(message, dedup_id)
        attempt = await self._attempt_number(message, dedup_id)
        handler = self._handler

        async def invoke() -> Any:
            # Return the raw handler value; serialization (for result-bearing
            # mode) happens AFTER the protected run so an unserializable result
            # fails closed instead of releasing the claim and re-running (§5.4).
            if self._handler_is_async:
                return await handler(message)
            # Synchronous handler: run it on a worker thread so a blocking call
            # doesn't stall the loop (and the heartbeat can keep renewing).
            return await asyncio.to_thread(handler, message)

        encode = self._codec.encode if self._cache_result else None

        try:
            outcome = await self.core.run_once(
                effective_key, self._fingerprint(message), invoke, encode=encode, heartbeat=True
            )
        except StorageError as exc:
            # The idempotency backend is unavailable — an INFRASTRUCTURE problem,
            # not a poison message. Leave it for redelivery WITHOUT counting it
            # against max_attempts, so an outage doesn't dead-letter healthy
            # messages the handler never even ran.
            return ConsumerResult(ConsumerAction.RETRY, None, attempt, error=exc)
        except Exception as exc:
            # The handler itself raised; run_once released the claim so the broker
            # may redeliver. Bounded by max_attempts (§7.2 #4).
            if attempt >= self.max_attempts:
                await self._exhaust(message, exc)
                return ConsumerResult(ConsumerAction.ACK, None, attempt, exhausted=True, error=exc)
            return ConsumerResult(ConsumerAction.RETRY, None, attempt, error=exc)

        status = outcome.status
        if status in (RunStatus.EXECUTED, RunStatus.REPLAYED, RunStatus.UNPROTECTED):
            result_value = self._decode_result(outcome.stored)
            return ConsumerResult(ConsumerAction.ACK, status, attempt, result=result_value)

        if status is RunStatus.MISMATCH:
            # Same dedup id, DIFFERENT validation payload (validation_fingerprint is
            # set and the bytes changed): a producer reused a message id with a new
            # body, or an id collision. Redelivery can't fix a body mismatch, so do
            # NOT retry forever — hand it to on_exhausted (DLQ) if set, then ack.
            mismatch = PayloadMismatch(
                f"idemkit: message dedup id {dedup_id!r} was already processed with a "
                "different validation payload; refusing to replay a mismatched body."
            )
            _logger.error("%s", mismatch)
            await self._exhaust(message, mismatch)
            return ConsumerResult(
                ConsumerAction.ACK, status, attempt, exhausted=True, error=mismatch
            )

        if status is RunStatus.ENCODE_FAILED:
            # The side effect ran once but its result couldn't be serialized. Ack
            # so the message is NOT redelivered and re-run; the result is simply
            # not cached. Loud because it's an application bug to fix.
            _logger.error(
                "idemkit: handler result for dedup id %r could not be serialized; "
                "acking so it is not re-run, but the result was not cached. Return "
                "a serializable value or set a matching result_codec (§5.4).",
                dedup_id,
            )
            return ConsumerResult(ConsumerAction.ACK, status, attempt)

        # CONFLICT or LEASE_LOST: a sibling consumer holds (or held) the claim
        # and we couldn't confirm completion. Decline WITHOUT acking — acking
        # would risk losing the message if the sibling then fails. Let the broker
        # redeliver; by then the record is likely COMPLETED and we replay.
        return ConsumerResult(ConsumerAction.RETRY, status, attempt)

    def dispatch_sync(self, message: Any) -> ConsumerResult:
        """Synchronous :meth:`dispatch` for a non-async poll loop.

        Runs the same dedup/claim/lease flow on a shared background event loop, so
        a Celery task or a threaded SQS/Kafka worker can call it like a normal
        function. The handler (async or sync) is driven there too. Do not call
        this from inside a running event loop — use ``await dispatch(...)``.
        """
        register_closable(self._backend)
        return run_sync(self.dispatch(message))

    # ----- internals -----

    def _effective_key(self, message: Any, dedup_id: str) -> str:
        scope = self.scope(message)
        if scope is None:
            scope_parts: list[str] = []
        elif isinstance(scope, str):
            scope_parts = [scope]
        else:
            scope_parts = [str(part) for part in scope]
        return compose_key([dedup_id, *scope_parts])

    async def _attempt_number(self, message: Any, dedup_id: str) -> int:
        # (a) broker-native receive count — the simplest, most honest source.
        if self.receive_count is not None:
            count = self.receive_count(message)
            if count is not None:
                return int(count)
        # (b) the separate attempts record (always present) — so max_attempts is
        # enforced even when (a) is configured but returned None this delivery.
        if isinstance(self._attempt_store, InMemoryAttemptStore):
            self._warn_inprocess_attempts()
        return await self._attempt_store.incr(dedup_id, self.expires_after_seconds)

    def _decode_result(self, stored: StoredResult | None) -> Any:
        if not self._cache_result or stored is None:
            return None
        return self._codec.decode(stored)

    async def _exhaust(self, message: Any, error: BaseException | None) -> None:
        if self.on_exhausted is None:
            _logger.warning(
                "idemkit: message hit max_attempts=%d with no on_exhausted hook "
                "configured; it will be acked and dropped. Set on_exhausted to "
                "route poison messages to a DLQ (spec §7.2 #4).",
                self.max_attempts,
            )
            return
        outcome = self.on_exhausted(message, error)
        if inspect.isawaitable(outcome):
            await outcome

    def _warn_inprocess_attempts(self) -> None:
        if self._warned_inprocess_attempts:
            return
        # With InMemoryBackend the whole consumer is single-process by definition
        # (its own warning already says so), so an in-process attempt counter is
        # perfectly correct — no need to add noise about multi-process here.
        if self.core.backend_name == "InMemoryBackend":
            self._warned_inprocess_attempts = True
            return
        self._warned_inprocess_attempts = True
        _logger.warning(
            "idemkit: counting delivery attempts in-process. This does NOT survive "
            "across consumer processes or restarts, so max_attempts may not be "
            "enforced correctly in a multi-consumer deployment. Pass receive_count "
            "(broker-native) or a durable AttemptStore (spec §7.2 #6)."
        )
