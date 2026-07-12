"""Queue consumer conformance — the spec §7.4 vectors, on every backend.

A generic in-memory broker harness stands in for SQS/Kafka/RabbitMQ: it delivers
at-least-once, redelivers anything the consumer declines, and tracks a receive
count. The six required vectors (§7.4) run against InMemory, real Redis, and real
PostgreSQL — plus MongoDB and DynamoDB when their endpoints are configured —
mirroring the HTTP suite's cross-backend shape.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

import pytest

from idemkit import QueueConfig

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")
from idemkit.adapters.queue import (  # noqa: E402
    QUEUE_FINGERPRINT,
    ConsumerAction,
    IdempotentConsumer,
)
from idemkit.backends.memory import InMemoryBackend  # noqa: E402
from idemkit.backends.postgres import PostgresBackend, init_pg  # noqa: E402
from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.core.exceptions import (  # noqa: E402
    ConfigurationError,
    PayloadMismatch,
    StorageError,
)
from idemkit.core.runner import RunStatus  # noqa: E402
from tests._backends import EXTRA_BACKENDS, make_dynamo_backend, make_mongo_backend  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture(scope="module", autouse=True)
async def _pg_schema():
    if PG_URL:
        try:
            await init_pg(PG_URL)
        except Exception:
            pass
    return


@pytest.fixture(params=["memory", "redis", "postgres", *EXTRA_BACKENDS])
async def backend(request):
    if request.param == "memory":
        yield InMemoryBackend()
    elif request.param == "redis":
        if REDIS_URL:
            import redis.asyncio as aioredis

            client = aioredis.from_url(REDIS_URL, decode_responses=False)
            await client.flushdb()
        else:
            client = fakeredis_aio.FakeRedis(decode_responses=False)
        b = RedisBackend(client)
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "postgres":
        if not PG_URL:
            pytest.skip("set IDEMKIT_TEST_PG_URL to enable PostgreSQL contract tests")
        b = PostgresBackend.from_url(PG_URL, min_size=2, max_size=8)
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "mongo":
        b = make_mongo_backend()
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "dynamodb":
        b = make_dynamo_backend()
        try:
            yield b
        finally:
            await b.aclose()


class Message:
    """A minimal broker message: a dedup id, a body, scope, and a receive count
    the broker bumps on each (re)delivery."""

    def __init__(
        self, dedup_id: str, body: bytes = b"", queue: str = "q", group: str = "g"
    ) -> None:
        self.dedup_id = dedup_id
        self.body = body
        self.queue = queue
        self.group = group
        self.receive_count = 0


class Broker:
    """An at-least-once in-memory broker. Redelivers anything declined."""

    def __init__(self) -> None:
        self._queue: list[Message] = []

    def send(self, message: Message) -> None:
        self._queue.append(message)

    @property
    def pending(self) -> int:
        return len(self._queue)

    async def deliver_next(self, consumer: IdempotentConsumer):
        message = self._queue.pop(0)
        message.receive_count += 1
        result = await consumer.dispatch(message)
        if result.action is ConsumerAction.RETRY:
            self._queue.append(message)
        return result


def _consumer(backend, **overrides) -> IdempotentConsumer:
    handler = overrides.pop("handler", None)
    cfg = {
        "dedup_id": lambda m: m.dedup_id,
        "visibility_timeout_seconds": 2.0,
        "scope": lambda m: (m.queue, m.group),
        "receive_count": lambda m: m.receive_count,
    }
    cfg.update(overrides)  # key/visibility/behaviour fields all live on QueueConfig now
    return IdempotentConsumer(
        backend=backend,
        config=QueueConfig(**cfg),
        handler=handler,
    )


def _k(suffix: str) -> str:
    return f"queue-{suffix}-{uuid.uuid4().hex}"


def test_lease_at_or_above_visibility_is_rejected() -> None:
    """A lease >= the visibility timeout lets a redelivery race a running handler.
    Rejected at construction (spec §7.4 queue-lease-shorter-than-visibility)."""
    backend = InMemoryBackend()
    with pytest.raises(ConfigurationError):
        IdempotentConsumer(
            backend=backend,
            config=QueueConfig(
                dedup_id=lambda m: m.dedup_id,
                visibility_timeout_seconds=10.0,
                lease_ttl_seconds=10.0,
            ),
        )
    with pytest.raises(ConfigurationError):
        IdempotentConsumer(
            backend=backend,
            config=QueueConfig(
                dedup_id=lambda m: m.dedup_id,
                visibility_timeout_seconds=10.0,
                lease_ttl_seconds=15.0,
            ),
        )
    c = IdempotentConsumer(
        backend=backend,
        config=QueueConfig(dedup_id=lambda m: m.dedup_id, visibility_timeout_seconds=10.0),
    )
    assert c.lease_ttl_seconds < c.visibility_timeout_seconds


async def test_sync_handler_runs_once_via_thread() -> None:
    """A synchronous handler is supported: run on a worker thread, deduped once
    under redelivery (sync-via-threadpool support). The async `dispatch` accepts
    it so a blocking handler doesn't stall the loop."""
    seen = 0
    consumer = IdempotentConsumer(
        backend=InMemoryBackend(),
        config=QueueConfig(dedup_id=lambda m: m.dedup_id, visibility_timeout_seconds=10.0),
    )

    @consumer.handle
    def process(msg) -> None:
        nonlocal seen
        seen += 1

    msg = Message("m-sync")
    r1 = await consumer.dispatch(msg)
    r2 = await consumer.dispatch(msg)
    assert seen == 1
    assert r1.action is ConsumerAction.ACK and r2.action is ConsumerAction.ACK


def test_dispatch_sync_runs_handler_once_from_sync_code() -> None:
    """`dispatch_sync` drives the whole flow from a synchronous caller (no event
    loop), running the side effect once across redeliveries."""
    seen = 0
    consumer = IdempotentConsumer(
        backend=InMemoryBackend(),
        config=QueueConfig(dedup_id=lambda m: m.dedup_id, visibility_timeout_seconds=10.0),
    )

    @consumer.handle
    def process(msg) -> None:
        nonlocal seen
        seen += 1

    msg = Message("m-dispatch-sync")
    r1 = consumer.dispatch_sync(msg)
    r2 = consumer.dispatch_sync(msg)
    assert seen == 1
    assert r1.action is ConsumerAction.ACK and r2.action is ConsumerAction.ACK


async def test_no_inprocess_attempt_warning_on_memory_backend(caplog) -> None:
    """With InMemoryBackend the consumer is single-process by definition, so the
    in-process attempt-counter warning is noise and must not fire."""
    consumer = IdempotentConsumer(
        backend=InMemoryBackend(),
        config=QueueConfig(dedup_id=lambda m: m.dedup_id, visibility_timeout_seconds=2.0),
    )

    @consumer.handle
    async def process(msg) -> None:
        raise RuntimeError("force the attempt counter to be consulted")

    msg = Message(_k("warn"))
    with caplog.at_level(logging.WARNING, logger="idemkit.adapters.queue"):
        await consumer.dispatch(msg)
    assert not any(
        "counting delivery attempts in-process" in r.getMessage() for r in caplog.records
    ), "the in-process attempt warning must not fire for InMemoryBackend"


async def test_at_least_once_dedup(backend) -> None:
    """Same dedup id delivered N times -> side effect once; every delivery acks."""
    calls = 0
    consumer = _consumer(backend)

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1

    msg = Message(_k("d1"))
    actions = []
    for _ in range(5):
        msg.receive_count += 1
        result = await consumer.dispatch(msg)
        actions.append(result.action)
    assert calls == 1, "the side effect must fire exactly once"
    assert all(a is ConsumerAction.ACK for a in actions)


async def test_concurrent_redelivery_exactly_once(backend) -> None:
    """N consumers get the same message at once -> exactly one executes; the rest
    decline without running."""
    calls = 0
    parallel = 8

    async def process(msg) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)

    consumers = [_consumer(backend, wait_timeout_seconds=3.0) for _ in range(parallel)]
    for c in consumers:
        c.handle(process)
    dedup = _k("concurrent")
    msgs = []
    for _ in range(parallel):
        m = Message(dedup)
        m.receive_count = 1
        msgs.append(m)
    results = await asyncio.gather(*(consumers[i].dispatch(msgs[i]) for i in range(parallel)))
    assert calls == 1, f"exactly one consumer must run the handler; ran {calls}x"
    executed = [r for r in results if r.status is RunStatus.EXECUTED]
    assert len(executed) == 1
    for r in results:
        assert r.action in (ConsumerAction.ACK, ConsumerAction.RETRY)


async def test_crash_recovery(backend) -> None:
    """A consumer claims then 'crashes' (no ack, no complete). After the lease
    expires the redelivery reclaims and processes once; the zombie's late
    completion is fenced out."""
    calls = 0
    consumer = _consumer(backend, visibility_timeout_seconds=0.6, lease_ttl_seconds=0.2)

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1

    msg = Message(_k("crash"))
    msg.receive_count = 1
    effective_key = consumer.effective_key(msg)
    zombie = await backend.claim(effective_key, QUEUE_FINGERPRINT, 1, 0.2)
    assert zombie.our_claim_token is not None
    await asyncio.sleep(0.4)
    result = await consumer.dispatch(msg)
    assert result.action is ConsumerAction.ACK
    assert calls == 1, "the redelivery must process exactly once"
    fenced = await backend.complete(effective_key, zombie.our_claim_token, 200, {}, b"", 3600.0)
    assert fenced is False, "a reclaimed owner's late completion MUST be rejected"


async def test_poison_message_dlq(backend) -> None:
    """A handler that always raises is retried up to max_attempts, then
    on_exhausted fires exactly once and the message is not retried further."""
    calls = 0
    exhausted: list[tuple] = []
    consumer = _consumer(
        backend, max_attempts=3, on_exhausted=lambda m, e: exhausted.append((m, e))
    )

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("poison")

    broker = Broker()
    broker.send(Message(_k("poison")))
    final = None
    for _ in range(10):
        if broker.pending == 0:
            break
        final = await broker.deliver_next(consumer)
    assert calls == 3, "retried exactly max_attempts times"
    assert len(exhausted) == 1, "on_exhausted fires exactly once"
    assert final is not None
    assert final.action is ConsumerAction.ACK
    assert final.exhausted is True
    assert broker.pending == 0, "the poison message is not retried further"


async def test_poison_dlq_with_attempt_store_fallback(backend) -> None:
    """Without a broker receive count, attempts are counted by the separate
    attempt store (spec §7.2 #6 fallback) and max_attempts still bounds retries."""
    calls = 0
    exhausted: list[tuple] = []
    consumer = _consumer(
        backend,
        receive_count=None,
        max_attempts=2,
        on_exhausted=lambda m, e: exhausted.append((m, e)),
    )

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("poison")

    broker = Broker()
    broker.send(Message(_k("poison-store")))
    for _ in range(10):
        if broker.pending == 0:
            break
        await broker.deliver_next(consumer)
    assert calls == 2
    assert len(exhausted) == 1
    assert broker.pending == 0


async def test_cache_result_replay(backend) -> None:
    """A result-bearing handler's return value is replayed on redelivery without
    re-executing the handler."""
    calls = 0
    consumer = _consumer(backend, cache_result=True)

    @consumer.handle
    async def process(msg) -> dict:
        nonlocal calls
        calls += 1
        return {"computed": msg.body.decode()}

    msg = Message(_k("rb"), body=b"42")
    msg.receive_count = 1
    first = await consumer.dispatch(msg)
    assert first.action is ConsumerAction.ACK
    assert first.status is RunStatus.EXECUTED
    assert first.result == {"computed": "42"}
    redelivery = Message(msg.dedup_id, body=b"42")
    redelivery.receive_count = 2
    second = await consumer.dispatch(redelivery)
    assert second.status is RunStatus.REPLAYED
    assert second.result == {"computed": "42"}
    assert calls == 1, "the handler must not re-run on the redelivery"


async def test_cache_result_unserializable_fails_closed(backend) -> None:
    """A result-bearing handler whose return value isn't serializable must NOT
    cause the side effect to re-run: the result couldn't be cached, but the
    message is acked (not redelivered) and the handler runs exactly once (§5.4)."""
    calls = 0
    consumer = _consumer(backend, cache_result=True, wait_timeout_seconds=0.3)

    @consumer.handle
    async def process(msg) -> object:
        nonlocal calls
        calls += 1
        return object()

    msg = Message(_k("unser"))
    msg.receive_count = 1
    first = await consumer.dispatch(msg)
    assert first.status is RunStatus.ENCODE_FAILED
    assert first.action is ConsumerAction.ACK
    redelivery = Message(msg.dedup_id)
    redelivery.receive_count = 2
    second = await consumer.dispatch(redelivery)
    assert second.action is ConsumerAction.RETRY
    assert calls == 1, "the side effect must not run a second time"


async def test_storage_outage_does_not_dlq_innocent_message(backend) -> None:
    """A backend outage (StorageError) must be retried WITHOUT counting against
    max_attempts, so an infrastructure problem doesn't dead-letter a healthy
    message the handler never ran."""
    exhausted: list = []
    consumer = _consumer(backend, max_attempts=2, on_exhausted=lambda m, e: exhausted.append(e))

    @consumer.handle
    async def process(msg) -> None:
        pass

    async def boom(*args, **kwargs):
        raise StorageError("idempotency backend down")

    backend.claim = boom
    msg = Message(_k("outage"))
    for rc in (1, 2, 3, 4):
        msg.receive_count = rc
        result = await consumer.dispatch(msg)
        assert result.action is ConsumerAction.RETRY
    assert exhausted == [], "a storage outage must not route the message to a DLQ"


async def test_validation_fingerprint_catches_reused_dedup_id(backend) -> None:
    """With validation_fingerprint set, a redelivery that reuses a dedup id with a
    DIFFERENT body is a PayloadMismatch: the handler is not re-run, and the message
    goes to on_exhausted (DLQ) and is acked rather than redelivered forever."""
    calls = 0
    dlq: list = []
    consumer = _consumer(
        backend,
        validation_fingerprint=lambda m: m.body,
        on_exhausted=lambda m, e: dlq.append(e),
    )

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1

    dedup = _k("reuse")
    first = await consumer.dispatch(Message(dedup, body=b"amount=100"))
    assert first.action is ConsumerAction.ACK
    # same id, same body -> replayed, handler not re-run
    replay = await consumer.dispatch(Message(dedup, body=b"amount=100"))
    assert replay.action is ConsumerAction.ACK
    assert calls == 1

    # same id, DIFFERENT body -> mismatch: acked, routed to on_exhausted, not re-run
    mismatch = await consumer.dispatch(Message(dedup, body=b"amount=999"))
    assert mismatch.action is ConsumerAction.ACK
    assert mismatch.status is RunStatus.MISMATCH
    assert mismatch.exhausted is True
    assert isinstance(mismatch.error, PayloadMismatch)
    assert len(dlq) == 1
    assert calls == 1, "the handler must not run for a mismatched body"


async def test_no_validation_fingerprint_replays_regardless_of_body(backend) -> None:
    """Without validation_fingerprint the broker's dedup id is authoritative: two
    deliveries of the same id replay even if the bodies differ (default behaviour)."""
    calls = 0
    consumer = _consumer(backend)  # no validation_fingerprint

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1

    dedup = _k("nofp")
    await consumer.dispatch(Message(dedup, body=b"a"))
    second = await consumer.dispatch(Message(dedup, body=b"b"))
    assert second.action is ConsumerAction.ACK
    assert calls == 1
