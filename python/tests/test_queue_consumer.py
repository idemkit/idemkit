"""Queue consumer conformance — the spec §7.4 vectors, on every backend.

A generic in-memory broker harness stands in for SQS/Kafka/RabbitMQ: it delivers
at-least-once, redelivers anything the consumer declines, and tracks a receive
count. The six required vectors (§7.4) run against InMemory, real Redis, and real
PostgreSQL, mirroring the HTTP suite's cross-backend shape.
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid

import pytest

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")

from idemkit.adapters.queue import (  # noqa: E402
    QUEUE_FINGERPRINT,
    ConsumerAction,
    IdempotentConsumer,
)
from idemkit.backends.memory import InMemoryBackend  # noqa: E402
from idemkit.backends.postgres import PostgresBackend, init_pg  # noqa: E402
from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.core.exceptions import ConfigurationError, StorageError  # noqa: E402
from idemkit.core.runner import RunStatus  # noqa: E402

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture(scope="module", autouse=True)
async def _pg_schema():
    if PG_URL:
        try:
            await init_pg(PG_URL)
        except Exception:
            pass
    yield


@pytest.fixture(params=["memory", "redis", "postgres"])
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


# ----- the broker harness -----


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
            self._queue.append(message)  # at-least-once: try again later
        return result


def _consumer(backend, **overrides) -> IdempotentConsumer:
    kwargs = dict(
        backend=backend,
        key=lambda m: m.dedup_id,
        scope=lambda m: (m.queue, m.group),
        visibility_timeout_seconds=2.0,
        receive_count=lambda m: m.receive_count,
    )
    kwargs.update(overrides)
    return IdempotentConsumer(**kwargs)  # type: ignore[arg-type]


def _k(suffix: str) -> str:
    return f"queue-{suffix}-{uuid.uuid4().hex}"


# ----- queue-lease-shorter-than-visibility -----


def test_lease_at_or_above_visibility_is_rejected() -> None:
    """A lease >= the visibility timeout lets a redelivery race a running handler.
    Rejected at construction (spec §7.4 queue-lease-shorter-than-visibility)."""
    backend = InMemoryBackend()
    with pytest.raises(ConfigurationError):
        IdempotentConsumer(
            backend=backend,
            key=lambda m: m.dedup_id,
            visibility_timeout_seconds=10.0,
            lease_ttl_seconds=10.0,  # equal -> rejected
        )
    with pytest.raises(ConfigurationError):
        IdempotentConsumer(
            backend=backend,
            key=lambda m: m.dedup_id,
            visibility_timeout_seconds=10.0,
            lease_ttl_seconds=15.0,  # longer -> rejected
        )

    # The derived default (unset lease) is accepted and is strictly shorter.
    c = IdempotentConsumer(
        backend=backend,
        key=lambda m: m.dedup_id,
        visibility_timeout_seconds=10.0,
    )
    assert c.lease_ttl_seconds < c.visibility_timeout_seconds


async def test_sync_handler_runs_once_via_thread() -> None:
    """A synchronous handler is supported: run on a worker thread, deduped once
    under redelivery (sync-via-threadpool support). The async `dispatch` accepts
    it so a blocking handler doesn't stall the loop."""
    seen = 0

    consumer = IdempotentConsumer(
        backend=InMemoryBackend(),
        key=lambda m: m.dedup_id,
        visibility_timeout_seconds=10.0,
    )

    @consumer.handle
    def process(msg) -> None:  # plain def, not async
        nonlocal seen
        seen += 1

    msg = Message("m-sync")
    r1 = await consumer.dispatch(msg)
    r2 = await consumer.dispatch(msg)  # redelivery
    assert seen == 1
    assert r1.action is ConsumerAction.ACK and r2.action is ConsumerAction.ACK


def test_dispatch_sync_runs_handler_once_from_sync_code() -> None:
    """`dispatch_sync` drives the whole flow from a synchronous caller (no event
    loop), running the side effect once across redeliveries."""
    seen = 0
    consumer = IdempotentConsumer(
        backend=InMemoryBackend(),
        key=lambda m: m.dedup_id,
        visibility_timeout_seconds=10.0,
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
        key=lambda m: m.dedup_id,
        visibility_timeout_seconds=2.0,
        # No receive_count -> falls back to the in-process attempt store.
    )

    @consumer.handle
    async def process(msg) -> None:
        raise RuntimeError("force the attempt counter to be consulted")

    msg = Message(_k("warn"))
    with caplog.at_level(logging.WARNING, logger="idemkit.adapters.queue"):
        await consumer.dispatch(msg)

    assert not any(
        "counting delivery attempts in-process" in r.getMessage()
        for r in caplog.records
    ), "the in-process attempt warning must not fire for InMemoryBackend"


# ----- queue-at-least-once-dedup -----


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


# ----- queue-concurrent-redelivery -----


async def test_concurrent_redelivery_exactly_once(backend) -> None:
    """N consumers get the same message at once -> exactly one executes; the rest
    decline without running."""
    calls = 0
    parallel = 8

    async def process(msg) -> None:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)  # hold the claim so duplicates pile up

    consumers = [_consumer(backend, wait_timeout_seconds=3.0) for _ in range(parallel)]
    for c in consumers:
        c.handle(process)

    dedup = _k("concurrent")
    msgs = []
    for _ in range(parallel):
        m = Message(dedup)
        m.receive_count = 1
        msgs.append(m)

    results = await asyncio.gather(
        *(consumers[i].dispatch(msgs[i]) for i in range(parallel))
    )

    assert calls == 1, f"exactly one consumer must run the handler; ran {calls}x"
    executed = [r for r in results if r.status is RunStatus.EXECUTED]
    assert len(executed) == 1
    # No consumer ran the handler twice; the rest replayed or declined (retry).
    for r in results:
        assert r.action in (ConsumerAction.ACK, ConsumerAction.RETRY)


# ----- queue-crash-recovery -----


async def test_crash_recovery(backend) -> None:
    """A consumer claims then 'crashes' (no ack, no complete). After the lease
    expires the redelivery reclaims and processes once; the zombie's late
    completion is fenced out."""
    calls = 0
    consumer = _consumer(
        backend, visibility_timeout_seconds=0.6, lease_ttl_seconds=0.2
    )

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1

    msg = Message(_k("crash"))
    msg.receive_count = 1
    effective_key = consumer.effective_key(msg)

    # The crashed consumer claimed but never completed or released.
    zombie = await backend.claim(effective_key, QUEUE_FINGERPRINT, 1, 0.2)
    assert zombie.our_claim_token is not None

    await asyncio.sleep(0.4)  # the lease lapses

    result = await consumer.dispatch(msg)
    assert result.action is ConsumerAction.ACK
    assert calls == 1, "the redelivery must process exactly once"

    # The zombie wakes up and tries to complete with its stale token: fenced.
    fenced = await backend.complete(
        effective_key, zombie.our_claim_token, 200, {}, b"", 3600.0
    )
    assert fenced is False, "a reclaimed owner's late completion MUST be rejected"


# ----- queue-poison-dlq -----


async def test_poison_message_dlq(backend) -> None:
    """A handler that always raises is retried up to max_attempts, then
    on_exhausted fires exactly once and the message is not retried further."""
    calls = 0
    exhausted: list[tuple] = []

    consumer = _consumer(
        backend,
        max_attempts=3,
        on_exhausted=lambda m, e: exhausted.append((m, e)),
    )

    @consumer.handle
    async def process(msg) -> None:
        nonlocal calls
        calls += 1
        raise RuntimeError("poison")

    broker = Broker()
    broker.send(Message(_k("poison")))

    final = None
    for _ in range(10):  # safety bound
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
        receive_count=None,  # force the AttemptStore path
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


# ----- queue-result-bearing-replay -----


async def test_result_bearing_replay(backend) -> None:
    """A result-bearing handler's return value is replayed on redelivery without
    re-executing the handler."""
    calls = 0
    consumer = _consumer(backend, result_bearing=True)

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


async def test_result_bearing_unserializable_fails_closed(backend) -> None:
    """A result-bearing handler whose return value isn't serializable must NOT
    cause the side effect to re-run: the result couldn't be cached, but the
    message is acked (not redelivered) and the handler runs exactly once (§5.4)."""
    calls = 0
    consumer = _consumer(backend, result_bearing=True, wait_timeout_seconds=0.3)

    @consumer.handle
    async def process(msg) -> object:
        nonlocal calls
        calls += 1
        return object()  # not JSON-serializable

    msg = Message(_k("unser"))
    msg.receive_count = 1
    first = await consumer.dispatch(msg)
    assert first.status is RunStatus.ENCODE_FAILED
    assert first.action is ConsumerAction.ACK  # acked, so the broker won't re-run

    # A redelivery (e.g. if the ack were lost) must NOT re-run the side effect:
    # the claim is held, so it conflicts rather than executing again.
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
    consumer = _consumer(
        backend, max_attempts=2, on_exhausted=lambda m, e: exhausted.append(e)
    )

    @consumer.handle
    async def process(msg) -> None:
        pass

    async def boom(*args, **kwargs):
        raise StorageError("idempotency backend down")

    backend.claim = boom  # type: ignore[method-assign]

    msg = Message(_k("outage"))
    for rc in (1, 2, 3, 4):  # more deliveries than max_attempts
        msg.receive_count = rc
        result = await consumer.dispatch(msg)
        assert result.action is ConsumerAction.RETRY

    assert exhausted == [], "a storage outage must not route the message to a DLQ"
