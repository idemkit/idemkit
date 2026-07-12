"""Every queue example handles an at-least-once message once across redeliveries.

Each example exposes a `consumer`; here we import it, dispatch the same message
twice (a redelivery), spy on the side-effect function, and assert it ran once.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from unittest import mock

from idemkit import ConsumerAction
from tests.examples._examples import load_module


async def test_getting_started() -> None:
    mod = load_module("queue/getting_started.py")
    msg = SimpleNamespace(message_id=f"m-{uuid.uuid4()}")  # unique per run
    with mock.patch.object(mod, "charge_customer", wraps=mod.charge_customer) as spy:
        r1 = await mod.consumer.dispatch(msg)
        await mod.consumer.dispatch(msg)  # redelivery
    assert r1.action is ConsumerAction.ACK
    assert spy.call_count == 1


def test_sqs() -> None:
    mod = load_module("queue/sqs.py")
    sqs_msg = {
        "MessageId": "sqs-1",
        "ReceiptHandle": "rh-1",
        "Body": "charge:42",
        "Attributes": {"ApproximateReceiveCount": "1"},
    }
    with mock.patch.object(mod, "charge_customer", wraps=mod.charge_customer) as spy:
        mod.consumer.dispatch_sync(sqs_msg)
        mod.consumer.dispatch_sync(sqs_msg)  # redelivery
    assert spy.call_count == 1


def test_kafka() -> None:
    mod = load_module("queue/kafka.py")
    record = SimpleNamespace(topic="charges", partition=0, offset=101, value=b"charge:42")
    with mock.patch.object(mod, "process_record", wraps=mod.process_record) as spy:
        mod.consumer.dispatch_sync(record)
        mod.consumer.dispatch_sync(record)  # re-consumed offset
    assert spy.call_count == 1


async def test_dead_letter() -> None:
    mod = load_module("queue/dead_letter.py")
    msg = SimpleNamespace(message_id="poison-1")
    attempts = 0
    exhausted = False
    for _ in range(6):
        attempts += 1
        result = await mod.consumer.dispatch(msg)
        if result.exhausted:  # on_exhausted (the DLQ handoff) fired here
            exhausted = True
            break
    assert exhausted  # gave up after max_attempts instead of retrying forever
    assert attempts == 3  # max_attempts


async def test_cache_result() -> None:
    mod = load_module("queue/cache_result.py")
    msg = SimpleNamespace(message_id="job-7")
    with mock.patch.object(mod, "enrich", wraps=mod.enrich) as spy:
        first = await mod.consumer.dispatch(msg)
        again = await mod.consumer.dispatch(msg)  # redelivery replays the cached value
    assert first.result == again.result
    assert spy.call_count == 1


async def test_payload_validation() -> None:
    from idemkit import PayloadMismatch

    mod = load_module("queue/payload_validation.py")
    with mock.patch.object(mod, "charge_customer", wraps=mod.charge_customer) as spy:
        await mod.consumer.dispatch(SimpleNamespace(message_id="m1", body=b"amount=100"))
        await mod.consumer.dispatch(SimpleNamespace(message_id="m1", body=b"amount=100"))  # replay
        mismatch = await mod.consumer.dispatch(
            SimpleNamespace(message_id="m1", body=b"amount=999")  # reused id, different body
        )
    assert spy.call_count == 1  # only the consistent body ran
    assert mismatch.action is ConsumerAction.ACK  # not retried forever
    assert mismatch.exhausted is True
    assert isinstance(mismatch.error, PayloadMismatch)


async def test_all_options() -> None:
    mod = load_module("queue/all_options.py")
    msg = SimpleNamespace(message_id="m-1", queue="charges", receive_count=1)
    with mock.patch.object(mod, "charge_customer", wraps=mod.charge_customer) as spy:
        await mod.consumer.dispatch(msg)
        await mod.consumer.dispatch(msg)
    assert spy.call_count == 1
    assert len(mod.events) >= 1
