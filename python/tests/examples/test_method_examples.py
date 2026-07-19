"""Every method / AI-tool example dedupes.

The examples are clean, copy-paste snippets with no test scaffolding. Each calls a
realistic side-effect function; here we import the example, spy on that function,
drive the decorated handler, and assert the side effect ran exactly once (dedup)
and the duplicate replayed the first result.
"""

from __future__ import annotations

import uuid
from unittest import mock

import pytest

from tests.examples._examples import load_module


async def test_getting_started() -> None:
    mod = load_module("method/getting_started.py")
    customer = f"acme-{uuid.uuid4().hex[:8]}"  # unique per run
    with mock.patch.object(mod, "create_invoice", wraps=mod.create_invoice) as spy:
        a = await mod.charge_subscription(customer=customer, plan="pro", period="2026-07")
        b = await mod.charge_subscription(customer=customer, plan="pro", period="2026-07")
    assert a == b  # duplicate replayed
    assert spy.call_count == 1  # side effect ran once


def test_cron_run_once() -> None:
    mod = load_module("method/cron_run_once.py")
    day = f"2026-07-{uuid.uuid4().hex[:6]}"  # unique per run (persistent backends)
    with mock.patch.object(mod, "deliver_digest", wraps=mod.deliver_digest) as spy:
        a = mod.send_daily_digest(run_date=day)
        b = mod.send_daily_digest(run_date=day)  # a second scheduler firing the same slot
    assert a == b  # replayed
    assert spy.call_count == 1  # the digest went out once


def test_sync_function() -> None:
    from idemkit import PayloadMismatch

    mod = load_module("method/sync_function.py")
    with mock.patch.object(mod, "issue_refund", wraps=mod.issue_refund) as spy:
        a = mod.refund(order_id="A1", amount=50)
        b = mod.refund(order_id="A1", amount=50)
    assert a == b
    assert spy.call_count == 1
    with pytest.raises(PayloadMismatch):  # same key, different amount
        mod.refund(order_id="A1", amount=999)


async def test_error_replay() -> None:
    mod = load_module("method/error_replay.py")
    # A successful charge replays its result on a duplicate.
    ok1 = await mod.charge(order_id="ok-1", amount=50)
    ok2 = await mod.charge(order_id="ok-1", amount=50)
    assert ok1 == ok2
    # A deterministic failure re-raises the SAME type on a duplicate (cached, not re-run).
    with pytest.raises(mod.InsufficientFunds):
        await mod.charge(order_id="bad-1", amount=999)
    with pytest.raises(mod.InsufficientFunds):
        await mod.charge(order_id="bad-1", amount=999)


async def test_payload_validation() -> None:
    from idemkit import PayloadMismatch

    mod = load_module("method/payload_validation.py")
    with mock.patch.object(mod, "do_refund", wraps=mod.do_refund) as spy:
        a = await mod.refund(order_id="A-1", amount=50)
        b = await mod.refund(order_id="A-1", amount=50)  # same key + amount: replayed
    assert a == b
    assert spy.call_count == 1
    with pytest.raises(PayloadMismatch):  # same key, different amount
        await mod.refund(order_id="A-1", amount=999)


async def test_agent_loop() -> None:
    mod = load_module("method/agent_loop.py")
    with mock.patch.object(mod, "book_flight_seat", wraps=mod.book_flight_seat) as spy:
        a = await mod.book_flight(origin="SFO", destination="JFK", date="2026-08-01")
        b = await mod.book_flight(origin="SFO", destination="JFK", date="2026-08-01")
    assert a == b
    assert spy.call_count == 1


def test_mcp() -> None:
    from idemkit.contrib.mcp import read_idempotent_hint

    mod = load_module("method/mcp.py")
    with mock.patch.object(mod, "do_refund", wraps=mod.do_refund) as spy:
        a = mod.refund(order_id="A123", amount=50)
        b = mod.refund(order_id="A123", amount=50)
    assert a == b
    assert spy.call_count == 1
    assert read_idempotent_hint(mod.refund) is True  # the hint is stamped and enforced


async def test_record_expiration() -> None:
    mod = load_module("method/record_expiration.py")
    with mock.patch.object(mod, "process_order", wraps=mod.process_order) as spy:
        await mod.process(order_id="A1")
        await mod.process(order_id="A1")  # within the hour: replayed
        assert spy.call_count == 1
        mod.clock.advance(3601)  # past expires_after_seconds
        await mod.process(order_id="A1")  # expired: runs again
        assert spy.call_count == 2


async def test_local_cache() -> None:
    mod = load_module("method/local_cache.py")
    with mock.patch.object(mod, "charge_card", wraps=mod.charge_card) as spy:
        await mod.charge(order_id="A1")
        await mod.charge(order_id="A1")  # replayed from the in-process cache
    assert spy.call_count == 1


async def test_exceptions() -> None:
    from idemkit import IdempotencyKeyMissing

    mod = load_module("method/exceptions.py")
    # require_key=True + no key strategy -> refused before running
    with pytest.raises(IdempotencyKeyMissing):
        await mod.charge(amount=100)
    # with an explicit key it runs, and a duplicate replays
    a = await mod.charge(amount=100, idempotency_key="c1")
    b = await mod.charge(amount=100, idempotency_key="c1")
    assert a == b
    await mod.main()  # the try/except demo runs without raising


async def test_result_codecs() -> None:
    mod = load_module("method/result_codecs.py")
    with mock.patch.object(mod, "reserve", wraps=mod.reserve) as spy:
        a = await mod.book(ref="R1")
        b = await mod.book(ref="R1")  # replayed, rebuilt as a Booking (not a dict)
    assert isinstance(b, mod.Booking) and a == b
    assert spy.call_count == 1


async def test_all_options() -> None:
    mod = load_module("method/all_options.py")
    with mock.patch.object(mod, "issue_refund", wraps=mod.issue_refund) as spy:
        await mod.refund(order_id="A1", amount=50)
        await mod.refund(order_id="A1", amount=50)
    assert spy.call_count == 1
    assert len(mod.events) >= 1  # event_handlers received structured events
