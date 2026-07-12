"""The shared (any-surface) examples: backend configuration and a custom backend."""

from __future__ import annotations

from unittest import mock

import pytest

from idemkit import InMemoryBackend
from tests.examples._examples import load_module


def test_backends_reference() -> None:
    mod = load_module("shared/backends.py")
    # in_memory() returns a usable backend; redis()/postgres() need a running server,
    # so they are shown but not called here.
    assert isinstance(mod.in_memory(), InMemoryBackend)


def test_backends_reference_builds_mongo_and_dynamo() -> None:
    # mongo()/dynamodb() construct lazily (no connection until first use), so we can
    # verify the example builds them when the drivers are installed.
    pytest.importorskip("pymongo")
    pytest.importorskip("aioboto3")
    from idemkit.backends.dynamodb import DynamoBackend
    from idemkit.backends.mongo import MongoBackend

    mod = load_module("shared/backends.py")
    assert isinstance(mod.mongo(), MongoBackend)
    assert isinstance(mod.dynamodb(), DynamoBackend)


async def test_custom_backend_dedupes() -> None:
    mod = load_module("shared/custom_backend.py")
    with mock.patch.object(mod, "do_work", wraps=mod.do_work) as spy:
        a = await mod.work(order_id="A1")
        b = await mod.work(order_id="A1")  # replayed through the custom backend
    assert a == b
    assert spy.call_count == 1  # side effect ran once
    assert mod.backend.claims == 2  # the custom backend saw both claims


async def test_observability_handlers_wire_up() -> None:
    import pytest

    pytest.importorskip("prometheus_client")
    mod = load_module("shared/observability.py")
    with mock.patch.object(mod, "charge_customer", wraps=mod.charge_customer) as spy:
        a = await mod.charge(order_id="A1")
        b = await mod.charge(order_id="A1")  # replayed; both ops go to the handlers
    assert a == b
    assert spy.call_count == 1  # the prometheus + logging handlers did not disrupt dedup
