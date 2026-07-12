"""Nested key_fields: dotted paths into nested dicts and objects.

`key_fields=["order.id", "customer.email"]` reaches into nested structures without
a normalize_args callable. Plain dots, no query language; works on Mapping keys
and object attributes alike.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from idemkit import InMemoryBackend, MethodConfig, idempotent
from idemkit.core.exceptions import ConfigurationError


def test_nested_dict_paths_dedupe_on_selected_fields() -> None:
    calls = {"n": 0}

    @idempotent(
        backend=InMemoryBackend(),
        config=MethodConfig(key_fields=["order.number", "customer.email"], scope=lambda a: "s"),
    )
    async def process(*, order, customer):
        calls["n"] += 1
        return "ok"

    async def main():
        await process(order={"number": 5, "note": "a"}, customer={"email": "x@y.com", "name": "A"})
        await process(order={"number": 5, "note": "Z"}, customer={"email": "x@y.com", "name": "B"})
        await process(order={"number": 6, "note": "a"}, customer={"email": "x@y.com", "name": "A"})

    asyncio.run(main())
    assert calls["n"] == 2


def test_nested_object_attribute_path() -> None:
    calls = {"n": 0}

    @idempotent(
        backend=InMemoryBackend(),
        config=MethodConfig(key_fields=["req.order_id"], scope=lambda a: "s", strict_keys=False),
    )
    async def handle(*, req):
        calls["n"] += 1
        return "ok"

    async def main():
        await handle(req=SimpleNamespace(order_id="A1", ts=1))
        await handle(req=SimpleNamespace(order_id="A1", ts=999))

    asyncio.run(main())
    assert calls["n"] == 1


def test_unknown_nested_root_raises_at_decoration() -> None:
    with pytest.raises(ConfigurationError):

        @idempotent(
            backend=InMemoryBackend(),
            config=MethodConfig(key_fields=["missing.id"], scope=lambda a: "s"),
        )
        async def f(*, order):
            return None


def test_missing_nested_segment_is_stable_none() -> None:
    calls = {"n": 0}

    @idempotent(
        backend=InMemoryBackend(),
        config=MethodConfig(key_fields=["order.number"], scope=lambda a: "s"),
    )
    async def process(*, order):
        calls["n"] += 1
        return "ok"

    async def main():
        await process(order={"note": "a"})
        await process(order={"note": "b"})

    asyncio.run(main())
    assert calls["n"] == 1
