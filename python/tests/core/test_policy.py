"""Flat per-surface config objects (HttpConfig / QueueConfig / MethodConfig).

Each surface takes one ``config=`` object; there are no loose keyword overrides. To
reuse settings across surfaces, write a factory that returns the config you want.
"""

from __future__ import annotations

import asyncio
import dataclasses

from idemkit import (
    HttpConfig,
    IdempotencyMiddleware,
    IdempotentConsumer,
    InMemoryBackend,
    MethodConfig,
    QueueConfig,
    idempotent,
)


def _consumer(config=None, **kwargs):
    # key/visibility live on QueueConfig now: fill them in on whatever config is given.
    cfg = config or QueueConfig()
    cfg = dataclasses.replace(cfg, dedup_id=lambda m: m["id"], visibility_timeout_seconds=30)
    return IdempotentConsumer(backend=InMemoryBackend(), config=cfg, **kwargs)


def test_queueconfig_supplies_all_options() -> None:
    cfg = QueueConfig(
        expires_after_seconds=123,
        wait_timeout_seconds=4,
        on_storage_error="fail_open",
        max_attempts=9,
        cache_result=True,
    )
    c = _consumer(config=cfg)
    assert c.core.expires_after_seconds == 123
    assert c.core.wait_timeout_seconds == 4
    assert c.core.on_storage_error == "fail_open"
    assert c.max_attempts == 9
    assert c._cache_result is True


def test_config_leaves_lease_to_surface_default() -> None:
    # lease_ttl_seconds unset -> the queue derives it from the visibility timeout.
    c = _consumer(config=QueueConfig(expires_after_seconds=123))
    assert c.lease_ttl_seconds == 15.0


def test_no_config_uses_surface_defaults() -> None:
    c = _consumer()
    assert c.core.expires_after_seconds == 86_400.0
    assert c.core.wait_timeout_seconds == 5.0  # queue surface default
    assert c.max_attempts == 5


def test_methodconfig_supplies_key_fields_and_scope() -> None:
    backend = InMemoryBackend()
    calls = {"n": 0}
    cfg = MethodConfig(key_fields=["x"], scope=lambda a: "s", expires_after_seconds=3600)

    @idempotent(backend=backend, config=cfg)
    async def op(*, x):
        calls["n"] += 1
        return x

    async def main():
        await op(x=1)
        await op(x=1)  # same args -> replay (key_fields came from the config)

    asyncio.run(main())
    assert calls["n"] == 1


def test_reuse_across_surfaces_via_factory() -> None:
    # No shared object to learn: a factory reuses the house settings.
    shared = {"expires_after_seconds": 3600, "on_storage_error": "fail_open"}
    c = _consumer(config=QueueConfig(**shared))
    assert c.core.expires_after_seconds == 3600
    mw = IdempotencyMiddleware(
        lambda *a: None,
        backend=InMemoryBackend(),
        config=HttpConfig(**shared, scope=lambda req: "t"),
    )
    assert mw.config.expires_after_seconds == 3600
    assert mw.config.on_storage_error == "fail_open"


def test_httpconfig_widens_onto_middleware() -> None:
    cfg = HttpConfig(expires_after_seconds=99, on_storage_error="fail_open", scope=lambda req: "t")
    mw = IdempotencyMiddleware(lambda *a: None, backend=InMemoryBackend(), config=cfg)
    assert mw.config.expires_after_seconds == 99
    assert mw.config.on_storage_error == "fail_open"
    assert mw.config.lease_ttl_seconds == 30.0  # HTTP default (config left it None)
    assert mw.config.max_request_body_bytes == 1024 * 1024  # HTTP default present
    assert mw.config.scope is not None
