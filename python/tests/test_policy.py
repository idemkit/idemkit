"""IdempotencyPolicy: a reusable core-policy config for queue and method surfaces.

A policy supplies the shared core-policy defaults (lease, wait, retention, storage
policy, local cache, events); an explicit keyword still overrides it per consumer
or decorator.
"""

from __future__ import annotations

import asyncio

from idemkit import IdempotencyPolicy, IdempotentConsumer, InMemoryBackend, idempotent


def _consumer(**kwargs):
    return IdempotentConsumer(
        backend=InMemoryBackend(),
        key=lambda m: m["id"],
        visibility_timeout_seconds=30,
        **kwargs,
    )


def test_policy_supplies_core_values() -> None:
    policy = IdempotencyPolicy(
        expires_after_seconds=123,
        wait_timeout_seconds=4,
        on_storage_error="fail_open",
        use_local_cache=True,
        local_cache_max_items=7,
    )
    c = _consumer(config=policy)
    assert c.core.expires_after_seconds == 123
    assert c.core.wait_timeout_seconds == 4
    assert c.core.on_storage_error == "fail_open"
    assert c.core.use_local_cache is True
    assert c.core.local_cache_max_items == 7


def test_explicit_keyword_overrides_policy() -> None:
    policy = IdempotencyPolicy(expires_after_seconds=123)
    c = _consumer(config=policy, expires_after_seconds=999)
    assert c.core.expires_after_seconds == 999  # explicit wins over the policy


def test_policy_leaves_lease_to_surface_default() -> None:
    # The policy does not pin lease_ttl_seconds, so the queue still derives it
    # from the visibility timeout (30 * 0.5).
    c = _consumer(config=IdempotencyPolicy(expires_after_seconds=123))
    assert c.lease_ttl_seconds == 15.0


def test_no_policy_uses_surface_defaults() -> None:
    c = _consumer()
    assert c.core.expires_after_seconds == 86_400.0
    assert c.core.wait_timeout_seconds == 5.0  # the queue surface default


def test_policy_reused_across_method_decorators() -> None:
    policy = IdempotencyPolicy(expires_after_seconds=3600, on_storage_error="fail_open")
    backend = InMemoryBackend()
    calls = {"a": 0, "b": 0}

    @idempotent(backend=backend, key_fields=["x"], scope=lambda a: "s1", config=policy)
    async def op_a(*, x):
        calls["a"] += 1
        return x

    @idempotent(backend=backend, key_fields=["x"], scope=lambda a: "s2", config=policy)
    async def op_b(*, x):
        calls["b"] += 1
        return x

    async def main():
        await op_a(x=1)
        await op_a(x=1)  # same args -> replay
        await op_b(x=2)
        await op_b(x=2)

    asyncio.run(main())
    assert calls == {"a": 1, "b": 1}  # one policy, both decorators dedupe


def test_policy_widens_onto_http_middleware() -> None:
    from idemkit import IdempotencyMiddleware

    policy = IdempotencyPolicy(expires_after_seconds=99, on_storage_error="fail_open")
    mw = IdempotencyMiddleware(
        lambda *a: None,
        backend=InMemoryBackend(),
        config=policy,
        scope=lambda req: "t",  # HTTP-specific keyword rides alongside the policy
    )
    assert mw.config.expires_after_seconds == 99            # from the policy
    assert mw.config.on_storage_error == "fail_open"        # from the policy
    assert mw.config.lease_ttl_seconds == 30.0              # HTTP default (policy left it None)
    assert mw.config.max_request_body_bytes == 1024 * 1024  # HTTP-specific default present
    assert mw.config.scope is not None                      # HTTP keyword applied
