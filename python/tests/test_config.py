"""IdempotencyConfig validation tests."""

from __future__ import annotations

import pytest

from idemkit.core.config import IdempotencyConfig
from idemkit.core.exceptions import ConfigurationError


def test_missing_caller_identity_runs_single_tenant_with_warning(caplog) -> None:
    # New default: no scope -> single-tenant + loud warning, NOT a raise.
    import logging

    with caplog.at_level(logging.WARNING):
        config = IdempotencyConfig()
    assert config.scope is None
    assert any("SINGLE-TENANT MODE" in r.message for r in caplog.records)


def test_strict_identity_hard_fails_without_caller_identity() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        IdempotencyConfig(scope_mode="strict")
    assert "scope" in str(exc_info.value)


def test_caller_identity_optional_silences_warning(caplog) -> None:
    import logging

    with caplog.at_level(logging.WARNING):
        config = IdempotencyConfig(scope_mode="single_tenant")
    assert config.scope is None
    assert not any("SINGLE-TENANT MODE" in r.message for r in caplog.records)


def test_explicit_caller_identity_works() -> None:
    config = IdempotencyConfig(scope=lambda req: "user-1")
    assert config.scope is not None


def test_methods_normalized_to_uppercase() -> None:
    config = IdempotencyConfig(
        scope_mode="single_tenant",
        applicable_methods={"post", "patch"},
    )
    assert config.applicable_methods == {"POST", "PATCH"}


def test_default_header_lists_safe() -> None:
    config = IdempotencyConfig(scope_mode="single_tenant")
    deny = config.effective_header_deny()
    assert "set-cookie" in deny
    assert "authorization" in deny
    assert "vary" in deny


def test_compat_mode_default_is_not_stripe() -> None:
    config = IdempotencyConfig(scope_mode="single_tenant")
    assert config.compat_mode == "default"
    assert config.is_stripe_compat is False


def test_compat_mode_stripe() -> None:
    config = IdempotencyConfig(scope_mode="single_tenant", compat_mode="stripe")
    assert config.is_stripe_compat is True


def test_invalid_compat_mode_raises() -> None:
    with pytest.raises(ConfigurationError):
        IdempotencyConfig(scope_mode="single_tenant", compat_mode="paypal")  # type: ignore[arg-type]


def test_default_cacheable_excludes_5xx() -> None:
    config = IdempotencyConfig(scope_mode="single_tenant")
    assert 500 not in config.cacheable_status
    assert 503 not in config.cacheable_status
    # 2xx default
    assert 200 in config.cacheable_status
    assert 201 in config.cacheable_status


def test_invalid_scope_mode_raises() -> None:
    with pytest.raises(ConfigurationError):
        IdempotencyConfig(scope_mode="lax")  # type: ignore[arg-type]
