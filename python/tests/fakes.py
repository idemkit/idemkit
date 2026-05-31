"""Test helper backends for failure-mode and corner-case tests against the engine.

These do not implement the full backend contract — they exist solely to
trigger engine code paths that real backends are designed not to enter.
"""

from __future__ import annotations

from idemkit.core.exceptions import StorageError
from idemkit.core.state import ClaimResult, StoredRecord


class FailingBackend:
    """Backend that raises ``StorageError`` on every operation.

    Used by tests that need to exercise the engine's ``on_storage_error``
    policy (spec §4.16) without bringing up a real backend.
    """

    async def claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        raise StorageError("FailingBackend: simulated outage")

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        completed_ttl_seconds: float,
    ) -> bool:
        raise StorageError("FailingBackend: simulated outage")

    async def release(self, effective_key: str, claim_token: str) -> bool:
        raise StorageError("FailingBackend: simulated outage")

    async def renew(
        self, effective_key: str, claim_token: str, lease_ttl_seconds: float
    ) -> bool:
        raise StorageError("FailingBackend: simulated outage")

    async def wait_for_completion(
        self, effective_key: str, timeout_seconds: float
    ) -> StoredRecord | None:
        raise StorageError("FailingBackend: simulated outage")
