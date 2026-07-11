"""Backend Protocol per spec §4.1.

Backend implementations included in v0.1: ``InMemoryBackend`` (dev/test only),
``RedisBackend``, ``PostgresBackend``.

All methods are async. Implementations MUST satisfy spec §4.1 atomicity:
``claim`` is a single-shot atomic insert; ``complete`` and ``release`` are
conditional state transitions that check ``claim_token`` to detect cases
where another process reclaimed an expired lease.
"""

from __future__ import annotations

from typing import Protocol

from idemkit.core.state import ClaimResult, StoredRecord


class IdempotencyBackend(Protocol):
    """The single storage interface idemkit uses.

    Anyone can write a new backend by implementing this Protocol. See
    ``InMemoryBackend`` for the reference shape.
    """

    async def claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        """Attempt to atomically claim ``effective_key``.

        Outcomes (spec §4.1):

        * ``NEW_CLAIMED`` — the record is now in storage with our claim_token.
        * ``LEASE_RECLAIMED`` — an existing CLAIMED record had an expired
          lease and we successfully reclaimed it; treated as NEW by the
          caller, but emits a different observability event.
        * ``ALREADY_CLAIMED`` — another process holds a live claim.
        * ``ALREADY_COMPLETED`` — a COMPLETED record exists.

        On the NEW / RECLAIMED paths, ``ClaimResult.our_claim_token`` is the
        value to present in subsequent ``complete`` / ``release`` calls.
        """
        ...

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        expires_after_seconds: float,
    ) -> bool:
        """Conditional transition CLAIMED → COMPLETED, spec §4.1.

        Returns ``True`` if the transition was applied. Returns ``False`` if
        the claim was reclaimed by another process while the handler was
        running — the caller MUST then emit ``idempotency.lease_reclaimed_loss``
        and discard the response from this execution.
        """
        ...

    async def release(self, effective_key: str, claim_token: str) -> bool:
        """Conditional release of a CLAIMED record, spec §4.1.

        Returns ``True`` if released. Returns ``False`` if our claim was
        already reclaimed by another process.
        """
        ...

    async def renew(
        self,
        effective_key: str,
        claim_token: str,
        lease_ttl_seconds: float,
    ) -> bool:
        """Extend a live claim's lease, spec §5.3.1 (lease renewal / heartbeat).

        Conditional on ownership, exactly like ``complete`` / ``release``: the
        lease is pushed out to ``now + lease_ttl_seconds`` (by the storage
        clock) ONLY if the record is still ``CLAIMED`` and ``claim_token``
        matches. Returns ``True`` on success.

        Returns ``False`` when the renewal could not be applied — the record is
        gone, already ``COMPLETED``, or was reclaimed by another executor (a new
        ``claim_token``). A ``False`` is the fencing signal a heartbeat uses to
        stop: it means this executor no longer holds the claim, so it MUST NOT
        keep running as if it did.

        This is what lets a long handler keep a SHORT lease: the lease tracks
        actual progress instead of a guessed upper bound, so a crash lapses the
        lease promptly while a slow-but-alive handler holds on. See §5.3.1 for
        why renewal is required off-Lambda (no hard deadline to derive from).
        """
        ...

    async def wait_for_completion(
        self,
        effective_key: str,
        timeout_seconds: float,
    ) -> StoredRecord | None:
        """Wait for an in-flight claim to complete, spec §4.3.

        MUST implement subscribe-before-read ordering: register for
        notifications BEFORE re-reading state, to avoid lost wakeups when
        the publisher fires between read and subscribe.

        Returns the COMPLETED record if it arrived within ``timeout_seconds``.
        Returns ``None`` on timeout, on release without completion, or when
        the in-flight claim's lease expired before completion.
        """
        ...
