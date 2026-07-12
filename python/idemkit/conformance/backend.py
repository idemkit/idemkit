"""Shared-core conformance vectors for the backend Protocol (spec §9).

These are the language-neutral correctness checks every ``IdempotencyBackend``
must pass — atomic claim, fencing, lease reclaim, in-flight wait, TTL expiry, and
lease renewal — packaged so ANY implementation can point them at itself, not just
idemkit's own three backends. That portability is the whole point of §11.1: a
public correctness suite the field can reference, rather than a private test file.

Usage from a third-party library whose backend implements the Protocol::

    from idemkit.conformance import BackendConformance

    report = await BackendConformance(MyBackend(...)).run()
    assert report.passed, report.report()

The vectors use a unique random key each, so they are safe to run repeatedly
against a shared, persistent backend (e.g. one PostgreSQL database) without
cross-run interference. They exercise only the public Protocol — backend-specific
behavior like corrupt-record recovery is tested inside idemkit itself, since it
needs internals a generic suite can't reach.
"""

from __future__ import annotations

import asyncio
import uuid
from collections import Counter
from collections.abc import Awaitable, Callable

from idemkit.backends.base import IdempotencyBackend
from idemkit.conformance.report import ConformanceReport, VectorResult
from idemkit.core.state import ClaimResultType, State


class BackendConformance:
    """Runs the shared-core vectors against one backend instance."""

    def __init__(self, backend: IdempotencyBackend, *, supports_renew: bool = True) -> None:
        self.backend = backend
        self.supports_renew = supports_renew

    async def run(self) -> ConformanceReport:
        """Run every applicable vector and collect the results."""
        report = ConformanceReport(backend_name=type(self.backend).__name__)
        for vector_id, description, fn in self._vectors():
            try:
                await fn()
                report.results.append(VectorResult(vector_id, True, description))
            except AssertionError as e:
                report.results.append(
                    VectorResult(vector_id, False, description, error=f"assertion: {e}")
                )
            except Exception as e:
                # A vector that errors (not just asserts) is still a failure, not
                # a crash of the whole run — record it and keep going.
                report.results.append(
                    VectorResult(vector_id, False, description, error=f"{type(e).__name__}: {e}")
                )
        return report

    def _vectors(self) -> list[tuple[str, str, Callable[[], Awaitable[None]]]]:
        vectors: list[tuple[str, str, Callable[[], Awaitable[None]]]] = [
            ("atomic-claim", "a fresh key claims NEW with a token", self._atomic_claim),
            (
                "duplicate-claim",
                "a second claim on a live key returns ALREADY_CLAIMED",
                self._duplicate_claim,
            ),
            (
                "conditional-complete",
                "complete transitions CLAIMED -> COMPLETED and replays",
                self._conditional_complete,
            ),
            (
                "fencing-wrong-token",
                "complete with a wrong token is rejected",
                self._fencing_wrong_token,
            ),
            (
                "release-clears-claim",
                "release with the right token frees the key",
                self._release_clears,
            ),
            (
                "lease-reclaim",
                "an expired lease is reclaimable with a new token",
                self._lease_reclaim,
            ),
            (
                "fenced-after-reclaim",
                "a reclaimed owner's late completion is rejected",
                self._fenced_after_reclaim,
            ),
            (
                "concurrent-claim-exactly-one",
                "N concurrent claims yield exactly one NEW",
                self._concurrent_claim,
            ),
            (
                "in-flight-wait",
                "a waiter receives the completed record",
                self._in_flight_wait,
            ),
            (
                "wait-timeout",
                "a waiter times out when the claim never completes",
                self._wait_timeout,
            ),
            (
                "completed-ttl-expiry",
                "a COMPLETED record past its TTL re-claims fresh",
                self._completed_ttl_expiry,
            ),
        ]
        if self.supports_renew:
            vectors.extend(
                [
                    (
                        "renew-extends-lease",
                        "renew pushes out a live lease so it isn't reclaimed",
                        self._renew_extends,
                    ),
                    (
                        "renew-fenced",
                        "renew with a wrong token, or after reclaim, is rejected",
                        self._renew_fenced,
                    ),
                ]
            )
        return vectors

    # ----- the vectors -----

    async def _atomic_claim(self) -> None:
        key = _k("atomic")
        result = await self.backend.claim(key, "fp", 1, 30.0)
        assert result.result == ClaimResultType.NEW_CLAIMED
        assert result.our_claim_token is not None
        assert result.record.state == State.CLAIMED

    async def _duplicate_claim(self) -> None:
        key = _k("dup")
        first = await self.backend.claim(key, "fp", 1, 30.0)
        second = await self.backend.claim(key, "fp", 1, 30.0)
        assert second.result == ClaimResultType.ALREADY_CLAIMED
        assert second.record.claim_token == first.our_claim_token

    async def _conditional_complete(self) -> None:
        key = _k("complete")
        first = await self.backend.claim(key, "fp", 1, 30.0)
        token = _token(first)
        ok = await self.backend.complete(key, token, 201, {"h": "v"}, b"body", 3600.0)
        assert ok is True
        after = await self.backend.claim(key, "fp", 1, 30.0)
        assert after.result == ClaimResultType.ALREADY_COMPLETED
        assert after.record.response_status == 201
        assert after.record.response_body == b"body"

    async def _fencing_wrong_token(self) -> None:
        key = _k("fence")
        await self.backend.claim(key, "fp", 1, 30.0)
        ok = await self.backend.complete(key, "not-the-token", 200, {}, b"x", 3600.0)
        assert ok is False

    async def _release_clears(self) -> None:
        key = _k("release")
        first = await self.backend.claim(key, "fp", 1, 30.0)
        assert await self.backend.release(key, _token(first)) is True
        after = await self.backend.claim(key, "fp", 1, 30.0)
        assert after.result == ClaimResultType.NEW_CLAIMED

    async def _lease_reclaim(self) -> None:
        key = _k("reclaim")
        first = await self.backend.claim(key, "fp", 1, 0.1)
        await asyncio.sleep(0.25)
        second = await self.backend.claim(key, "fp", 1, 30.0)
        assert second.result == ClaimResultType.LEASE_RECLAIMED
        assert second.our_claim_token != first.our_claim_token

    async def _fenced_after_reclaim(self) -> None:
        key = _k("fenced-reclaim")
        first = await self.backend.claim(key, "fp", 1, 0.1)
        await asyncio.sleep(0.25)
        second = await self.backend.claim(key, "fp", 1, 30.0)
        assert second.result == ClaimResultType.LEASE_RECLAIMED
        ok = await self.backend.complete(key, _token(first), 200, {}, b"x", 3600.0)
        assert ok is False
        after = await self.backend.claim(key, "fp", 1, 30.0)
        assert after.record.claim_token == second.our_claim_token

    async def _concurrent_claim(self) -> None:
        key = _k("concurrent")
        parallel = 25
        results = await asyncio.gather(
            *(self.backend.claim(key, "fp", 1, 30.0) for _ in range(parallel))
        )
        outcomes = Counter(r.result for r in results)
        assert outcomes[ClaimResultType.NEW_CLAIMED] == 1
        assert outcomes[ClaimResultType.ALREADY_CLAIMED] == parallel - 1

    async def _in_flight_wait(self) -> None:
        key = _k("wait")
        first = await self.backend.claim(key, "fp", 1, 30.0)
        token = _token(first)

        async def completer() -> None:
            await asyncio.sleep(0.1)
            await self.backend.complete(key, token, 200, {}, b"hi", 3600.0)

        waited, _ = await asyncio.gather(
            self.backend.wait_for_completion(key, timeout_seconds=2.0), completer()
        )
        assert waited is not None
        assert waited.state == State.COMPLETED
        assert waited.response_body == b"hi"

    async def _wait_timeout(self) -> None:
        key = _k("wait-timeout")
        await self.backend.claim(key, "fp", 1, 30.0)
        result = await self.backend.wait_for_completion(key, timeout_seconds=0.3)
        assert result is None

    async def _completed_ttl_expiry(self) -> None:
        key = _k("ttl")
        first = await self.backend.claim(key, "fp", 1, 30.0)
        await self.backend.complete(key, _token(first), 200, {}, b"x", 0.1)
        immediate = await self.backend.claim(key, "fp", 1, 30.0)
        assert immediate.result == ClaimResultType.ALREADY_COMPLETED
        await asyncio.sleep(0.4)
        after = await self.backend.claim(key, "fp", 1, 30.0)
        assert after.result == ClaimResultType.NEW_CLAIMED

    async def _renew_extends(self) -> None:
        key = _k("renew")
        first = await self.backend.claim(key, "fp", 1, 0.2)
        assert await self.backend.renew(key, _token(first), 30.0) is True
        await asyncio.sleep(0.4)
        second = await self.backend.claim(key, "fp", 1, 30.0)
        assert second.result == ClaimResultType.ALREADY_CLAIMED

    async def _renew_fenced(self) -> None:
        key = _k("renew-fenced")
        await self.backend.claim(key, "fp", 1, 30.0)
        assert await self.backend.renew(key, "not-the-token", 30.0) is False


def _token(result: object) -> str:
    """The claim token from a NEW/RECLAIMED claim, asserted present."""
    token = getattr(result, "our_claim_token", None)
    assert token is not None, "a NEW/RECLAIMED claim must carry a claim token"
    return str(token)


def _k(suffix: str) -> str:
    return f"conformance-{suffix}-{uuid.uuid4().hex}"
