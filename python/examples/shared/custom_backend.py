"""Store idempotency state in ANY datastore: the backend is a Protocol.

idemkit ships InMemory, Redis, Postgres, MongoDB, and DynamoDB backends, but the
backend is a Protocol (not a base class), so you can back it with etcd, a SQL
table of your own, Cassandra, anything: implement these five methods and pass the
object to any surface. Validate it against the shared vectors with
`idemkit conformance`. Here we wrap InMemoryBackend and count calls to show the
shape without a real external store.

Run:  python examples/shared/custom_backend.py
"""

from typing import Any

from idemkit import InMemoryBackend, MethodConfig, idempotent
from idemkit.core.state import ClaimResult, StoredRecord


class CountingBackend:
    """The five methods a backend must implement (their exact signatures shown).

    This one delegates to InMemoryBackend and counts claims; a real one would talk
    to DynamoDB, etcd, your SQL table, etc. Atomic claim and fenced complete are the
    parts that must be correct: check yours against the vectors with
    `idemkit conformance`.
    """

    def __init__(self, inner: Any) -> None:
        self._inner = inner
        self.claims = 0

    async def claim(
        self,
        effective_key: str,
        fingerprint: str,
        fingerprint_version: int,
        lease_ttl_seconds: float,
    ) -> ClaimResult:
        self.claims += 1
        return await self._inner.claim(
            effective_key, fingerprint, fingerprint_version, lease_ttl_seconds
        )

    async def complete(
        self,
        effective_key: str,
        claim_token: str,
        response_status: int,
        response_headers: dict[str, str],
        response_body: bytes,
        expires_after_seconds: float,
    ) -> bool:
        return await self._inner.complete(
            effective_key,
            claim_token,
            response_status,
            response_headers,
            response_body,
            expires_after_seconds,
        )

    async def release(self, effective_key: str, claim_token: str) -> bool:
        return await self._inner.release(effective_key, claim_token)

    async def renew(self, effective_key: str, claim_token: str, lease_ttl_seconds: float) -> bool:
        return await self._inner.renew(effective_key, claim_token, lease_ttl_seconds)

    async def wait_for_completion(
        self, effective_key: str, timeout_seconds: float
    ) -> StoredRecord | None:
        return await self._inner.wait_for_completion(effective_key, timeout_seconds)


backend = CountingBackend(InMemoryBackend())


def do_work(order_id: str) -> dict:
    return {"order_id": order_id}


@idempotent(
    backend=backend, config=MethodConfig(key_fields=["order_id"], scope=lambda a: "tenant-1")
)
async def work(*, order_id: str) -> dict:
    return do_work(order_id)


if __name__ == "__main__":
    import asyncio

    async def _demo() -> None:
        await work(order_id="A1")
        await work(order_id="A1")  # a duplicate: replayed through the custom backend
        print(f"the custom backend saw {backend.claims} claims")  # 2

    asyncio.run(_demo())
