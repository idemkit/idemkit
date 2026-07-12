"""Spec §4.9 response body size enforcement tests.

When a response exceeds ``max_body_bytes``, idemkit MUST:
- Deliver the full response to the client (streaming through).
- NOT cache it. A retry MUST re-execute (the claim is released).
"""

from __future__ import annotations

import httpx
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import PlainTextResponse, Response, StreamingResponse
from starlette.routing import Route

from idemkit import IdempotencyMiddleware, InMemoryBackend
from idemkit.core.config import IdempotencyConfig


async def test_response_exceeding_max_is_not_cached() -> None:
    """Large response → first client gets it, second request re-executes (no replay)."""
    call_counter = {"n": 0}

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = b"x" * 2048  # 2 KiB
        return PlainTextResponse(body, status_code=200)

    config = IdempotencyConfig(
        scope=lambda req: "test-user",
        max_body_bytes=1024,  # 1 KiB cap
    )
    app = Starlette(routes=[Route("/big", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/big", headers={"Idempotency-Key": "k-big"})
        r2 = await client.post("/big", headers={"Idempotency-Key": "k-big"})

    # First client receives the full 2 KiB response (streamed through)
    assert r1.status_code == 200
    assert len(r1.content) == 2048
    # Spec §4.9 / §6.2: the bypass reason MUST be announced to the client.
    assert r1.headers.get("idempotency-replay-unavailable") == "size-exceeded"

    # Second request was NOT replayed (no Idempotency-Replayed header)
    # and the handler ran again
    assert call_counter["n"] == 2
    assert r2.status_code == 200
    assert "idempotency-replayed" not in {k.lower() for k in r2.headers}


async def test_response_below_max_is_cached_normally() -> None:
    """Tiny response → cached normally, second request replays."""
    call_counter = {"n": 0}

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return PlainTextResponse(b"hello", status_code=200)

    config = IdempotencyConfig(
        scope=lambda req: "test-user",
        max_body_bytes=1024 * 1024,  # default 1 MiB
    )
    app = Starlette(routes=[Route("/tiny", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/tiny", headers={"Idempotency-Key": "k-tiny"})
        r2 = await client.post("/tiny", headers={"Idempotency-Key": "k-tiny"})

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert call_counter["n"] == 1
    assert r2.headers["idempotency-replayed"] == "true"


async def test_response_exactly_at_max_is_cached() -> None:
    """Boundary: exactly max_body_bytes should be cacheable (the cap is exclusive)."""
    call_counter = {"n": 0}

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        return PlainTextResponse(b"x" * 1024, status_code=200)

    config = IdempotencyConfig(
        scope=lambda req: "test-user",
        max_body_bytes=1024,
    )
    app = Starlette(routes=[Route("/exact", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/exact", headers={"Idempotency-Key": "k-exact"})
        r2 = await client.post("/exact", headers={"Idempotency-Key": "k-exact"})

    # At the boundary (size == max), engine releases (length > max is the cutoff).
    # This documents the exclusive boundary behavior.
    assert call_counter["n"] == 1
    assert r2.headers.get("idempotency-replayed") == "true"


async def test_oversized_request_body_bypasses_idempotency_but_succeeds() -> None:
    """A request body over max_request_body_bytes is streamed to the handler
    (bounded memory) WITHOUT idempotency — the handler still sees the full body,
    but duplicates are not deduplicated."""
    call_counter = {"n": 0}
    seen_sizes: list[int] = []

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1
        body = await request.body()
        seen_sizes.append(len(body))
        return PlainTextResponse("ok", status_code=200)

    config = IdempotencyConfig(
        scope=lambda req: "test-user",
        max_request_body_bytes=1024,  # cap request buffering at 1 KiB
    )
    app = Starlette(routes=[Route("/ingest", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)

    big = b"y" * 4096  # 4 KiB request body, over the cap
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/ingest", content=big, headers={"Idempotency-Key": "k-bigreq"})
        r2 = await client.post("/ingest", content=big, headers={"Idempotency-Key": "k-bigreq"})

    # Handler saw the entire body both times (nothing truncated)...
    assert seen_sizes == [4096, 4096]
    # ...and idempotency was bypassed, so both requests executed.
    assert r1.status_code == 200
    assert r2.status_code == 200
    assert call_counter["n"] == 2
    assert "idempotency-replayed" not in {k.lower() for k in r2.headers}


async def test_oversized_request_without_key_still_rejected_when_required() -> None:
    """The require_key_for_mutations gate MUST still apply to an oversized
    request, even though idempotency itself is bypassed."""

    async def handler(request: Request) -> Response:
        return PlainTextResponse("ok", status_code=200)

    config = IdempotencyConfig(
        scope=lambda req: "test-user",
        max_request_body_bytes=1024,
        require_key_for_mutations=True,
    )
    app = Starlette(routes=[Route("/ingest", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)

    big = b"y" * 4096
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.post("/ingest", content=big)  # no Idempotency-Key

    assert r.status_code == 400
    assert b"urn:idemkit:missing-key" in r.content


async def test_streaming_response_bypasses_cache_with_streaming_header() -> None:
    """An incremental (multi-chunk) response is streamed straight through,
    announced with Idempotency-Replay-Unavailable: streaming, and not cached —
    so it is never buffered and never blocks the client (spec §4.9)."""
    call_counter = {"n": 0}

    async def handler(request: Request) -> Response:
        call_counter["n"] += 1

        async def gen():
            yield b"chunk-1;"
            yield b"chunk-2;"

        return StreamingResponse(gen(), status_code=200, media_type="text/plain")

    config = IdempotencyConfig(scope=lambda req: "test-user")
    app = Starlette(routes=[Route("/stream", handler, methods=["POST"])])
    app.add_middleware(IdempotencyMiddleware, backend=InMemoryBackend(), config=config)

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r1 = await client.post("/stream", headers={"Idempotency-Key": "k-stream"})
        r2 = await client.post("/stream", headers={"Idempotency-Key": "k-stream"})

    # Full streamed body reached the client, with the bypass header.
    assert r1.status_code == 200
    assert r1.content == b"chunk-1;chunk-2;"
    assert r1.headers.get("idempotency-replay-unavailable") == "streaming"
    # Not cached: the second request re-executes, not a replay.
    assert call_counter["n"] == 2
    assert "idempotency-replayed" not in {k.lower() for k in r2.headers}
