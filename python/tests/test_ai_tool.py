"""AI / LLM tool-call conformance — the spec §8.4 vectors, on every backend.

A side-effect counter stands in for a tool that charges a card or books a flight:
it must run once per (tool, version, selected-args, caller), no matter how often
an agent re-emits the call. The six required vectors (§8.4) run against InMemory,
real Redis, and real PostgreSQL.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from dataclasses import dataclass

import pytest

fakeredis_aio = pytest.importorskip("fakeredis.aioredis")

from idemkit.adapters.ai import (  # noqa: E402
    AI_FINGERPRINT,
    idempotent,
    idempotent_sync,
)
from idemkit.backends.memory import InMemoryBackend  # noqa: E402
from idemkit.backends.postgres import PostgresBackend, init_pg  # noqa: E402
from idemkit.backends.redis import RedisBackend  # noqa: E402
from idemkit.core.exceptions import (  # noqa: E402
    ConfigurationError,
    IdempotencyConflict,
    PayloadMismatch,
)


async def test_validation_fingerprint_catches_payload_mismatch(backend) -> None:
    """validation_fingerprint must MATCH on a key hit but isn't part of the key: a
    reused key whose fingerprint differs is a PayloadMismatch, not a wrong replay
    (spec §5.1)."""
    calls = 0

    oid = uuid.uuid4().hex  # unique key so a persistent backend doesn't collide across runs

    @idempotent(
        backend=backend,
        key_fields=["order_id"],
        validation_fingerprint=lambda a: str(a["amount"]).encode(),
        scope=lambda a: "s",
    )
    async def charge(*, order_id, amount):
        nonlocal calls
        calls += 1
        return {"charged": amount}

    await charge(order_id=oid, amount=100)
    # Same key (order_id), same amount -> replay.
    assert await charge(order_id=oid, amount=100) == {"charged": 100}
    assert calls == 1
    # Same key, DIFFERENT amount -> mismatch (amount isn't in the key, but must match).
    with pytest.raises(PayloadMismatch):
        await charge(order_id=oid, amount=200)

PG_URL = os.environ.get("IDEMKIT_TEST_PG_URL")
REDIS_URL = os.environ.get("IDEMKIT_TEST_REDIS_URL")


@pytest.fixture(scope="module", autouse=True)
async def _pg_schema():
    if PG_URL:
        try:
            await init_pg(PG_URL)
        except Exception:
            pass
    yield


@pytest.fixture(params=["memory", "redis", "postgres"])
async def backend(request):
    if request.param == "memory":
        yield InMemoryBackend()
    elif request.param == "redis":
        if REDIS_URL:
            import redis.asyncio as aioredis

            client = aioredis.from_url(REDIS_URL, decode_responses=False)
            await client.flushdb()
        else:
            client = fakeredis_aio.FakeRedis(decode_responses=False)
        b = RedisBackend(client)
        try:
            yield b
        finally:
            await b.aclose()
    elif request.param == "postgres":
        if not PG_URL:
            pytest.skip("set IDEMKIT_TEST_PG_URL to enable PostgreSQL contract tests")
        b = PostgresBackend.from_url(PG_URL, min_size=2, max_size=8)
        try:
            yield b
        finally:
            await b.aclose()


def _sid() -> str:
    return f"sess-{uuid.uuid4().hex}"


@dataclass
class Booking:
    # Module-level so the dataclass codec can resolve the return annotation
    # via typing.get_type_hints (a local class wouldn't be looked up).
    ref: str
    seats: int


# ----- tool-retry-dedup + tool-arg-normalization -----


async def test_retry_dedup_and_arg_normalization(backend) -> None:
    """Same tool + same selected args called N times -> side effect once, later
    calls return the cached result. A field that is NOT a key field (request_id)
    doesn't affect dedup; a field that IS does."""
    calls = 0
    session = _sid()

    @idempotent(
        backend=backend,
        key_fields=["origin", "destination"],
        scope=lambda a: a["session"],
    )
    async def book_flight(*, origin, destination, session, request_id=None):
        nonlocal calls
        calls += 1
        return {"booking": f"{origin}-{destination}"}

    r1 = await book_flight(
        origin="SFO", destination="JFK", session=session, request_id="req-1"
    )
    # Differs only in request_id (not a key field) -> dedupes to the first call.
    r2 = await book_flight(
        origin="SFO", destination="JFK", session=session, request_id="req-2"
    )
    assert r1 == r2 == {"booking": "SFO-JFK"}
    assert calls == 1, "the side effect must run once for identical key fields"

    # Differs in a key field (destination) -> genuinely a different call.
    r3 = await book_flight(origin="SFO", destination="LAX", session=session)
    assert r3 == {"booking": "SFO-LAX"}
    assert calls == 2


async def test_scope_isolates_callers(backend) -> None:
    """Same args from a different scope do not collide."""
    calls = 0

    @idempotent(
        backend=backend,
        key_fields=["x"],
        scope=lambda a: a["session"],
    )
    async def tool(*, x, session):
        nonlocal calls
        calls += 1
        return {"r": x, "by": session}

    # Unique per run so a persistent backend (Postgres) doesn't see a stale key.
    sess_a, sess_b = f"A-{uuid.uuid4().hex}", f"B-{uuid.uuid4().hex}"
    a = await tool(x="v", session=sess_a)
    b = await tool(x="v", session=sess_b)
    assert a == {"r": "v", "by": sess_a}
    assert b == {"r": "v", "by": sess_b}
    assert calls == 2, "different sessions are different operations"


# ----- tool-concurrent-calls -----


async def test_concurrent_identical_calls(backend) -> None:
    """N concurrent identical calls -> one execution; the rest wait and replay."""
    calls = 0
    session = _sid()

    @idempotent(
        backend=backend,
        key_fields=["x"],
        scope=lambda a: a["session"],
        wait_timeout_seconds=3.0,
    )
    async def tool(*, x, session):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.05)  # hold the claim so duplicates wait
        return {"r": x}

    results = await asyncio.gather(
        *(tool(x="v", session=session) for _ in range(10))
    )
    assert calls == 1, f"exactly one real execution; ran {calls}x"
    assert all(r == {"r": "v"} for r in results)


# ----- tool-crash-recovery -----


async def test_crash_recovery(backend) -> None:
    """A tool that 'crashes' mid-call releases the claim so a retry re-runs once."""
    attempts = 0
    session = _sid()

    @idempotent(
        backend=backend,
        key_fields=["x"],
        scope=lambda a: a["session"],
    )
    async def tool(*, x, session):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("simulated crash mid-call")
        return {"r": x}

    with pytest.raises(RuntimeError):
        await tool(x="v", session=session)

    # The claim was released on the crash, so the retry gets a fresh execution.
    result = await tool(x="v", session=session)
    assert result == {"r": "v"}
    assert attempts == 2


# ----- tool-unserializable-result-fails-closed -----


async def test_unserializable_result_fails_closed(backend) -> None:
    """A non-JSON result with the JSON codec surfaces an error rather than
    silently risking a second side effect, and an immediate retry does NOT
    re-run the side effect (the claim is held)."""
    calls = 0
    session = _sid()

    @idempotent(
        backend=backend,
        key_fields=["x"],
        scope=lambda a: a["session"],
        wait_timeout_seconds=0.3,  # so the retry conflicts quickly
    )
    async def tool(*, x, session):
        nonlocal calls
        calls += 1
        return object()  # not JSON-serializable

    with pytest.raises(TypeError):
        await tool(x="v", session=session)
    assert calls == 1

    # The result wasn't cached and the claim wasn't released, so an immediate
    # retry sees the in-flight claim and conflicts — it does not re-run.
    with pytest.raises(IdempotencyConflict):
        await tool(x="v", session=session)
    assert calls == 1, "the side effect must not run a second time"


# ----- tool-pickle-opt-in-warns -----


def test_pickle_codec_emits_security_warning(backend) -> None:
    """Enabling the pickle codec emits a security warning (it's an RCE vector)."""
    with pytest.warns(UserWarning, match="pickle"):

        @idempotent(
            backend=backend,
            key_fields=["x"],
            scope=lambda a: "s",
            result_codec="pickle",
        )
        async def tool(*, x):
            return x


# ----- explicit key, strict rail, typed codecs -----


async def test_explicit_key_is_authoritative(backend) -> None:
    """An explicit idempotency_key wins over derived args: same key + different
    args dedupes; a different key is a different operation (§8.2 #1)."""
    calls = 0
    session = _sid()
    key1, key2 = f"K1-{uuid.uuid4().hex}", f"K2-{uuid.uuid4().hex}"

    @idempotent(backend=backend, scope=lambda a: a["session"])
    async def tool(*, x, session):
        nonlocal calls
        calls += 1
        return {"r": x}

    r1 = await tool(x="a", session=session, idempotency_key=key1)
    r2 = await tool(x="b", session=session, idempotency_key=key1)  # same key -> replay
    assert r1 == r2 == {"r": "a"}
    assert calls == 1

    r3 = await tool(x="c", session=session, idempotency_key=key2)
    assert r3 == {"r": "c"}
    assert calls == 2


async def test_strict_rail_warns_on_volatile_field(backend) -> None:
    """Deriving the key from all args warns when a volatile-looking field would
    silently land in the key (§8.2 #2)."""

    @idempotent(backend=backend, scope=lambda a: "s")
    async def tool(*, amount, request_id):
        return {"r": amount}

    with pytest.warns(UserWarning, match="request_id"):
        await tool(amount=100, request_id="abc-123")


def test_idempotency_key_parameter_collision_is_rejected(backend) -> None:
    """A tool that declares its own `idempotency_key` parameter would have it
    silently hijacked by the reserved call-time kwarg — rejected at decoration."""
    with pytest.raises(ConfigurationError, match="idempotency_key"):

        @idempotent(backend=backend, scope=lambda a: "s")
        async def tool(*, idempotency_key):
            return idempotency_key


def test_key_fields_typo_is_rejected(backend) -> None:
    """A key_field that isn't a real parameter is almost always a typo and would
    silently collapse distinct calls onto one key — rejected at decoration."""
    with pytest.raises(ConfigurationError, match="key_fields"):

        @idempotent(
            backend=backend,
            scope=lambda a: "s",
            key_fields=["origin", "destinations"],  # typo: extra 's'
        )
        async def book(*, origin, destination):
            return {"booked": f"{origin}-{destination}"}


async def test_explicit_volatile_key_warns(backend) -> None:
    """Using a per-turn provider tool_call_id (or a UUID) as the explicit key
    means identical calls won't dedupe — the sharpest AI footgun (review C-1)."""

    @idempotent(backend=backend, scope=lambda a: "s")
    async def charge(*, amount):
        return {"r": amount}

    with pytest.warns(UserWarning, match="per-turn provider call id"):
        await charge(amount=100, idempotency_key="call_turn1_abc")
    # A stable business key must NOT warn.
    import warnings as _w

    with _w.catch_warnings():
        _w.simplefilter("error")  # any warning would raise
        await charge(amount=100, idempotency_key="order-2026-0042")


async def test_ambient_caller_identity_zero_arg(backend) -> None:
    """A zero-arg scope (reads ambient context, e.g. a contextvar) keeps
    the scope OUT of the tool's LLM-visible signature (review C-2)."""
    import contextvars

    session = contextvars.ContextVar("session")
    calls = 0

    @idempotent(backend=backend, key_fields=["amount"], scope=lambda: session.get())
    async def charge(*, amount):
        nonlocal calls
        calls += 1
        return {"n": calls}

    session.set("agent-1")
    await charge(amount=100)
    await charge(amount=100)  # same ambient session + amount -> deduped
    assert calls == 1
    session.set("agent-2")
    await charge(amount=100)  # different ambient session -> runs
    assert calls == 2


def test_sync_tool_is_rejected_at_decoration(backend) -> None:
    """The async decorator still rejects a sync tool (it would be wrapped in a
    coroutine the caller never awaits). The message points at idempotent_sync."""
    with pytest.raises(ConfigurationError, match="async def"):

        @idempotent(
            backend=backend,
            scope=lambda a: "s",
            key_fields=["x"],
        )
        def tool(*, x):  # plain def, not async
            return {"r": x}


def test_idempotent_tool_sync_dedups_from_sync_code() -> None:
    """idempotent_sync returns a normal sync callable (no event loop) that
    runs the side effect once per key — sync-via-threadpool support."""
    calls = 0

    @idempotent_sync(
        backend=InMemoryBackend(),
        key_fields=["amount"],
        scope=lambda a: a["user"],
    )
    def charge(*, amount, user):  # plain sync function
        nonlocal calls
        calls += 1
        return {"charged": amount, "n": calls}

    first = charge(amount=100, user="u1")
    second = charge(amount=100, user="u1")  # same key -> replayed, not re-run
    assert calls == 1
    assert first == second == {"charged": 100, "n": 1}
    # Different scope -> runs again.
    charge(amount=100, user="u2")
    assert calls == 2


def test_idempotent_tool_sync_rejects_async_fn() -> None:
    """An async tool belongs on the async decorator; the sync facade rejects it."""
    with pytest.raises(ConfigurationError, match="idempotent"):

        @idempotent_sync(backend=InMemoryBackend(), key_fields=["x"])
        async def tool(*, x):  # async def on the sync decorator
            return {"r": x}


def test_volatile_field_named_in_key_fields_warns(backend) -> None:
    """Listing a volatile-looking field in key_fields is the same footgun as it
    slipping into the derive-from-all-args path — warn at decoration (review P2-2)."""
    with pytest.warns(UserWarning, match="volatile"):

        @idempotent(
            backend=backend,
            scope=lambda a: "s",
            key_fields=["request_id"],
        )
        async def tool(*, request_id):
            return {"r": request_id}


def test_key_fields_allowed_with_var_keyword(backend) -> None:
    """A tool taking **kwargs can name key fields not in its explicit signature —
    we can't validate those, so don't reject them."""

    @idempotent(
        backend=backend,
        scope=lambda a: "s",
        key_fields=["anything"],
    )
    async def tool(**kwargs):
        return kwargs

    assert tool is not None  # decoration succeeded


async def test_caller_identity_typo_gives_actionable_error(backend) -> None:
    """A scope that indexes a non-existent arg surfaces a clear
    ConfigurationError (with the available keys), not a raw KeyError."""

    @idempotent(
        backend=backend,
        key_fields=["x"],
        scope=lambda args: args["sesion_id"],  # typo: missing 's'
    )
    async def tool(*, x, session_id):
        return {"r": x}

    with pytest.raises(ConfigurationError, match="scope"):
        await tool(x="v", session_id="s1")


def test_dataclass_codec_with_unresolvable_annotation_errors_clearly(backend) -> None:
    """A dataclass return type defined in a local scope can't be resolved; the
    error explains that instead of a cryptic 'is not a dataclass'."""
    from dataclasses import dataclass

    @dataclass
    class LocalResult:  # local, so get_type_hints can't look it up
        value: str

    with pytest.raises(ConfigurationError, match="resolve the return type"):

        @idempotent(
            backend=backend,
            key_fields=["x"],
            scope=lambda a: "s",
            result_codec="dataclass",
        )
        async def tool(*, x) -> LocalResult:
            return LocalResult(value=x)


async def test_dataclass_codec_roundtrips(backend) -> None:
    """The dataclass codec stores JSON and reconstructs the typed value on
    replay (§5.4 typed serializers)."""
    calls = 0
    session = _sid()

    @idempotent(
        backend=backend,
        key_fields=["ref"],
        scope=lambda a: a["session"],
        result_codec="dataclass",
    )
    async def book(*, ref, session) -> Booking:
        nonlocal calls
        calls += 1
        return Booking(ref=ref, seats=2)

    first = await book(ref="ABC", session=session)
    assert isinstance(first, Booking) and first.ref == "ABC" and first.seats == 2

    replay = await book(ref="ABC", session=session)
    assert isinstance(replay, Booking)
    assert replay == first
    assert calls == 1


async def test_crash_recovery_via_lease_expiry(backend) -> None:
    """A truly crashed process (claim, no release) is recovered by lease expiry:
    a retry reclaims and runs once, and the zombie's late completion is fenced."""
    calls = 0
    session = _sid()

    @idempotent(
        backend=backend,
        key_fields=["x"],
        scope=lambda a: a["session"],
        lease_ttl_seconds=0.2,
    )
    async def tool(*, x, session):
        nonlocal calls
        calls += 1
        return {"r": x}

    engine = tool.idemkit_engine  # type: ignore[attr-defined]
    effective_key = engine.effective_key(x="v", session=session)

    # A crashed process claimed but never completed or released.
    zombie = await backend.claim(effective_key, AI_FINGERPRINT, 1, 0.2)
    await asyncio.sleep(0.4)  # lease lapses

    result = await tool(x="v", session=session)
    assert result == {"r": "v"}
    assert calls == 1

    fenced = await backend.complete(
        effective_key, zombie.our_claim_token, 200, {}, b"", 3600.0
    )
    assert fenced is False
