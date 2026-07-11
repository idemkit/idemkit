"""Method-level idempotency for keyless callers, per spec §8.

``idempotent`` wraps one side-effectful function so a caller that re-invokes it
with the same arguments runs the real side effect once and replays the cached
result. It is a drop-in decorator, not a workflow engine (the §8.1 line idemkit
will not cross). The callers it is for are the ones that *don't supply an
idempotency key*:

* an LLM agent emitting the same tool call after a retry, a re-planning loop, or
  in two parallel branches (the leading case; OpenAI / Anthropic / MCP) — and a
  provider's per-turn ``tool_call_id`` is the wrong key because it changes every
  turn;
* a background job / cron / Celery task that can run again after a failure;
* an internal call (another service, a worker, a CLI) with no idempotency
  convention.

Because there is no caller-supplied key, the operation's identity is its
**arguments**, and the decorator is loud about getting that right (§8.2):

* **``key_fields=[...]`` is the default and the right choice.** Name the argument
  fields that define "the same operation". Volatile fields you never list
  (``request_id`` and friends) simply don't affect the key — cleaner than a
  deny-list.
* **An explicit ``idempotency_key=...`` at call time overrides ``key_fields``** and
  is authoritative — but reserve it for a value that is STABLE across retries (an
  order id), never a per-turn provider id. idemkit warns when a volatile-looking
  value lands in the key, by any path.
* **Deriving from all args is the fallback**, with a strict-mode rail: a
  volatile-looking field in the key triggers a warning, because mis-keying here
  silently causes a duplicate side effect or a lost call.

Deriving a key from arguments asserts an intent the caller never stated — two
genuinely-distinct calls with identical arguments collapse into one — so name
``key_fields`` deliberately (§8.2 #1).

Side-effect classification is the caller's: wrap a side-effectful function, leave a
pure one undecorated (§8.2 #4). idemkit caches the FIRST result for a key; it does
not make a nondeterministic function deterministic (§8.2 #6).
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import inspect
import json
import logging
import re
import typing
import warnings
from collections.abc import Awaitable, Callable, Mapping
from typing import Any, Literal

from idemkit.backends.base import IdempotencyBackend
from idemkit.core.codecs import (
    CustomResultCodec,
    DataclassResultCodec,
    JsonResultCodec,
    PickleResultCodec,
    PydanticResultCodec,
    ResultCodec,
)
from idemkit.core.events import EventEmitter, EventHandler
from idemkit.core.exceptions import (
    ConfigurationError,
    IdempotencyConflict,
    PayloadMismatch,
    StorageUnavailable,
)
from idemkit.core.fingerprint import compose_key
from idemkit.core.policy import UNSET, IdempotencyPolicy, pick
from idemkit.core.runner import CoreOutcome, IdempotentCore
from idemkit.core.sync_bridge import register_closable, run_sync

_logger = logging.getLogger(__name__)

# A tool call's identity is its (tool, version, args/explicit-key, caller); the
# fingerprint plays no role on this surface, so a constant value means the core's
# fingerprint check never trips (mirrors the queue surface, §5.1).
AI_FINGERPRINT = "idemkit-ai-tool-v1"

# Single-tenant fallback when no scope is configured (warned once at
# decoration). A configured extractor that then yields nothing fails closed.
_ANON_IDENTITY = "_idemkit_ai_anonymous"

# Either arg-derived — ``lambda args: args["session_id"]`` — or ambient (zero-arg,
# reads a contextvar). The arity is detected at decoration; see _caller_identity.
CallerIdentity = Callable[..., "str | None"]
NormalizeArgs = Callable[[Mapping[str, Any]], Mapping[str, Any]]
# Validation fingerprint (the method-surface analogue of HTTP's body_fingerprint,
# §5.1): bound arguments -> bytes that must MATCH on a key hit. A different result
# for the same key is a PayloadMismatch, not a wrong replay. The bytes are not part
# of the key, so a mismatch errors rather than creating a new dedupe bucket.
ValidationFingerprint = Callable[[Mapping[str, Any]], bytes]


def idempotent(
    *,
    backend: IdempotencyBackend,
    version: str = "1",
    scope: CallerIdentity | None = None,
    key_fields: list[str] | None = None,
    validation_fingerprint: ValidationFingerprint | None = None,
    normalize_args: NormalizeArgs | None = None,
    result_codec: Any = "json",
    strict_keys: bool = True,
    lease_ttl_seconds: float = UNSET,
    wait_timeout_seconds: float = UNSET,
    on_storage_error: Literal["fail_closed", "fail_open"] = UNSET,
    expires_after_seconds: float = UNSET,
    use_local_cache: bool = UNSET,
    local_cache_max_items: int = UNSET,
    event_handlers: list[EventHandler] | None = UNSET,
    config: IdempotencyPolicy | None = None,
) -> Callable[[Callable[..., Awaitable[Any]]], Callable[..., Awaitable[Any]]]:
    """Decorate a side-effectful async function so identical calls dedupe (§8.3).

    All options are plain keywords. ``version`` invalidates cached results when the
    function's behavior changes. ``result_codec`` is ``"json"`` (default),
    ``"dataclass"`` / ``"pydantic"``, a ``(to_dict, from_dict)`` pair, or any
    ``ResultCodec``. ``lease_ttl_seconds`` is renewed while the function runs (§5.3.1).
    """

    def decorator(
        fn: Callable[..., Awaitable[Any]],
    ) -> Callable[..., Awaitable[Any]]:
        engine = _ToolEngine(
            fn,
            backend=backend,
            version=version,
            scope=scope,
            key_fields=key_fields,
            validation_fingerprint=validation_fingerprint,
            normalize_args=normalize_args,
            result_codec=result_codec,
            strict_keys=strict_keys,
            lease_ttl_seconds=pick(lease_ttl_seconds, config, "lease_ttl_seconds", 60.0),
            wait_timeout_seconds=pick(wait_timeout_seconds, config, "wait_timeout_seconds", 10.0),
            on_storage_error=pick(on_storage_error, config, "on_storage_error", "fail_closed"),
            expires_after_seconds=pick(
                expires_after_seconds, config, "expires_after_seconds", 86_400.0
            ),
            use_local_cache=pick(use_local_cache, config, "use_local_cache", False),
            local_cache_max_items=pick(
                local_cache_max_items, config, "local_cache_max_items", 1024
            ),
            event_handlers=list(pick(event_handlers, config, "event_handlers", None) or []),
        )

        @functools.wraps(fn)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            return await engine.call(args, kwargs)

        # Exposed for tests that set up a record by hand (spec §9.4 testability).
        wrapper.idemkit_engine = engine  # type: ignore[attr-defined]
        return wrapper

    return decorator


def idempotent_sync(
    *,
    backend: IdempotencyBackend,
    version: str = "1",
    scope: CallerIdentity | None = None,
    key_fields: list[str] | None = None,
    validation_fingerprint: ValidationFingerprint | None = None,
    normalize_args: NormalizeArgs | None = None,
    result_codec: Any = "json",
    strict_keys: bool = True,
    lease_ttl_seconds: float = UNSET,
    wait_timeout_seconds: float = UNSET,
    on_storage_error: Literal["fail_closed", "fail_open"] = UNSET,
    expires_after_seconds: float = UNSET,
    use_local_cache: bool = UNSET,
    local_cache_max_items: int = UNSET,
    event_handlers: list[EventHandler] | None = UNSET,
    config: IdempotencyPolicy | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Synchronous variant of :func:`idempotent` for non-async codebases.

    Decorate a **synchronous** side-effectful function (a Celery task, a
    Flask/Django view helper, a plain script) and call it like a normal function —
    no event loop required. The same dedup core runs on a shared background loop;
    the body runs on a worker thread. Every option matches ``idempotent``.

    Do not call the returned function from inside a running event loop — use the
    async :func:`idempotent` there instead.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        engine = _ToolEngine(
            fn,
            backend=backend,
            version=version,
            scope=scope,
            key_fields=key_fields,
            validation_fingerprint=validation_fingerprint,
            normalize_args=normalize_args,
            result_codec=result_codec,
            strict_keys=strict_keys,
            runs_blocking=True,
            lease_ttl_seconds=pick(lease_ttl_seconds, config, "lease_ttl_seconds", 60.0),
            wait_timeout_seconds=pick(wait_timeout_seconds, config, "wait_timeout_seconds", 10.0),
            on_storage_error=pick(on_storage_error, config, "on_storage_error", "fail_closed"),
            expires_after_seconds=pick(
                expires_after_seconds, config, "expires_after_seconds", 86_400.0
            ),
            use_local_cache=pick(use_local_cache, config, "use_local_cache", False),
            local_cache_max_items=pick(
                local_cache_max_items, config, "local_cache_max_items", 1024
            ),
            event_handlers=list(pick(event_handlers, config, "event_handlers", None) or []),
        )
        register_closable(backend)

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return run_sync(engine.call(args, kwargs))

        wrapper.idemkit_engine = engine  # type: ignore[attr-defined]
        return wrapper

    return decorator


class _ToolEngine:
    """Per-decorated-function state and the call flow."""

    def __init__(
        self,
        fn: Callable[..., Awaitable[Any]],
        *,
        backend: IdempotencyBackend,
        version: str,
        scope: CallerIdentity | None,
        key_fields: list[str] | None,
        validation_fingerprint: ValidationFingerprint | None,
        normalize_args: NormalizeArgs | None,
        result_codec: Any,
        lease_ttl_seconds: float,
        wait_timeout_seconds: float,
        on_storage_error: Literal["fail_closed", "fail_open"],
        strict_keys: bool,
        expires_after_seconds: float,
        use_local_cache: bool,
        local_cache_max_items: int,
        event_handlers: list[EventHandler] | None,
        runs_blocking: bool = False,
    ) -> None:
        self.fn = fn
        self.tool_name = getattr(fn, "__name__", "tool")
        self.version = str(version)
        self._runs_blocking = runs_blocking
        is_async = inspect.iscoroutinefunction(fn)
        if not runs_blocking and not is_async:
            # The async surface awaits the tool and the wrapper is itself an
            # `async def`. A plain `def` would be wrapped in a coroutine the caller
            # never awaits — the side effect would silently never run (review
            # P2-1). Fail loudly; point at the sync decorator and the to_thread
            # escape hatch.
            raise ConfigurationError(
                f"idemkit: idempotent requires an 'async def' tool, but "
                f"{self.tool_name!r} is a regular (synchronous) function. For a "
                "synchronous codebase use idempotent_sync; inside an async "
                "tool wrap the blocking call: "
                "`async def tool(...): return await asyncio.to_thread(blocking, ...)`."
            )
        if runs_blocking and is_async:
            # The sync facade runs the tool on a worker thread, which would not
            # await a coroutine. An async tool belongs on the async decorator.
            raise ConfigurationError(
                f"idemkit: idempotent_sync wraps a synchronous tool, but "
                f"{self.tool_name!r} is 'async def'. Use idempotent for it."
            )
        self.signature = inspect.signature(fn)
        if "idempotency_key" in self.signature.parameters:
            # The call-time `idempotency_key=` kwarg is reserved for the explicit
            # key (§8.2); a tool parameter of the same name would be silently
            # hijacked. Fail loudly at decoration rather than at the first call.
            raise ConfigurationError(
                f"idemkit: tool {self.tool_name!r} declares a parameter named "
                "'idempotency_key', which idempotent reserves for the "
                "explicit key. Rename the tool's parameter."
            )
        if key_fields is not None:
            # A key field that isn't an actual parameter is almost always a typo,
            # and it is the WORST kind: every call hashes that field to None, so
            # distinct operations silently collapse to one key and replay each
            # other's result. Catch it at decoration unless the tool takes
            # **kwargs (where we genuinely can't know the field set).
            params = self.signature.parameters
            takes_var_kw = any(
                p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
            )
            unknown = [f for f in key_fields if f.split(".", 1)[0] not in params]
            if unknown and not takes_var_kw:
                raise ConfigurationError(
                    f"idemkit: key_fields {unknown} are not parameters of tool "
                    f"{self.tool_name!r} (its parameters are {list(params)}). This "
                    "is almost certainly a typo — it would silently collapse "
                    "distinct calls onto one key. Fix the names, or use an "
                    "explicit idempotency_key / normalize_args."
                )
            if strict_keys:
                # A volatile-looking field named *in* key_fields is the same
                # footgun as one slipping in via the derive-from-all-args path
                # (review P2-2): retries that carry a new value won't dedupe. We
                # only have field names at decoration time, so warn by name here
                # (the value-based check still runs per call on the no-selector
                # path). Listing it deliberately is legitimate, hence a warning,
                # not an error.
                volatile = [
                    f for f in key_fields if _name_looks_volatile(f.rsplit(".", 1)[-1])
                ]
                if volatile:
                    warnings.warn(
                        f"idemkit: tool {self.tool_name!r} lists volatile-looking "
                        f"field(s) {volatile} in key_fields. If the value changes "
                        "between an honest retry and its original call, the two "
                        "WON'T dedupe. Use the stable fields only, or pass an "
                        "explicit idempotency_key when that field IS the key. (§8.2)",
                        stacklevel=4,
                    )
        self.scope = scope
        # scope may be either ambient (zero-arg, e.g. reads a
        # contextvars.ContextVar) or arg-derived (one positional, receives the
        # bound arguments). Detect which so the ambient form doesn't force a
        # scope field like ``session_id`` into the tool's LLM-visible signature
        # (review C-2). Default to passing the args when arity can't be read.
        self._identity_wants_args = _accepts_one_positional(scope)
        self.key_fields = key_fields
        self.validation_fingerprint = validation_fingerprint
        self.normalize_args = normalize_args
        self.strict_keys = strict_keys
        self._warned_volatile: set[str] = set()
        self._warned_volatile_explicit = False
        self.codec: ResultCodec[Any, Any] = _resolve_codec(
            result_codec, _resolve_return_type(fn, self.signature)
        )
        self.core = IdempotentCore(
            backend,
            emitter=EventEmitter(event_handlers or []),
            backend_name=type(backend).__name__,
            surface="ai_tool",
            on_storage_error=on_storage_error,
            lease_ttl_seconds=lease_ttl_seconds,
            wait_timeout_seconds=wait_timeout_seconds,
            expires_after_seconds=expires_after_seconds,
            use_local_cache=use_local_cache,
            local_cache_max_items=local_cache_max_items,
        )

        if scope is None:
            warnings.warn(
                f"idemkit: tool {self.tool_name!r} has no scope, so all "
                "callers share one idempotency namespace (single-tenant). For "
                "per-agent / per-session scope, pass scope. See §5.1.",
                stacklevel=4,
            )

    def effective_key(self, *args: Any, **kwargs: Any) -> str:
        """Compute the effective key for a hypothetical call — for tests."""
        explicit_key = kwargs.pop("idempotency_key", None)
        arguments = self._bind(args, kwargs)
        caller_id = self._caller_identity(arguments)
        return self._effective_key(explicit_key, arguments, caller_id)

    async def call(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> Any:
        explicit_key = kwargs.pop("idempotency_key", None)
        arguments = self._bind(args, kwargs)
        caller_id = self._caller_identity(arguments)
        effective_key = self._effective_key(explicit_key, arguments, caller_id)

        decision = await self.core.decide(effective_key, self._fingerprint(arguments))
        outcome = decision.outcome

        if outcome is CoreOutcome.REPLAY:
            assert decision.stored is not None
            return self.codec.decode(decision.stored)
        if outcome is CoreOutcome.MISMATCH:
            # Only reachable if the same key was stored under a different
            # fingerprint, which the constant AI fingerprint precludes — guard
            # anyway rather than replay the wrong result.
            raise PayloadMismatch(
                f"idemkit: tool {self.tool_name!r} key collided with a different "
                "stored payload; refusing to replay."
            )
        if outcome is CoreOutcome.CONFLICT:
            raise IdempotencyConflict(
                f"idemkit: tool {self.tool_name!r} is already in progress for this "
                f"key ({effective_key[:12]}…) and did not finish within the wait "
                "timeout. Retry with bounded backoff; do not loop forever."
            )
        if outcome is CoreOutcome.STORAGE_REFUSE:
            raise StorageUnavailable(
                f"idemkit: storage unavailable for tool {self.tool_name!r} and "
                "on_storage_error=fail_closed. Retry after a short backoff."
            )

        token = decision.claim_token
        unprotected = outcome is CoreOutcome.STORAGE_PASS

        async def raw() -> Any:
            if self._runs_blocking:
                # Synchronous tool: run it on a worker thread so it doesn't block
                # the event loop (and so the heartbeat can renew the lease while
                # it runs).
                return await asyncio.to_thread(self.fn, *args, **kwargs)
            return await self.fn(*args, **kwargs)

        if token is None or unprotected:
            # fail_open: storage is down, so run this once without dedup.
            return await raw()

        try:
            value, lease_lost = await self.core.execute_with_heartbeat(
                effective_key, token, raw
            )
        except BaseException:
            # The tool itself raised (or was cancelled). Release the claim so a
            # retry re-runs it once (spec §8.4 tool-crash-recovery). This is the
            # crash path, distinct from the unserializable-result path below
            # which deliberately keeps the claim.
            await self.core.release(effective_key, token)
            raise
        if lease_lost:
            # The lease lapsed mid-call (renewal couldn't be confirmed) and the
            # handler was cancelled. Surface it; a retry re-runs the tool once.
            raise IdempotencyConflict(
                f"idemkit: tool {self.tool_name!r} lost its lease mid-call "
                "(renewal failed) and was cancelled. Retry — it will re-run."
            )

        try:
            stored = self.codec.encode(value)
        except Exception:
            # Fail closed (§5.4): the result can't be serialized. Do NOT cache,
            # and do NOT release the claim — releasing would let an immediate
            # retry re-run the side effect. Surface the error so the operator
            # fixes the codec; the lease expires on its own as the safety net.
            _logger.error(
                "idemkit: tool %r produced a result the codec could not serialize. "
                "NOT caching and NOT releasing the claim, so a retry won't silently "
                "re-run the side effect. Fix result_codec or return a serializable "
                "value (§5.4).",
                self.tool_name,
            )
            raise

        await self.core.complete(effective_key, token, stored)
        return value

    # ----- key derivation -----

    def _bind(self, args: tuple[Any, ...], kwargs: dict[str, Any]) -> dict[str, Any]:
        bound = self.signature.bind(*args, **kwargs)
        bound.apply_defaults()
        return dict(bound.arguments)

    def _caller_identity(self, arguments: Mapping[str, Any]) -> str:
        if self.scope is None:
            return _ANON_IDENTITY
        try:
            # Ambient identity (zero-arg, e.g. reads a contextvar) is called with
            # no arguments; the arg-derived form receives the bound arguments.
            value = (
                self.scope(arguments)
                if self._identity_wants_args
                else self.scope()
            )
        except Exception as e:
            # The extractor receives the bound arguments as a dict; the most
            # common failure is indexing a key that isn't a parameter (a typo).
            # Turn the raw KeyError/AttributeError into an actionable message.
            raise ConfigurationError(
                f"idemkit: scope for tool {self.tool_name!r} raised "
                f"{type(e).__name__}: {e}. It receives the call's bound arguments "
                f"as a dict (available keys: {list(arguments)}); select a field by "
                "name, e.g. lambda args: args['session_id']. For ambient scope, "
                "use a zero-arg callable that reads a contextvars.ContextVar."
            ) from e
        if not value:
            # A configured extractor that yields nothing is a runtime ambiguity,
            # not a declared single-tenant choice — fail closed (§5.1).
            raise ConfigurationError(
                f"idemkit: scope for tool {self.tool_name!r} returned an "
                "empty value. Return a stable, non-empty identity, or drop "
                "scope to run single-tenant."
            )
        return str(value)

    def _fingerprint(self, arguments: Mapping[str, Any]) -> str:
        """The payload fingerprint compared on a key hit (spec §5.1). With no
        ``validation_fingerprint`` it is a constant, so the core's check never
        trips; with it, a key hit whose fingerprint bytes differ is a payload
        mismatch (PayloadMismatch), not a wrong replay."""
        if self.validation_fingerprint is None:
            return AI_FINGERPRINT
        return hashlib.sha256(self.validation_fingerprint(arguments)).hexdigest()

    def _effective_key(
        self,
        explicit_key: Any,
        arguments: Mapping[str, Any],
        caller_id: str,
    ) -> str:
        if explicit_key is not None:
            key_component = str(explicit_key)
            self._warn_on_volatile_explicit(key_component)
        else:
            key_component = self._derive_args_hash(arguments)
        return compose_key(
            [self.tool_name, self.version, key_component, caller_id]
        )

    def _warn_on_volatile_explicit(self, key_value: str) -> None:
        # The single sharpest AI-surface footgun (review C-1): a provider's
        # per-turn tool_call_id (``call_…``, ``toolu_…``, ``run_…``, ``fc_…``) or
        # a bare UUID looks like the right "explicit key" — but OpenAI/Anthropic
        # mint a NEW one each turn, so using it makes an honest retry MISS the
        # dedupe and run the side effect twice. The stable key for a tool call is
        # its arguments (key_fields), not the call id. Warn once.
        if not self.strict_keys or self._warned_volatile_explicit:
            return
        if _value_looks_like_call_id(key_value):
            self._warned_volatile_explicit = True
            warnings.warn(
                f"idemkit: tool {self.tool_name!r} got an explicit idempotency_key "
                f"({key_value[:16]}…) that looks like a per-turn provider call id or "
                "a UUID. These change on every retry/re-plan, so identical calls "
                "WON'T dedupe and the side effect runs again. Dedupe on the call's "
                "arguments with key_fields=[...] instead; reserve idempotency_key "
                "for a value that is STABLE across retries of the same operation. (§8.2)",
                stacklevel=2,
            )

    def _derive_args_hash(self, arguments: Mapping[str, Any]) -> str:
        if self.key_fields is not None:
            # A field may be a dotted path into a nested dict or object
            # (e.g. "order.id", "customer.address.zip"), resolved against both
            # Mapping keys and object attributes.
            selected: Mapping[str, Any] = {
                field: _resolve_path(arguments, field) for field in self.key_fields
            }
        elif self.normalize_args is not None:
            selected = self.normalize_args(arguments)
        else:
            selected = dict(arguments)
            self._warn_on_volatile(selected)
        return _canonical_hash(selected)

    def _warn_on_volatile(self, selected: Mapping[str, Any]) -> None:
        if not self.strict_keys:
            return
        for name, value in selected.items():
            if name in self._warned_volatile:
                continue
            if _looks_volatile(name, value):
                self._warned_volatile.add(name)
                warnings.warn(
                    f"idemkit: tool {self.tool_name!r} is deriving its key from "
                    f"all arguments, and {name!r} looks volatile (an id, "
                    "timestamp, nonce, or UUID/ISO-8601 value). A volatile field "
                    "in the key means retries with a new value DON'T dedupe. Name "
                    "the stable fields with key_fields=[...], or pass an explicit "
                    "idempotency_key. (§8.2)",
                    stacklevel=2,
                )


def _resolve_codec(spec: Any, return_annotation: Any) -> ResultCodec[Any, Any]:
    if spec is None or spec == "json":
        return JsonResultCodec()
    if spec == "dataclass":
        return DataclassResultCodec(_require_annotation(return_annotation, "dataclass"))
    if spec == "pydantic":
        return PydanticResultCodec(_require_annotation(return_annotation, "pydantic"))
    if spec == "pickle":
        return PickleResultCodec()
    if isinstance(spec, str):
        raise ConfigurationError(
            f"idemkit: unknown result_codec {spec!r}. Use 'json', 'dataclass', "
            "'pydantic', 'pickle', a (to_dict, from_dict) tuple, or a ResultCodec."
        )
    if isinstance(spec, tuple) and len(spec) == 2:
        return CustomResultCodec(spec[0], spec[1])
    # Assume a ResultCodec instance (duck-typed: has encode/decode).
    if hasattr(spec, "encode") and hasattr(spec, "decode"):
        return spec  # type: ignore[no-any-return]
    raise ConfigurationError(
        f"idemkit: result_codec {spec!r} is not a recognized codec spec."
    )


def _resolve_return_type(
    fn: Callable[..., Awaitable[Any]], signature: inspect.Signature
) -> Any:
    """The decorated function's resolved return type, for the dataclass/pydantic codecs.

    With ``from __future__ import annotations`` (PEP 563) every annotation is a
    string, so ``get_type_hints`` is needed to turn it back into the class.
    Falls back to the raw signature annotation when resolution fails (e.g. a
    type defined in a local scope, where the string can't be looked up) so the
    error message in ``_require_annotation`` stays useful.
    """
    try:
        hints = typing.get_type_hints(fn)
    except Exception:
        return signature.return_annotation
    return hints.get("return", signature.return_annotation)


def _require_annotation(return_annotation: Any, codec_name: str) -> type:
    if return_annotation is inspect.Signature.empty:
        raise ConfigurationError(
            f"idemkit: result_codec={codec_name!r} infers the type from the tool's "
            "return annotation, but the tool has none. Add a return annotation, or "
            "use result_codec='json' / a custom codec."
        )
    if isinstance(return_annotation, str):
        # get_type_hints couldn't turn the annotation string into a class —
        # typically a type defined in a local scope (it can't be looked up), or
        # `from __future__ import annotations` with a type that isn't importable
        # at module level.
        raise ConfigurationError(
            f"idemkit: result_codec={codec_name!r} couldn't resolve the return "
            f"type {return_annotation!r} to a class. Define the return type at "
            "module level (not inside a function), or pass an explicit codec — a "
            "ResultCodec instance, or a (to_dict, from_dict) pair."
        )
    return return_annotation  # type: ignore[no-any-return]


def _resolve_path(arguments: Mapping[str, Any], path: str) -> Any:
    """Resolve a (possibly dotted) key-field path against the bound arguments.

    ``"order.id"`` walks ``arguments["order"]`` then its ``["id"]`` (on a Mapping)
    or ``.id`` (on an object). A missing segment yields ``None`` (the same as a
    missing top-level argument), so a typo collapses to a stable, visible value
    rather than raising mid-call.
    """
    root, _, rest = path.partition(".")
    value: Any = arguments.get(root)
    if not rest:
        return value
    for segment in rest.split("."):
        if value is None:
            return None
        value = value.get(segment) if isinstance(value, Mapping) else getattr(value, segment, None)
    return value


def _canonical_hash(selected: Mapping[str, Any]) -> str:
    """SHA-256 of the canonical JSON of the selected key fields (§8.2).

    Inherits JSON sorted-keys canonicalization's documented limits (numeric
    1 vs 1.0, Unicode NFC). A value that isn't JSON-serializable fails loudly
    rather than producing an unstable repr-based key.
    """
    try:
        canonical = json.dumps(
            selected, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        )
    except (TypeError, ValueError) as e:
        raise ConfigurationError(
            f"idemkit: key fields are not JSON-serializable ({e}). Select "
            "serializable fields with key_fields=[...], pass an explicit "
            "idempotency_key, or supply a normalize_args that returns scalars."
        ) from e
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


_VOLATILE_NAMES = frozenset(
    {"request_id", "requestid", "nonce", "timestamp", "ts", "idempotency_key"}
)
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_ISO8601_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")


def _accepts_one_positional(fn: Callable[..., Any] | None) -> bool:
    """Whether ``fn`` takes the bound-arguments mapping (arg-derived identity) vs.
    being a zero-arg ambient callable (e.g. reads a contextvar).

    Defaults to ``True`` (pass the args) when the arity can't be introspected —
    builtins, some ``functools.partial`` objects — preserving the existing
    arg-derived behaviour.
    """
    if fn is None:
        return True
    try:
        params = inspect.signature(fn).parameters.values()
    except (TypeError, ValueError):
        return True
    return any(
        p.kind
        in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.VAR_POSITIONAL,
        )
        for p in params
    )


# Provider tool-call id prefixes (OpenAI ``call_``/``fc_``, Anthropic ``toolu_``,
# agent frameworks ``run_``). These are minted per turn, so they are the wrong
# thing to use as an idempotency key — see _warn_on_volatile_explicit.
_CALL_ID_RE = re.compile(r"^(call_|toolu_|run_|fc_|msg_)", re.IGNORECASE)


def _value_looks_like_call_id(value: str) -> bool:
    return bool(_CALL_ID_RE.match(value) or _UUID_RE.match(value))


def _name_looks_volatile(name: str) -> bool:
    """Whether a field name alone (no value) reads as volatile.

    Used at decoration time, where only the names in ``key_fields`` are known.
    """
    lowered = name.lower()
    if lowered in _VOLATILE_NAMES or lowered.endswith("_id"):
        return True
    return "timestamp" in lowered or "nonce" in lowered


def _looks_volatile(name: str, value: Any) -> bool:
    if _name_looks_volatile(name):
        return True
    if isinstance(value, str) and (_UUID_RE.match(value) or _ISO8601_RE.match(value)):
        return True
    return False
