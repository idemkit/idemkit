"""Result codecs — the per-surface serialization seam (spec §5.4).

The core stores a result as opaque bytes (``StoredResult.blob``). A codec maps a
typed return value to and from those bytes. Following AWS Powertools' model, the
on-the-wire form is JSON and the codec records what it serialized so a replay can
reconstruct the typed value.

This module ships the surface-neutral codecs the queue surface needs now:

* ``SideEffectCodec`` — for handlers that return nothing. A completed record is
  just a "processed" marker; replay means *skip*, not *return garbage* (§5.4).
* ``JsonResultCodec`` — JSON in, JSON out, for result-bearing handlers whose
  return value is already JSON-friendly.

The richer typed codecs (dataclass, pydantic v1/v2, custom, and the opt-in
``pickle`` with its security warning) land with the AI tool surface in a later
phase; they plug into the same ``ResultCodec`` protocol.

Fail-closed rule (§5.4): a value that cannot be serialized MUST raise here. The
caller treats that as "do not cache" and re-runs rather than silently risking a
second side effect.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import warnings
from collections.abc import Callable
from typing import Any, Protocol, TypeVar

from idemkit.core.runner import StoredResult

_logger = logging.getLogger(__name__)

T = TypeVar("T")
T_contra = TypeVar("T_contra", contravariant=True)
T_co = TypeVar("T_co", covariant=True)


class ResultCodec(Protocol[T_contra, T_co]):
    """Encode a handler's return value to bytes and back (spec §5.4).

    Split into a contravariant input type and covariant output type so a codec
    can advertise a precise input it accepts while its decode returns the same
    or a wider type. Most codecs use the same type for both.
    """

    def encode(self, value: T_contra) -> StoredResult:
        """Serialize ``value`` into a ``StoredResult``. MUST raise (fail closed)
        if the value cannot be serialized — never return a lossy placeholder."""
        ...

    def decode(self, stored: StoredResult) -> T_co:
        """Reconstruct the value from a stored record on replay."""
        ...


class SideEffectCodec:
    """Codec for side-effect-only handlers that return ``None`` (spec §5.4).

    Stores an empty marker. A completed record means "this dedup id was already
    processed — skip it", and decoding always yields ``None`` (there is no
    payload to replay).
    """

    def encode(self, value: Any) -> StoredResult:
        # The value is ignored on purpose: a side-effect-only handler returns
        # None, and there is nothing to replay. ``value`` is typed ``Any`` only
        # so this codec is interchangeable with the result-bearing ones.
        return StoredResult(blob=b"", meta={"codec": "side_effect"})

    def decode(self, stored: StoredResult) -> None:
        return None


class JsonResultCodec:
    """JSON-on-the-wire codec for result-bearing handlers (spec §5.4).

    The return value must be JSON-serializable. If it is not, ``encode`` raises
    (``TypeError``) and the caller fails closed rather than caching a broken
    result. Use a typed codec (dataclass / pydantic) for richer return types.
    """

    def encode(self, value: Any) -> StoredResult:
        try:
            blob = json.dumps(value).encode("utf-8")
        except (TypeError, ValueError) as e:
            # Fail closed (§5.4): do not store a placeholder for an
            # unserializable result, or a retry would re-run the side effect.
            raise TypeError(
                f"idemkit: result is not JSON-serializable ({e}). Use a typed "
                "ResultCodec, or fix the return value. Refusing to cache so the "
                "operation isn't silently re-run on retry."
            ) from e
        return StoredResult(blob=blob, meta={"codec": "json"})

    def decode(self, stored: StoredResult) -> Any:
        if not stored.blob:
            return None
        return json.loads(stored.blob)


class DataclassResultCodec:
    """Typed codec for a dataclass return type (spec §5.4).

    JSON on the wire; the dataclass type is inferred from the tool's return
    annotation. ``encode`` flattens to a dict via ``dataclasses.asdict``;
    ``decode`` rebuilds the instance via ``cls(**data)``. Nested dataclasses
    round-trip through ``asdict`` but rebuild as plain dicts unless the type's
    own ``__init__`` reconstructs them — keep return types flat, or supply a
    custom codec for nested shapes.
    """

    def __init__(self, cls: type) -> None:
        if not dataclasses.is_dataclass(cls):
            raise TypeError(
                f"idemkit: result_codec='dataclass' needs a dataclass return "
                f"annotation; {cls!r} is not a dataclass."
            )
        self._cls = cls

    def encode(self, value: Any) -> StoredResult:
        if not dataclasses.is_dataclass(value) or isinstance(value, type):
            raise TypeError(
                f"idemkit: expected a {self._cls.__name__} instance, got "
                f"{type(value).__name__}. Refusing to cache (fail closed, §5.4)."
            )
        blob = json.dumps(dataclasses.asdict(value)).encode("utf-8")
        return StoredResult(blob=blob, meta={"codec": "dataclass"})

    def decode(self, stored: StoredResult) -> Any:
        data = json.loads(stored.blob) if stored.blob else {}
        return self._cls(**data)


class PydanticResultCodec:
    """Typed codec for a pydantic model return type, v1 and v2 (spec §5.4).

    JSON on the wire. Works against either pydantic generation by feature-probing
    the model class (``model_validate``/``model_dump`` on v2,
    ``parse_obj``/``dict`` on v1) rather than importing pydantic, so idemkit
    keeps its zero-dependency core.
    """

    def __init__(self, model: type) -> None:
        # Probe for the v2 then v1 API so we fail loudly at decoration time, not
        # mid-call, if the annotation isn't a pydantic model.
        if not (
            hasattr(model, "model_validate") or hasattr(model, "parse_obj")
        ):
            raise TypeError(
                f"idemkit: result_codec='pydantic' needs a pydantic model return "
                f"annotation; {model!r} has neither model_validate (v2) nor "
                "parse_obj (v1)."
            )
        # Typed Any: we feature-probe the v1/v2 API rather than depend on pydantic.
        self._model: Any = model

    def encode(self, value: Any) -> StoredResult:
        if hasattr(value, "model_dump"):
            data = value.model_dump(mode="json")  # pydantic v2
        elif hasattr(value, "dict"):
            data = value.dict()  # pydantic v1
        else:
            raise TypeError(
                f"idemkit: expected a pydantic model instance, got "
                f"{type(value).__name__}. Refusing to cache (fail closed, §5.4)."
            )
        return StoredResult(
            blob=json.dumps(data).encode("utf-8"), meta={"codec": "pydantic"}
        )

    def decode(self, stored: StoredResult) -> Any:
        data = json.loads(stored.blob) if stored.blob else {}
        if hasattr(self._model, "model_validate"):
            return self._model.model_validate(data)  # v2
        return self._model.parse_obj(data)  # v1


class CustomResultCodec:
    """Codec from a ``(to_dict, from_dict)`` pair (spec §5.4).

    The escape hatch for return types the built-in codecs don't cover. JSON on
    the wire; ``to_dict`` must return something ``json.dumps`` accepts.
    """

    def __init__(
        self,
        to_dict: Callable[[Any], Any],
        from_dict: Callable[[Any], Any],
    ) -> None:
        self._to_dict = to_dict
        self._from_dict = from_dict

    def encode(self, value: Any) -> StoredResult:
        blob = json.dumps(self._to_dict(value)).encode("utf-8")
        return StoredResult(blob=blob, meta={"codec": "custom"})

    def decode(self, stored: StoredResult) -> Any:
        data = json.loads(stored.blob) if stored.blob else None
        return self._from_dict(data)


class PickleResultCodec:
    """Pickle codec — OPT-IN, and a remote-code-execution risk (spec §5.4, §12).

    Constructing one emits a security warning, because a stored pickle that an
    attacker can influence is an RCE vector on ``decode``. Use a JSON or typed
    codec unless you fully control and trust the backend contents. Provided so
    arbitrary return types can be cached when the operator accepts the risk.
    """

    def __init__(self) -> None:
        warnings.warn(
            "idemkit: the pickle result codec is a remote-code-execution risk — "
            "a stored pickle is executed on decode. Only use it if you fully "
            "control and trust the backend's contents; prefer JSON or a typed "
            "codec otherwise (spec §5.4, §12).",
            stacklevel=2,
        )
        _logger.warning(
            "idemkit: pickle result codec enabled (RCE risk on decode, §5.4)."
        )

    def encode(self, value: Any) -> StoredResult:
        import pickle

        return StoredResult(blob=pickle.dumps(value), meta={"codec": "pickle"})

    def decode(self, stored: StoredResult) -> Any:
        import pickle

        return pickle.loads(stored.blob)
