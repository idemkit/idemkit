"""Unit tests for the result codecs (spec §5.4).

Covers the codecs the method/AI surface advertises: JSON, dataclass, pydantic
(v1/v2 feature-probed), a custom (to_dict, from_dict) pair, opt-in pickle, and the
side-effect marker. Each is checked for a round-trip and for fail-closed behavior.
"""

from __future__ import annotations

import dataclasses

import pytest

from idemkit.core.codecs import (
    CustomResultCodec,
    DataclassResultCodec,
    JsonResultCodec,
    PickleResultCodec,
    PydanticResultCodec,
    SideEffectCodec,
)


def test_json_roundtrip():
    codec = JsonResultCodec()
    assert codec.decode(codec.encode({"a": 1, "b": [2, 3]})) == {"a": 1, "b": [2, 3]}


def test_json_fails_closed_on_unserializable():
    with pytest.raises(TypeError):
        JsonResultCodec().encode({1, 2, 3})  # a set is not JSON-serializable


def test_side_effect_decodes_to_none():
    codec = SideEffectCodec()
    assert codec.decode(codec.encode("ignored")) is None


@dataclasses.dataclass
class Booking:
    pnr: str
    seats: int


def test_dataclass_roundtrip():
    codec = DataclassResultCodec(Booking)
    assert codec.decode(codec.encode(Booking("X1", 2))) == Booking("X1", 2)


def test_dataclass_fails_closed_on_wrong_type():
    with pytest.raises(TypeError):
        DataclassResultCodec(Booking).encode({"pnr": "X1", "seats": 2})


def test_dataclass_rejects_non_dataclass_annotation():
    with pytest.raises(TypeError):
        DataclassResultCodec(dict)


def test_custom_roundtrip():
    codec = CustomResultCodec(
        to_dict=lambda v: {"upper": v.upper()},
        from_dict=lambda d: d["upper"],
    )
    assert codec.decode(codec.encode("hello")) == "HELLO"


def test_pydantic_roundtrip():
    pydantic = pytest.importorskip("pydantic")

    class Refund(pydantic.BaseModel):
        order_id: str
        amount: int

    codec = PydanticResultCodec(Refund)
    out = codec.decode(codec.encode(Refund(order_id="A1", amount=50)))
    assert isinstance(out, Refund)
    assert out.order_id == "A1" and out.amount == 50


def test_pydantic_rejects_non_model_annotation():
    with pytest.raises(TypeError):
        PydanticResultCodec(dict)  # not a pydantic model


def test_pydantic_fails_closed_on_wrong_instance():
    pydantic = pytest.importorskip("pydantic")

    class Refund(pydantic.BaseModel):
        order_id: str

    with pytest.raises(TypeError):
        PydanticResultCodec(Refund).encode("not a model")


def test_pickle_roundtrip_and_warns():
    with pytest.warns(UserWarning):
        codec = PickleResultCodec()
    stored = codec.encode({"x": (1, 2)})  # a tuple survives pickle, unlike JSON
    assert codec.decode(stored) == {"x": (1, 2)}
