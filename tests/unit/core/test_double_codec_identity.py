"""JSON persistence preserves double bits and existing ownership conventions."""

import json
import struct

import pytest

from bson import BSON

from mongoeco.core.bson_scalars import BsonDouble
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.identity import canonical_document_id


@pytest.mark.parametrize(
    "bits",
    [
        "000000000000f87f",
        "010000000000f87f",
        "010000000000f8ff",
        "000000000000f07f",
        "000000000000f0ff",
    ],
)
@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("backend", ["stdlib", "orjson"])
def test_json_storage_preserves_nonfinite_bits_and_ownership(bits, wrapped, backend):
    number = struct.unpack("<d", bytes.fromhex(bits))[0]
    value = BsonDouble(number) if wrapped else number
    encoded = DocumentCodec.encode({"_id": value, "nested": [value]})
    serializer = pytest.importorskip("orjson") if backend == "orjson" else json
    restored = DocumentCodec.decode(
        serializer.loads(serializer.dumps(encoded)), preserve_bson_wrappers=True
    )
    assert type(restored["_id"]) is type(value)
    assert repr(canonical_document_id(restored["_id"])) == repr(
        canonical_document_id(value)
    )
    expected = BSON.encode({"v": number})
    for item in (restored["_id"], restored["nested"][0]):
        actual = item.value if isinstance(item, BsonDouble) else item
        assert BSON.encode({"v": actual}) == expected


@pytest.mark.parametrize("preserve", [False, True])
def test_existing_double_and_raw_float_formats_remain_readable(preserve):
    old = {"$mongoeco": {"type": "double", "value": float("inf")}}
    value = DocumentCodec.decode(old, preserve_bson_wrappers=preserve)
    assert type(value) is (BsonDouble if preserve else float)
    assert (value.value if preserve else value) == float("inf")
    assert (
        type(DocumentCodec.decode(float("nan"), preserve_bson_wrappers=preserve))
        is float
    )
