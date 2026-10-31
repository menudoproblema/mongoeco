"""Document aliases and cloned NaNs retain key coherence at real boundaries."""

import struct

from decimal import Decimal

import pytest

from bson import BSON, DBRef as SDKDBRef, Decimal128 as SDKDecimal128
from hypothesis import given, settings, strategies as st

from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.identity import assert_document_matches_storage_key
from mongoeco.engines import MemoryEngine
from mongoeco.errors import WriteError
from mongoeco.types import DBRef


@settings(max_examples=100, deadline=None)
@given(
    name=st.text(alphabet="abcdefgh", min_size=1, max_size=8),
    value=st.one_of(
        st.integers(-(2**31), 2**31 - 1), st.text(max_size=12), st.binary(max_size=8)
    ),
    tenant=st.integers(-(2**31), 2**31 - 1),
)
def test_dbref_index_keys_agree_for_bson_identical_document_representations(
    name, value, tenant
):
    sdk = SDKDBRef(name, value, tenant=tenant)
    document = dict(sdk.as_doc())
    # Independent wire encoding establishes equivalence, rather than a second
    # production matcher deciding what the key should mean.
    assert BSON.encode({"v": sdk}) == BSON.encode({"v": document})
    expected = bson_equality_key(document)
    local = DBRef(name, value, extras={"tenant": tenant})
    for representation in (
        sdk,
        local,
        document,
        DocumentCodec.to_internal(sdk),
        DocumentCodec.to_internal(document),
        DocumentCodec.decode(DocumentCodec.encode(local)),
        DocumentCodec.to_pymongo(DocumentCodec.to_internal(document)),
    ):
        assert bson_equality_key(representation) == expected
    assert bson_equality_key(SDKDBRef(name, value, tenant=tenant + 1)) != expected


def _sdk_numeric_values(value):
    if isinstance(value, BsonDouble):
        return value.value
    if isinstance(value, BsonDecimal128):
        return SDKDecimal128(value.value)
    if isinstance(value, dict):
        return {key: _sdk_numeric_values(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sdk_numeric_values(item) for item in value]
    return value


@settings(max_examples=100, deadline=None)
@given(
    payload=st.integers(1, 2**51 - 1),
    nested=st.booleans(),
    kind=st.sampled_from(("raw", "double", "decimal", "signaling_decimal")),
)
def test_memory_storage_guard_checks_cloned_nan_payload(payload, nested, kind):
    number = struct.unpack("!d", (0x7FF8000000000000 | payload).to_bytes(8, "big"))[0]
    other = struct.unpack(
        "!d", (0x7FF8000000000000 | (payload ^ 1)).to_bytes(8, "big")
    )[0]
    if kind == "double":
        number, other = BsonDouble(number), BsonDouble(other)
    elif kind in {"decimal", "signaling_decimal"}:
        spelling = "sNaN" if kind == "signaling_decimal" else "NaN"
        number = BsonDecimal128(Decimal(spelling))
        other = BsonDecimal128(Decimal("-" + spelling))
    value = {"n": [{"v": number}]} if nested else number
    different = {"n": [{"v": other}]} if nested else other
    encoded = BSON.encode(_sdk_numeric_values({"_id": value}))
    clone = DocumentCodec.decode(
        DocumentCodec.encode({"_id": value}), preserve_bson_wrappers=True
    )
    assert BSON.encode(_sdk_numeric_values(clone)) == encoded
    engine = MemoryEngine()
    key = engine._storage_key(value)
    assert_document_matches_storage_key(
        clone, key, storage_key_for_id=engine._storage_key
    )
    with pytest.raises(WriteError) as error:
        assert_document_matches_storage_key(
            {"_id": different}, key, storage_key_for_id=engine._storage_key
        )
    assert error.value.code == 66  # noqa: PLR2004 - native immutable-ID code
