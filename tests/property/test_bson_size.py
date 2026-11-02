"""The SDK's independently encoded bytes are the size oracle."""

import re

from datetime import UTC, datetime
from uuid import UUID

import pytest

from bson import (
    BSON,
    Binary,
    Code,
    DBRef,
    Decimal128,
    Int64,
    MaxKey,
    MinKey,
    ObjectId,
    Regex,
    Timestamp,
)
from bson.binary import UuidRepresentation
from bson.codec_options import CodecOptions
from hypothesis import example, given, settings, strategies as st

from mongoeco.core.aggregation.runtime import _bson_document_size
from mongoeco.core.codec import DocumentCodec


CODEC = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)
SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(-(2**63), 2**63 - 1),
    st.integers(-(2**63), 2**63 - 1).map(Int64),
    st.floats(),
    st.text(),
    st.binary(max_size=24),
    st.sampled_from(
        [
            Decimal128("1.00"),
            Decimal128("NaN"),
            Decimal128("sNaN"),
            Code("x"),
            Code("á雪", {}),
            Code("x", {"n": Int64(1), "b": Binary(b"ab", 2)}),
            DBRef("c", 1),
            DBRef("c", Int64(1), "db", tenant="t"),
            Regex("á雪", "imsux"),
            re.compile("a", re.IGNORECASE),
            ObjectId("0123456789abcdef01234567"),
            Timestamp(1, 2),
            datetime(2038, 4, 22, tzinfo=UTC),
            MinKey(),
            MaxKey(),
            UUID("12345678-1234-5678-1234-567812345678"),
        ]
    ),
    st.builds(Binary, st.binary(max_size=24), st.sampled_from((0, 2, 3, 4, 5, 128))),
)
KEYS = st.text(alphabet="abá雪012", min_size=1, max_size=8)
VALUES = st.recursive(
    SCALARS,
    lambda child: st.one_of(
        st.lists(child, max_size=4), st.dictionaries(KEYS, child, max_size=4)
    ),
    max_leaves=12,
)


@settings(max_examples=300, deadline=None)
@given(document=st.dictionaries(KEYS, VALUES, max_size=5))
@example(document={"v": Binary(b"ab", 2)})
@example(document={"v": Code("x", {"n": Int64(1)})})
@example(document={"v": re.compile("^a")})
def test_bson_size_matches_sdk_bytes_across_owned_and_stored_views(document):
    expected = len(BSON.encode(document, codec_options=CODEC))
    for represented in (
        document,
        DocumentCodec.to_internal(document),
        DocumentCodec.decode(
            DocumentCodec.encode(document), preserve_bson_wrappers=True
        ),
        DocumentCodec.to_pymongo(DocumentCodec.to_internal(document)),
    ):
        assert _bson_document_size(represented) == expected


@pytest.mark.parametrize("scalar_type", [str, int, float, list, dict])
def test_size_preserves_subclass_fallback(scalar_type):
    class Subclass(scalar_type):
        pass

    values = {str: "á雪", int: 2**40, float: -0.0, list: [1, "á"], dict: {"雪": 1}}
    document = {"v": Subclass(values[scalar_type])}
    assert _bson_document_size(document) == len(BSON.encode(document))


@pytest.mark.parametrize("value", [-(2**63) - 1, 2**63])
def test_size_retains_integer_overflow(value):
    with pytest.raises(OverflowError):
        _bson_document_size({"v": value})


def test_size_retains_cstring_and_utf8_errors():
    with pytest.raises(ValueError, match="NUL"):
        _bson_document_size({"bad\x00key": 1})
    with pytest.raises(UnicodeEncodeError):
        _bson_document_size({"v": "\ud800"})
