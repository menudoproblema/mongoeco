"""Differential BSON/codec output and conversion-before-callback guarantees."""

import datetime
import uuid

from typing import get_type_hints
from unittest.mock import patch

import pytest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.core import codec
from mongoeco.core._public_materialization import materialize_public_document
from mongoeco.core.bson_scalars import BsonInt64
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import (
    SON,
    Binary,
    CodecOptions,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    Timestamp,
    UuidRepresentation,
)


def reference(value, options):
    return DocumentCodec.apply_codec_options(
        DocumentCodec.to_pymongo(value), codec_options=options
    )


def shape(value):
    if isinstance(value, dict):
        return type(value), [(key, shape(item)) for key, item in value.items()]
    if isinstance(value, (list, tuple)):
        return type(value), [shape(item) for item in value]
    return type(value), value


@pytest.fixture
def document():
    return {
        "nested": [{"plain": [None, True, 2, 2.5, "text", b"bytes"]}],
        "int64": BsonInt64(3),
        "binary": Binary(b"payload", subtype=4),
        "oid": ObjectId("0123456789abcdef01234567"),
        "decimal": Decimal128("3.25"),
        "regex": Regex("ab", "i"),
        "timestamp": Timestamp(10, 2),
        "naive": datetime.datetime(2100, 1, 1, tzinfo=datetime.UTC).replace(
            tzinfo=None
        ),
        "aware": datetime.datetime(2100, 1, 1, tzinfo=datetime.UTC),
        "uuid": uuid.UUID("01234567-89ab-cdef-0123-456789abcdef"),
        "tuple": (
            {"values": [1, 2]},
            datetime.datetime(2100, 1, 2, tzinfo=datetime.UTC).replace(tzinfo=None),
        ),
        "son": SON([("second", 2), ("first", 1)]),
        "ref": DBRef("records", {"id": BsonInt64(4)}, extras={"owner": "one"}),
    }


@pytest.mark.parametrize("representation", list(UuidRepresentation))
@pytest.mark.parametrize("aware", [False, True])
def test_fused_materialization_preserves_types_order_and_options(
    document, representation, aware
):
    options = CodecOptions(tz_aware=aware, uuid_representation=representation)
    expected = reference(document, options)
    actual = materialize_public_document(document, codec_options=options)
    assert shape(actual) == shape(expected)
    actual["nested"][0]["plain"].clear()
    actual["tuple"][0]["values"].append(99)
    assert document["nested"] == [{"plain": [None, True, 2, 2.5, "text", b"bytes"]}]
    assert document["tuple"][0] == {"values": [1, 2]}


def test_custom_document_class_and_decoders_keep_bottom_up_order():
    events = []

    class LoggedDocument(dict):
        def __init__(self, value):
            events.append(("document", list(value)))
            super().__init__(value)

    def decode(value):
        events.append(("decode", value))
        return value + 1

    options = CodecOptions(document_class=LoggedDocument, type_registry={int: decode})
    value = {"first": {"value": 1}, "second": [2, {"third": 3}]}
    expected = reference(value, options)
    expected_events = events[:]
    events.clear()
    actual = materialize_public_document(value, codec_options=options)
    assert shape(actual) == shape(expected)
    assert events == expected_events


@pytest.mark.parametrize("registry", [False, True])
def test_all_bson_conversion_finishes_before_any_codec_effect(registry):
    events = []

    class FailingTimezone(datetime.tzinfo):
        def utcoffset(self, value):
            events.append("timezone")
            message = "codec failure"
            raise RuntimeError(message)

    def fail_bson(_value):
        message = "BSON failure"
        raise ValueError(message)

    options = CodecOptions(
        type_registry={str: lambda value: events.append("decoder") or value}
        if registry
        else None
    )
    value = {
        "first": "decoded only after BSON conversion",
        "date": datetime.datetime(2100, 1, 1, tzinfo=FailingTimezone()),
        "last": ObjectId("0123456789abcdef01234567"),
    }
    with (
        patch.object(codec, "BsonObjectId", fail_bson),
        pytest.raises(ValueError, match="BSON failure"),
    ):
        materialize_public_document(value, codec_options=options)
    assert events == []


def test_deferred_tuple_and_dates_preserve_codec_traversal_order():
    events = []

    class LoggedTimezone(datetime.tzinfo):
        def __init__(self, name):
            self.name = name

        def utcoffset(self, value):
            events.append(self.name)
            return datetime.timedelta(0)

    def date(name):
        return datetime.datetime(2100, 1, 1, tzinfo=LoggedTimezone(name))

    value = {"a": date("a"), "tuple": (date("tuple"),), "b": date("b")}
    options = CodecOptions()
    expected = reference(value, options)
    expected_events = events[:]
    events.clear()
    actual = materialize_public_document(value, codec_options=options)
    assert shape(actual) == shape(expected)
    assert events == expected_events


@pytest.mark.parametrize("options", [None, CodecOptions(tz_aware=True)])
def test_optional_bson_unavailable_keeps_original_boundary(document, options):
    with patch.object(codec, "BsonObjectId", None):
        actual = materialize_public_document(document, codec_options=options)
        assert shape(actual) == shape(reference(document, options))


def test_bson_code_scope_is_converted_but_not_recursively_codec_decoded():
    bson = pytest.importorskip("bson")
    naive = datetime.datetime(2100, 1, 1, tzinfo=datetime.UTC).replace(tzinfo=None)
    value = {"code": bson.Code("value", {"date": naive})}
    options = CodecOptions(tz_aware=True)
    actual = materialize_public_document(value, codec_options=options)
    assert shape(actual) == shape(reference(value, options))
    assert actual["code"].scope["date"].tzinfo is None


def test_collection_public_annotations_remain_resolvable():
    assert "engine" in get_type_hints(AsyncCollection.__init__)
    assert "pipeline" in get_type_hints(AsyncCollection.aggregate)


def test_options_subclass_is_not_inspected_before_bson_conversion():
    events = []

    class CustomOptions(CodecOptions):
        def __getattribute__(self, name):
            if name == "document_class":
                events.append("options")
            return super().__getattribute__(name)

    options = CustomOptions()
    events.clear()
    with (
        patch.object(codec, "BsonObjectId", side_effect=ValueError("BSON first")),
        pytest.raises(ValueError, match="BSON first"),
    ):
        materialize_public_document(
            {"id": ObjectId("0123456789abcdef01234567")}, codec_options=options
        )
    assert events == []
