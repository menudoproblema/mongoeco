"""Differential BSON/codec output and conversion-before-callback guarantees."""

import datetime
import uuid

from typing import get_type_hints
from unittest.mock import patch

import pytest

from mongoeco.api._async.collection import AsyncCollection
from mongoeco.core import _public_materialization as materializer, codec
from mongoeco.core._public_materialization import materialize_public_document
from mongoeco.core.bson_scalars import BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import (
    SON,
    UNDEFINED,
    Binary,
    CodecOptions,
    DBRef,
    Decimal128,
    ObjectId,
    Regex,
    Timestamp,
    UuidRepresentation,
)


def test_response_fallback_rejects_arbitrary_objects_but_materializes_undefined():
    def response(value):
        return materializer.materialize_cursor_response(
            [{"v": value}],
            codec_options=CodecOptions(document_class=SON),
            cursor_id=0,
            namespace="test.rows",
            initial=True,
        )

    assert response(UNDEFINED) == [{"v": None}]
    with pytest.raises(TypeError, match="unsupported BSON response type"):
        response(object())


def test_portable_decoder_failure_keeps_the_decoding_boundary(monkeypatch):
    def fail(value):
        message = "decode failure"
        raise ValueError(message)

    monkeypatch.setattr(materializer, "_BSON", None)
    with pytest.raises(materializer._InvalidBSON, match="decode failure") as error:
        materialize_public_document(
            {"v": 1}, codec_options=CodecOptions(type_registry={int: fail})
        )
    assert isinstance(error.value.__cause__, ValueError)


def reference(value, options):
    return DocumentCodec.apply_codec_options(
        DocumentCodec.to_pymongo(DocumentCodec.to_public(value)), codec_options=options
    )


def shape(value):
    if isinstance(value, dict):
        return type(value), [(key, shape(item)) for key, item in value.items()]
    if isinstance(value, (list, tuple)):
        return type(value), [shape(item) for item in value]
    return type(value), value


@pytest.mark.parametrize("representation", list(UuidRepresentation))
@pytest.mark.parametrize("subtype", [3, 4])
def test_uuid_read_policy_matches_independent_bson_decode(representation, subtype):
    bson = pytest.importorskip("bson")

    payload = bytes(range(16))
    stored = bson.Binary(payload, subtype)
    expected = bson.BSON(bson.BSON.encode({"v": stored})).decode(
        codec_options=bson.CodecOptions(
            uuid_representation=getattr(
                bson.binary.UuidRepresentation, representation.name
            )
        )
    )
    options = CodecOptions(uuid_representation=representation)
    assert shape(
        DocumentCodec.apply_codec_options({"v": stored}, codec_options=options)
    ) == shape(expected)
    assert shape(
        materialize_public_document({"v": stored}, codec_options=options)
    ) == shape(expected)


@pytest.mark.parametrize("kind", [dict, list])
def test_portable_response_walk_preserves_nested_and_batch_callbacks(monkeypatch, kind):
    value = [{"_id": 1, "object": {"v": 2}, "array": [{"v": 3}]}]
    observations = []

    def observe(container):
        observations.append(list(container) if kind is dict else len(container))
        return container

    options = CodecOptions(type_registry={kind: observe})
    expected = materializer.materialize_cursor_response(
        value, codec_options=options, cursor_id=0, namespace="test.rows", initial=True
    )
    expected_observations = observations[:]
    observations.clear()
    monkeypatch.setattr(materializer, "_BSON", None)
    actual = materializer.materialize_cursor_response(
        value, codec_options=options, cursor_id=0, namespace="test.rows", initial=True
    )
    assert shape(actual) == shape(expected)
    assert observations == expected_observations


def test_decoded_binary_zero_matches_sdk_inside_containers_and_bson_values():
    bson = pytest.importorskip("bson")
    zero = bson.Binary(b"zero", 0)
    five = bson.Binary(b"five", 5)
    value = {
        "zero": zero,
        "five": five,
        "nested": [{"zero": zero}],
        "code": bson.Code("return value", {"zero": zero}),
        "reference": bson.DBRef("other", zero, binary=zero),
    }
    expected = bson.BSON(bson.BSON.encode(value)).decode()
    actual = materialize_public_document(
        DocumentCodec.to_internal(value), codec_options=CodecOptions()
    )
    assert shape(actual) == shape(expected)
    assigned = DocumentCodec.to_pymongo(DocumentCodec.to_internal(zero))
    assert type(assigned) is bson.Binary


def test_binary_zero_is_decoded_before_custom_decoder_dispatch():
    bson = pytest.importorskip("bson")
    seen = []

    def decode(value):
        seen.append(type(value))
        return value.hex()

    actual = materialize_public_document(
        DocumentCodec.to_internal({"v": bson.Binary(b"zero", 0)}),
        codec_options=CodecOptions(type_registry={bytes: decode}),
    )
    assert actual == {"v": b"zero".hex()}
    assert seen == [bytes]


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


def test_custom_document_factory_and_setters_follow_independent_sdk_order():
    from bson import BSON  # noqa: PLC0415 - independent SDK oracle
    from bson.codec_options import (  # noqa: PLC0415
        CodecOptions as SDKOptions,
        TypeDecoder,
        TypeRegistry,
    )

    events = []

    class LoggedDocument(dict):
        def __init__(self):
            events.append(("document",))
            super().__init__()

        def __setitem__(self, key, value):
            events.append(("set", key))
            super().__setitem__(key, value)

    def decode(value):
        events.append(("decode", value))
        return value + 1

    options = CodecOptions(document_class=LoggedDocument, type_registry={int: decode})
    value = {"first": {"value": 1}, "second": [2, {"third": 3}]}

    class Decode(TypeDecoder):
        bson_type = int

        def transform_bson(self, value):
            return decode(value)

    expected = BSON(BSON.encode(value)).decode(
        codec_options=SDKOptions(
            document_class=LoggedDocument, type_registry=TypeRegistry([Decode()])
        )
    )
    expected_events = events[:]
    events.clear()
    actual = materialize_public_document(value, codec_options=options)
    assert shape(actual) == shape(expected)
    assert events == expected_events


@pytest.mark.parametrize("initial", [False, True])
def test_portable_cursor_metadata_retains_int64_dispatch_without_hiding_data_id(
    initial,
):
    calls = []
    constructed = []

    class Document(dict):
        def __init__(self):
            constructed.append(self)
            super().__init__()

        def __setitem__(self, key, value):
            if key == "id":
                assert isinstance(value, int)
            super().__setitem__(key, value)

    def number(value):
        calls.append(value)
        return value + 1

    options = CodecOptions(document_class=Document, type_registry={int: number})
    with (
        patch.object(materializer, "_BSON", None),
        patch.object(codec, "BsonInt64Public", None),
    ):
        rows = materializer.materialize_cursor_response(
            [{"id": 9, "n": 2}],
            codec_options=options,
            namespace="db.rows",
            cursor_id=0,
            initial=initial,
        )
    assert calls == [9, 2]
    assert rows == [{"id": 10, "n": 3}]
    key = "firstBatch" if initial else "nextBatch"
    assert [list(row) for row in constructed] == [
        ["cursor", "ok"],
        [key, "id", "ns"],
        [key, "id", "ns"],
        ["id", "n"],
    ]


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


@pytest.mark.parametrize("options", [None, CodecOptions()])
def test_sdk_free_numeric_results_leave_no_engine_wrappers(options):
    value = {"n": [BsonInt32(1), BsonInt64(2), {"v": BsonDouble(3.5)}]}
    with (
        patch.object(codec, "BsonObjectId", None),
        patch.object(codec, "BsonInt64Public", None),
    ):
        actual = materialize_public_document(value, codec_options=options)
    assert actual == {"n": [1, 2, {"v": 3.5}]}
    assert type(actual["n"][0]) is int
    assert type(actual["n"][1]) is int
    assert type(actual["n"][2]["v"]) is float
    assert isinstance(value["n"][0], BsonInt32)


def test_bson_code_scope_nested_date_follows_sdk_timezone_decoding():
    bson = pytest.importorskip("bson")
    naive = datetime.datetime(2100, 1, 1, tzinfo=datetime.UTC).replace(tzinfo=None)
    value = {"code": bson.Code("value", {"date": naive})}
    options = CodecOptions(tz_aware=True)
    actual = materialize_public_document(value, codec_options=options)
    assert shape(actual) == shape(reference(value, options))
    expected = bson.BSON(bson.BSON.encode(value)).decode(
        codec_options=bson.codec_options.CodecOptions(tz_aware=True)
    )
    assert actual["code"].scope["date"] == expected["code"].scope["date"]
    assert actual["code"].scope["date"].tzinfo is not None


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


@pytest.mark.parametrize("aware", [False, True])
def test_decoded_dbref_shapes_and_code_scopes_follow_sdk_root_boundaries(aware):
    bson = pytest.importorskip("bson")
    sdk_codec_options = pytest.importorskip("bson.codec_options").CodecOptions

    date = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC).replace(tzinfo=None)
    reference = {"$id": date, "$ref": "c"}
    value = {
        "$ref": "root",
        "$id": 1,
        "nested": [reference],
        "code": bson.Code("$literal", {"$ref": "scope", "$id": 2, "v": reference}),
    }
    expected = bson.BSON(bson.BSON.encode(value)).decode(
        codec_options=sdk_codec_options(tz_aware=aware)
    )
    actual = materialize_public_document(
        DocumentCodec.to_internal(value), codec_options=CodecOptions(tz_aware=aware)
    )
    assert isinstance(actual, dict)
    assert isinstance(actual["code"].scope, dict)
    assert isinstance(actual["nested"][0], bson.DBRef)
    assert isinstance(actual["code"].scope["v"], bson.DBRef)
    assert shape(actual) == shape(expected)
    assert (actual["nested"][0].id.tzinfo is not None) is aware
    assert (actual["code"].scope["v"].id.tzinfo is not None) is aware


def test_dbref_and_code_nested_values_are_decoded_once_before_container_callbacks():
    bson = pytest.importorskip("bson")
    seen = []

    def decode_date(value):
        seen.append(("date", value))
        return "decoded-date"

    def decode_ref(value):
        seen.append(("ref", value.id))
        return value

    date = datetime.datetime(2020, 1, 1, tzinfo=datetime.UTC).replace(tzinfo=None)
    value = {
        "literal": {"$ref": "c", "$id": date},
        "sdk": bson.DBRef("c", date),
        "code": bson.Code("x", {"d": date}),
    }
    actual = materialize_public_document(
        DocumentCodec.to_internal(value),
        codec_options=CodecOptions(
            type_registry={datetime.datetime: decode_date, bson.DBRef: decode_ref}
        ),
    )
    assert actual["literal"].id == actual["sdk"].id == "decoded-date"
    assert actual["code"].scope["d"] == "decoded-date"
    assert sum(kind == "date" for kind, _value in seen) == len(value)
    assert [value for kind, value in seen if kind == "ref"] == ["decoded-date"] * 2


@pytest.mark.parametrize(
    "document",
    [
        {"$ref": "c", "$id": 1},
        {"$id": 1, "$ref": "c", "$db": None},
        {"$ref": "c", "$id": 1, "$db": 42},
        {"$ref": 42, "$id": 1},
        {"$ref": "c"},
    ],
)
def test_response_dbref_hook_matches_sdk_shape_rules(document):
    bson = pytest.importorskip("bson")
    value = {"v": document}
    expected = bson.BSON(bson.BSON.encode(value)).decode()
    actual = materialize_public_document(value, codec_options=CodecOptions())
    assert shape(actual) == shape(expected)


def test_sdk_dbref_decoder_accepts_code_ref_and_db_without_expression_bracketing():
    bson = pytest.importorskip("bson")
    value = {"v": {"$ref": bson.Code("c"), "$id": 1, "$db": bson.Code("d")}}
    expected = bson.BSON(bson.BSON.encode(value)).decode()
    actual = materialize_public_document(value, codec_options=CodecOptions())
    assert shape(actual) == shape(expected)
    assert type(actual["v"].collection) is bson.Code
    assert type(actual["v"].database) is bson.Code


def test_decoded_sdk_reference_children_do_not_mutate_the_input_object():
    bson = pytest.importorskip("bson")
    child = {"$id": 2, "$ref": "nested"}
    value = {"v": bson.DBRef("c", {"nested": child})}
    before = bson.BSON.encode(value)
    actual = materialize_public_document(value, codec_options=CodecOptions())
    assert type(value["v"].id["nested"]) is dict
    assert value["v"].id["nested"] is child
    assert bson.BSON.encode(value) == before
    assert isinstance(actual["v"].id["nested"], bson.DBRef)
