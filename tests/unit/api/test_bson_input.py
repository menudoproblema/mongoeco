"""Input encoding uses the driver's BSON contract without its document decoder."""

import re

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from bson import BSON, Binary, Code, DBRef, Decimal128, Int64
from bson.binary import UuidRepresentation as DriverUuidRepresentation
from bson.codec_options import CodecOptions as DriverCodecOptions
from bson.errors import InvalidDocument

from mongoeco import MongoClient
from mongoeco.api import bson_input, operations
from mongoeco.api.bson_input import encode_bson_request
from mongoeco.core.bson_scalars import BsonInt32
from mongoeco.core.codec import DocumentCodec
from mongoeco.engines import MemoryEngine
from mongoeco.types import (
    Binary as LocalBinary,
    CodecOptions,
    DBRef as LocalDBRef,
    Decimal128 as LocalDecimal128,
    ObjectId as LocalObjectId,
    Regex as LocalRegex,
    Timestamp as LocalTimestamp,
    UuidRepresentation,
)


UUID_VALUE = UUID("00112233-4455-6677-8899-aabbccddeeff")
MODES = {
    UuidRepresentation.UNSPECIFIED: DriverUuidRepresentation.UNSPECIFIED,
    UuidRepresentation.STANDARD: DriverUuidRepresentation.STANDARD,
    UuidRepresentation.PYTHON_LEGACY: DriverUuidRepresentation.PYTHON_LEGACY,
    UuidRepresentation.JAVA_LEGACY: DriverUuidRepresentation.JAVA_LEGACY,
    UuidRepresentation.CSHARP_LEGACY: DriverUuidRepresentation.CSHARP_LEGACY,
}


def test_internal_numeric_wrappers_are_not_admitted_as_public_input():
    with pytest.raises(InvalidDocument, match="cannot encode object"):
        encode_bson_request({"v": BsonInt32(1)}, codec_options=CodecOptions())


@pytest.mark.parametrize(
    "value",
    [
        None,
        True,
        1,
        Int64(1),
        1.5,
        "a",
        b"a",
        Binary(b"a", 0),
        Binary(b"a", 5),
        Decimal128("1.20"),
        Decimal128("-sNaN"),
        datetime(1970, 1, 1, 0, 0, 1, 1000, tzinfo=UTC),
        Code("x", {"x": 1}),
        DBRef("c", Int64(1)),
        [1, None],
        (1, "a"),
    ],
)
def test_supported_values_retain_driver_bson(value):
    expected = BSON.encode({"v": value})
    actual = encode_bson_request({"v": value}, codec_options=CodecOptions())
    assert BSON.encode(DocumentCodec.to_pymongo(actual)) == expected


@pytest.mark.parametrize(
    "value",
    [
        Decimal("1.25"),
        object(),
        {1},
        frozenset({1}),
        bytearray(b"a"),
        {"bad\x00key": 1},
        {1: "x"},
        1 << 70,
        "\ud800",
        Code("x", {"x": Decimal(1)}),
        DBRef("c", object()),
    ],
)
def test_rejections_have_the_driver_exception_class(value):
    with pytest.raises(
        (InvalidDocument, OverflowError, UnicodeEncodeError)
    ) as expected:
        BSON.encode({"v": value})
    with pytest.raises(type(expected.value)):
        encode_bson_request({"v": value}, codec_options=CodecOptions())


@pytest.mark.parametrize("mode", MODES)
def test_uuid_writing_uses_request_options(mode):
    request = {"v": UUID_VALUE}
    if mode is UuidRepresentation.UNSPECIFIED:
        with pytest.raises(ValueError, match="UNSPECIFIED"):
            encode_bson_request(
                request, codec_options=CodecOptions(uuid_representation=mode)
            )
    else:
        expected = BSON.encode(
            request, codec_options=DriverCodecOptions(uuid_representation=MODES[mode])
        )
        actual = encode_bson_request(
            request, codec_options=CodecOptions(uuid_representation=mode)
        )
        assert BSON.encode(DocumentCodec.to_pymongo(actual)) == expected


def test_public_local_binary_keeps_subtype_and_plain_dbref_keeps_order():
    request = {"v": {"$id": 1, "$ref": "c", "$db": None, "x": LocalBinary(b"ab", 5)}}
    expected = BSON.encode(
        {"v": {"$id": 1, "$ref": "c", "$db": None, "x": Binary(b"ab", 5)}}
    )
    actual = encode_bson_request(request, codec_options=CodecOptions())
    assert BSON.encode(DocumentCodec.to_pymongo(actual)) == expected


def test_no_input_decoders_or_factories_run_during_encoding():
    calls = []

    class Document(dict):
        def __init__(self, *args, **kwargs):
            calls.append("document")
            super().__init__(*args, **kwargs)

    options = CodecOptions(document_class=Document, type_registry={int: calls.append})
    result = encode_bson_request({"v": 1}, codec_options=options)
    assert result["v"] == 1
    assert calls == []


def test_each_read_encodes_once_and_closing_an_unused_find_encodes_nothing(monkeypatch):

    encoded = []
    encode = bson_input.encode_bson_request

    def observe(*args, **kwargs):
        encoded.append(args[0])
        return encode(*args, **kwargs)

    with MongoClient(MemoryEngine()) as client:
        collection = client.test.rows
        collection.insert_one({"_id": 0, "v": 1})
        monkeypatch.setattr(bson_input, "encode_bson_request", observe)
        collection.find({"v": Decimal("1.25")}).close()
        assert encoded == []
        operations = (
            lambda: list(collection.find({"v": 1})),
            lambda: collection.find_one({"v": 1}),
            lambda: collection.count_documents({"v": 1}),
            lambda: collection.distinct("v", {"v": 1}),
            lambda: list(collection.aggregate([{"$match": {"v": 1}}])),
        )
        for operation in operations:
            encoded.clear()
            operation()
            assert len(encoded) == 1


def test_failed_context_is_not_published_as_a_prepared_cursor(monkeypatch):

    with MongoClient(MemoryEngine()) as client:
        collection = client.test.rows._async_collection()
        create = collection._new_operation_context
        attempts = []

        def context(**kwargs):
            attempts.append(kwargs)
            if len(attempts) == 1:
                message = "context failed"
                raise RuntimeError(message)
            return create(**kwargs)

        monkeypatch.setattr(collection, "_new_operation_context", context)
        cursor = collection.find({"v": 1})
        with pytest.raises(RuntimeError, match="context failed"):
            cursor._as_operation()
        operation = cursor._as_operation()
        assert operation.context is not None
        expected_attempts = 2
        assert len(attempts) == expected_attempts


def test_raw_find_compiles_once_with_its_final_expression_frame(monkeypatch):

    calls = []
    compile_operation = operations.compile_find_operation

    def compile_once(*args, **kwargs):
        calls.append(kwargs)
        return compile_operation(*args, **kwargs)

    with MongoClient(MemoryEngine()) as client:
        collection = client.test.rows._async_collection()
        cursor = collection.find(
            {"$expr": {"$eq": ["$v", "$$x"]}}, let={"x": {"$add": [1, 1]}}
        )
        monkeypatch.setattr(operations, "compile_find_operation", compile_once)
        operation = cursor._as_operation()
        assert len(calls) == 1
        assert operation.let is operation.context.expressions


def test_explain_does_not_freeze_the_original_read_request():
    with MongoClient(MemoryEngine()) as client:
        collection = client.test.rows
        collection.insert_many([{"_id": 0, "v": 1}, {"_id": 1, "v": 2}])
        query = {"v": 1}
        cursor = collection.find(query)
        cursor.explain()
        query["v"] = 2
        assert [row["_id"] for row in cursor] == [1]


def test_raw_bson_mapping_keeps_undefined_and_document_order():
    from bson.raw_bson import RawBSONDocument  # noqa: PLC0415 - SDK-only input witness

    from mongoeco.types import UNDEFINED  # noqa: PLC0415

    raw = BSON.encode({"a": None, "ref": {"$id": 1, "$ref": "c"}}).replace(
        b"\x0aa\x00", b"\x06a\x00"
    )
    document = RawBSONDocument(raw)
    # Reading the SDK mapping would turn undefined into null before admission.
    assert document["a"] is None
    for request in (document, {"documents": [document]}):
        actual = encode_bson_request(request, codec_options=CodecOptions())
        result = actual if request is document else actual["documents"][0]
        assert result["a"] is UNDEFINED
        assert list(result) == ["a", "ref"]
        assert list(result["ref"]) == ["$id", "$ref"]
        assert result["ref"]["$id"] == 1


@pytest.mark.parametrize("mode", MODES)
def test_portable_admission_preserves_bson_and_uuid_policy(monkeypatch, mode):
    # The actual SDK-free profile is exercised separately. This unit seam also
    # measures its admission paths in the ordinary suite, using SDK bytes as
    # the independent oracle rather than reproducing the portable dispatch.
    request = {
        "scalars": [None, True, 1.5, b"a", -(2**63), 2**63 - 1, "á"],
        "tuple": (1, "a"),
        "ref": LocalDBRef("c", 1, "db", {"extra": [1]}),
        "binary": LocalBinary(b"ab", 5),
        "decimal": LocalDecimal128("1.25"),
        "oid": LocalObjectId("00112233445566778899aabb"),
        "timestamp": LocalTimestamp(2, 1),
        "date": datetime(1970, 1, 1, tzinfo=UTC),
        "regex": LocalRegex("ab", "i"),
        "uuid": UUID_VALUE,
    }
    driver_options = DriverCodecOptions(uuid_representation=MODES[mode])
    expected = DocumentCodec.to_pymongo(request)
    monkeypatch.setattr(bson_input, "BSON", None)
    options = CodecOptions(uuid_representation=mode)
    if mode is UuidRepresentation.UNSPECIFIED:
        with pytest.raises(ValueError, match="UNSPECIFIED"):
            encode_bson_request(request, codec_options=options)
        request.pop("uuid")
        expected.pop("uuid")
    actual = encode_bson_request(request, codec_options=options)
    assert BSON.encode(
        DocumentCodec.to_pymongo(actual), codec_options=driver_options
    ) == BSON.encode(expected, codec_options=driver_options)


@pytest.mark.parametrize(
    ["value", "error"],
    [
        ({1: "x"}, InvalidDocument),
        ({"bad\x00key": 1}, InvalidDocument),
        (1 << 70, OverflowError),
        ("\ud800", UnicodeEncodeError),
        (LocalRegex(b"\xff", ""), bson_input.InvalidStringData),
        (LocalRegex("a\x00b", ""), InvalidDocument),
        (Decimal("1.25"), InvalidDocument),
        (LocalDBRef("c", object()), InvalidDocument),
    ],
)
def test_portable_admission_rejects_invalid_requests(monkeypatch, value, error):
    monkeypatch.setattr(bson_input, "BSON", None)
    with pytest.raises(error):
        encode_bson_request({"v": value}, codec_options=CodecOptions())


def test_portable_regex_decodes_valid_utf8_bytes(monkeypatch):
    monkeypatch.setattr(bson_input, "BSON", None)
    actual = encode_bson_request(
        {"v": LocalRegex(b"ab", "i")}, codec_options=CodecOptions()
    )
    assert BSON.encode(DocumentCodec.to_pymongo(actual)) == BSON.encode(
        {"v": re.compile(b"ab", re.IGNORECASE)}
    )
