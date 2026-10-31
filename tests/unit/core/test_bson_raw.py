"""Transport decoding preserves physical document shape before query parsing."""

import struct

import pytest

from bson import BSON, Binary, Code, DBRef
from bson.errors import InvalidBSON

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.aggregation.preparation import prepare_expression
from mongoeco.core.bson_raw import decode_bson_document
from mongoeco.errors import OperationFailure
from mongoeco.types import UNDEFINED
from mongoeco.wire.bson_bridge import decode_wire_value


@pytest.mark.parametrize(
    "wrapper", [lambda v: v, lambda v: [v], lambda v: Code("x", v)]
)
def test_dbref_documents_keep_original_order_and_mapping(wrapper):
    value = {"$id": 1, "$ref": "c", "z": {"$id": 2, "$ref": "d"}}
    raw = BSON.encode({"v": wrapper(value)})
    result = decode_bson_document(raw)
    assert BSON.encode(result) == raw
    leaf = result["v"]
    if isinstance(leaf, list):
        leaf = leaf[0]
    if isinstance(leaf, Code):
        leaf = leaf.scope
    assert isinstance(leaf, dict)
    assert list(leaf) == ["$id", "$ref", "z"]
    assert not isinstance(leaf, DBRef)


@pytest.mark.parametrize("subtype", [0, 2, 3, 4, 5, 128])
@pytest.mark.parametrize("payload", [b"", b"ab", bytes(range(16))])
def test_binary_framing_preserves_value_without_expression_validation(subtype, payload):
    raw = BSON.encode({"v": Binary(payload, subtype)})
    result = decode_bson_document(raw)
    assert bytes(result["v"]) == payload
    assert getattr(result["v"], "subtype", 0) == subtype
    assert BSON.encode(result) == raw


def test_undefined_nested_in_dbref_remains_distinct_from_null():
    raw = BSON.encode({"v": {"$id": None, "$ref": "c"}, "null": None})
    raw = raw.replace(b"\x0a$id\0", b"\x06$id\0")
    result = decode_bson_document(raw)
    assert result["v"]["$id"] is UNDEFINED
    assert result["null"] is None


@pytest.mark.parametrize(
    "document",
    [
        {"v": True},
        {"v": Binary(b"ab", 2)},
        {"v": {"a": 1}},
        {"v": [1]},
        {"v": Code("x", {"a": 1})},
    ],
)
def test_all_truncated_buffers_are_invalid(document):
    raw = BSON.encode(document)
    for end in range(len(raw)):
        with pytest.raises(InvalidBSON):
            decode_bson_document(raw[:end])


@pytest.mark.parametrize(
    "value", [b"\x08a\0\0", b"\x0ba\0\0", b"\x03a\0\x05\0\0\0\0", b"\x05a\0\0\0\0\0\0"]
)
def test_elements_cannot_consume_the_outer_terminator(value):
    raw = struct.pack("<i", len(value) + 4) + value
    with pytest.raises(InvalidBSON):
        decode_bson_document(raw)


def test_old_binary_rejects_inconsistent_inner_length():
    raw = BSON.encode({"v": Binary(b"ab", 2)})
    raw = raw[:12] + struct.pack("<i", 99) + raw[16:]
    with pytest.raises(InvalidBSON):
        decode_bson_document(raw)


def test_readonly_driver_buffer_preserves_document_order():
    raw = BSON.encode({"v": {"$id": 1, "$ref": "c"}})
    assert BSON.encode(decode_bson_document(memoryview(raw))) == raw


def test_array_decoding_keeps_elements_even_when_names_repeat():
    raw = BSON.encode({"v": [1, 2]}).replace(b"\x101\0", b"\x100\0")
    assert decode_bson_document(raw) == BSON(raw).decode()


@pytest.mark.parametrize(
    "wrapper", [lambda v: DBRef("c", v), lambda v: Code("x", {"v": v})]
)
@pytest.mark.parametrize("subtype", [3, 5])
@pytest.mark.parametrize("wire", [False, True])
def test_binary_expression_validation_reaches_dbref_and_code_scope(
    wrapper, subtype, wire
):

    expression = {"$literal": wrapper(Binary(b"ab", subtype))}
    if wire:
        expression = decode_wire_value(
            decode_bson_document(BSON.encode({"v": expression}))
        )["v"]
    with pytest.raises(OperationFailure) as error:
        prepare_expression(expression, dialect=MONGODB_DIALECT_70)
    assert error.value.code == 9  # noqa: PLR2004 - native FailedToParse
    prepare_expression(
        {"$literal": wrapper(Binary(bytes(16), subtype))},
        dialect=MONGODB_DIALECT_70,
    )
