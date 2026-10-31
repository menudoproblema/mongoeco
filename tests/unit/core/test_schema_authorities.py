"""Structural and ambient-context controls behind H40-H44."""

from decimal import ROUND_DOWN, ROUND_UP, FloatOperation, Inexact, localcontext
from unittest.mock import patch

import pytest

from bson import BSON, Code, Decimal128

from mongoeco import UNDEFINED
from mongoeco.compat import resolve_mongodb_dialect
from mongoeco.core.bson_raw import decode_bson_document
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter
from mongoeco.core.schema_validation import (
    CompiledJsonSchema,
    compile_collection_validator,
)
from mongoeco.engines.semantic_core import enforce_collection_document_validation
from mongoeco.errors import DocumentValidationFailure, OperationFailure


@pytest.mark.parametrize("precision", [6, 28, 34])
@pytest.mark.parametrize("rounding", [ROUND_UP, ROUND_DOWN])
def test_schema_numeric_decisions_preserve_decimal_context(precision, rounding):
    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        context.traps[FloatOperation] = context.traps[Inexact] = True
        before = dict(context.flags)
        assert (
            not CompiledJsonSchema({"maximum": Decimal128("0.1")}).validate(0.1).valid
        )
        assert CompiledJsonSchema({"multipleOf": 0.1}).validate(0.3).valid
        assert CompiledJsonSchema({"minimum": 0}).validate(float("inf")).valid
        assert not CompiledJsonSchema({"minimum": 0}).validate(float("nan")).valid
        assert dict(context.flags) == before


def test_rejection_renders_the_recorded_schema_decision():
    validator = compile_collection_validator(
        {"validator": {"$jsonSchema": {"properties": {"a": {"enum": [1]}}}}}
    )
    with patch.object(
        CompiledJsonSchema,
        "validate",
        autospec=True,
        side_effect=CompiledJsonSchema.validate,
    ) as evaluate:
        with pytest.raises(DocumentValidationFailure) as error:
            enforce_collection_document_validation(
                {"_id": 1, "a": True}, options=validator
            )
        assert evaluate.call_count == 1
    assert (
        error.value.details["errInfo"]["details"]["schemaRulesNotSatisfied"][0][
            "operatorName"
        ]
        == "properties"
    )


def test_moderate_keeps_prepared_dialect_without_an_override():
    validator = compile_collection_validator(
        {"validator": {"a": {"$ne": None}}, "validationLevel": "moderate"},
        dialect=resolve_mongodb_dialect("9.0"),
    )
    assert not validator.validate_document(
        {"a": None}, original_document={"a": UNDEFINED}
    ).valid


@pytest.mark.parametrize(
    "schema",
    [
        {"dependencies": {}},
        {"patternProperties": {}},
        {"minProperties": 1},
        {"type": [1]},
        {"required": [1]},
        {"properties": []},
        {"properties": {"a": 1}},
        {"additionalProperties": []},
        {"items": []},
        {"not": 1},
        {"allOf": []},
        {"anyOf": [1]},
        {"oneOf": "x"},
        {"minItems": -1},
        {"maxItems": True},
        {"minLength": 0.5},
        {"maxLength": "x"},
        {"pattern": 1},
        {"title": False},
        {"description": 1},
        {"minimum": "x"},
        {"maximum": True},
        {"uniqueItems": 1},
        {"exclusiveMinimum": True},
        {"maximum": 0, "exclusiveMaximum": 1},
        {"enum": "x"},
        {"required": "x"},
        {"type": "integer"},
    ],
)
def test_schema_definition_fails_without_evaluating_a_document(schema):
    with pytest.raises(OperationFailure):
        CompiledJsonSchema(schema)


@pytest.mark.parametrize("bound", [float("inf"), float("nan")])
def test_nonfinite_multiple_fails_in_native_execution_phase(bound):
    schema = CompiledJsonSchema({"multipleOf": bound})
    with pytest.raises(OperationFailure) as error:
        schema.validate(1)
    assert error.value.code == 2  # noqa: PLR2004 - native BadValue


def test_raw_bson_preserves_null_and_undefined_in_nested_shapes():
    original = {
        "a": None,
        "nested": {"a": None},
        "items": [None, {"a": None}],
        "code": Code("x", {"a": None}),
        "null": None,
    }
    raw = (
        BSON.encode(original)
        .replace(b"\x0aa\x00", b"\x06a\x00")
        .replace(b"\x0a0\x00", b"\x060\x00")
    )
    decoded = decode_bson_document(raw)
    assert decoded["a"] is UNDEFINED
    assert decoded["nested"]["a"] is UNDEFINED
    assert decoded["items"][0] is UNDEFINED
    assert decoded["items"][1]["a"] is UNDEFINED
    assert decoded["code"].scope["a"] is UNDEFINED
    assert decoded["null"] is None


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_filter_prepares_schema_once_and_preserves_its_dialect(dialect):
    effective = resolve_mongodb_dialect(dialect)
    with patch(
        "mongoeco.core.schema_validation.CompiledJsonSchema", wraps=CompiledJsonSchema
    ) as prepare:
        plan = compile_filter(
            {"$jsonSchema": {"properties": {"a": {"bsonType": "string"}}}},
            dialect=effective,
        )
        assert prepare.call_count == 1
        assert prepare.call_args.kwargs["dialect"] is effective
        assert QueryEngine.match_plan({"a": "abc"}, plan, dialect=effective)
        assert not QueryEngine.match_plan({"a": Code("abc")}, plan, dialect=effective)
        assert prepare.call_count == 1
