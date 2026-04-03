import unittest
import datetime
import decimal
import re
import uuid

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.schema_validation import CompiledJsonSchema, compile_collection_validator
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, DBRef, Decimal128, ObjectId, Regex, SON, Timestamp


class SchemaValidationTests(unittest.TestCase):
    def test_compile_collection_validator_supports_json_schema_and_query_validator(self):
        validator = compile_collection_validator(
            {
                "validator": {
                    "tenant": "a",
                    "$jsonSchema": {
                        "required": ["name"],
                        "properties": {"name": {"bsonType": "string"}},
                    },
                },
                "validationLevel": "strict",
                "validationAction": "error",
            },
            dialect=MONGODB_DIALECT_70,
        )

        assert validator is not None
        self.assertEqual(validator.validation_level, "strict")
        self.assertEqual(validator.validation_action, "error")
        self.assertIsNotNone(validator.query_validator)
        self.assertIsNotNone(validator.json_schema)

    def test_json_schema_validator_reports_required_and_type_issues(self):
        validator = compile_collection_validator(
            {
                "validator": {
                    "$jsonSchema": {
                        "required": ["name"],
                        "properties": {"name": {"bsonType": "string"}},
                    }
                }
            }
        )

        assert validator is not None
        missing = validator.validate_document({"_id": "1"})
        wrong_type = validator.validate_document({"_id": "1", "name": 3})
        valid = validator.validate_document({"_id": "1", "name": "Ada"})

        self.assertFalse(missing.valid)
        self.assertIn("field is required", missing.first_message)
        self.assertFalse(wrong_type.valid)
        self.assertIn("type mismatch", wrong_type.first_message)
        self.assertTrue(valid.valid)

    def test_moderate_validation_skips_updates_for_legacy_invalid_documents(self):
        validator = compile_collection_validator(
            {
                "validator": {
                    "$jsonSchema": {
                        "required": ["name"],
                        "properties": {
                            "name": {"bsonType": "string"},
                            "age": {"bsonType": "int"},
                        },
                    }
                },
                "validationLevel": "moderate",
            }
        )

        assert validator is not None
        result = validator.validate_document(
            {"_id": "1", "age": 30},
            original_document={"_id": "1"},
        )

        self.assertTrue(result.valid)

    def test_compile_collection_validator_rejects_invalid_action(self):
        with self.assertRaises(OperationFailure):
            compile_collection_validator(
                {
                    "validator": {"$jsonSchema": {"required": ["name"]}},
                    "validationAction": "explode",
                }
            )

    def test_json_schema_validates_strings_arrays_enums_and_additional_properties(self):
        schema = CompiledJsonSchema(
            {
                "bsonType": "object",
                "required": ["name", "tags"],
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "bsonType": "string",
                        "minLength": 2,
                        "maxLength": 4,
                        "pattern": "^A",
                    },
                    "tags": {
                        "bsonType": "array",
                        "minItems": 1,
                        "maxItems": 2,
                        "items": {"enum": ["admin", "staff"]},
                    },
                },
            }
        )

        valid = schema.validate({"name": "Ada", "tags": ["admin"]})
        invalid = schema.validate({"name": "B", "tags": ["other", "staff", "admin"], "extra": 1})

        self.assertTrue(valid.valid)
        self.assertFalse(invalid.valid)
        rendered = [issue.render() for issue in invalid.issues]
        self.assertTrue(any("string shorter than 2" in message for message in rendered))
        self.assertTrue(any("string does not match pattern" in message for message in rendered))
        self.assertTrue(any("array has more than 2 items" in message for message in rendered))
        self.assertTrue(any("value is not in enum" in message for message in rendered))
        self.assertTrue(any("additional properties are not allowed" in message for message in rendered))

    def test_json_schema_supports_logical_operators(self):
        schema = CompiledJsonSchema(
            {
                "allOf": [
                    {"bsonType": "object"},
                    {"required": ["name"]},
                ],
                "anyOf": [
                    {"properties": {"name": {"bsonType": "string"}}},
                    {"properties": {"age": {"bsonType": "int"}}},
                ],
                "oneOf": [
                    {"required": ["name"]},
                    {"required": ["age"]},
                ],
                "not": {"required": ["blocked"]},
            }
        )

        valid = schema.validate({"name": "Ada"})
        invalid = schema.validate({"name": "Ada", "age": 3, "blocked": True})

        self.assertTrue(valid.valid)
        self.assertFalse(invalid.valid)
        rendered = [issue.render() for issue in invalid.issues]
        self.assertTrue(any("oneOf" in message for message in rendered))
        self.assertTrue(any("must not satisfy not" in message for message in rendered))

    def test_json_schema_accepts_public_bson_helper_classes(self):
        schema = CompiledJsonSchema(
            {
                "bsonType": "object",
                "properties": {
                    "payload": {"bsonType": "binData"},
                    "amount": {"bsonType": "decimal"},
                    "pattern": {"bsonType": "regex"},
                    "ts": {"bsonType": "timestamp"},
                    "ref": {"bsonType": "object"},
                },
            }
        )

        result = schema.validate(
            {
                "payload": Binary(b"abc", subtype=0),
                "amount": Decimal128("1.25"),
                "pattern": Regex("^ad", "i"),
                "ts": Timestamp(123, 1),
                "ref": DBRef("users", ObjectId("0123456789abcdef01234567"), database="db", extras={"meta": SON([("tenant", "t1")])}),
            }
        )
        self.assertTrue(result.valid)

    def test_json_schema_supports_bson_scalar_types_and_numeric_bounds(self):
        schema = CompiledJsonSchema(
            {
                "properties": {
                    "small": {"bsonType": "int"},
                    "large": {"bsonType": "long"},
                    "ratio": {"bsonType": "double", "minimum": 1.5, "maximum": 2.5},
                    "amount": {"bsonType": "decimal", "minimum": decimal.Decimal("2.0")},
                    "oid": {"bsonType": "objectId"},
                    "created_at": {"bsonType": "date"},
                    "payload": {"bsonType": "binData"},
                    "matcher": {"bsonType": "regex"},
                }
            }
        )

        valid = schema.validate(
            {
                "small": BsonInt32(2),
                "large": BsonInt64(2**40),
                "ratio": BsonDouble(2.0),
                "amount": BsonDecimal128(decimal.Decimal("2.5")),
                "oid": ObjectId("0123456789abcdef01234567"),
                "created_at": datetime.datetime(2025, 1, 1, tzinfo=datetime.UTC),
                "payload": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                "matcher": re.compile("^a"),
            }
        )
        invalid = schema.validate(
            {
                "small": BsonInt64(3),
                "large": BsonInt32(3),
                "ratio": BsonDouble(3.5),
                "amount": BsonDecimal128(decimal.Decimal("1.5")),
                "oid": "not-an-object-id",
                "created_at": "2025-01-01",
                "payload": {"bytes": True},
                "matcher": ".*",
            }
        )

        self.assertTrue(valid.valid)
        self.assertFalse(invalid.valid)
        rendered = [issue.render() for issue in invalid.issues]
        self.assertTrue(any("type mismatch" in message for message in rendered))
        self.assertTrue(any("greater than maximum" in message for message in rendered))
        self.assertTrue(any("less than minimum" in message for message in rendered))

    def test_json_schema_rejects_invalid_schema_shapes(self):
        for schema in (
            {"type": 1},
            {"properties": []},
            {"required": "name"},
            {"additionalProperties": "yes"},
            {"items": []},
            {"minItems": -1},
            {"pattern": 1},
            {"minimum": "x"},
            {"not": []},
            {"anyOf": []},
            {"allOf": [1]},
            {"oneOf": "bad"},
        ):
            with self.subTest(schema=schema):
                with self.assertRaises(OperationFailure):
                    CompiledJsonSchema(schema)

    def test_collection_validator_supports_off_level_and_warn_action(self):
        validator = compile_collection_validator(
            {
                "validator": {"$jsonSchema": {"required": ["name"]}},
                "validationLevel": "off",
                "validationAction": "warn",
            }
        )

        assert validator is not None
        result = validator.validate_document({"_id": "1"})

        self.assertTrue(result.valid)
        self.assertEqual(validator.validation_action, "warn")

    def test_collection_validator_query_expression_and_json_schema_both_contribute_issues(self):
        validator = compile_collection_validator(
            {
                "validator": {
                    "tenant": "a",
                    "$jsonSchema": {
                        "properties": {
                            "name": {"bsonType": "string"},
                        }
                    },
                }
            }
        )

        assert validator is not None
        result = validator.validate_document({"tenant": "b", "name": 3})

        self.assertFalse(result.valid)
        rendered = [issue.render() for issue in result.issues]
        self.assertTrue(any("validator expression" in message for message in rendered))
        self.assertTrue(any("type mismatch" in message for message in rendered))

    def test_schema_validation_helper_branches_cover_private_validation_paths(self):
        schema = CompiledJsonSchema({"type": "string"})
        self.assertEqual(schema.validate("Ada").first_message, "document passed validation")

        with self.assertRaisesRegex(OperationFailure, "enum must be a list"):
            schema._validate_schema({"enum": "bad"}, "Ada", ())
        with self.assertRaisesRegex(OperationFailure, "required must be a list of strings"):
            schema._validate_object({"required": "name"}, {}, ())
        with self.assertRaisesRegex(OperationFailure, "properties must be a document"):
            schema._validate_object({"properties": []}, {}, ())
        with self.assertRaisesRegex(OperationFailure, "property definitions must be documents"):
            schema._validate_object({"properties": {1: {}}}, {}, ())
        with self.assertRaisesRegex(OperationFailure, "additionalProperties must be a bool"):
            schema._validate_object({"additionalProperties": "yes"}, {}, ())
        with self.assertRaisesRegex(OperationFailure, "minItems must be a non-negative integer"):
            schema._validate_array({"minItems": -1}, [], ())
        with self.assertRaisesRegex(OperationFailure, "maxItems must be a non-negative integer"):
            schema._validate_array({"maxItems": False}, [], ())
        with self.assertRaisesRegex(OperationFailure, "items must be a document"):
            schema._validate_array({"items": []}, [], ())
        with self.assertRaisesRegex(OperationFailure, "minLength must be a non-negative integer"):
            schema._validate_string({"minLength": -1}, "Ada", ())
        with self.assertRaisesRegex(OperationFailure, "maxLength must be a non-negative integer"):
            schema._validate_string({"maxLength": False}, "Ada", ())
        with self.assertRaisesRegex(OperationFailure, "pattern must be a string"):
            schema._validate_string({"pattern": 1}, "Ada", ())
        with self.assertRaisesRegex(OperationFailure, "minimum must be numeric"):
            schema._validate_numeric({"minimum": "bad"}, 3, ())

        with self.assertRaisesRegex(OperationFailure, "entries must be strings"):
            schema._normalize_type_names([1], field_name="type")
        with self.assertRaisesRegex(OperationFailure, "not supported"):
            schema._normalize_type_names(["future"], field_name="type")

        self.assertIsNone(schema._as_decimal(BsonDouble(float("nan"))))
        self.assertIsNone(schema._as_decimal(float("inf")))
        self.assertTrue(schema._matches_type(BsonDouble(1.5), "double"))
        self.assertTrue(schema._matches_type(BsonInt64(2**40), "long"))
        self.assertTrue(schema._matches_type(BsonInt32(3), "number"))
        self.assertTrue(schema._matches_type(Binary(b"x", subtype=0), "binData"))
        self.assertTrue(schema._matches_type(Regex("^a"), "regex"))
        self.assertFalse(schema._matches_type(object(), "future"))

    def test_compile_collection_validator_helper_branches_cover_none_and_invalid_shapes(self):
        self.assertIsNone(compile_collection_validator(None))
        self.assertIsNone(compile_collection_validator({}))
        self.assertIsNone(compile_collection_validator({"validator": None}))

        with self.assertRaisesRegex(OperationFailure, "collection validator must be a document"):
            compile_collection_validator({"validator": []})
        with self.assertRaisesRegex(OperationFailure, "validationLevel must be"):
            compile_collection_validator({"validator": {}, "validationLevel": "bad"})
        with self.assertRaisesRegex(OperationFailure, "validationAction must be"):
            compile_collection_validator({"validator": {}, "validationAction": "bad"})
        with self.assertRaisesRegex(OperationFailure, "\\$jsonSchema validator must be a document"):
            compile_collection_validator({"validator": {"$jsonSchema": []}})

        validator = compile_collection_validator(
            {
                "validator": {"name": "Ada"},
                "validationLevel": "moderate",
            }
        )
        assert validator is not None
        self.assertFalse(
            validator.should_validate(
                original_document={"name": "Grace"},
                is_upsert_insert=False,
            )
        )

    def test_json_schema_issue_and_decimal_helper_branches(self):
        schema = CompiledJsonSchema(
            {
                "properties": {
                    "tags": {"minItems": 2},
                    "name": {"maxLength": 3},
                },
                "anyOf": [
                    {"properties": {"kind": {"enum": ["user"]}}},
                    {"properties": {"kind": {"enum": ["admin"]}}},
                ],
            }
        )

        result = schema.validate({"tags": ["only"], "name": "Grace", "kind": "guest"})
        rendered = [issue.render() for issue in result.issues]
        self.assertTrue(any("fewer than 2 items" in message for message in rendered))
        self.assertTrue(any("longer than 3" in message for message in rendered))
        self.assertTrue(any("does not satisfy anyOf" in message for message in rendered))
        self.assertIsNone(schema._as_decimal(object()))
