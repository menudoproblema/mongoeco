import unittest

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.schema_validation import compile_collection_validator
from mongoeco.errors import OperationFailure


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
