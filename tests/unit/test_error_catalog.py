import unittest

from mongoeco.error_catalog import (
    DUPLICATE_KEY_ERROR,
    EXECUTION_TIMEOUT_ERROR,
    build_error_metadata,
    descriptor_for,
)
from mongoeco.errors import DuplicateKeyError, ExecutionTimeout, OperationFailure


class ErrorCatalogUnitTests(unittest.TestCase):
    def test_error_catalog_assigns_stable_default_codes(self):
        duplicate = DuplicateKeyError("duplicate key")
        timeout = ExecutionTimeout("operation exceeded time limit")

        self.assertEqual(duplicate.code, 11000)
        self.assertEqual(duplicate.code_name, "DuplicateKey")
        self.assertIsNone(duplicate.details)
        self.assertEqual(timeout.code, 50)
        self.assertEqual(timeout.code_name, "MaxTimeMSExpired")
        self.assertIsNone(timeout.details)

    def test_error_catalog_merges_error_labels_into_details(self):
        failure = OperationFailure(
            "write concern failed",
            error_labels=("RetryableWriteError",),
        )

        self.assertEqual(
            failure.details,
            {"errorLabels": ["RetryableWriteError"]},
        )

    def test_error_catalog_exposes_descriptors_by_name(self):
        self.assertIs(descriptor_for("DuplicateKeyError"), DUPLICATE_KEY_ERROR)
        self.assertIs(descriptor_for("ExecutionTimeout"), EXECUTION_TIMEOUT_ERROR)

    def test_build_error_metadata_merges_code_name_and_unique_labels(self):
        code, code_name, details = build_error_metadata(
            DUPLICATE_KEY_ERROR,
            details={"keyPattern": {"email": 1}},
            error_labels=("RetryableWriteError", "RetryableWriteError"),
        )

        self.assertEqual(code, 11000)
        self.assertEqual(code_name, "DuplicateKey")
        self.assertEqual(
            details,
            {
                "keyPattern": {"email": 1},
                "codeName": "DuplicateKey",
                "errorLabels": ["RetryableWriteError"],
            },
        )
