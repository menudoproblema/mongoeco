import unittest

from mongoeco.errors import DuplicateKeyError, ExecutionTimeout, OperationFailure


class ErrorCatalogUnitTests(unittest.TestCase):
    def test_error_catalog_assigns_stable_default_codes(self):
        duplicate = DuplicateKeyError("duplicate key")
        timeout = ExecutionTimeout("operation exceeded time limit")

        self.assertEqual(duplicate.code, 11000)
        self.assertIsNone(duplicate.details)
        self.assertEqual(timeout.code, 50)
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
