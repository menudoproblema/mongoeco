import builtins
import importlib.util
from pathlib import Path
import sys
import unittest

from mongoeco.error_catalog import (
    DUPLICATE_KEY_ERROR,
    EXECUTION_TIMEOUT_ERROR,
    SERVER_SELECTION_TIMEOUT_ERROR,
    build_error_metadata,
    descriptor_for,
)
import mongoeco.errors as errors_module
from mongoeco.errors import (
    BulkWriteError,
    CollectionInvalid,
    ConnectionFailure,
    DocumentValidationFailure,
    DuplicateKeyError,
    ExecutionTimeout,
    InvalidOperation,
    OperationFailure,
    PyMongoError,
    ServerSelectionTimeoutError,
    WriteError,
)

try:
    import pymongo.errors as pymongo_errors
except Exception:  # pragma: no cover - pymongo is optional
    pymongo_errors = None


def _load_errors_module_without_pymongo():
    module_name = "mongoeco_errors_no_pymongo_test"
    if module_name in sys.modules:
        del sys.modules[module_name]
    module_path = Path(__file__).resolve().parents[2] / "src" / "mongoeco" / "errors.py"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive guard
        raise AssertionError("could not load mongoeco.errors module spec")
    module = importlib.util.module_from_spec(spec)
    original_import = builtins.__import__

    def _blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "pymongo" or name.startswith("pymongo."):
            raise ImportError("blocked pymongo import for fallback test")
        return original_import(name, globals, locals, fromlist, level)

    try:
        builtins.__import__ = _blocked_import
        spec.loader.exec_module(module)
    finally:
        builtins.__import__ = original_import
    return module


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

    def test_public_error_types_preserve_error_labels(self):
        cases = [
            (PyMongoError("bad", error_labels=("RetryableWriteError",)), ("RetryableWriteError",)),
            (ConnectionFailure("down", error_labels=("TransientTransactionError",)), ("TransientTransactionError",)),
            (InvalidOperation("invalid", error_labels=("RetryableWriteError",)), ("RetryableWriteError",)),
            (CollectionInvalid("invalid coll", error_labels=("RetryableWriteError",)), ("RetryableWriteError",)),
        ]

        for error, expected_labels in cases:
            with self.subTest(error=type(error).__name__):
                self.assertTrue(hasattr(error, "error_labels"))
                self.assertEqual(tuple(error.error_labels), expected_labels)

    def test_server_selection_timeout_exposes_catalog_metadata(self):
        error = ServerSelectionTimeoutError(
            "timed out",
            errors={"localhost:27017": "down"},
        )

        self.assertEqual(error.code, SERVER_SELECTION_TIMEOUT_ERROR.code)
        self.assertEqual(error.code_name, SERVER_SELECTION_TIMEOUT_ERROR.code_name)
        self.assertEqual(error.details, {"localhost:27017": "down"})
        self.assertEqual(error.errors, {"localhost:27017": "down"})
        self.assertEqual(tuple(error.error_labels), SERVER_SELECTION_TIMEOUT_ERROR.error_labels)

    def test_bulk_write_error_uses_details_labels_and_optional_code(self):
        error = BulkWriteError(
            "bulk failed",
            code=91,
            details={
                "writeErrors": [],
                "writeConcernErrors": [],
                "errorLabels": ["RetryableWriteError"],
            },
        )

        self.assertEqual(error.code, 91)
        self.assertEqual(
            tuple(error.error_labels),
            ("RetryableWriteError",),
        )

    def test_errors_fallback_without_pymongo_preserves_message_and_labels(self):
        fallback = _load_errors_module_without_pymongo()
        cases = [
            fallback.PyMongoError("bad", error_labels=("RetryableWriteError",)),
            fallback.ConnectionFailure("down", error_labels=("TransientTransactionError",)),
            fallback.InvalidOperation("invalid", error_labels=("RetryableWriteError",)),
            fallback.CollectionInvalid("invalid coll", error_labels=("RetryableWriteError",)),
        ]

        self.assertEqual(str(cases[0]), "bad")
        self.assertEqual(tuple(cases[0].error_labels), ("RetryableWriteError",))
        self.assertEqual(str(cases[1]), "down")
        self.assertEqual(tuple(cases[1].error_labels), ("TransientTransactionError",))
        self.assertEqual(str(cases[2]), "invalid")
        self.assertEqual(str(cases[3]), "invalid coll")

    def test_operation_failure_like_fallback_without_pymongo_sets_metadata(self):
        fallback = _load_errors_module_without_pymongo()
        error = fallback.OperationFailure(
            "failed",
            code=42,
            details={"stage": "find"},
            error_labels=("RetryableWriteError",),
        )

        self.assertEqual(str(error), "failed")
        self.assertEqual(error.code, 42)
        self.assertIsNone(error.code_name)
        self.assertEqual(
            error.details,
            {
                "stage": "find",
                "errorLabels": ["RetryableWriteError"],
            },
        )
        self.assertEqual(tuple(error.error_labels), ("RetryableWriteError",))

    def test_server_selection_timeout_fallback_without_pymongo_sets_details(self):
        fallback = _load_errors_module_without_pymongo()
        error = fallback.ServerSelectionTimeoutError(
            "timed out",
            errors={"localhost:27017": "down"},
        )

        self.assertEqual(str(error), "timed out")
        self.assertEqual(error.details, {"localhost:27017": "down"})
        self.assertEqual(error.errors, {"localhost:27017": "down"})

    def test_bulk_write_error_fallback_without_pymongo_uses_write_error_path(self):
        fallback = _load_errors_module_without_pymongo()
        error = fallback.BulkWriteError(
            "bulk failed",
            details={"writeErrors": []},
            error_labels=("RetryableWriteError",),
        )

        self.assertEqual(str(error), "bulk failed")
        self.assertEqual(error.code, 65)
        self.assertIsNone(error.code_name)
        self.assertEqual(
            error.details,
            {"writeErrors": [], "errorLabels": ["RetryableWriteError"]},
        )
        self.assertEqual(tuple(error.error_labels), ("RetryableWriteError",))

    def test_document_validation_failure_uses_document_validation_descriptor(self):
        error = DocumentValidationFailure("invalid document")

        self.assertEqual(error.code, 121)
        self.assertEqual(error.code_name, "DocumentValidationFailure")

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

    @unittest.skipIf(pymongo_errors is None, "pymongo is not installed")
    def test_errors_are_compatible_with_matching_pymongo_exceptions(self):
        cases = [
            (PyMongoError("bad"), pymongo_errors.PyMongoError),
            (ConnectionFailure("down"), pymongo_errors.ConnectionFailure),
            (ServerSelectionTimeoutError("timeout"), pymongo_errors.ServerSelectionTimeoutError),
            (InvalidOperation("invalid"), pymongo_errors.InvalidOperation),
            (CollectionInvalid("invalid coll"), pymongo_errors.CollectionInvalid),
            (OperationFailure("failed", code=42, details={"stage": "find"}), pymongo_errors.OperationFailure),
            (WriteError("write failed", code=64, details={"path": "email"}), pymongo_errors.WriteError),
            (DuplicateKeyError("duplicate key"), pymongo_errors.DuplicateKeyError),
            (BulkWriteError("bulk failed", details={"writeErrors": [], "writeConcernErrors": []}), pymongo_errors.BulkWriteError),
            (ExecutionTimeout("timed out"), pymongo_errors.ExecutionTimeout),
        ]

        for error, expected_type in cases:
            with self.subTest(error=type(error).__name__):
                self.assertIsInstance(error, expected_type)
