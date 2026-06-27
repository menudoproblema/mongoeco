import unittest

from mongoeco.core.identity import (
    assert_document_kept_storage_key,
    assert_document_matches_storage_key,
    assert_document_matches_stored_lookup,
    canonical_document_id,
)
from mongoeco.errors import WriteError


class _Dialect:
    @staticmethod
    def values_equal(left, right):
        return left == right


def _storage_key_for_id(value):
    return repr(canonical_document_id(value))


class IdentityTests(unittest.TestCase):
    def test_canonical_document_id_supports_dicts_lists_and_unhashable_values(self):
        self.assertEqual(
            canonical_document_id({"tenant": [1, 2]}),
            ("dict", (("tenant", ("list", ((int, 1), (int, 2)))),)),
        )

        class UnhashableValue:
            __hash__ = None

            def __repr__(self) -> str:
                return "unhashable"

        self.assertEqual(canonical_document_id(UnhashableValue()), ("repr", "unhashable"))

    def test_canonical_document_id_distinguishes_bool_and_int_inside_compound_values(self):
        self.assertNotEqual(canonical_document_id(True), canonical_document_id(1))
        self.assertNotEqual(canonical_document_id({"a": True}), canonical_document_id({"a": 1}))
        self.assertNotEqual(canonical_document_id([True]), canonical_document_id([1]))

    def test_document_matches_storage_key_accepts_stable_selected_documents(self):
        assert_document_matches_storage_key(
            {"_id": {"tenant": [1]}, "name": "Ada"},
            _storage_key_for_id({"tenant": [1]}),
            storage_key_for_id=_storage_key_for_id,
        )
        assert_document_matches_storage_key(
            {"name": "legacy"},
            _storage_key_for_id(None),
            storage_key_for_id=_storage_key_for_id,
        )

    def test_document_matches_storage_key_rejects_array_id_before_mismatch(self):
        with self.assertRaises(WriteError) as context:
            assert_document_matches_storage_key(
                {"_id": [1]},
                _storage_key_for_id("other"),
                storage_key_for_id=_storage_key_for_id,
            )

        self.assertEqual(context.exception.code, 53)

    def test_document_matches_storage_key_rejects_mismatched_storage_key(self):
        with self.assertRaises(WriteError) as context:
            assert_document_matches_storage_key(
                {"_id": "new"},
                _storage_key_for_id("old"),
                storage_key_for_id=_storage_key_for_id,
            )

        self.assertEqual(context.exception.code, 66)
        self.assertIn("does not match its storage key", str(context.exception))

    def test_document_matches_stored_lookup_uses_same_selected_document_errors(self):
        assert_document_matches_stored_lookup(
            {"_id": "same", "name": "Ada"},
            {"_id": "same", "name": "Ada"},
            dialect=_Dialect(),
        )

        with self.assertRaises(WriteError) as missing_context:
            assert_document_matches_stored_lookup(
                {"_id": "same", "name": "Ada"},
                None,
                dialect=_Dialect(),
            )
        self.assertEqual(missing_context.exception.code, 66)

        with self.assertRaises(WriteError) as array_context:
            assert_document_matches_stored_lookup(
                {"_id": [1], "name": "Ada"},
                {"_id": [1], "name": "Ada"},
                dialect=_Dialect(),
            )
        self.assertEqual(array_context.exception.code, 53)

    def test_document_kept_storage_key_rejects_update_retarget(self):
        assert_document_kept_storage_key(
            {"_id": "same", "name": "Ada"},
            _storage_key_for_id("same"),
            storage_key_for_id=_storage_key_for_id,
        )

        with self.assertRaises(WriteError) as context:
            assert_document_kept_storage_key(
                {"_id": "new", "name": "Ada"},
                _storage_key_for_id("old"),
                storage_key_for_id=_storage_key_for_id,
            )

        self.assertEqual(context.exception.code, 66)
        self.assertIn("immutable", str(context.exception))
