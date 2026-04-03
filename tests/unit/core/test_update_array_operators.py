import unittest

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.update_array_operators import apply_push_values, normalize_push_modifiers
from mongoeco.errors import OperationFailure


class UpdateArrayOperatorsTests(unittest.TestCase):
    def test_normalize_push_modifiers_rejects_unsupported_field_and_bad_sort_spec(self):
        with self.assertRaisesRegex(OperationFailure, "supports the \\$each, \\$position, \\$slice and \\$sort modifiers"):
            normalize_push_modifiers({"$each": [], "$bad": 1})
        with self.assertRaisesRegex(OperationFailure, "\\$push \\$sort fields must be strings"):
            normalize_push_modifiers({"$each": [], "$sort": {1: 1}})
        with self.assertRaisesRegex(OperationFailure, "\\$push \\$sort directions must be 1 or -1"):
            normalize_push_modifiers({"$each": [], "$sort": {"score": 0}})

    def test_apply_push_values_covers_noop_and_document_sort_validation(self):
        current = [1, 2]
        self.assertFalse(
            apply_push_values(
                current,
                [],
                position=None,
                slice_value=None,
                sort_spec=None,
                dialect=MONGODB_DIALECT_70,
            )
        )
        with self.assertRaisesRegex(OperationFailure, "requires array elements to be documents"):
            apply_push_values(
                [1, 2],
                [],
                position=None,
                slice_value=None,
                sort_spec=[("score", 1)],
                dialect=MONGODB_DIALECT_70,
            )
