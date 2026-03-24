import unittest

from mongoeco.core.operators import UpdateEngine
from mongoeco.errors import OperationFailure


class UpdateEngineTests(unittest.TestCase):
    def test_set_none_creates_missing_field(self):
        document = {}

        modified = UpdateEngine.apply_update(document, {"$set": {"field": None}})

        self.assertTrue(modified)
        self.assertEqual(document, {"field": None})

    def test_unknown_operator_raises_operation_failure(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"arr": []}, {"$push": {"arr": 1}})

    def test_set_cannot_modify_immutable_id(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"_id": "old", "name": "Ada"}, {"$set": {"_id": "new"}})

    def test_unset_cannot_modify_immutable_id(self):
        with self.assertRaises(OperationFailure):
            UpdateEngine.apply_update({"_id": "old", "name": "Ada"}, {"$unset": {"_id": ""}})

    def test_set_same_value_is_noop(self):
        document = {"field": 1}

        modified = UpdateEngine.apply_update(document, {"$set": {"field": 1}})

        self.assertFalse(modified)
        self.assertEqual(document, {"field": 1})

    def test_set_nested_replaces_non_dict_parent(self):
        document = {"profile": "invalid"}

        modified = UpdateEngine.apply_update(document, {"$set": {"profile.name": "Ada"}})

        self.assertTrue(modified)
        self.assertEqual(document, {"profile": {"name": "Ada"}})

    def test_unset_existing_and_missing_fields(self):
        document = {"profile": {"name": "Ada"}, "role": "admin"}

        modified_existing = UpdateEngine.apply_update(document, {"$unset": {"profile.name": ""}})
        modified_missing = UpdateEngine.apply_update(document, {"$unset": {"profile.age": ""}})

        self.assertTrue(modified_existing)
        self.assertFalse(modified_missing)
        self.assertEqual(document, {"profile": {}, "role": "admin"})

    def test_unset_nested_path_returns_false_when_parent_is_missing(self):
        self.assertFalse(UpdateEngine.apply_update({}, {"$unset": {"profile.name": ""}}))

    def test_set_supports_numeric_segments_for_arrays(self):
        document = {"tags": ["old", "keep"]}

        modified = UpdateEngine.apply_update(document, {"$set": {"tags.0": "new"}})

        self.assertTrue(modified)
        self.assertEqual(document, {"tags": ["new", "keep"]})

    def test_unset_supports_numeric_segments_for_arrays(self):
        document = {"tags": ["old", "keep"]}

        modified = UpdateEngine.apply_update(document, {"$unset": {"tags.0": ""}})

        self.assertTrue(modified)
        self.assertEqual(document, {"tags": [None, "keep"]})
