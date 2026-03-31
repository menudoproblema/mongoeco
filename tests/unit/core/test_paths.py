import unittest
import math

import mongoeco.core.paths as paths_module
from mongoeco.core.paths import delete_document_value, get_document_value, set_document_value
from mongoeco.errors import OperationFailure
from mongoeco.types import DBRef


class PathHelpersTests(unittest.TestCase):
    def test_set_document_value_returns_false_for_non_numeric_list_leaf_segment(self):
        document = []

        changed = set_document_value(document, "name", "Ada")

        self.assertFalse(changed)
        self.assertEqual(document, [])

    def test_set_document_value_extends_list_and_detects_noop_on_leaf_assignment(self):
        document = []

        changed = set_document_value(document, "2", "Ada")
        unchanged = set_document_value(document, "2", "Ada")

        self.assertTrue(changed)
        self.assertFalse(unchanged)
        self.assertEqual(document, [None, None, "Ada"])

    def test_set_document_value_distinguishes_bool_and_number(self):
        document = {"flag": 1, "items": [1]}

        changed_dict = set_document_value(document, "flag", True)
        changed_list = set_document_value(document["items"], "0", True)

        self.assertTrue(changed_dict)
        self.assertTrue(changed_list)
        self.assertEqual(document, {"flag": True, "items": [True]})

    def test_set_document_value_distinguishes_int_and_float(self):
        document = {"count": 1, "items": [1]}

        changed_dict = set_document_value(document, "count", 1.0)
        changed_list = set_document_value(document["items"], "0", 1.0)

        self.assertTrue(changed_dict)
        self.assertTrue(changed_list)
        self.assertEqual(document, {"count": 1.0, "items": [1.0]})

    def test_set_document_value_treats_reordered_subdocuments_as_changes(self):
        document = {
            "profile": {"kind": "a", "qty": 1},
            "items": [{"kind": "a", "qty": 1}],
        }

        changed_dict = set_document_value(document, "profile", {"qty": 1, "kind": "a"})
        changed_list = set_document_value(document["items"], "0", {"qty": 1, "kind": "a"})

        self.assertTrue(changed_dict)
        self.assertTrue(changed_list)
        self.assertEqual(
            document,
            {
                "profile": {"qty": 1, "kind": "a"},
                "items": [{"qty": 1, "kind": "a"}],
            },
        )

    def test_set_document_value_returns_false_for_equal_nested_documents_and_lists(self):
        document = {
            "profile": {"kind": "a", "qty": 1},
            "items": [{"kind": "a", "qty": 1}],
        }

        changed_dict = set_document_value(document, "profile", {"kind": "a", "qty": 1})
        changed_list = set_document_value(document["items"], "0", {"kind": "a", "qty": 1})
        changed_whole_list = set_document_value(document, "items", [{"kind": "a", "qty": 1}])

        self.assertFalse(changed_dict)
        self.assertFalse(changed_list)
        self.assertFalse(changed_whole_list)

    def test_set_document_value_treats_nan_as_unchanged(self):
        document = {"value": math.nan, "items": [math.nan]}

        changed_scalar = set_document_value(document, "value", math.nan)
        changed_list = set_document_value(document["items"], "0", math.nan)

        self.assertFalse(changed_scalar)
        self.assertFalse(changed_list)

    def test_set_document_value_returns_false_for_non_numeric_nested_list_segment(self):
        document = []

        changed = set_document_value(document, "name.value", "Ada")

        self.assertFalse(changed)
        self.assertEqual(document, [])

    def test_set_document_value_extends_nested_list_and_creates_container(self):
        document = []

        changed = set_document_value(document, "2.name", "Ada")

        self.assertTrue(changed)
        self.assertEqual(document, [None, None, {"name": "Ada"}])

    def test_set_document_value_handles_deep_array_path_with_gap_expansion(self):
        document = {"items": []}

        changed = set_document_value(document, "items.2.name", "Ada")

        self.assertTrue(changed)
        self.assertEqual(document, {"items": [None, None, {"name": "Ada"}]})

    def test_set_document_value_creates_nested_documents_for_missing_numeric_segments_under_dicts(self):
        document = {}

        changed = set_document_value(document, "items.2.name", "Ada")

        self.assertTrue(changed)
        self.assertEqual(document, {"items": {"2": {"name": "Ada"}}})

    def test_set_document_value_rejects_crossing_scalar_parent(self):
        with self.assertRaises(OperationFailure):
            set_document_value({"profile": 1}, "profile.name", "Ada")

        with self.assertRaises(OperationFailure):
            set_document_value([1], "0.name", "Ada")

    def test_set_document_value_rejects_array_indexes_beyond_limit(self):
        with self.assertRaises(OperationFailure):
            set_document_value([], "10001", "Ada")

        with self.assertRaises(OperationFailure):
            set_document_value({"items": []}, "items.10001.name", "Ada")

    def test_max_array_index_is_configurable(self):
        original_limit = paths_module.get_max_array_index()
        try:
            paths_module.set_max_array_index(2)
            with self.assertRaises(OperationFailure):
                set_document_value([], "3", "Ada")
            changed = set_document_value([], "2", "Ada")
            self.assertTrue(changed)
            self.assertEqual(paths_module.get_max_array_index(), 2)
        finally:
            paths_module.set_max_array_index(original_limit)

    def test_set_max_array_index_rejects_invalid_values(self):
        with self.assertRaises(ValueError):
            paths_module.set_max_array_index(-1)
        with self.assertRaises(ValueError):
            paths_module.set_max_array_index(True)
        with self.assertRaises(ValueError):
            paths_module.set_max_array_index(1.5)

    def test_delete_document_value_handles_list_leaf_miss_and_existing_none(self):
        document = [None]

        missing = delete_document_value(document, "5")
        already_none = delete_document_value(document, "0")

        self.assertFalse(missing)
        self.assertFalse(already_none)
        self.assertEqual(document, [None])

    def test_delete_document_value_returns_false_for_non_numeric_nested_list_segment(self):
        document = [{"name": "Ada"}]

        changed = delete_document_value(document, "x.name")

        self.assertFalse(changed)
        self.assertEqual(document, [{"name": "Ada"}])

    def test_delete_document_value_descends_into_nested_list_values(self):
        document = [{"name": "Ada"}]

        changed = delete_document_value(document, "0.name")

        self.assertTrue(changed)
        self.assertEqual(document, [{}])

    def test_delete_document_value_returns_false_for_scalar_nested_list_value(self):
        document = [1]

        changed = delete_document_value(document, "0.name")

        self.assertFalse(changed)
        self.assertEqual(document, [1])

    def test_delete_document_value_returns_false_for_out_of_range_array_path(self):
        document = {"items": ["a"]}

        changed = delete_document_value(document, "items.3")

        self.assertFalse(changed)
        self.assertEqual(document, {"items": ["a"]})

    def test_get_document_value_supports_dicts_lists_and_missing_paths(self):
        document = {"profile": {"name": "Ada"}, "items": [{"name": "Grace"}]}

        self.assertEqual(get_document_value(document, "profile.name"), (True, "Ada"))
        self.assertEqual(get_document_value(["Ada"], "0"), (True, "Ada"))
        self.assertEqual(get_document_value(document, "items.0.name"), (True, "Grace"))
        self.assertEqual(get_document_value([1], "0.name"), (False, None))
        self.assertEqual(get_document_value(document, "items.2.name"), (False, None))
        self.assertEqual(get_document_value(document, "items.x"), (False, None))

    def test_get_document_value_supports_dbref_subfields_and_extras(self):
        document = {
            "author": DBRef(
                "users",
                "ada",
                database="observe",
                extras={"tenant": "t1", "meta": {"region": "eu"}},
            )
        }

        self.assertEqual(get_document_value(document, "author.$ref"), (True, "users"))
        self.assertEqual(get_document_value(document, "author.$id"), (True, "ada"))
        self.assertEqual(get_document_value(document, "author.$db"), (True, "observe"))
        self.assertEqual(get_document_value(document, "author.tenant"), (True, "t1"))
        self.assertEqual(get_document_value(document, "author.meta.region"), (True, "eu"))
