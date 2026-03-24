import unittest

from mongoeco.core.paths import delete_document_value, set_document_value


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
