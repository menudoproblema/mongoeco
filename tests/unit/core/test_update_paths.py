import unittest

from mongoeco.core.update_paths import (
    compile_update_path,
    parse_update_path,
    update_path_has_numeric_segment,
    update_path_has_positional_segment,
)
from mongoeco.errors import OperationFailure


class UpdatePathParsingTests(unittest.TestCase):
    def test_parse_update_path_classifies_field_index_and_positional_segments(self):
        parsed = parse_update_path("items.$[entry].tags.$[].0.name")

        self.assertEqual(
            [(segment.kind, segment.raw, segment.index, segment.identifier) for segment in parsed],
            [
                ("field", "items", None, None),
                ("filtered_positional", "$[entry]", None, "entry"),
                ("field", "tags", None, None),
                ("all_positional", "$[]", None, None),
                ("index", "0", 0, None),
                ("field", "name", None, None),
            ],
        )

    def test_compile_update_path_preserves_raw_string(self):
        compiled = compile_update_path("items.$[].name")

        self.assertEqual(compiled.raw, "items.$[].name")
        self.assertEqual(
            [segment.kind for segment in compiled.segments],
            ["field", "all_positional", "field"],
        )

    def test_update_path_helpers_detect_numeric_and_positional_segments(self):
        compiled = compile_update_path("items.$[].0.name")

        self.assertTrue(update_path_has_numeric_segment("items.0.name"))
        self.assertFalse(update_path_has_numeric_segment("items.name"))
        self.assertTrue(update_path_has_numeric_segment(compiled))
        self.assertTrue(update_path_has_positional_segment("items.$[entry].name"))
        self.assertTrue(update_path_has_positional_segment("items.$[].name"))
        self.assertTrue(update_path_has_positional_segment(compiled))
        self.assertFalse(update_path_has_positional_segment("items.0.name"))

    def test_parse_update_path_rejects_empty_segments_and_invalid_identifiers(self):
        with self.assertRaises(OperationFailure):
            parse_update_path("")
        with self.assertRaises(OperationFailure):
            parse_update_path("items..name")
        with self.assertRaises(OperationFailure):
            parse_update_path("items.$[].")
        with self.assertRaises(OperationFailure):
            parse_update_path("items.$[_bad].name")
        with self.assertRaises(OperationFailure):
            parse_update_path("items.$[1bad].name")
