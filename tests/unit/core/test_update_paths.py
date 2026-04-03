import unittest

from mongoeco.core.update_paths import (
    CompiledUpdatePath,
    UpdatePathSegment,
    compile_update_path,
    parse_update_path,
    ResolvedUpdatePath,
    expand_positional_update_paths,
    resolve_positional_update_paths,
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

    def test_resolve_positional_update_paths_returns_typed_targets(self):
        resolved = resolve_positional_update_paths(
            {"items": [{"name": "Ada"}, {"name": "Linus"}]},
            "items.$[].name",
            filtered_matcher=lambda _identifier, _candidate: True,
        )

        self.assertEqual(
            resolved,
            [
                ResolvedUpdatePath(
                    requested=compile_update_path("items.$[].name"),
                    concrete_path="items.0.name",
                ),
                ResolvedUpdatePath(
                    requested=compile_update_path("items.$[].name"),
                    concrete_path="items.1.name",
                ),
            ],
        )

    def test_parse_update_path_rejects_empty_segments_and_invalid_identifiers(self):
        with self.assertRaises(OperationFailure):
            parse_update_path(1)  # type: ignore[arg-type]
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

    def test_expand_positional_update_paths_private_branches_cover_legacy_and_invalid_segments(self):
        with self.assertRaises(OperationFailure):
            expand_positional_update_paths(
                {"items": [1]},
                "items.$.name",
                filtered_matcher=lambda _identifier, _candidate: True,
            )

        self.assertEqual(
            expand_positional_update_paths(
                {"items": "not-a-list"},
                "items.$[].name",
                filtered_matcher=lambda _identifier, _candidate: True,
            ),
            [],
        )
        self.assertEqual(
            expand_positional_update_paths(
                {"items": "not-a-list"},
                "items.$[entry].name",
                filtered_matcher=lambda _identifier, _candidate: True,
            ),
            [],
        )

        bad_path = CompiledUpdatePath(
            raw="items.bogus.$[]",
            segments=(
                UpdatePathSegment("field", "items"),
                UpdatePathSegment("bogus", "bogus"),  # type: ignore[arg-type]
                UpdatePathSegment("all_positional", "$[]"),
            ),
        )
        with self.assertRaises(AssertionError):
            expand_positional_update_paths(
                {"items": [{"name": "Ada"}]},
                bad_path,
                filtered_matcher=lambda _identifier, _candidate: True,
            )

    def test_expand_positional_update_paths_covers_numeric_index_segments(self):
        self.assertEqual(
            expand_positional_update_paths(
                {"items": [{"tags": ["a"]}, {"tags": ["b", "c"]}]},
                "items.1.tags.0",
                filtered_matcher=lambda _identifier, _candidate: True,
            ),
            ["items.1.tags.0"],
        )
        self.assertEqual(
            expand_positional_update_paths(
                {"items": [{"tags": ["a"]}]},
                "items.3.tags.0",
                filtered_matcher=lambda _identifier, _candidate: True,
            ),
            ["items.3.tags.0"],
        )
        compiled = CompiledUpdatePath(
            raw="items.2.tags.0",
            segments=(
                UpdatePathSegment("field", "items"),
                UpdatePathSegment("index", "2", index=2),
                UpdatePathSegment("field", "tags"),
                UpdatePathSegment("index", "0", index=0),
            ),
        )
        self.assertEqual(
            expand_positional_update_paths(
                {"items": [{"tags": ["a"]}]},
                compiled,
                filtered_matcher=lambda _identifier, _candidate: True,
            ),
            ["items.2.tags.0"],
        )

    def test_expand_positional_update_paths_walks_index_segments_after_positional_expansion(self):
        self.assertEqual(
            expand_positional_update_paths(
                {"items": [{"tags": ["a", "b"]}, {"tags": ["c"]}]},
                "items.$[].tags.1",
                filtered_matcher=lambda _identifier, _candidate: True,
            ),
            ["items.0.tags.1", "items.1.tags.1"],
        )
