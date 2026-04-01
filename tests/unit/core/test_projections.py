import unittest

from mongoeco.compat import MongoDialect
from mongoeco.core.projections import apply_projection, validate_projection_spec
from mongoeco.errors import OperationFailure


class ProjectionTests(unittest.TestCase):
    def test_projection_can_use_custom_dialect_flag_rules(self):
        class _NoBooleanFlagDialect(MongoDialect):
            def projection_flag(self, value: object) -> int | None:
                if isinstance(value, bool):
                    return None
                return super().projection_flag(value)

        with self.assertRaises(OperationFailure):
            apply_projection(
                {"_id": 1, "name": "Val"},
                {"name": True},
                dialect=_NoBooleanFlagDialect(
                    key="test",
                    server_version="test",
                    label="No Bool Projection Flag",
                ),
            )

    def test_inclusion_projection(self):
        doc = {"_id": 1, "name": "Val", "age": 30, "profile": {"city": "Madrid", "job": "Dev"}}

        self.assertEqual(apply_projection(doc, {"name": 1}), {"_id": 1, "name": "Val"})
        self.assertEqual(
            apply_projection(doc, {"profile.city": 1}),
            {"_id": 1, "profile": {"city": "Madrid"}},
        )

    def test_inclusion_projection_supports_dot_notation_through_list_of_documents(self):
        doc = {"_id": 1, "items": [{"name": "Ada", "age": 1}, {"name": "Grace", "age": 2}]}

        self.assertEqual(
            apply_projection(doc, {"items.name": 1}),
            {"_id": 1, "items": [{"name": "Ada"}, {"name": "Grace"}]},
        )

    def test_inclusion_projection_supports_nested_lists(self):
        doc = {"_id": 1, "items": [[{"name": "Ada", "age": 1}], [{"name": "Grace", "age": 2}], []]}

        self.assertEqual(
            apply_projection(doc, {"items.name": 1}),
            {"_id": 1, "items": [[{"name": "Ada"}], [{"name": "Grace"}], []]},
        )

    def test_inclusion_projection_preserves_positions_for_scalar_items_in_arrays(self):
        doc = {"_id": 1, "a": [{"b": 1}, 5, {"b": 2}]}

        self.assertEqual(
            apply_projection(doc, {"a.b": 1}),
            {"_id": 1, "a": [{"b": 1}, {"b": 2}]},
        )

    def test_inclusion_projection_does_not_alias_nested_values(self):
        doc = {"_id": 1, "profile": {"city": "Madrid"}}

        projected = apply_projection(doc, {"profile": 1})
        projected["profile"]["city"] = "Berlin"

        self.assertEqual(doc, {"_id": 1, "profile": {"city": "Madrid"}})

    def test_exclusion_projection_keeps_deepcopied_id_value(self):
        doc = {"_id": {"tenant": "a"}, "name": "Ada"}

        projected = apply_projection(doc, {"name": 0})
        projected["_id"]["tenant"] = "b"

        self.assertEqual(doc, {"_id": {"tenant": "a"}, "name": "Ada"})

    def test_exclusion_projection(self):
        doc = {"_id": 1, "name": "Val", "age": 30, "profile": {"city": "Madrid", "job": "Dev"}}

        self.assertEqual(
            apply_projection(doc, {"age": 0}),
            {"_id": 1, "name": "Val", "profile": {"city": "Madrid", "job": "Dev"}},
        )
        self.assertEqual(
            apply_projection(doc, {"profile.job": 0}),
            {"_id": 1, "name": "Val", "age": 30, "profile": {"city": "Madrid"}},
        )

    def test_exclusion_projection_supports_dot_notation_through_list_of_documents(self):
        doc = {"_id": 1, "items": [{"name": "Ada", "age": 1}, {"name": "Grace", "age": 2}]}

        self.assertEqual(
            apply_projection(doc, {"items.name": 0}),
            {"_id": 1, "items": [{"age": 1}, {"age": 2}]},
        )

    def test_id_projection(self):
        doc = {"_id": 1, "name": "Val"}

        self.assertEqual(apply_projection(doc, {"_id": 1}), {"_id": 1})
        self.assertEqual(apply_projection(doc, {"_id": 0}), {"name": "Val"})
        self.assertEqual(apply_projection(doc, {"name": 1, "_id": 0}), {"name": "Val"})
        self.assertEqual(apply_projection(doc, {"name": 0, "_id": 0}), {})

    def test_excluding_nested_field_keeps_empty_parent_document(self):
        doc = {"_id": 1, "a": {"b": 1}}

        self.assertEqual(apply_projection(doc, {"a.b": 0}), {"_id": 1, "a": {}})

    def test_id_only_exclusion_returns_deep_copy(self):
        doc = {"_id": 1, "profile": {"city": "Madrid"}}

        projected = apply_projection(doc, {"_id": 0})
        projected["profile"]["city"] = "Berlin"

        self.assertEqual(projected, {"profile": {"city": "Berlin"}})
        self.assertEqual(doc, {"_id": 1, "profile": {"city": "Madrid"}})

    def test_empty_projection_returns_full_document(self):
        doc = {"_id": 1, "name": "Ada", "profile": {"city": "Madrid"}}

        projected = apply_projection(doc, {})
        projected["profile"]["city"] = "Berlin"

        self.assertEqual(projected, {"_id": 1, "name": "Ada", "profile": {"city": "Berlin"}})
        self.assertEqual(doc, {"_id": 1, "name": "Ada", "profile": {"city": "Madrid"}})

    def test_projection_rejects_mixed_inclusion_exclusion_and_invalid_flags(self):
        doc = {"_id": 1, "a": 1, "b": 2}

        with self.assertRaises(OperationFailure):
            apply_projection(doc, [("a", 1)])  # type: ignore[arg-type]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"a": 1, "b": 0})
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"a": 2})
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {1: 1})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"score": {"$meta": "textScore"}})  # type: ignore[dict-item]

    def test_projection_supports_slice_as_exclusion_style_projection(self):
        doc = {"_id": 1, "name": "Ada", "items": [1, 2, 3, 4], "role": "admin"}

        self.assertEqual(
            apply_projection(doc, {"items": {"$slice": 2}}),
            {"_id": 1, "name": "Ada", "items": [1, 2], "role": "admin"},
        )
        self.assertEqual(
            apply_projection(doc, {"items": {"$slice": -2}, "role": 0}),
            {"_id": 1, "name": "Ada", "items": [3, 4]},
        )

    def test_projection_supports_slice_as_inclusion_style_projection(self):
        doc = {
            "_id": 1,
            "name": "Ada",
            "profile": {"colors": ["red", "blue", "green"], "city": "Madrid"},
            "role": "admin",
        }

        self.assertEqual(
            apply_projection(doc, {"name": 1, "profile.colors": {"$slice": [1, 2]}, "_id": 0}),
            {"name": "Ada", "profile": {"colors": ["blue", "green"]}},
        )

    def test_projection_supports_elem_match_and_omits_field_when_there_is_no_match(self):
        doc = {
            "_id": 1,
            "students": [
                {"name": "john", "school": 102, "age": 10},
                {"name": "jess", "school": 102, "age": 11},
            ],
            "zipcode": "63109",
        }
        missing = {
            "_id": 2,
            "students": [
                {"name": "ajax", "school": 100, "age": 7},
                {"name": "achilles", "school": 100, "age": 8},
            ],
            "zipcode": "63109",
        }

        self.assertEqual(
            apply_projection(doc, {"students": {"$elemMatch": {"school": 102}}}),
            {"_id": 1, "students": [{"name": "john", "school": 102, "age": 10}]},
        )
        self.assertEqual(
            apply_projection(
                doc,
                {"zipcode": 1, "students": {"$elemMatch": {"school": 102, "age": {"$gt": 10}}}},
            ),
            {"_id": 1, "zipcode": "63109", "students": [{"name": "jess", "school": 102, "age": 11}]},
        )
        self.assertEqual(
            apply_projection(missing, {"students": {"$elemMatch": {"school": 102}}}),
            {"_id": 2},
        )

    def test_projection_supports_positional_operator_with_selector_filter(self):
        doc = {
            "_id": 1,
            "students": [
                {"school": 100, "age": 7},
                {"school": 102, "age": 10},
                {"school": 102, "age": 11},
            ],
            "name": "Ada",
        }

        self.assertEqual(
            apply_projection(
                doc,
                {"students.$": 1, "_id": 0},
                selector_filter={"students.school": 102, "students.age": {"$gt": 10}},
            ),
            {"students": [{"school": 102, "age": 11}]},
        )

    def test_positional_projection_requires_selector_filter_and_terminal_segment(self):
        doc = {"_id": 1, "students": [{"school": 102}]}

        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"students.$": 1})
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"students.$.school": 1}, selector_filter={"students.school": 102})
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"students.$": 0}, selector_filter={"students.school": 102})

    def test_projection_rejects_invalid_projection_operator_specs_and_collisions(self):
        doc = {"_id": 1, "items": [{"kind": "a"}], "profile": {"colors": [1, 2, 3]}}

        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"items": {"$slice": "bad"}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"items": {"$slice": [1]}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"items": {"$elemMatch": 1}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"items": {"$elemMatch": {"kind": "a"}}, "name": 0})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"profile": {"$slice": 1}, "profile.colors": 1})  # type: ignore[dict-item]

    def test_projection_validation_private_branches_cover_operator_and_slice_edge_cases(self):
        doc = {"_id": 1, "items": "value", "profile": {"colors": [1, 2, 3]}}

        self.assertEqual(validate_projection_spec({"name": 1}), {"name": 1})
        with self.assertRaises(OperationFailure):
            validate_projection_spec({"_id": {"$slice": 1}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            validate_projection_spec({"items.$": 1, "other.$": 1})
        with self.assertRaises(OperationFailure):
            validate_projection_spec({"items.$": 1, "name": 0})
        with self.assertRaises(OperationFailure):
            validate_projection_spec({"field": {"$slice": 1, "$elemMatch": {"x": 1}}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            validate_projection_spec({"$": 1})

        self.assertEqual(apply_projection(doc, {"missing": {"$slice": 1}}), {"_id": 1, "items": "value", "profile": {"colors": [1, 2, 3]}})
        self.assertEqual(
            apply_projection(doc, {"items": {"$slice": 1}, "_id": 0}),
            {"items": "value", "profile": {"colors": [1, 2, 3]}},
        )
        self.assertEqual(
            apply_projection({"_id": 1, "items": []}, {"items": {"$slice": [3, 2]}}),
            {"_id": 1, "items": []},
        )
        self.assertEqual(
            apply_projection({"_id": 1, "items": [1, 2, 3]}, {"items": {"$elemMatch": {"kind": "a"}}}),
            {"_id": 1},
        )
        self.assertEqual(
            apply_projection({"_id": 1, "items": {"kind": "a"}}, {"items": {"$elemMatch": {"kind": "a"}}}),
            {"_id": 1},
        )
        self.assertEqual(
            apply_projection({"_id": 1, "items": "not-list"}, {"items.$": 1}, selector_filter={"items": "x"}),
            {"_id": 1},
        )
