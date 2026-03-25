import unittest

from mongoeco.compat import MongoDialect
from mongoeco.core.projections import apply_projection
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
            {"_id": 1, "a": [{"b": 1}, {}, {"b": 2}]},
        )

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
            apply_projection(doc, {"items.$": 1})
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"items": {"$slice": 1}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"items": {"$elemMatch": {"kind": "a"}}})  # type: ignore[dict-item]
        with self.assertRaises(OperationFailure):
            apply_projection(
                doc,
                {"items": {"$slice": 1}, "items.name": 1},  # type: ignore[dict-item]
            )
        with self.assertRaises(OperationFailure):
            apply_projection(doc, {"score": {"$meta": "textScore"}})  # type: ignore[dict-item]
