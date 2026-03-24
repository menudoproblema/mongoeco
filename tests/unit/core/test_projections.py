import unittest

from mongoeco.core.projections import apply_projection


class ProjectionTests(unittest.TestCase):
    def test_inclusion_projection(self):
        doc = {"_id": 1, "name": "Val", "age": 30, "profile": {"city": "Madrid", "job": "Dev"}}

        self.assertEqual(apply_projection(doc, {"name": 1}), {"_id": 1, "name": "Val"})
        self.assertEqual(
            apply_projection(doc, {"profile.city": 1}),
            {"_id": 1, "profile": {"city": "Madrid"}},
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
