import unittest

from mongoeco.core.search_filter_prefilter import (
    collect_filterable_values,
    evaluate_candidate_filter,
    filter_value_key,
    flatten_candidate_filter_clauses,
    matches_candidateable_filter,
    value_key_matches_range,
)


class SearchFilterPrefilterTests(unittest.TestCase):
    def test_filter_value_key_and_collect_filterable_values_cover_scalar_shapes(self):
        self.assertEqual(filter_value_key(None), ("null", None))
        self.assertEqual(filter_value_key(True), ("bool", True))
        self.assertEqual(filter_value_key(3), ("number", 3.0))
        self.assertEqual(filter_value_key("ada"), ("string", "ada"))
        values = dict(collect_filterable_values({"kind": "note", "tags": ["a", "b"]}))
        self.assertIn("kind", values)
        self.assertIn(("string", "note"), values["kind"])
        self.assertIn("tags", values)

    def test_value_key_matches_range_and_candidateable_match_cover_ranges_and_booleans(self):
        self.assertTrue(value_key_matches_range(("number", 12.0), {"$gte": 10, "$lt": 20}))
        self.assertFalse(value_key_matches_range(("number", 25.0), {"$gte": 10, "$lt": 20}))
        document = {"score": 12, "kind": "note", "tags": ["a", "b"]}
        self.assertTrue(matches_candidateable_filter(document, {"score": {"$gte": 10, "$lt": 20}}))
        self.assertTrue(matches_candidateable_filter(document, {"kind": {"$in": ["note", "ref"]}}))
        self.assertFalse(matches_candidateable_filter(document, {"kind": "ref"}))
        self.assertIsNone(matches_candidateable_filter(document, {"kind": {"$regex": "^n"}}))

    def test_flatten_and_evaluate_candidate_filter_preserve_boolean_shape(self):
        clauses = flatten_candidate_filter_clauses({"$and": [{"kind": "note"}, {"$or": [{"score": {"$gte": 10}}, {"score": {"$lt": 0}}]}]})
        self.assertEqual(clauses[0], ("kind", "note"))
        ordered = ("a", "b", "c", "d")

        def resolver(path: str, clause: object):
            if path == "kind" and clause == "note":
                return ("a", "c"), "eq"
            if path == "score" and clause == {"$gte": 10}:
                return ("a", "b"), "range"
            if path == "score" and clause == {"$lt": 0}:
                return ("d",), "range"
            return None, "eq"

        result = evaluate_candidate_filter(
            {"$and": [{"kind": "note"}, {"$or": [{"score": {"$gte": 10}}, {"score": {"$lt": 0}}]}]},
            all_candidates=ordered,
            ordered_candidates=ordered,
            clause_resolver=resolver,
        )
        assert result is not None
        self.assertEqual(result.matches, ("a",))
        self.assertEqual(result.plan.supported_clause_count, 3)
        self.assertEqual(result.plan.shape, "$and")
