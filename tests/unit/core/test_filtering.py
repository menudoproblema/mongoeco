import unittest
import math

from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import QueryNode
from mongoeco.errors import OperationFailure
from mongoeco.types import ObjectId


class QueryEngineTests(unittest.TestCase):
    def test_query_engine_matches_empty_filter(self):
        self.assertTrue(QueryEngine.match({"a": 1}, {}))

    def test_query_engine_supports_dot_notation(self):
        document = {"user": {"profile": {"name": "Ada"}}}
        self.assertTrue(QueryEngine.match(document, {"user.profile.name": "Ada"}))

    def test_query_engine_returns_false_for_missing_dot_notation_path(self):
        document = {"user": {"profile": {"name": "Ada"}}}
        self.assertFalse(QueryEngine.match(document, {"user.profile.age": 30}))

    def test_query_engine_handles_unequal_embedded_documents_without_crashing(self):
        document = {"data": {"b": 1}}
        self.assertFalse(QueryEngine.match(document, {"data": {"b": 2}}))

    def test_query_engine_treats_equal_numeric_values_as_equal(self):
        self.assertTrue(QueryEngine.match({"value": 1.0}, {"value": 1}))

    def test_query_engine_does_not_treat_bool_and_number_as_equal(self):
        self.assertFalse(QueryEngine.match({"value": 1}, {"value": True}))
        self.assertFalse(QueryEngine.match({"value": True}, {"value": 1}))

    def test_query_engine_matches_scalar_against_array_members(self):
        document = {"tags": ["python", "mongodb"]}
        self.assertTrue(QueryEngine.match(document, {"tags": "python"}))

    def test_query_engine_supports_dot_notation_through_list_of_documents(self):
        document = {"items": [{"kind": "a"}, {"kind": "b"}]}
        self.assertTrue(QueryEngine.match(document, {"items.kind": "b"}))

    def test_query_engine_supports_top_level_or(self):
        document = {"a": 1, "b": 3}
        self.assertTrue(QueryEngine.match(document, {"$or": [{"a": 2}, {"b": 3}]}))
        self.assertFalse(QueryEngine.match(document, {"$or": [{"a": 2}, {"b": 4}]}))

    def test_query_engine_supports_top_level_and(self):
        document = {"a": 1, "b": 3}
        self.assertTrue(QueryEngine.match(document, {"$and": [{"a": 1}, {"b": 3}]}))
        self.assertFalse(QueryEngine.match(document, {"$and": [{"a": 1}, {"b": 4}]}))

    def test_query_engine_rejects_invalid_boolean_operator_payloads(self):
        with self.assertRaises(ValueError):
            QueryEngine.match({"a": 1}, {"$and": {"a": 1}})

        with self.assertRaises(ValueError):
            QueryEngine.match({"a": 1}, {"$or": {"a": 1}})

    def test_query_engine_rejects_invalid_in_payloads(self):
        with self.assertRaises(ValueError):
            QueryEngine.match({"a": 1}, {"a": {"$in": 1}})

        with self.assertRaises(ValueError):
            QueryEngine.match({"a": 1}, {"a": {"$nin": 1}})

    def test_query_engine_rejects_unknown_operator(self):
        with self.assertRaises(OperationFailure):
            QueryEngine.match({"a": 1}, {"a": {"$unknown": 1}})

    def test_query_engine_handles_nan_comparison_without_crashing(self):
        self.assertTrue(QueryEngine.match({"value": math.nan}, {"value": math.nan}))

    def test_bson_comparator_orders_objectid_as_distinct_bson_type(self):
        smaller = ObjectId("000000000000000000000001")
        larger = ObjectId("ffffffffffffffffffffffff")

        self.assertFalse(QueryEngine.match({"value": smaller}, {"value": "000000000000000000000001"}))
        self.assertTrue(QueryEngine.match({"value": smaller}, {"value": smaller}))
        self.assertTrue(QueryEngine.match({"value": larger}, {"value": {"$gt": smaller}}))

    def test_query_engine_does_not_match_comparison_operator_when_field_is_missing(self):
        self.assertFalse(QueryEngine.match({}, {"value": {"$lt": 5}}))
        self.assertFalse(QueryEngine.match({}, {"value": {"$gt": None}}))

    def test_query_engine_returns_false_for_nested_path_on_scalar_value(self):
        self.assertFalse(QueryEngine.match({"a": 1}, {"a.b": 1}))

    def test_query_engine_returns_false_for_missing_first_segment_in_nested_path(self):
        self.assertFalse(QueryEngine.match({"a": 1}, {"b.c": 1}))

    def test_query_engine_rejects_unknown_query_plan_node(self):
        class UnknownPlan(QueryNode):
            pass

        with self.assertRaises(TypeError):
            QueryEngine.match_plan({"a": 1}, UnknownPlan())

    def test_query_engine_rejects_unknown_comparison_kind(self):
        with self.assertRaises(ValueError):
            QueryEngine._evaluate_comparison({"a": 1}, "a", 1, "between")
