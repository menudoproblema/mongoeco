import datetime
import decimal
import math
import re
import unittest
import uuid

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.errors import OperationFailure
from mongoeco.types import ObjectId, UNDEFINED


class QueryEngineTests(unittest.TestCase):
    def test_query_engine_uses_custom_dialect_for_equality_membership_and_ranges(self):
        class CaseInsensitiveReverseRangeDialect(type(MONGODB_DIALECT_70)):
            pass

        class CustomDialect(CaseInsensitiveReverseRangeDialect):
            def values_equal(self, left, right):
                if isinstance(left, str) and isinstance(right, str):
                    return left.lower() == right.lower()
                return super().values_equal(left, right)

            def compare_values(self, left, right):
                if (
                    isinstance(left, (int, float))
                    and not isinstance(left, bool)
                    and isinstance(right, (int, float))
                    and not isinstance(right, bool)
                    and left != right
                ):
                    return -1 if left > right else 1
                return super().compare_values(left, right)

        dialect = CustomDialect()
        document = {"name": "Ada", "rank": 2, "tags": ["Ada", "Mongo"]}

        self.assertTrue(
            QueryEngine.match_plan(
                document,
                compile_filter({"name": "ada"}, dialect=dialect),
                dialect=dialect,
            )
        )
        self.assertTrue(
            QueryEngine.match_plan(
                document,
                compile_filter({"tags": {"$in": ["mongo"]}}, dialect=dialect),
                dialect=dialect,
            )
        )
        self.assertTrue(
            QueryEngine.match_plan(
                document,
                compile_filter({"rank": {"$lt": 1}}, dialect=dialect),
                dialect=dialect,
            )
        )
        self.assertFalse(
            QueryEngine.match_plan(
                document,
                compile_filter({"rank": {"$gt": 1}}, dialect=dialect),
                dialect=dialect,
            )
        )

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

    def test_query_equality_to_null_matches_undefined_in_7_but_not_in_8(self):
        document = {"value": UNDEFINED}
        array_document = {"value": ["x", UNDEFINED]}

        plan_70 = compile_filter({"value": None}, dialect=MONGODB_DIALECT_70)
        plan_80 = compile_filter({"value": None}, dialect=MONGODB_DIALECT_80)

        self.assertTrue(QueryEngine.match_plan(document, plan_70))
        self.assertFalse(QueryEngine.match_plan(document, plan_80))
        self.assertTrue(QueryEngine.match_plan(array_document, plan_70))
        self.assertFalse(QueryEngine.match_plan(array_document, plan_80))

    def test_query_including_null_matches_undefined_in_7_but_not_in_8(self):
        document = {"value": UNDEFINED}

        plan_70 = compile_filter({"value": {"$in": [None]}}, dialect=MONGODB_DIALECT_70)
        plan_80 = compile_filter({"value": {"$in": [None]}}, dialect=MONGODB_DIALECT_80)

        self.assertTrue(QueryEngine.match_plan(document, plan_70))
        self.assertFalse(QueryEngine.match_plan(document, plan_80))

    def test_query_ne_null_on_missing_field_differs_between_7_and_8(self):
        document = {}

        self.assertFalse(QueryEngine.match(document, {"value": {"$ne": None}}, dialect=MONGODB_DIALECT_70))
        self.assertTrue(QueryEngine.match(document, {"value": {"$ne": None}}, dialect=MONGODB_DIALECT_80))

    def test_query_nin_null_on_missing_field_differs_between_7_and_8(self):
        document = {}

        self.assertFalse(QueryEngine.match(document, {"value": {"$nin": [None]}}, dialect=MONGODB_DIALECT_70))
        self.assertTrue(QueryEngine.match(document, {"value": {"$nin": [None]}}, dialect=MONGODB_DIALECT_80))

    def test_query_ne_and_nin_on_missing_field_still_match_non_null_values(self):
        document = {}

        self.assertTrue(QueryEngine.match(document, {"value": {"$ne": 1}}, dialect=MONGODB_DIALECT_70))
        self.assertTrue(QueryEngine.match(document, {"value": {"$ne": 1}}, dialect=MONGODB_DIALECT_80))
        self.assertTrue(QueryEngine.match(document, {"value": {"$nin": [1]}}, dialect=MONGODB_DIALECT_70))
        self.assertTrue(QueryEngine.match(document, {"value": {"$nin": [1]}}, dialect=MONGODB_DIALECT_80))

    def test_query_engine_does_not_treat_bool_and_number_as_equal(self):
        self.assertFalse(QueryEngine.match({"value": 1}, {"value": True}))
        self.assertFalse(QueryEngine.match({"value": True}, {"value": 1}))

    def test_query_engine_matches_scalar_against_array_members(self):
        document = {"tags": ["python", "mongodb"]}
        self.assertTrue(QueryEngine.match(document, {"tags": "python"}))

    def test_query_engine_supports_dot_notation_through_list_of_documents(self):
        document = {"items": [{"kind": "a"}, {"kind": "b"}]}
        self.assertTrue(QueryEngine.match(document, {"items.kind": "b"}))

    def test_query_engine_supports_numeric_array_segments_in_paths(self):
        document = {"a": [1, 2], "items": [{"kind": "a"}, {"kind": "b"}]}
        self.assertTrue(QueryEngine.match(document, {"a.0": 1}))
        self.assertTrue(QueryEngine.match(document, {"a.1": {"$gt": 1}}))
        self.assertFalse(QueryEngine.match(document, {"a.1": 1}))
        self.assertTrue(QueryEngine.match(document, {"items.1.kind": "b"}))
        self.assertFalse(QueryEngine.match(document, {"items.2.kind": "b"}))

    def test_query_engine_supports_top_level_or(self):
        document = {"a": 1, "b": 3}
        self.assertTrue(QueryEngine.match(document, {"$or": [{"a": 2}, {"b": 3}]}))
        self.assertFalse(QueryEngine.match(document, {"$or": [{"a": 2}, {"b": 4}]}))

    def test_query_engine_supports_top_level_and(self):
        document = {"a": 1, "b": 3}
        self.assertTrue(QueryEngine.match(document, {"$and": [{"a": 1}, {"b": 3}]}))
        self.assertFalse(QueryEngine.match(document, {"$and": [{"a": 1}, {"b": 4}]}))

    def test_query_engine_supports_top_level_expr(self):
        document = {"tenant": "a", "score": 7}

        self.assertTrue(
            QueryEngine.match(
                document,
                {"$expr": {"$and": [{"$eq": ["$tenant", "a"]}, {"$gt": ["$score", 5]}]}},
            )
        )
        self.assertFalse(
            QueryEngine.match(
                document,
                {"$expr": {"$lt": ["$score", 5]}},
            )
        )

    def test_query_engine_expr_can_use_bound_variables(self):
        document = {"tenant": "a", "score": 7}
        plan = compile_filter(
            {"$expr": {"$and": [{"$eq": ["$tenant", "$$tenant"]}, {"$gt": ["$score", "$$min_score"]}]}},
            variables={"tenant": "a", "min_score": 5},
        )

        self.assertTrue(QueryEngine.match_plan(document, plan))
        self.assertFalse(
            QueryEngine.match_plan(
                document,
                compile_filter(
                    {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
                    variables={"tenant": "b"},
                ),
            )
        )

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

    def test_query_engine_mod_handles_large_integers_without_float_precision_loss(self):
        self.assertTrue(
            QueryEngine.match(
                {"value": 10**20},
                {"value": {"$mod": [3, 1]}},
            )
        )

    def test_query_engine_rejects_unknown_operator(self):
        with self.assertRaises(OperationFailure):
            QueryEngine.match({"a": 1}, {"a": {"$unknown": 1}})

    def test_query_engine_supports_type_aliases_codes_and_array_semantics(self):
        document = {
            "name": "Ada",
            "score": 7,
            "ratio": 1.5,
            "tags": ["python", 5],
            "payload": {"active": True},
            "blob": b"abc",
            "oid": ObjectId(),
            "created_at": datetime.datetime(2024, 1, 1),
            "pattern": re.compile("^A"),
            "missing_like": UNDEFINED,
        }

        self.assertTrue(QueryEngine.match(document, {"name": {"$type": "string"}}))
        self.assertTrue(QueryEngine.match(document, {"score": {"$type": 16}}))
        self.assertTrue(QueryEngine.match(document, {"ratio": {"$type": "number"}}))
        self.assertTrue(QueryEngine.match(document, {"tags": {"$type": "array"}}))
        self.assertTrue(QueryEngine.match(document, {"tags": {"$type": "int"}}))
        self.assertTrue(QueryEngine.match(document, {"blob": {"$type": 5}}))
        self.assertTrue(QueryEngine.match(document, {"oid": {"$type": "objectId"}}))
        self.assertTrue(QueryEngine.match(document, {"created_at": {"$type": "date"}}))
        self.assertTrue(QueryEngine.match(document, {"pattern": {"$type": "regex"}}))
        self.assertTrue(QueryEngine.match(document, {"missing_like": {"$type": "undefined"}}))
        self.assertFalse(QueryEngine.match(document, {"score": {"$type": "string"}}))
        self.assertFalse(QueryEngine.match(document, {"missing": {"$type": "null"}}))

    def test_query_engine_type_rejects_unsupported_or_invalid_type_specs(self):
        with self.assertRaises(ValueError):
            QueryEngine.match({"score": 7}, {"score": {"$type": []}})
        self.assertTrue(QueryEngine.match({"score": decimal.Decimal("7.5")}, {"score": {"$type": "decimal"}}))
        self.assertTrue(QueryEngine.match({"score": decimal.Decimal("7.5")}, {"score": {"$type": 19}}))
        with self.assertRaises(ValueError):
            QueryEngine.match({"score": 7}, {"score": {"$type": True}})

    def test_query_engine_supports_bitwise_query_operators(self):
        document = {
            "mask": 0b1010,
            "negative": -5,
            "float_mask": 10.0,
            "binary": bytes([0b00000101]),
            "array": [0b1010],
        }

        self.assertTrue(QueryEngine.match(document, {"mask": {"$bitsAllSet": 0b1000}}))
        self.assertTrue(QueryEngine.match(document, {"mask": {"$bitsAnySet": [0, 1]}}))
        self.assertTrue(QueryEngine.match(document, {"mask": {"$bitsAllClear": 0b0001}}))
        self.assertTrue(QueryEngine.match(document, {"mask": {"$bitsAnyClear": [0, 2]}}))
        self.assertTrue(QueryEngine.match(document, {"float_mask": {"$bitsAllSet": 0b0010}}))
        self.assertTrue(QueryEngine.match(document, {"binary": {"$bitsAllSet": bytes([0b00000101])}}))
        self.assertTrue(QueryEngine.match(document, {"negative": {"$bitsAllSet": 1 << 10}}))
        self.assertFalse(QueryEngine.match(document, {"mask": {"$bitsAllSet": 0b0101}}))
        self.assertFalse(QueryEngine.match(document, {"array": {"$bitsAnySet": 0b0010}}))

    def test_query_engine_bitwise_rejects_invalid_masks(self):
        with self.assertRaises(ValueError):
            QueryEngine.match({"mask": 0b1010}, {"mask": {"$bitsAllSet": -1}})
        with self.assertRaises(ValueError):
            QueryEngine.match({"mask": 0b1010}, {"mask": {"$bitsAnySet": [1, -1]}})
        with self.assertRaises(ValueError):
            QueryEngine.match({"mask": 0b1010}, {"mask": {"$bitsAllClear": 1 << 63}})
        with self.assertRaises(ValueError):
            QueryEngine.match({"mask": 0b1010}, {"mask": {"$bitsAnyClear": 1.5}})

    def test_query_engine_supports_all_operator(self):
        document = {"tags": ["python", "mongodb", "sqlite"]}
        self.assertTrue(QueryEngine.match(document, {"tags": {"$all": ["python", "sqlite"]}}))
        self.assertFalse(QueryEngine.match(document, {"tags": {"$all": ["python", "redis"]}}))
        self.assertTrue(QueryEngine.match({"tags": "python"}, {"tags": {"$all": ["python"]}}))
        self.assertFalse(QueryEngine.match({"tags": "python"}, {"tags": {"$all": ["python", "mongodb"]}}))
        self.assertTrue(
            QueryEngine.match(
                {"items": [{"kind": "a"}, {"kind": "b"}]},
                {"items.kind": {"$all": ["a", "b"]}},
            )
        )
        self.assertFalse(
            QueryEngine.match(
                {"tags": [["python", "mongodb"]]},
                {"tags": {"$all": ["python"]}},
            )
        )

    def test_query_engine_supports_all_with_elem_match_clauses(self):
        document = {
            "items": [
                {"kind": "a", "qty": 1},
                {"kind": "b", "qty": 2},
                {"kind": "b", "qty": 5},
            ]
        }

        self.assertTrue(
            QueryEngine.match(
                document,
                {
                    "items": {
                        "$all": [
                            {"$elemMatch": {"kind": "a"}},
                            {"$elemMatch": {"kind": "b", "qty": {"$gte": 5}}},
                        ]
                    }
                },
            )
        )
        self.assertFalse(
            QueryEngine.match(
                document,
                {
                    "items": {
                        "$all": [
                            {"$elemMatch": {"kind": "a", "qty": {"$gte": 2}}},
                            {"$elemMatch": {"kind": "b", "qty": {"$gte": 5}}},
                        ]
                    }
                },
            )
        )

    def test_query_engine_treats_subdocument_order_as_significant_in_in_operator(self):
        self.assertFalse(
            QueryEngine.match(
                {"value": {"b": 2, "a": 1}},
                {"value": {"$in": [{"a": 1, "b": 2}]}},
            )
        )

    def test_query_engine_treats_subdocument_order_as_significant_in_exact_equality(self):
        self.assertFalse(
            QueryEngine.match(
                {"value": {"b": 2, "a": 1}},
                {"value": {"a": 1, "b": 2}},
            )
        )

    def test_query_engine_supports_size_operator(self):
        document = {"tags": ["python", "mongodb"]}
        self.assertTrue(QueryEngine.match(document, {"tags": {"$size": 2}}))
        self.assertFalse(QueryEngine.match(document, {"tags": {"$size": 3}}))
        self.assertFalse(QueryEngine.match({"tags": "python"}, {"tags": {"$size": 1}}))
        self.assertFalse(QueryEngine.match({"tags": [[1, 2]]}, {"tags": {"$size": 2}}))

    def test_query_engine_supports_mod_operator(self):
        self.assertTrue(QueryEngine.match({"count": 10}, {"count": {"$mod": [3, 1]}}))
        self.assertFalse(QueryEngine.match({"count": 10}, {"count": {"$mod": [3, 0]}}))
        self.assertFalse(QueryEngine.match({"count": "10"}, {"count": {"$mod": [3, 1]}}))
        self.assertFalse(QueryEngine.match({"count": float("inf")}, {"count": {"$mod": [3, 1]}}))
        self.assertFalse(QueryEngine.match({"count": 10}, {"count": {"$mod": [float("inf"), 1]}}))

    def test_query_engine_supports_lte_operator(self):
        self.assertTrue(QueryEngine.match({"count": 10}, {"count": {"$lte": 10}}))
        self.assertTrue(QueryEngine.match({"count": 9}, {"count": {"$lte": 10}}))
        self.assertFalse(QueryEngine.match({"count": 11}, {"count": {"$lte": 10}}))

    def test_query_engine_supports_mod_with_negative_and_float_values(self):
        self.assertFalse(QueryEngine.match({"count": -5}, {"count": {"$mod": [3, 1]}}))
        self.assertTrue(QueryEngine.match({"count": -5}, {"count": {"$mod": [3, -2]}}))
        self.assertTrue(QueryEngine.match({"count": 5.5}, {"count": {"$mod": [2.5, 0.5]}}))
        self.assertFalse(QueryEngine.match({"count": 5.5}, {"count": {"$mod": [2.5, 0.0]}}))

    def test_query_engine_supports_regex_operator(self):
        self.assertTrue(QueryEngine.match({"name": "Ada"}, {"name": {"$regex": "^Ad"}}))
        self.assertTrue(QueryEngine.match({"name": "ada"}, {"name": {"$regex": "^ad", "$options": "i"}}))
        self.assertTrue(QueryEngine.match({"tags": ["python", "mongodb"]}, {"tags": {"$regex": "^py"}}))
        self.assertTrue(QueryEngine.match({"name": "ada"}, {"name": {"$regex": re.compile("^ad", re.IGNORECASE)}}))
        self.assertFalse(QueryEngine.match({"name": "Grace"}, {"name": {"$regex": "^Ad"}}))

    def test_query_engine_supports_implicit_regex_literals_but_not_explicit_eq(self):
        self.assertTrue(QueryEngine.match({"name": "MongoDB2"}, {"name": re.compile("MongoDB")}))
        self.assertFalse(QueryEngine.match({"name": "MongoDB2"}, {"name": {"$eq": re.compile("MongoDB")}}))
        self.assertTrue(
            QueryEngine.match(
                {"name": re.compile("MongoDB")},
                {"name": {"$eq": re.compile("MongoDB")}},
            )
        )

    def test_query_engine_supports_regex_literals_inside_in_and_nin(self):
        self.assertTrue(
            QueryEngine.match(
                {"tags": ["beta", "stable"]},
                {"tags": {"$in": [re.compile("^be"), re.compile("^zz")]}}
            )
        )
        self.assertFalse(
            QueryEngine.match(
                {"tags": ["beta", "stable"]},
                {"tags": {"$nin": [re.compile("^be"), re.compile("^zz")]}}
            )
        )
        self.assertTrue(
            QueryEngine.match(
                {"tags": ["alpha", "stable"]},
                {"tags": {"$nin": [re.compile("^be"), re.compile("^zz")]}}
            )
        )

    def test_query_engine_supports_not_operator(self):
        self.assertTrue(QueryEngine.match({"name": "Ada"}, {"name": {"$not": {"$regex": "^Gr"}}}))
        self.assertFalse(QueryEngine.match({"name": "Ada"}, {"name": {"$not": {"$regex": "^Ad"}}}))
        self.assertTrue(QueryEngine.match({"value": 2}, {"value": {"$not": {"$gt": 3}}}))

    def test_query_engine_supports_not_with_elem_match(self):
        document = {"scores": [1, 4, 7]}

        self.assertTrue(QueryEngine.match(document, {"scores": {"$not": {"$elemMatch": {"$gt": 7}}}}))
        self.assertFalse(QueryEngine.match(document, {"scores": {"$not": {"$elemMatch": {"$gt": 3, "$lt": 5}}}}))

    def test_query_engine_supports_elem_match_for_scalars_and_documents(self):
        self.assertTrue(QueryEngine.match({"scores": [1, 4, 7]}, {"scores": {"$elemMatch": {"$gt": 3, "$lt": 5}}}))
        self.assertFalse(QueryEngine.match({"scores": [1, 5, 7]}, {"scores": {"$elemMatch": {"$gt": 3, "$lt": 5}}}))
        self.assertTrue(
            QueryEngine.match(
                {"items": [{"kind": "a", "qty": 1}, {"kind": "b", "qty": 2}]},
                {"items": {"$elemMatch": {"kind": "b", "qty": 2}}},
            )
        )
        self.assertFalse(QueryEngine.match({"items": "x"}, {"items": {"$elemMatch": {"kind": "b"}}}))
        self.assertFalse(QueryEngine.match({"items": [1, 2]}, {"items": {"$elemMatch": {"kind": "b"}}}))

    def test_query_engine_supports_elem_match_with_compound_subdocument_operators(self):
        document = {
            "items": [
                {"kind": "a", "qty": 1, "price": 5},
                {"kind": "b", "qty": 3, "price": 9},
                {"kind": "b", "qty": 5, "price": 12},
            ]
        }

        self.assertTrue(
            QueryEngine.match(
                document,
                {"items": {"$elemMatch": {"kind": "b", "qty": {"$gte": 3}, "price": {"$lt": 10}}}},
            )
        )
        self.assertFalse(
            QueryEngine.match(
                document,
                {"items": {"$elemMatch": {"kind": "b", "qty": {"$gte": 4}, "price": {"$lt": 10}}}},
            )
        )

    def test_query_engine_elem_match_private_helper_supports_direct_scalar_condition(self):
        self.assertTrue(QueryEngine._match_elem_match_candidate("python", "python"))
        self.assertFalse(QueryEngine._match_elem_match_candidate("python", "mongodb"))

    def test_query_engine_rejects_unsupported_regex_option(self):
        with self.assertRaises(OperationFailure):
            QueryEngine.match({"name": "Ada"}, {"name": {"$regex": "^Ad", "$options": "z"}})

    def test_query_engine_handles_nan_comparison_without_crashing(self):
        self.assertTrue(QueryEngine.match({"value": math.nan}, {"value": math.nan}))

    def test_bson_comparator_orders_nan_before_other_numbers(self):
        nan = math.nan

        self.assertEqual(BSONComparator.compare(nan, math.nan), 0)
        self.assertLess(BSONComparator.compare(nan, 5), 0)
        self.assertGreater(BSONComparator.compare(5, nan), 0)

    def test_bson_comparator_orders_objectid_as_distinct_bson_type(self):
        smaller = ObjectId("000000000000000000000001")
        larger = ObjectId("ffffffffffffffffffffffff")

        self.assertFalse(QueryEngine.match({"value": smaller}, {"value": "000000000000000000000001"}))
        self.assertTrue(QueryEngine.match({"value": smaller}, {"value": smaller}))
        self.assertTrue(QueryEngine.match({"value": larger}, {"value": {"$gt": smaller}}))

    def test_bson_comparator_respects_document_and_array_structure(self):
        self.assertNotEqual(
            BSONComparator.compare({"a": 1, "b": 2}, {"b": 2, "a": 1}),
            0,
        )
        self.assertNotEqual(
            BSONComparator.compare([{"a": 1, "b": 2}], [{"b": 2, "a": 1}]),
            0,
        )

    def test_bson_comparator_orders_shorter_documents_and_lists_first(self):
        self.assertLess(BSONComparator.compare({"a": 1}, {"a": 1, "b": 2}), 0)
        self.assertLess(BSONComparator.compare([1], [1, 2]), 0)
        self.assertEqual(BSONComparator.compare([1, 2], [1, 2]), 0)

    def test_bson_comparator_returns_zero_for_equal_ordered_values_and_string_fallback(self):
        self.assertEqual(BSONComparator.compare({"a": 1}, {"a": 1}), 0)

        class _Unordered:
            def __init__(self, value):
                self.value = value

            def __repr__(self) -> str:
                return self.value

        self.assertLess(BSONComparator.compare(_Unordered("a"), _Unordered("b")), 0)
        self.assertEqual(BSONComparator.compare(_Unordered("same"), _Unordered("same")), 0)

        class _NonOrdering:
            def __eq__(self, other) -> bool:
                return False

            def __lt__(self, other) -> bool:
                return False

            def __gt__(self, other) -> bool:
                return False

        self.assertEqual(BSONComparator.compare(_NonOrdering(), _NonOrdering()), 0)

    def test_extract_values_supports_empty_path_and_indexed_nested_arrays(self):
        self.assertEqual(QueryEngine.extract_values([1, [2, 3]], ""), [[1, [2, 3]], [2, 3]])
        self.assertEqual(QueryEngine.extract_values([[1, 2]], "0"), [[1, 2], 1, 2])

    def test_get_field_value_returns_false_for_invalid_list_paths(self):
        self.assertEqual(QueryEngine._get_field_value([1, 2], "x"), (False, None))
        self.assertEqual(QueryEngine._get_field_value([1, 2], "5"), (False, None))
        self.assertEqual(QueryEngine._get_field_value([1, 2], "1"), (True, 2))
        self.assertEqual(QueryEngine._get_field_value([1, 2], "1.name"), (False, None))
        self.assertEqual(QueryEngine._get_field_value([1, 2], "x.name"), (False, None))
        self.assertEqual(QueryEngine._get_field_value([1], "2.name"), (False, None))
        self.assertEqual(QueryEngine._get_field_value([{"name": "Ada"}], "0.name"), (True, "Ada"))
        self.assertEqual(QueryEngine._get_field_value(1, "a"), (False, None))
        self.assertEqual(QueryEngine._get_field_value({"items": 1}, "items.name"), (False, None))

    def test_query_engine_all_returns_false_when_no_candidates_exist(self):
        self.assertFalse(QueryEngine.match({}, {"tags": {"$all": ["python"]}}))

    def test_query_engine_match_plan_rejects_unknown_plan_node(self):
        class UnknownPlan(QueryNode):
            pass

        with self.assertRaises(TypeError):
            QueryEngine.match_plan({"a": 1}, UnknownPlan())

    def test_query_engine_extract_values_handles_indexed_scalar_nested_array(self):
        document = {"items": [[1, 2], [3, 4]]}
        self.assertTrue(QueryEngine.match(document, {"items.1.0": 3}))

    def test_query_engine_values_equal_distinguishes_bool_and_int_inside_compound_values(self):
        self.assertFalse(QueryEngine._values_equal({"a": True}, {"a": 1}))
        self.assertFalse(QueryEngine._values_equal([True], [1]))

    def test_query_engine_values_equal_treats_int_and_float_as_equal_inside_compound_values(self):
        self.assertTrue(QueryEngine._values_equal({"a": 1}, {"a": 1.0}))
        self.assertTrue(QueryEngine._values_equal([1], [1.0]))

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

    def test_query_engine_supports_exists_true_and_false(self):
        self.assertTrue(QueryEngine.match({"a": 1}, {"a": {"$exists": True}}))
        self.assertTrue(QueryEngine.match({}, {"a": {"$exists": False}}))
        self.assertFalse(QueryEngine.match({"a": 1}, {"a": {"$exists": False}}))
