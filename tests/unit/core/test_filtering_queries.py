import datetime
import decimal
import math
import re
import unittest
import uuid
from unittest.mock import patch

try:
    from bson.code import Code as BsonCode
except Exception:  # pragma: no cover - optional dependency
    BsonCode = None

import mongoeco.core.filtering as filtering_module
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.filtering import BSONComparator, HANDLED_QUERY_NODE_TYPES, QueryEngine
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, Decimal128, ObjectId, Regex, Timestamp, UNDEFINED, PlanningMode


class QueryEngineTests(unittest.TestCase):
    def test_query_engine_dispatch_covers_all_concrete_query_nodes(self):
        def _collect_concrete_subclasses(base: type[QueryNode]) -> set[type[QueryNode]]:
            concrete: set[type[QueryNode]] = set()
            for subclass in base.__subclasses__():
                if subclass.__module__ == "mongoeco.core.query_plan":
                    concrete.add(subclass)
                concrete.update(_collect_concrete_subclasses(subclass))
            return concrete

        self.assertEqual(
            set(HANDLED_QUERY_NODE_TYPES),
            _collect_concrete_subclasses(QueryNode),
        )

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

    def test_query_engine_hashable_lookup_key_handles_special_scalar_types(self):
        now = datetime.datetime(2024, 1, 1)
        oid = ObjectId()
        uid = uuid.uuid4()
        timestamp = Timestamp(10, 1)

        self.assertEqual(QueryEngine._hashable_in_lookup_key(None), ("none", None))
        self.assertEqual(QueryEngine._hashable_in_lookup_key(True), ("bool", True))
        self.assertEqual(QueryEngine._hashable_in_lookup_key(b"abc"), ("bytes", b"abc"))
        self.assertEqual(QueryEngine._hashable_in_lookup_key(now), ("date", now))
        self.assertEqual(QueryEngine._hashable_in_lookup_key(uid), ("uuid", uid))
        self.assertEqual(QueryEngine._hashable_in_lookup_key(oid), ("objectid", oid))
        self.assertEqual(QueryEngine._hashable_in_lookup_key(timestamp), ("timestamp", timestamp))
        self.assertIsNone(QueryEngine._hashable_in_lookup_key("ada"))

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

    def test_type_query_distinguishes_bson_int_and_long_by_range(self):
        self.assertTrue(QueryEngine.match({"value": 1}, {"value": {"$type": "int"}}))
        self.assertFalse(QueryEngine.match({"value": 1}, {"value": {"$type": "long"}}))
        self.assertTrue(QueryEngine.match({"value": 1 << 40}, {"value": {"$type": "long"}}))
        self.assertFalse(QueryEngine.match({"value": 1 << 40}, {"value": {"$type": "int"}}))

    def test_type_query_accepts_public_bson_classes(self):
        self.assertTrue(QueryEngine.match({"value": Binary(b"abc", subtype=4)}, {"value": {"$type": "binData"}}))
        self.assertTrue(QueryEngine.match({"value": Decimal128("1.5")}, {"value": {"$type": "decimal"}}))
        self.assertTrue(QueryEngine.match({"value": Regex("^ad", "i")}, {"value": {"$type": "regex"}}))
        self.assertTrue(QueryEngine.match({"value": Timestamp(10, 1)}, {"value": {"$type": "timestamp"}}))
        if BsonCode is not None:
            self.assertTrue(QueryEngine.match({"value": BsonCode("function() { return 1; }")}, {"value": {"$type": 13}}))
            self.assertTrue(
                QueryEngine.match(
                    {"value": BsonCode("function() { return x; }", {"x": 1})},
                    {"value": {"$type": "javascriptWithScope"}},
                )
            )

    def test_query_plan_accepts_public_regex(self):
        self.assertTrue(QueryEngine.match({"name": "Ada"}, {"name": Regex("^ad", "i")}))

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

    def test_query_nin_null_treats_undefined_like_null_in_7_but_not_in_8(self):
        document = {"value": UNDEFINED}

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

    def test_query_engine_all_matches_direct_array_members_without_considering_array_as_scalar(self):
        document = {"tags": ["python", "mongodb"]}

        self.assertTrue(QueryEngine.match(document, {"tags": {"$all": ["python", "mongodb"]}}))
        self.assertFalse(QueryEngine.match(document, {"tags": {"$all": [["python", "mongodb"]]}}))

    def test_query_engine_all_uses_single_resolution_for_array_traversal_and_hashable_fast_path(self):
        document = {"items": [{"tags": [b"a", b"b"]}, {"tags": [b"c"]}]}

        with (
            patch.object(QueryEngine, "_get_field_value", side_effect=AssertionError("unexpected fast-path lookup")),
            patch.object(QueryEngine, "_values_equal", side_effect=AssertionError("unexpected equality fallback")),
        ):
            self.assertTrue(QueryEngine.match(document, {"items.tags": {"$all": [b"a", b"c"]}}))

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
        self.assertTrue(QueryEngine.match({"score": 7.5}, {"score": {"$type": "Double"}}))
        self.assertTrue(QueryEngine.match({"score": 7}, {"score": {"$type": "INT"}}))
        self.assertTrue(QueryEngine.match({"score": decimal.Decimal("7.5")}, {"score": {"$type": "Decimal"}}))
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
        self.assertFalse(QueryEngine.match(document, {"tags": {"$all": []}}))
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

    def test_query_engine_supports_implicit_and_explicit_eq_regex_literals(self):
        self.assertTrue(QueryEngine.match({"name": "MongoDB2"}, {"name": re.compile("MongoDB")}))
        self.assertTrue(QueryEngine.match({"name": "MongoDB2"}, {"name": {"$eq": re.compile("MongoDB")}}))
        self.assertFalse(QueryEngine.match({"name": re.compile("MongoDB")}, {"name": {"$eq": re.compile("MongoDB")}}))

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

    def test_query_engine_elem_match_reuses_compiled_plans(self):
        scalar_plan = compile_filter({"scores": {"$elemMatch": {"$gt": 3, "$lt": 5}}})
        document_plan = compile_filter({"items": {"$elemMatch": {"kind": "b", "qty": 2}}})

        with patch.object(QueryEngine, "match", side_effect=AssertionError("unexpected runtime recompilation")):
            self.assertTrue(
                QueryEngine.match_plan(
                    {"scores": [1, 4, 7]},
                    scalar_plan,
                )
            )
            self.assertTrue(
                QueryEngine.match_plan(
                    {"items": [{"kind": "a", "qty": 1}, {"kind": "b", "qty": 2}]},
                    document_plan,
                )
            )

    def test_query_engine_elem_match_private_helper_supports_direct_scalar_condition(self):
        self.assertTrue(QueryEngine._match_elem_match_candidate("python", "python"))
        self.assertFalse(QueryEngine._match_elem_match_candidate("python", "mongodb"))

    def test_query_engine_supports_eq_with_regex_literals(self):
        self.assertTrue(QueryEngine.match({"name": "Ada"}, {"name": {"$eq": re.compile("^Ad")}}))
        self.assertFalse(QueryEngine.match({"name": "Grace"}, {"name": {"$eq": re.compile("^Ad")}}))

    def test_query_engine_rejects_unsupported_regex_option(self):
        with self.assertRaises(OperationFailure):
            QueryEngine.match({"name": "Ada"}, {"name": {"$regex": "^Ad", "$options": "z"}})
