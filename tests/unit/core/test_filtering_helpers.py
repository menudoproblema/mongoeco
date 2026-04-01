import datetime
import decimal
import math
import re
import unittest
import uuid
from unittest.mock import patch

import mongoeco.core.filtering as filtering_module
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.bson_scalars import BsonInt32, BsonInt64
from mongoeco.core.filtering import BSONComparator, QueryEngine
from mongoeco.core.query_plan import QueryNode, compile_filter
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, DBRef, Decimal128, ObjectId, Regex, Timestamp, UNDEFINED, PlanningMode


class FilteringHelperTests(unittest.TestCase):
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

    def test_bson_comparator_accepts_explicit_dialect(self):
        self.assertEqual(
            BSONComparator.compare(1, 1.0, dialect=MONGODB_DIALECT_80),
            0,
        )

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

    def test_get_field_value_and_extract_values_support_dbref_subfields(self):
        document = {
            "author": DBRef(
                "users",
                "ada",
                database="observe",
                extras={"tenant": "t1", "meta": {"region": "eu"}},
            ),
            "authors": [DBRef("users", "ada"), DBRef("users", "grace", extras={"tenant": "t2"})],
        }

        self.assertEqual(QueryEngine._get_field_value(document, "author.$id"), (True, "ada"))
        self.assertEqual(QueryEngine._get_field_value(document, "author.meta.region"), (True, "eu"))
        self.assertEqual(QueryEngine.extract_values(document, "authors.$id"), ["ada", "grace"])
        self.assertEqual(QueryEngine.extract_values(document, "authors.tenant"), ["t2"])

    def test_get_field_value_returns_document_for_empty_path(self):
        document = {"name": "Ada"}
        self.assertEqual(QueryEngine._get_field_value(document, ""), (True, document))

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

    def test_query_engine_rejects_unknown_comparison_kind(self):
        with self.assertRaises(ValueError):
            QueryEngine._evaluate_comparison({"a": 1}, "a", 1, "between")

    def test_query_engine_supports_exists_true_and_false(self):
        self.assertTrue(QueryEngine.match({"a": 1}, {"a": {"$exists": True}}))
        self.assertTrue(QueryEngine.match({}, {"a": {"$exists": False}}))
        self.assertFalse(QueryEngine.match({"a": 1}, {"a": {"$exists": False}}))

    def test_query_engine_not_equals_treats_undefined_like_null(self):
        self.assertFalse(QueryEngine.match({"value": UNDEFINED}, {"value": {"$ne": None}}))

    def test_query_engine_type_accepts_case_insensitive_aliases(self):
        self.assertTrue(QueryEngine.match({"value": 1}, {"value": {"$type": "INT"}}))
        self.assertTrue(QueryEngine.match({"value": "Ada"}, {"value": {"$type": "STRING"}}))
        self.assertTrue(QueryEngine.match({"value": ObjectId()}, {"value": {"$type": "OBJECTID"}}))

    def test_query_engine_bitwise_rejects_out_of_range_bit_positions(self):
        with self.assertRaises(ValueError):
            QueryEngine.match({"flags": 1}, {"flags": {"$bitsAllSet": [64]}})

    def test_query_engine_all_empty_never_matches(self):
        self.assertFalse(QueryEngine.match({"tags": ["python"]}, {"tags": {"$all": []}}))

    def test_query_engine_size_expands_nested_arrays(self):
        self.assertTrue(
            QueryEngine.match(
                {"tags": [{"values": [1, 2, 3]}]},
                {"tags.values": {"$size": 3}},
            )
        )

    def test_query_engine_bitwise_expands_nested_array_candidates(self):
        self.assertTrue(
            QueryEngine.match(
                {"items": [{"flags": 3}, {"flags": 5}]},
                {"items.flags": {"$bitsAllSet": 1}},
            )
        )

    def test_query_engine_elem_match_rejects_mixed_operator_and_field_conditions(self):
        with self.assertRaises(OperationFailure):
            QueryEngine._match_elem_match_candidate(
                {"name": "x"},
                {"$gt": 5, "name": "x"},
            )

    def test_query_engine_regex_reuses_compiled_patterns(self):
        filtering_module._compile_regex.cache_clear()
        with patch("mongoeco.core.filtering.re.compile", wraps=re.compile) as compile_regex:
            self.assertTrue(QueryEngine.match({"name": "Ada"}, {"name": {"$regex": "^a", "$options": "i"}}))
            self.assertTrue(QueryEngine.match({"name": "Alan"}, {"name": {"$regex": "^a", "$options": "i"}}))

        self.assertEqual(compile_regex.call_count, 1)

    def test_query_engine_regex_rejects_duplicate_options(self):
        with self.assertRaises(OperationFailure):
            QueryEngine.match({"name": "Ada"}, {"name": {"$regex": "^a", "$options": "ii"}})

    def test_query_engine_deferred_query_node_raises_operation_failure(self):
        plan = compile_filter(
            {"value": {"$unknownOperator": 1}},
            planning_mode=PlanningMode.RELAXED,
        )

        with self.assertRaises(OperationFailure) as ctx:
            QueryEngine.match_plan({"value": 1}, plan)

        self.assertIn("deferred validation", str(ctx.exception))

    def test_query_engine_numeric_index_access_on_nested_array_of_documents(self):
        document = {"matrix": [[1, 2, 3], [4, 5, 6]]}

        self.assertTrue(QueryEngine.match(document, {"matrix.0": [1, 2, 3]}))
        self.assertFalse(QueryEngine.match(document, {"matrix.0": [4, 5, 6]}))

        doc2 = {"rows": [{"cells": [10, 20]}, {"cells": [30, 40]}]}
        self.assertTrue(QueryEngine.match(doc2, {"rows.0.cells": 10}))
        self.assertFalse(QueryEngine.match(doc2, {"rows.2.cells": 10}))

    def test_query_engine_in_with_multiline_regex_matches_across_lines(self):
        document = {"text": "first\nsecond"}

        self.assertTrue(
            QueryEngine.match(
                document,
                {"text": {"$in": [Regex("^second", "m")]}},
            )
        )
        self.assertFalse(
            QueryEngine.match(
                document,
                {"text": {"$in": [Regex("^second")]}},
            )
        )

    def test_query_engine_extract_values_traverses_arrays_at_non_terminal_path_segments(self):
        document = {"a": [{"b": [{"c": 1}, {"c": 2}]}, {"b": [{"c": 3}]}]}

        self.assertTrue(QueryEngine.match(document, {"a.b.c": 2}))
        self.assertTrue(QueryEngine.match(document, {"a.b.c": 3}))
        self.assertFalse(QueryEngine.match(document, {"a.b.c": 99}))

    def test_extract_all_candidates_covers_empty_and_indexed_nested_list_paths(self):
        document = {
            "items": [[1, 2], [3, 4]],
            "nested": [{"tags": ["a", "b"]}, {"tags": ["c"]}],
            "groups": [[{"scores": [1, 2]}], [{"scores": [3]}]],
        }

        self.assertEqual(QueryEngine._extract_all_candidates(document, ""), [])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "items"), [[1, 2], [3, 4]])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "items.0"), [1, 2, 1, 3])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "items.1"), [3, 4, 2, 4])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "nested.tags"), ["a", "b", "c", "a", "b", "c"])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "nested.0.tags"), ["a", "b"])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "nested.0.tags.1"), ["b"])
        self.assertEqual(QueryEngine._extract_all_candidates(document, "groups.scores.0"), [1, 3])
        self.assertEqual(QueryEngine._extract_all_candidates({"profile": {"name": "Ada"}}, "profile.name.first"), [])

    def test_comparison_helpers_cover_numeric_fast_paths_and_candidate_expansion(self):
        self.assertTrue(QueryEngine._comparison_matches_candidate(2, 1.0, "gt"))
        self.assertTrue(QueryEngine._comparison_matches_candidate(2, 2.0, "gte"))
        self.assertTrue(QueryEngine._comparison_matches_candidate(2, 3.0, "lt"))
        self.assertTrue(QueryEngine._comparison_matches_candidate(2, 2.0, "lte"))
        self.assertTrue(QueryEngine._match_top_level_comparison({"score": 2.0}, "score", 2, "lte"))
        self.assertTrue(QueryEngine._match_top_level_comparison({"values": [1, 2]}, "values", 1, "gt"))
        self.assertFalse(QueryEngine._comparison_matches_candidate([2], 5, "gt"))
        self.assertFalse(QueryEngine._match_top_level_comparison({"values": [2]}, "values", 5, "gt"))
        self.assertTrue(QueryEngine._match_top_level_comparison({"values": [1, 10]}, "values", 5, "gt"))
        self.assertTrue(QueryEngine._match_top_level_equals({"values": [1, 2]}, "values", [1, 2]))
        self.assertFalse(QueryEngine._evaluate_comparison({}, "items.score", 2, "gt"))
        self.assertTrue(
            QueryEngine._evaluate_comparison(
                {"items": [{"score": 1}, {"score": 2}]},
                "items.score",
                2,
                "lte",
            )
        )
        self.assertTrue(
            QueryEngine._evaluate_comparison(
                {"items": [{"score": 1}, {"score": 2}]},
                "items.score",
                1,
                "gt",
            )
        )
        self.assertTrue(
            QueryEngine._evaluate_comparison(
                {"items": [{"score": 1}, {"score": 2}]},
                "items.score",
                3,
                "lt",
            )
        )
        with self.assertRaises(ValueError):
            QueryEngine._comparison_matches_candidate(1, 1, "between")

    def test_type_helpers_cover_multiple_alias_forms_and_direct_matches(self):
        created_at = datetime.datetime(2024, 1, 1)
        timestamp = Timestamp(10, 1)

        self.assertEqual(QueryEngine._normalize_type_specifier(8), ("bool",))
        self.assertEqual(QueryEngine._normalize_type_specifier(" number "), ("double", "int", "long", "decimal"))
        with self.assertRaises(ValueError):
            QueryEngine._normalize_type_specifier(True)
        self.assertEqual(QueryEngine._normalize_type_specifier(6), ("dbPointer",))
        self.assertEqual(QueryEngine._normalize_type_specifier(13), ("javascript",))
        self.assertEqual(QueryEngine._normalize_type_specifier(15), ("javascriptWithScope",))
        with self.assertRaises(ValueError):
            QueryEngine._normalize_type_specifier("nonsense")
        with self.assertRaises(ValueError):
            QueryEngine._normalize_type_specifier(object())

        self.assertTrue(QueryEngine._matches_bson_type(decimal.Decimal("1.5"), "number"))
        self.assertTrue(QueryEngine._matches_bson_type({"a": 1}, "object"))
        self.assertTrue(QueryEngine._matches_bson_type(True, "bool"))
        self.assertTrue(QueryEngine._matches_bson_type(created_at, "date"))
        self.assertTrue(QueryEngine._matches_bson_type(timestamp, "timestamp"))
        self.assertTrue(QueryEngine._matches_bson_type(None, "null"))
        self.assertTrue(QueryEngine._matches_bson_type(re.compile("^a"), "regex"))
        self.assertTrue(QueryEngine._matches_bson_type(UNDEFINED, "undefined"))
        self.assertFalse(QueryEngine._matches_bson_type("Ada", "unsupported"))
        self.assertFalse(QueryEngine._matches_bson_type("Ada", "object"))

        self.assertTrue(QueryEngine._evaluate_type({"value": [1, "x"]}, "value", ("int", "string")))
        self.assertTrue(QueryEngine._evaluate_type({"value": {"a": 1}}, "value", (), aliases=frozenset({"object"})))

    def test_bitwise_helpers_cover_bytes_uuid_lists_and_unsupported_operator(self):
        uid = uuid.UUID("12345678-1234-5678-1234-567812345678")

        self.assertEqual(QueryEngine._coerce_bitwise_mask(bytes([0b00000101])), 5)
        self.assertEqual(QueryEngine._coerce_bitwise_mask(BsonInt32(5)), 5)
        self.assertEqual(
            QueryEngine._coerce_bitwise_mask(uid),
            int.from_bytes(uid.bytes, byteorder="little", signed=False),
        )
        self.assertEqual(QueryEngine._coerce_bitwise_mask([0, 3]), 0b1001)
        with self.assertRaises(ValueError):
            QueryEngine._coerce_bitwise_mask(True)
        with self.assertRaises(ValueError):
            QueryEngine._coerce_bitwise_mask(-1)
        with self.assertRaises(ValueError):
            QueryEngine._coerce_bitwise_mask([True])
        with self.assertRaises(ValueError):
            QueryEngine._coerce_bitwise_mask([64])
        with self.assertRaises(ValueError):
            QueryEngine._coerce_bitwise_mask("mask")
        self.assertIsNone(QueryEngine._coerce_bitwise_candidate(True))
        self.assertIsNone(QueryEngine._coerce_bitwise_candidate(float("inf")))
        self.assertIsNone(QueryEngine._coerce_bitwise_candidate(1 << 80))
        self.assertIsNone(QueryEngine._coerce_bitwise_candidate(float(1 << 80)))
        self.assertEqual(QueryEngine._coerce_bitwise_candidate(10.0), 10)
        self.assertEqual(QueryEngine._coerce_bitwise_candidate(BsonInt64(10)), 10)
        self.assertEqual(QueryEngine._coerce_bitwise_candidate(bytes([0b00000101])), 5)
        self.assertEqual(
            QueryEngine._coerce_bitwise_candidate(uid),
            int.from_bytes(uid.bytes, byteorder="little", signed=False),
        )
        self.assertFalse(QueryEngine._evaluate_bitwise({}, "flags", "$bitsAllSet", 1))
        self.assertTrue(QueryEngine._evaluate_bitwise({"flags": BsonInt64(0b1010)}, "flags", "$bitsAllSet", BsonInt32(0b1000)))
        self.assertFalse(QueryEngine._evaluate_bitwise({"flags": -1}, "flags", "$bitsAllSet", bytes([0] * 12 + [0b1])))
        self.assertTrue(QueryEngine._mod_remainder_matches(0.09999999999999998, 0.1))
        self.assertTrue(QueryEngine._evaluate_mod({"value": 0.9}, "value", 0.3, 0.0))
        with self.assertRaises(ValueError):
            QueryEngine._evaluate_bitwise({"flags": 3}, "flags", "$bitsNope", 1)

    def test_membership_helpers_cover_collation_agnostic_literals_regex_and_null_defaults(self):
        document = {"value": ["Ada", "Grace", None]}

        self.assertTrue(QueryEngine._in_item_matches_candidate("Grace", Regex("^gr", "i"), null_matches_undefined=False))
        self.assertTrue(QueryEngine._evaluate_in(document, "value", ("Ada",)))
        self.assertTrue(QueryEngine._evaluate_in(document, "value", (Regex("^gr", "i"),)))
        self.assertTrue(QueryEngine._evaluate_in({}, "value", (None,), null_matches_undefined=True))
        self.assertFalse(QueryEngine._evaluate_not_in(document, "value", ("Ada",)))

    def test_elem_match_candidate_returns_false_for_non_document_candidate(self):
        self.assertFalse(QueryEngine._match_elem_match_candidate("python", {"kind": "book"}))

    def test_query_engine_supports_top_level_json_schema(self):
        schema_filter = {
            "$jsonSchema": {
                "required": ["name"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "age": {"bsonType": "int"},
                },
            }
        }

        self.assertTrue(QueryEngine.match({"name": "Ada", "age": 10}, schema_filter))
        self.assertFalse(QueryEngine.match({"age": 10}, schema_filter))
        self.assertFalse(QueryEngine.match({"name": "Ada", "age": "old"}, schema_filter))

    def test_query_engine_combines_top_level_json_schema_with_other_clauses(self):
        filter_spec = {
            "$jsonSchema": {
                "required": ["tenant", "name"],
                "properties": {
                    "tenant": {"bsonType": "string"},
                    "name": {"bsonType": "string"},
                },
            },
            "tenant": "a",
        }

        self.assertTrue(QueryEngine.match({"tenant": "a", "name": "Ada"}, filter_spec))
        self.assertFalse(QueryEngine.match({"tenant": "b", "name": "Ada"}, filter_spec))
        self.assertFalse(QueryEngine.match({"tenant": "a"}, filter_spec))

    def test_query_engine_matches_dbref_subfields_in_filters(self):
        document = {
            "author": DBRef(
                "users",
                ObjectId("0123456789abcdef01234567"),
                database="observe",
                extras={"tenant": "t1"},
            )
        }

        self.assertTrue(QueryEngine.match(document, {"author.$ref": "users"}))
        self.assertTrue(
            QueryEngine.match(
                document,
                {"author.$id": ObjectId("0123456789abcdef01234567")},
            )
        )
        self.assertTrue(QueryEngine.match(document, {"author.$db": "observe"}))
        self.assertTrue(QueryEngine.match(document, {"author.tenant": "t1"}))
        self.assertFalse(QueryEngine.match(document, {"author.$id": "0123456789abcdef01234567"}))

    def test_query_engine_supports_json_schema_inside_logical_top_level_clauses(self):
        schema_clause = {
            "$jsonSchema": {
                "required": ["name", "age"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "age": {"bsonType": "int"},
                },
            }
        }

        self.assertTrue(
            QueryEngine.match(
                {"tenant": "a", "name": "Ada", "age": 10},
                {"$and": [schema_clause, {"tenant": "a"}]},
            )
        )
        self.assertTrue(
            QueryEngine.match(
                {"tenant": "b", "age": 11},
                {"$or": [schema_clause, {"tenant": "b"}]},
            )
        )
        self.assertFalse(
            QueryEngine.match(
                {"tenant": "a", "name": "Ada", "age": 10},
                {"$nor": [schema_clause]},
            )
        )
        self.assertTrue(
            QueryEngine.match(
                {"tenant": "b", "age": 11},
                {"$nor": [schema_clause]},
            )
        )
