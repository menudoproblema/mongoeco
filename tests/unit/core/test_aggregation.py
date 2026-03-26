import datetime
import math
import re
import unittest
import uuid
from unittest.mock import ANY

from mongoeco.compat import MongoDialect
from mongoeco.core.aggregation import (
    _ACCUMULATOR_FLAGS_KEY,
    _MISSING,
    _accumulator_flags,
    _aggregation_key,
    _apply_accumulators,
    _apply_group,
    _finalize_accumulators,
    _initialize_accumulators,
    _is_simple_projection,
    _match_spec_contains_expr,
    _require_projection,
    _resolve_aggregation_field_path,
    apply_pipeline,
    evaluate_expression,
    split_pushdown_pipeline,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import ObjectId, UNDEFINED


class AggregationTests(unittest.TestCase):
    def test_accumulator_flags_initializes_missing_internal_state(self):
        bucket: dict[object, object] = {_ACCUMULATOR_FLAGS_KEY: "invalid"}

        flags = _accumulator_flags(bucket)

        self.assertEqual(flags, {})
        self.assertIs(bucket[_ACCUMULATOR_FLAGS_KEY], flags)

    def test_aggregation_key_covers_supported_scalar_and_container_types(self):
        self.assertEqual(_aggregation_key(1.5), ("float", 1.5))
        self.assertEqual(_aggregation_key(b"abc"), ("bytes", b"abc"))
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        self.assertEqual(_aggregation_key(session_id), ("uuid", session_id))
        object_id = ObjectId("0123456789abcdef01234567")
        self.assertEqual(_aggregation_key(object_id), ("objectid", object_id))
        created_at = datetime.datetime(2026, 3, 25, 10, 0, 0)
        self.assertEqual(_aggregation_key(created_at), ("datetime", created_at))
        self.assertEqual(
            _aggregation_key({"a": [1, 2]}),
            ("dict", (("a", ("list", (("int", 1), ("int", 2)))),)),
        )

        class _Hashable:
            def __hash__(self) -> int:
                return 1

        self.assertEqual(_aggregation_key(_Hashable()), (_Hashable, ANY))

        class _Unhashable:
            __hash__ = None

            def __repr__(self) -> str:
                return "unhashable"

        self.assertEqual(_aggregation_key(_Unhashable()), ("repr", "unhashable"))

    def test_match_spec_contains_expr_and_field_path_resolution_cover_edge_cases(self):
        self.assertFalse(_match_spec_contains_expr("invalid"))
        self.assertTrue(_match_spec_contains_expr({"$and": [{"a": 1}, {"$expr": True}]}))
        self.assertTrue(_match_spec_contains_expr({"$nor": [{"a": 1}, {"$expr": True}]}))
        self.assertEqual(_resolve_aggregation_field_path([{"name": "Ada"}], "0.name"), "Ada")
        self.assertIs(_resolve_aggregation_field_path([{"name": "Ada"}], "2.name"), _MISSING)
        self.assertEqual(_resolve_aggregation_field_path([{"name": "Ada"}, 1], "name"), ["Ada"])
        self.assertIs(_resolve_aggregation_field_path(1, "name"), _MISSING)

    def test_evaluate_expression_supports_common_boolean_and_arithmetic_operators(self):
        document = {
            "score": 10,
            "bonus": 2,
            "tags": ["a", "b", "c"],
            "nested": {"value": 3},
            "other_tags": ["b", "d"],
        }

        self.assertTrue(evaluate_expression(document, {"$eq": ["$score", 10]}))
        self.assertTrue(evaluate_expression(document, {"$ne": ["$score", 9]}))
        self.assertTrue(evaluate_expression(document, {"$gt": ["$score", 9]}))
        self.assertTrue(evaluate_expression(document, {"$gte": ["$score", 10]}))
        self.assertTrue(evaluate_expression(document, {"$lt": ["$bonus", 3]}))
        self.assertTrue(evaluate_expression(document, {"$lte": ["$bonus", 2]}))
        self.assertTrue(evaluate_expression(document, {"$and": [True, {"$gt": ["$score", 5]}]}))
        self.assertTrue(evaluate_expression(document, {"$or": [False, {"$eq": ["$bonus", 2]}]}))
        self.assertTrue(evaluate_expression(document, {"$in": ["$bonus", [1, 2, 3]]}))
        self.assertEqual(evaluate_expression(document, {"$add": ["$score", "$bonus", 3]}), 15)
        self.assertEqual(evaluate_expression(document, {"$multiply": ["$bonus", 4]}), 8)
        self.assertEqual(evaluate_expression(document, {"$subtract": ["$score", "$bonus"]}), 8)
        self.assertEqual(evaluate_expression(document, {"$divide": ["$score", 2]}), 5)
        self.assertEqual(evaluate_expression(document, {"$mod": ["$score", 4]}), 2)
        self.assertEqual(evaluate_expression({"score": -5}, {"$mod": ["$score", 3]}), -2)
        self.assertEqual(evaluate_expression(document, {"$floor": [3.8]}), 3)
        self.assertEqual(evaluate_expression(document, {"$ceil": [3.2]}), 4)
        self.assertEqual(evaluate_expression(document, {"$size": "$tags"}), 3)
        self.assertEqual(evaluate_expression(document, {"$arrayElemAt": ["$tags", 1]}), "b")
        self.assertEqual(evaluate_expression(document, {"$arrayElemAt": ["$tags", -1]}), "c")
        self.assertIsNone(evaluate_expression(document, {"$arrayElemAt": ["$tags", 20]}))
        self.assertIsNone(evaluate_expression(document, {"$arrayElemAt": ["$missing", 0]}))
        self.assertIsNone(evaluate_expression(document, {"$arrayElemAt": ["$tags", "$missing"]}))
        self.assertEqual(evaluate_expression(document, {"$toString": "$score"}), "10")
        self.assertEqual(evaluate_expression({"flag": True}, {"$toString": "$flag"}), "true")
        self.assertEqual(
            evaluate_expression({"created_at": datetime.datetime(2021, 1, 1, 0, 0, 0)}, {"$toString": "$created_at"}),
            "2021-01-01T00:00:00.000Z",
        )
        self.assertEqual(evaluate_expression({"value": float("nan")}, {"$toString": "$value"}), "NaN")
        self.assertEqual(evaluate_expression({"value": float("inf")}, {"$toString": "$value"}), "Infinity")
        self.assertEqual(evaluate_expression({"value": float("-inf")}, {"$toString": "$value"}), "-Infinity")
        self.assertEqual(evaluate_expression({"value": b"hello"}, {"$toString": "$value"}), "aGVsbG8=")
        self.assertEqual(
            evaluate_expression(document, {"$let": {"vars": {"extra": 5}, "in": {"$add": ["$score", "$$extra"]}}}),
            15,
        )
        self.assertEqual(evaluate_expression(document, {"$first": "$tags"}), "a")
        self.assertEqual(evaluate_expression(document, {"$first": "$score"}), 10)
        self.assertEqual(evaluate_expression(document, {"$concat": ["$tags.0", "-", "$tags.1"]}), "a-b")
        self.assertEqual(evaluate_expression({"text": "Ada Lovelace"}, {"$toLower": "$text"}), "ada lovelace")
        self.assertEqual(evaluate_expression({"text": "Ada Lovelace"}, {"$toUpper": "$text"}), "ADA LOVELACE")
        self.assertEqual(evaluate_expression({"text": "  Ada  "}, {"$trim": {"input": "$text"}}), "Ada")
        self.assertEqual(evaluate_expression({"text": "\x00 Ada \x00"}, {"$trim": {"input": "$text"}}), "Ada")
        self.assertEqual(evaluate_expression({"text": "\u00a0Ada\u00a0"}, {"$trim": {"input": "$text"}}), "Ada")
        self.assertEqual(evaluate_expression({"text": "..Ada"}, {"$ltrim": {"input": "$text", "chars": "."}}), "Ada")
        self.assertEqual(evaluate_expression({"text": "Ada.."}, {"$rtrim": {"input": "$text", "chars": "."}}), "Ada")
        self.assertEqual(evaluate_expression({"text": "..Ada.."}, {"$trim": {"input": "$text", "chars": "."}}), "Ada")
        self.assertEqual(
            evaluate_expression({"text": "Ada Lovelace"}, {"$replaceOne": {"input": "$text", "find": "a", "replacement": "A"}}),
            "AdA Lovelace",
        )
        self.assertEqual(
            evaluate_expression({"text": "banana"}, {"$replaceAll": {"input": "$text", "find": "a", "replacement": "A"}}),
            "bAnAnA",
        )
        self.assertEqual(evaluate_expression({"left": "A", "right": "a"}, {"$strcasecmp": ["$left", "$right"]}), 0)
        self.assertEqual(evaluate_expression({"left": "beta", "right": "Alpha"}, {"$strcasecmp": ["$left", "$right"]}), 1)
        self.assertEqual(evaluate_expression({"left": "Alpha", "right": "beta"}, {"$strcasecmp": ["$left", "$right"]}), -1)
        self.assertEqual(evaluate_expression({"text": "Ada Lovelace"}, {"$substr": ["$text", 4, 4]}), "Love")
        self.assertEqual(evaluate_expression({"text": "Ada Lovelace"}, {"$substr": ["$text", -1, 3]}), "")
        self.assertEqual(evaluate_expression({"text": "Ada Lovelace"}, {"$substr": ["$text", 4, -1]}), "Lovelace")
        self.assertEqual(evaluate_expression({"text": "Ada"}, {"$substr": ["$text", 99, 2]}), "")
        self.assertEqual(evaluate_expression({"text": "cafétéria"}, {"$substr": ["$text", 0, 3]}), "caf")
        self.assertEqual(evaluate_expression({"text": "cafétéria"}, {"$substr": ["$text", 3, 2]}), "é")
        self.assertEqual(evaluate_expression({"text": "Ada Lovelace"}, {"$split": ["$text", " "]}), ["Ada", "Lovelace"])
        self.assertEqual(evaluate_expression({"start": 0, "end": 5, "step": 2}, {"$range": ["$start", "$end", "$step"]}), [0, 2, 4])
        self.assertEqual(evaluate_expression(document, {"$concatArrays": ["$tags", "$other_tags"]}), ["a", "b", "c", "b", "d"])
        self.assertEqual(evaluate_expression(document, {"$reverseArray": "$tags"}), ["c", "b", "a"])
        self.assertTrue(evaluate_expression({"values": ["x", []]}, {"$allElementsTrue": "$values"}))
        self.assertTrue(evaluate_expression({"values": [0, [], False]}, {"$anyElementTrue": "$values"}))
        self.assertEqual(evaluate_expression(document, {"$setUnion": ["$tags", "$other_tags"]}), ["a", "b", "c", "d"])
        self.assertEqual(evaluate_expression(document, {"$setDifference": ["$tags", "$other_tags"]}), ["a", "c"])
        self.assertEqual(evaluate_expression(document, {"$setIntersection": ["$tags", "$other_tags"]}), ["b"])
        self.assertTrue(evaluate_expression(document, {"$setEquals": [["a", "b", "b"], ["b", "a"]]}))
        self.assertTrue(evaluate_expression(document, {"$setIsSubset": [["a", "b"], ["b", "c", "a"]]}))
        self.assertIsNone(evaluate_expression(document, {"$concat": ["$tags.0", "$missing"]}))
        self.assertIsNone(evaluate_expression(document, {"$trim": {"input": "$missing"}}))
        self.assertIsNone(evaluate_expression(document, {"$replaceOne": {"input": "$missing", "find": "a", "replacement": "A"}}))
        self.assertIsNone(evaluate_expression(document, {"$strcasecmp": ["$missing", "A"]}))
        self.assertEqual(evaluate_expression(document, {"$substr": ["$missing", 0, 2]}), "")
        self.assertEqual(evaluate_expression(document, {"$toLower": "$missing"}), "")
        self.assertEqual(evaluate_expression(document, {"$toUpper": "$missing"}), "")
        self.assertIsNone(evaluate_expression(document, {"$split": ["$missing", " "]}))
        self.assertIsNone(evaluate_expression(document, {"$concatArrays": ["$tags", "$missing"]}))
        self.assertIsNone(evaluate_expression(document, {"$setUnion": ["$tags", "$missing"]}))
        self.assertEqual(
            evaluate_expression(document, {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}}),
            ["a", "b", "c"],
        )
        self.assertEqual(
            evaluate_expression(document, {"$map": {"input": "$tags", "in": "$$this"}}),
            ["a", "b", "c"],
        )
        self.assertIsNone(
            evaluate_expression(
                document,
                {"$map": {"input": "$tags", "as": "tag", "in": "$$this"}},
            )[0]
        )
        self.assertEqual(
            evaluate_expression(document, {"$filter": {"input": "$tags", "as": "tag", "cond": {"$in": ["$$tag", ["a", "c"]]}}}),
            ["a", "c"],
        )
        self.assertEqual(
            evaluate_expression(document, {"$reduce": {"input": [1, 2, 3], "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}}),
            6,
        )
        self.assertIsNone(evaluate_expression(document, {"$map": {"input": "$missing", "as": "tag", "in": "$$tag"}}))
        self.assertIsNone(evaluate_expression(document, {"$filter": {"input": "$missing", "as": "tag", "cond": True}}))
        self.assertIsNone(
            evaluate_expression(document, {"$reduce": {"input": "$missing", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}})
        )
        self.assertIsNone(evaluate_expression(document, {"$add": ["$missing", 5]}))
        self.assertIsNone(evaluate_expression(document, {"$multiply": ["$missing", 2]}))
        self.assertIsNone(evaluate_expression(document, {"$subtract": ["$missing", 2]}))
        self.assertIsNone(evaluate_expression(document, {"$divide": ["$missing", 2]}))
        self.assertIsNone(evaluate_expression(document, {"$mod": ["$missing", 2]}))
        self.assertIsNone(evaluate_expression(document, {"$floor": ["$missing"]}))
        self.assertIsNone(evaluate_expression(document, {"$ceil": ["$missing"]}))
        self.assertTrue(math.isnan(evaluate_expression({"value": float("inf")}, {"$mod": ["$value", 2]})))
        self.assertTrue(math.isnan(evaluate_expression({"value": float("nan")}, {"$mod": ["$value", 2]})))
        self.assertTrue(math.isinf(evaluate_expression(document, {"$floor": [float("inf")]})))
        self.assertTrue(math.isinf(evaluate_expression(document, {"$ceil": [float("-inf")]})))
        self.assertTrue(math.isnan(evaluate_expression(document, {"$floor": [float("nan")]})))
        self.assertTrue(math.isnan(evaluate_expression(document, {"$ceil": [float("nan")]})))
        self.assertIsNone(evaluate_expression(document, {"$in": ["$bonus", "$missing"]}))
        self.assertIsNone(evaluate_expression(document, {"$size": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$arrayToObject": "$missing"}))
        self.assertEqual(evaluate_expression(document, {"literal": "$score"}), {"literal": 10})
        self.assertEqual(evaluate_expression(document, ["$score", "$bonus"]), [10, 2])
        self.assertEqual(evaluate_expression(document, {"$literal": "$score"}), "$score")

    def test_evaluate_expression_supports_date_arithmetic(self):
        created_at = datetime.datetime(2024, 1, 1, 12, 0, 0)
        updated_at = datetime.datetime(2024, 1, 1, 12, 0, 5)

        self.assertEqual(
            evaluate_expression({"created_at": created_at}, {"$add": ["$created_at", 1500]}),
            datetime.datetime(2024, 1, 1, 12, 0, 1, 500000),
        )
        self.assertEqual(
            evaluate_expression({"updated_at": updated_at}, {"$subtract": ["$updated_at", 500]}),
            datetime.datetime(2024, 1, 1, 12, 0, 4, 500000),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": created_at, "updated_at": updated_at},
                {"$subtract": ["$updated_at", "$created_at"]},
            ),
            5000,
        )

        with self.assertRaises(OperationFailure):
            evaluate_expression(
                {"created_at": created_at, "updated_at": updated_at},
                {"$add": ["$created_at", "$updated_at"]},
            )

        with self.assertRaises(OperationFailure):
            evaluate_expression(
                {"created_at": created_at},
                {"$subtract": [5, "$created_at"]},
            )

    def test_evaluate_expression_date_trunc_evaluates_dynamic_parameters(self):
        document = {
            "created_at": datetime.datetime(2026, 3, 24, 10, 37, 52),
            "unit": "day",
            "bin": 2,
            "start": "monday",
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateTrunc": {
                        "date": "$created_at",
                        "unit": "$unit",
                        "binSize": "$bin",
                        "startOfWeek": "$start",
                    }
                },
            ),
            datetime.datetime(2026, 3, 24, 0, 0, 0),
        )

    def test_evaluate_expression_uses_mongo_truthiness_rules(self):
        document = {
            "empty_array": [],
            "non_empty_array": [1],
            "zero": 0,
            "one": 1,
            "text": "",
            "flag": False,
            "legacy": UNDEFINED,
        }

        self.assertTrue(evaluate_expression(document, {"$and": ["$empty_array", "$non_empty_array", "$one", "$text"]}))
        self.assertFalse(evaluate_expression(document, {"$and": ["$empty_array", "$zero"]}))
        self.assertTrue(evaluate_expression(document, {"$or": ["$flag", "$empty_array"]}))
        self.assertEqual(
            evaluate_expression(document, {"$cond": {"if": "$empty_array", "then": "truthy", "else": "falsey"}}),
            "truthy",
        )
        self.assertEqual(
            evaluate_expression(
                {"items": [0, [], None, False, "ok"]},
                {"$filter": {"input": "$items", "as": "item", "cond": "$$item"}},
            ),
            [[], "ok"],
        )
        self.assertEqual(
            evaluate_expression(document, {"$cond": {"if": "$legacy", "then": "truthy", "else": "falsey"}}),
            "falsey",
        )

    def test_evaluate_expression_and_pipeline_can_use_custom_dialect_truthiness(self):
        class _ArrayFalseDialect(MongoDialect):
            def expression_truthy(self, value: object) -> bool:
                if isinstance(value, list):
                    return False
                return super().expression_truthy(value)

        dialect = _ArrayFalseDialect(
            key="test",
            server_version="test",
            label="Array False Dialect",
        )

        self.assertFalse(
            evaluate_expression(
                {"empty_array": []},
                {"$or": ["$empty_array", False]},
                dialect=dialect,
            )
        )
        self.assertEqual(
            apply_pipeline(
                [{"_id": "1", "items": []}, {"_id": "2", "items": [1]}],
                [{"$match": {"$expr": "$items"}}],
                dialect=dialect,
            ),
            [],
        )

    def test_apply_pipeline_can_use_custom_dialect_stage_catalog(self):
        class _NoProjectDialect(MongoDialect):
            def supports_aggregation_stage(self, name: str) -> bool:
                return False if name == "$project" else super().supports_aggregation_stage(name)

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "name": "Ada"}],
                [{"$project": {"name": 1}}],
                dialect=_NoProjectDialect(
                    key="test",
                    server_version="test",
                    label="No Project",
                ),
            )

    def test_require_projection_wrapper_uses_default_dialect(self):
        self.assertEqual(_require_projection({"name": 1, "_id": 0}), {"name": 1, "_id": 0})

    def test_evaluate_expression_rejects_custom_supported_but_unimplemented_operator(self):
        class _FutureExpressionDialect(MongoDialect):
            def supports_aggregation_expression_operator(self, name: str) -> bool:
                return True if name == "$futureExpr" else super().supports_aggregation_expression_operator(name)

        with self.assertRaises(OperationFailure):
            evaluate_expression(
                {"value": 1},
                {"$futureExpr": "$value"},
                dialect=_FutureExpressionDialect(
                    key="test",
                    server_version="test",
                    label="Future Expr",
                ),
            )

    def test_group_rejects_unsupported_accumulator_for_default_and_custom_dialects(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$group": {"_id": None, "value": {"$futureAccumulator": "$value"}}}],
            )

        class _FutureGroupDialect(MongoDialect):
            def supports_group_accumulator(self, name: str) -> bool:
                return True if name == "$futureAccumulator" else super().supports_group_accumulator(name)

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$group": {"_id": None, "value": {"$futureAccumulator": "$value"}}}],
                dialect=_FutureGroupDialect(
                    key="test",
                    server_version="test",
                    label="Future Group",
                ),
            )

    def test_apply_group_directly_rejects_unsupported_accumulator_under_default_dialect(self):
        with self.assertRaises(OperationFailure):
            _apply_group(
                [{"value": 1}],
                {"_id": None, "value": {"$futureAccumulator": "$value"}},
                {},
            )

    def test_group_preserves_user_fields_with_has_prefix(self):
        grouped = _apply_group(
            [{"kind": "a", "value": 1}],
            {"_id": "$kind", "__has_total": {"$first": "$value"}},
        )

        self.assertEqual(grouped, [{"_id": "a", "__has_total": 1}])

    def test_finalize_accumulators_preserves_user_fields_with_has_prefix(self):
        bucket = _initialize_accumulators({"__has_total": {"$first": "$value"}})
        _apply_accumulators(bucket, {"__has_total": {"$first": "$value"}}, {"value": 7})

        self.assertEqual(_finalize_accumulators(bucket), {"__has_total": 7})

    def test_bucket_auto_rejects_unsupported_accumulator_for_default_and_custom_dialects(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [
                    {
                        "$bucketAuto": {
                            "groupBy": "$value",
                            "buckets": 1,
                            "output": {"value": {"$futureAccumulator": "$value"}},
                        }
                    }
                ],
            )

        class _FutureBucketDialect(MongoDialect):
            def supports_group_accumulator(self, name: str) -> bool:
                return True if name == "$futureAccumulator" else super().supports_group_accumulator(name)

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [
                    {
                        "$bucketAuto": {
                            "groupBy": "$value",
                            "buckets": 1,
                            "output": {"value": {"$futureAccumulator": "$value"}},
                        }
                    }
                ],
                dialect=_FutureBucketDialect(
                    key="test",
                    server_version="test",
                    label="Future Bucket",
                ),
            )

    def test_initialize_accumulators_directly_rejects_unsupported_accumulator_under_default_dialect(self):
        with self.assertRaises(OperationFailure):
            _initialize_accumulators({"value": {"$futureAccumulator": "$value"}})

    def test_set_window_fields_rejects_supported_but_unimplemented_accumulator(self):
        class _FutureWindowDialect(MongoDialect):
            def supports_window_accumulator(self, name: str) -> bool:
                return True if name == "$rank" else super().supports_window_accumulator(name)

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"kind": "a", "value": 1}],
                [
                    {
                        "$setWindowFields": {
                            "partitionBy": "$kind",
                            "sortBy": {"value": 1},
                            "output": {"rank": {"$rank": "$value"}},
                        }
                    }
                ],
                dialect=_FutureWindowDialect(
                    key="test",
                    server_version="test",
                    label="Future Window",
                ),
            )

    def test_apply_pipeline_rejects_custom_supported_but_unimplemented_stage(self):
        class _FutureStageDialect(MongoDialect):
            def supports_aggregation_stage(self, name: str) -> bool:
                return True if name == "$futureStage" else super().supports_aggregation_stage(name)

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1"}],
                [{"$futureStage": {"value": 1}}],
                dialect=_FutureStageDialect(
                    key="test",
                    server_version="test",
                    label="Future Stage",
                ),
            )

    def test_evaluate_expression_rejects_currently_unsupported_operators_explicitly(self):
        document = {"value": "10", "tags": ["a", "b"]}

        unsupported_specs = [
            {"$convert": {"input": "$value", "to": "int"}},
            {"$setField": {"field": "name", "input": "$$ROOT", "value": "Ada"}},
            {"$percentile": {"input": "$value", "p": [0.5], "method": "approximate"}},
            {"$median": {"input": "$value", "method": "approximate"}},
        ]

        for spec in unsupported_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(OperationFailure):
                    evaluate_expression(document, spec)

    def test_evaluate_expression_rejects_broader_unsupported_inventory_explicitly(self):
        document = {
            "value": 10,
            "text": "Ada",
            "items": [1, 2, 3],
            "created_at": datetime.datetime(2026, 3, 25, 10, 0, 0),
        }

        unsupported_specs = [
            {"$bsonSize": "$$ROOT"},
            {"$rand": {}},
            {"$sampleRate": 0.5},
            {"$toHashedIndexKey": "$text"},
            {"$toDecimal": "$text"},
            {"$function": {"body": "function() { return 1; }", "args": [], "lang": "js"}},
            {"$accumulator": {"init": "function(){}", "accumulate": "function(){}", "accumulateArgs": [], "merge": "function(){}", "finalize": "function(x){return x;}", "lang": "js"}},
        ]

        for spec in unsupported_specs:
            with self.subTest(spec=spec):
                with self.assertRaises(OperationFailure):
                    evaluate_expression(document, spec)

    def test_evaluate_expression_supports_is_number_and_type(self):
        document = {
            "value": 10,
            "ratio": 1.5,
            "text": "Ada",
            "items": [1, "x"],
            "blob": b"abc",
            "created_at": datetime.datetime(2026, 3, 25, 10, 0, 0),
            "legacy": UNDEFINED,
        }

        self.assertTrue(evaluate_expression(document, {"$isNumber": "$value"}))
        self.assertTrue(evaluate_expression(document, {"$isNumber": "$ratio"}))
        self.assertFalse(evaluate_expression(document, {"$isNumber": "$text"}))
        self.assertFalse(evaluate_expression(document, {"$isNumber": "$missing"}))
        self.assertEqual(evaluate_expression(document, {"$type": "$value"}), "int")
        self.assertEqual(evaluate_expression(document, {"$type": "$ratio"}), "double")
        self.assertEqual(evaluate_expression(document, {"$type": "$items"}), "array")
        self.assertEqual(evaluate_expression(document, {"$type": "$blob"}), "binData")
        self.assertEqual(evaluate_expression(document, {"$type": "$created_at"}), "date")
        self.assertEqual(evaluate_expression(document, {"$type": "$legacy"}), "undefined")
        self.assertEqual(evaluate_expression(document, {"$type": "$missing"}), "missing")

    def test_evaluate_expression_supports_scalar_coercions(self):
        document = {
            "int_text": "42",
            "float_text": "3.5",
            "truthy_text": "false",
            "date": datetime.datetime(2026, 3, 25, 10, 0, 0),
            "zero": 0,
            "flag": True,
            "missing": None,
        }

        self.assertEqual(evaluate_expression(document, {"$toInt": "$int_text"}), 42)
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$float_text"}), 3.5)
        self.assertEqual(evaluate_expression(document, {"$toLong": "$flag"}), 1)
        self.assertEqual(evaluate_expression(document, {"$toBool": "$zero"}), False)
        self.assertEqual(evaluate_expression(document, {"$toBool": "$truthy_text"}), True)
        self.assertEqual(evaluate_expression(document, {"$toLong": "$date"}), 1774432800000)
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$date"}), 1774432800000.0)
        self.assertIsNone(evaluate_expression(document, {"$toInt": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$toLong": "$unknown"}))

    def test_evaluate_expression_scalar_coercions_reject_invalid_values(self):
        document = {
            "bad_text": "4.2",
            "array_value": [1],
            "huge": 1 << 70,
            "fractional": 3.5,
        }

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toInt": "$bad_text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toInt": "$fractional"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toLong": "$huge"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDouble": "$array_value"})

    def test_evaluate_expression_supports_date_add_subtract_and_diff(self):
        document = {
            "start": datetime.datetime(2026, 3, 25, 10, 0, 0),
            "end": datetime.datetime(2026, 3, 27, 9, 0, 0),
            "month_end": datetime.datetime(2026, 1, 31, 12, 0, 0),
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": 2}},
            ),
            datetime.datetime(2026, 3, 27, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateSubtract": {"startDate": "$start", "unit": "hour", "amount": 3}},
            ),
            datetime.datetime(2026, 3, 25, 7, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$month_end", "unit": "month", "amount": 1}},
            ),
            datetime.datetime(2026, 2, 28, 12, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "day"}},
            ),
            2,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "hour"}},
            ),
            47,
        )

    def test_evaluate_expression_date_math_supports_timezone_and_week_diff(self):
        document = {
            "start": datetime.datetime(2026, 3, 29, 0, 30, 0),
            "end": datetime.datetime(2026, 4, 5, 0, 30, 0),
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateAdd": {
                        "startDate": "$start",
                        "unit": "day",
                        "amount": 1,
                        "timezone": "+02:00",
                    }
                },
            ),
            datetime.datetime(2026, 3, 30, 0, 30, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateDiff": {
                        "startDate": "$start",
                        "endDate": "$end",
                        "unit": "week",
                        "startOfWeek": "sunday",
                    }
                },
            ),
            1,
        )

    def test_evaluate_expression_date_math_rejects_invalid_shapes(self):
        document = {"start": datetime.datetime(2026, 3, 25, 10, 0, 0)}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateAdd": {"startDate": "$start", "unit": "day"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": 1.5}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateSubtract": {"startDate": "$start", "unit": "century", "amount": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateDiff": {"startDate": "$start", "endDate": "$start", "unit": "week", "startOfWeek": 1}})

    def test_evaluate_expression_supports_substr_and_strlen_byte_and_codepoint_variants(self):
        document = {"text": "é寿司A"}

        self.assertEqual(evaluate_expression(document, {"$substrBytes": ["$text", 0, 2]}), "é")
        self.assertEqual(evaluate_expression(document, {"$substrCP": ["$text", 1, 2]}), "寿司")
        self.assertEqual(evaluate_expression(document, {"$strLenBytes": "$text"}), len("é寿司A".encode("utf-8")))
        self.assertEqual(evaluate_expression(document, {"$strLenCP": "$text"}), 4)
        self.assertEqual(evaluate_expression(document, {"$substrBytes": ["$missing", 0, 2]}), "")
        self.assertEqual(evaluate_expression(document, {"$substrCP": ["$missing", 0, 2]}), "")

    def test_evaluate_expression_substr_and_strlen_variants_reject_invalid_values(self):
        document = {"text": "é寿司A", "items": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substrBytes": ["$text", 1, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substrCP": ["$items", 0, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$strLenBytes": "$missing"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$strLenCP": "$items"})

    def test_evaluate_expression_supports_string_index_and_binary_size_variants(self):
        document = {"text": "é寿司A", "blob": b"abcd", "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678")}

        self.assertEqual(evaluate_expression(document, {"$indexOfBytes": ["$text", "寿", 0, 9]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfCP": ["$text", "司A", 0, 4]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfCP": ["$text", "x"]}), -1)
        self.assertEqual(evaluate_expression(document, {"$binarySize": "$blob"}), 4)
        self.assertEqual(evaluate_expression(document, {"$binarySize": "$uuid"}), 16)
        self.assertIsNone(evaluate_expression(document, {"$binarySize": "$missing"}))

    def test_evaluate_expression_index_and_binary_size_variants_reject_invalid_values(self):
        document = {"text": "é寿司A", "blob": b"abcd", "items": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfBytes": ["$text", "寿", -1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfCP": ["$items", "1"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$binarySize": "$text"})

    def test_evaluate_expression_supports_regex_match_find_and_find_all(self):
        document = {"text": "Ada and ada", "missing": None}

        self.assertTrue(
            evaluate_expression(document, {"$regexMatch": {"input": "$text", "regex": "^ada", "options": "i"}})
        )
        self.assertEqual(
            evaluate_expression(document, {"$regexFind": {"input": "$text", "regex": "(a)(d)a", "options": "i"}}),
            {"match": "Ada", "idx": 0, "captures": ["A", "d"]},
        )
        self.assertEqual(
            evaluate_expression(document, {"$regexFindAll": {"input": "$text", "regex": "a", "options": "i"}}),
            [
                {"match": "A", "idx": 0, "captures": []},
                {"match": "a", "idx": 2, "captures": []},
                {"match": "a", "idx": 4, "captures": []},
                {"match": "a", "idx": 8, "captures": []},
                {"match": "a", "idx": 10, "captures": []},
            ],
        )
        self.assertFalse(evaluate_expression(document, {"$regexMatch": {"input": "$missing", "regex": "a"}}))
        self.assertIsNone(evaluate_expression(document, {"$regexFind": {"input": "$missing", "regex": "a"}}))
        self.assertEqual(evaluate_expression(document, {"$regexFindAll": {"input": "$missing", "regex": "a"}}), [])

    def test_evaluate_expression_regex_variants_reject_invalid_values(self):
        document = {"text": "Ada", "items": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexMatch": {"input": "$items", "regex": "a"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexFind": {"input": "$text", "regex": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexFindAll": {"input": "$text", "regex": "a", "options": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexMatch": {"input": "$text", "regex": re.compile("a", re.DOTALL)}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$regexMatch": {"input": "$text", "regex": re.compile("a"), "options": "i"}})

    def test_evaluate_expression_supports_numeric_math_variants(self):
        document = {"value": 19.25, "base": 100, "power": 3}

        self.assertEqual(evaluate_expression(document, {"$abs": -4}), 4)
        self.assertAlmostEqual(evaluate_expression(document, {"$exp": 1}), math.e)
        self.assertAlmostEqual(evaluate_expression(document, {"$ln": math.e}), 1.0)
        self.assertAlmostEqual(evaluate_expression(document, {"$log": ["$base", 10]}), 2.0)
        self.assertAlmostEqual(evaluate_expression(document, {"$log10": "$base"}), 2.0)
        self.assertEqual(evaluate_expression(document, {"$pow": [2, "$power"]}), 8.0)
        self.assertEqual(evaluate_expression(document, {"$round": ["$value", 1]}), 19.2)
        self.assertEqual(evaluate_expression(document, {"$round": [25, 0]}), 25)
        self.assertEqual(evaluate_expression(document, {"$trunc": ["$value", 1]}), 19.2)
        self.assertEqual(evaluate_expression(document, {"$trunc": ["$value", -1]}), 10)
        self.assertEqual(evaluate_expression(document, {"$sqrt": 25}), 5.0)
        self.assertIsNone(evaluate_expression(document, {"$ln": "$missing"}))

    def test_evaluate_expression_numeric_math_variants_reject_invalid_values(self):
        document = {"value": -1}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sqrt": "$value"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$ln": 0})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$log": [8, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$round": [1.25, 101]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trunc": [1.25, True]})

    def test_evaluate_expression_supports_week_variants(self):
        document = {"created_at": datetime.datetime(2026, 1, 1, 23, 30, 0)}

        self.assertEqual(evaluate_expression(document, {"$week": "$created_at"}), 0)
        self.assertEqual(
            evaluate_expression(document, {"$week": {"date": "$created_at", "timezone": "+02:00"}}),
            0,
        )
        self.assertEqual(evaluate_expression(document, {"$isoWeek": "$created_at"}), 1)
        self.assertEqual(evaluate_expression(document, {"$isoWeekYear": "$created_at"}), 2026)

    def test_evaluate_expression_week_variants_reject_invalid_values(self):
        document = {"text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$week": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$isoWeek": {"timezone": "UTC"}})

    def test_evaluate_expression_supports_date_string_parts_and_parsing_variants(self):
        document = {"created_at": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000)}

        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToString": {"date": "$created_at", "format": "%Y-%m-%d %H:%M:%S.%L", "timezone": "UTC"}},
            ),
            "2026-03-25 10:05:06.789",
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToParts": {"date": "$created_at", "timezone": "UTC"}},
            ),
            {
                "year": 2026,
                "month": 3,
                "day": 25,
                "hour": 10,
                "minute": 5,
                "second": 6,
                "millisecond": 789,
            },
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToParts": {"date": "$created_at", "timezone": "UTC", "iso8601": True}},
            ),
            {
                "isoWeekYear": 2026,
                "isoWeek": 13,
                "isoDayOfWeek": 3,
                "hour": 10,
                "minute": 5,
                "second": 6,
                "millisecond": 789,
            },
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromString": {"dateString": "2026-03-25T10:05:06.789Z"}},
            ),
            datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {
                    "$dateFromString": {
                        "dateString": "2026-03-25 12:05:06.789",
                        "format": "%Y-%m-%d %H:%M:%S.%L",
                        "timezone": "+02:00",
                    }
                },
            ),
            datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
        )

    def test_evaluate_expression_date_string_parts_and_parsing_variants_reject_invalid_values(self):
        document = {"text": "Ada", "created_at": datetime.datetime(2026, 3, 25, 10, 5, 6)}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateToString": {"date": "$text"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateToParts": {"date": "$created_at", "iso8601": "yes"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromString": {"dateString": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromString": {"dateString": "2026-03-25", "format": "%Q"}})

    def test_evaluate_expression_supports_object_to_array_and_zip(self):
        document = {"doc": {"a": 1, "b": 2}, "left": ["a", "b"], "right": [1], "defaults": ["x", 0]}

        self.assertEqual(
            evaluate_expression(document, {"$objectToArray": "$doc"}),
            [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
        )
        self.assertEqual(
            evaluate_expression(document, {"$zip": {"inputs": ["$left", "$right"]}}),
            [["a", 1]],
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$zip": {"inputs": ["$left", "$right"], "useLongestLength": True, "defaults": "$defaults"}},
            ),
            [["a", 1], ["b", 0]],
        )

    def test_evaluate_expression_object_to_array_and_zip_reject_invalid_values(self):
        document = {"text": "Ada", "left": ["a"], "right": [1]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$objectToArray": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$zip": {"inputs": ["$left", "$right"], "useLongestLength": "yes"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$zip": {"inputs": ["$left", "$right"], "defaults": [0]}})

    def test_evaluate_expression_supports_to_date_and_date_from_parts(self):
        oid = ObjectId("65f0a1000000000000000000")
        document = {"millis": 1_711_361_506_789, "text": "2026-03-25T10:05:06.789Z", "oid": oid}

        self.assertEqual(
            evaluate_expression(document, {"$toDate": "$millis"}),
            datetime.datetime(2024, 3, 25, 10, 11, 46, 789000),
        )
        self.assertEqual(
            evaluate_expression(document, {"$toDate": "$text"}),
            datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
        )
        self.assertEqual(
            evaluate_expression(document, {"$toDate": "$oid"}),
            datetime.datetime.fromtimestamp(oid.generation_time, tz=datetime.UTC).replace(tzinfo=None),
        )
        self.assertEqual(
            evaluate_expression(document, {"$toObjectId": "65f0a1000000000000000000"}),
            oid,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromParts": {"year": 2026, "month": 3, "day": 25, "hour": 12, "timezone": "+02:00"}},
            ),
            datetime.datetime(2026, 3, 25, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromParts": {"isoWeekYear": 2026, "isoWeek": 13, "isoDayOfWeek": 3, "hour": 10}},
            ),
            datetime.datetime(2026, 3, 25, 10, 0, 0),
        )

    def test_evaluate_expression_to_date_and_date_from_parts_reject_invalid_values(self):
        document = {"text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDate": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toObjectId": "$text"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromParts": {"year": 2026, "isoWeekYear": 2026}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateFromParts": {"month": 3}})

    def test_evaluate_expression_supports_switch_and_bitwise_variants(self):
        document = {"value": 5}

        self.assertEqual(
            evaluate_expression(
                document,
                {"$switch": {"branches": [{"case": {"$gt": ["$value", 10]}, "then": "big"}], "default": "small"}},
            ),
            "small",
        )
        self.assertEqual(evaluate_expression(document, {"$bitAnd": [7, 3]}), 3)
        self.assertEqual(evaluate_expression(document, {"$bitOr": [4, 1]}), 5)
        self.assertEqual(evaluate_expression(document, {"$bitXor": [7, 3]}), 4)
        self.assertEqual(evaluate_expression(document, {"$bitNot": 7}), -8)

    def test_evaluate_expression_switch_and_bitwise_variants_reject_invalid_values(self):
        document = {"value": 5}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$switch": {"branches": []}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$switch": {"branches": [{"case": True}]}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$bitAnd": [7, 3.5]})

    def test_group_and_set_window_fields_reject_unsupported_accumulator_inventory(self):
        documents = [{"_id": "1", "group": "a", "value": 10}]

        unsupported_group_accumulators = [
            "$bottom", "$bottomN", "$firstN", "$lastN",
            "$maxN", "$median", "$minN", "$percentile", "$stdDevPop",
            "$stdDevSamp", "$top", "$topN",
        ]
        for operator in unsupported_group_accumulators:
            with self.subTest(group_operator=operator):
                with self.assertRaises(OperationFailure):
                    apply_pipeline(documents, [{"$group": {"_id": "$group", "result": {operator: "$value"}}}])

        unsupported_window_operators = [
            "$bottom", "$bottomN", "$covariancePop", "$covarianceSamp",
            "$denseRank", "$derivative", "$documentNumber", "$expMovingAvg", "$integral",
            "$linearFill", "$locf", "$minN", "$rank", "$shift", "$stdDevPop",
            "$stdDevSamp", "$top", "$topN",
        ]
        for operator in unsupported_window_operators:
            with self.subTest(window_operator=operator):
                with self.assertRaises(OperationFailure):
                    apply_pipeline(
                        documents,
                        [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {operator: "$value"}}}}],
                    )

    def test_set_window_fields_rejects_rank_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$rank": {}}}}}],
            )

    def test_set_window_fields_rejects_dense_rank_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$denseRank": {}}}}}],
            )

    def test_set_window_fields_rejects_shift_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$shift": {"output": "$value", "by": 1}}}}}],
            )

    def test_set_window_fields_rejects_locf_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$locf": "$value"}}}}],
            )

    def test_set_window_fields_rejects_linear_fill_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1", "value": 10}],
                [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"result": {"$linearFill": "$value"}}}}],
            )

    def test_evaluate_expression_supports_dotted_variable_paths(self):
        document = {"profile": {"city": "Sevilla"}}
        variables = {
            "item": {"name": "Ada", "profile": {"city": "Madrid"}},
            "ROOT": {"profile": {"city": "Bilbao"}},
        }

        self.assertEqual(evaluate_expression(document, "$$item.name", variables), "Ada")
        self.assertEqual(evaluate_expression(document, "$$item.profile.city", variables), "Madrid")
        self.assertEqual(evaluate_expression(document, "$$ROOT.profile.city", variables), "Sevilla")
        self.assertIsNone(evaluate_expression(document, "$$item.missing", variables))
        self.assertIsNone(evaluate_expression(document, "$$missing.name", variables))

    def test_evaluate_expression_preserves_array_traversal_for_field_and_variable_paths(self):
        document = {
            "items": [{"kind": "a", "tags": ["x", "y"]}, {"kind": "b", "tags": ["z"]}],
        }
        variables = {
            "item": {"children": [{"name": "Ada"}, {"name": "Grace"}]},
        }

        self.assertEqual(evaluate_expression(document, "$items.kind"), ["a", "b"])
        self.assertEqual(evaluate_expression(document, "$items.tags"), [["x", "y"], ["z"]])
        self.assertEqual(evaluate_expression(document, "$$item.children.name", variables), ["Ada", "Grace"])

    def test_pipeline_project_preserves_array_traversal(self):
        documents = [{"_id": "1", "items": [{"kind": "a"}, {"kind": "b"}]}]

        self.assertEqual(
            apply_pipeline(documents, [{"$project": {"_id": 0, "kinds": "$items.kind"}}]),
            [{"kinds": ["a", "b"]}],
        )

    def test_pipeline_match_supports_nested_expr_inside_and_or(self):
        documents = [
            {"_id": "1", "a": 5, "b": 11},
            {"_id": "2", "a": 5, "b": 9},
            {"_id": "3", "a": 4, "b": 20},
        ]

        self.assertEqual(
            apply_pipeline(
                documents,
                [{"$match": {"$and": [{"a": 5}, {"$expr": {"$gt": ["$b", 10]}}]}}],
            ),
            [{"_id": "1", "a": 5, "b": 11}],
        )

    def test_pipeline_match_expr_can_compare_two_fields_from_same_document(self):
        documents = [
            {"_id": "1", "spent": 12, "budget": 10},
            {"_id": "2", "spent": 8, "budget": 10},
        ]

        self.assertEqual(
            apply_pipeline(documents, [{"$match": {"$expr": {"$gt": ["$spent", "$budget"]}}}]),
            [{"_id": "1", "spent": 12, "budget": 10}],
        )

    def test_pipeline_match_expr_uses_mongo_truthiness_rules_for_arrays(self):
        documents = [
            {"_id": "1", "flag": [], "kind": "keep"},
            {"_id": "2", "flag": 0, "kind": "drop"},
        ]

        self.assertEqual(
            apply_pipeline(documents, [{"$match": {"$expr": "$flag"}}]),
            [{"_id": "1", "flag": [], "kind": "keep"}],
        )

    def test_evaluate_expression_rejects_convert_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": "10"}, {"$convert": {"input": "$value", "to": "int"}})

    def test_evaluate_expression_rejects_set_field_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": 1}, {"$setField": {"field": "name", "input": "$$ROOT", "value": "Ada"}})

    def test_evaluate_expression_rejects_percentile_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": 10}, {"$percentile": {"input": "$value", "p": [0.5], "method": "approximate"}})

    def test_evaluate_expression_rejects_median_with_dedicated_test(self):
        with self.assertRaises(OperationFailure):
            evaluate_expression({"value": 10}, {"$median": {"input": "$value", "method": "approximate"}})

    def test_pipeline_match_supports_nested_expr_inside_nor(self):
        documents = [
            {"_id": "1", "a": 6, "kind": "drop"},
            {"_id": "2", "a": 4, "kind": "keep"},
            {"_id": "3", "a": 8, "kind": "keep"},
        ]

        self.assertEqual(
            apply_pipeline(
                documents,
                [{"$match": {"$nor": [{"$expr": {"$gt": ["$a", 5]}}, {"kind": "drop"}]}}],
            ),
            [{"_id": "2", "a": 4, "kind": "keep"}],
        )

    def test_pipeline_match_rejects_non_list_and_or_payloads_and_filters_false_expr(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": True, "$and": {"a": 1}}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": True, "$or": {"a": 1}}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": True, "$nor": {"a": 1}}}])

        self.assertEqual(
            apply_pipeline([{"_id": "1", "a": 1}], [{"$match": {"$expr": {"$gt": ["$a", 5]}}}]),
            [],
        )
        self.assertEqual(
            apply_pipeline(
                [{"_id": "1", "a": 1}],
                [{"$match": {"$expr": True, "$or": [{"a": 2}, {"$expr": False}]}}],
            ),
            [],
        )

    def test_evaluate_expression_rejects_invalid_string_operator_arguments(self):
        document = {"text": "Ada", "number": 7}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$concat": ["$text", "$number"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toLower": "$number"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toUpper": "$number"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$split": ["$text", "$number"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$split": ["$text", ""]})

    def test_evaluate_expression_covers_string_array_and_set_edge_cases(self):
        document = {
            "text": "Ada",
            "tags": ["a", "b"],
            "none_chars": None,
            "number": 7,
        }

        self.assertIsNone(evaluate_expression(document, {"$reverseArray": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$allElementsTrue": "$missing"}))
        self.assertIsNone(evaluate_expression(document, {"$setDifference": ["$tags", "$missing"]}))
        self.assertIsNone(evaluate_expression(document, {"$setEquals": ["$tags", "$missing"]}))
        self.assertFalse(evaluate_expression(document, {"$setEquals": [["a"], ["a", "b"]]}))
        self.assertFalse(evaluate_expression(document, {"$setEquals": [["a", "c"], ["a", "b"]]}))
        self.assertFalse(evaluate_expression(document, {"$setIsSubset": [["a", "z"], ["a", "b"]]}))
        self.assertIsNone(evaluate_expression(document, {"$trim": {"input": "$text", "chars": "$none_chars"}}))
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$setIsSubset": [["a"], ["a", "b"], ["a"]]})

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$range": ["$number", 5, 0]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$range": [0, 5, 1.5]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trim": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trim": {"input": "$number"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$trim": {"input": "$text", "chars": "$number"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$replaceOne": {"input": "$text", "find": "a"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$replaceAll": {"input": "$text", "find": "$text", "replacement": "$number"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression({"text": "cafétéria"}, {"$substr": ["$text", 3, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": ["$text", True, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": ["$text", 0, True]})

    def test_evaluate_expression_supports_cond_object_and_if_null_fallback_to_none(self):
        document = {"score": 2, "bonus": None}

        self.assertEqual(
            evaluate_expression(
                document,
                {"$cond": {"if": {"$gt": ["$score", 5]}, "then": "high", "else": "low"}},
            ),
            "low",
        )
        self.assertIsNone(evaluate_expression(document, {"$ifNull": ["$bonus", None]}))
        self.assertEqual(
            evaluate_expression({"legacy": UNDEFINED}, {"$ifNull": ["$legacy", "fallback"]}),
            "fallback",
        )
        self.assertIsNone(evaluate_expression(document, "$$missing"))
        self.assertIsNone(evaluate_expression(document, "$missing"))
        self.assertIsNone(evaluate_expression(document, {"$toString": None}))
        self.assertEqual(
            evaluate_expression(
                {"profile": {"name": "Ada"}, "fallback": {"city": "Sevilla"}},
                {"$mergeObjects": [None, "$fallback", {"name": {"$getField": {"field": "name", "input": "$profile"}}}]},
            ),
            {"city": "Sevilla", "name": "Ada"},
        )
        self.assertEqual(evaluate_expression({"profile": {"name": "Ada"}}, {"$getField": "profile"}), {"name": "Ada"})
        self.assertIsNone(evaluate_expression({"profile": None}, {"$getField": {"field": "name", "input": "$profile"}}))
        self.assertEqual(
            evaluate_expression(
                {"literal": "$score", "score": 10},
                {"$getField": {"field": "literal"}},
            ),
            "$score",
        )
        self.assertEqual(
            evaluate_expression(
                {"a.b.c": 1, "$price": 2, "x..y": 3},
                {"$getField": {"field": "a.b.c"}},
            ),
            1,
        )
        self.assertEqual(
            evaluate_expression(
                {"a.b.c": 1, "$price": 2, "x..y": 3},
                {"$getField": {"field": {"$literal": "$price"}}},
            ),
            2,
        )
        self.assertEqual(
            evaluate_expression(
                {"a.b.c": 1, "$price": 2, "x..y": 3},
                {"$getField": {"field": "x..y"}},
            ),
            3,
        )
        self.assertIsNone(
            evaluate_expression(
                {"profile": {"name": "Ada"}},
                {"$getField": {"field": "$missing", "input": "$$CURRENT"}},
            )
        )

    def test_pipeline_supports_remove_variable_in_add_fields_and_project(self):
        documents = [{"_id": "1", "keep": 1, "drop": 2}]

        self.assertEqual(
            apply_pipeline(documents, [{"$addFields": {"drop": "$$REMOVE"}}]),
            [{"_id": "1", "keep": 1}],
        )
        self.assertEqual(
            apply_pipeline(documents, [{"$project": {"_id": 0, "keep": 1, "drop": "$$REMOVE"}}]),
            [{"keep": 1}],
        )

    def test_evaluate_expression_supports_array_to_object_index_of_array_and_sort_array(self):
        document = {
            "pairs": [["a", 1], ["b", 2]],
            "kv_pairs": [{"k": "x", "v": 10}, {"k": "y", "v": 20}],
            "numbers": [4, 1, 3, 2],
            "items": [{"rank": 3, "name": "c"}, {"rank": 1, "name": "a"}, {"rank": 2, "name": "b"}],
        }

        self.assertEqual(
            evaluate_expression(document, {"$arrayToObject": "$pairs"}),
            {"a": 1, "b": 2},
        )
        self.assertEqual(
            evaluate_expression(document, {"$arrayToObject": "$kv_pairs"}),
            {"x": 10, "y": 20},
        )
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 3]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 3, 3]}), -1)
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 3, 0, 3]}), 2)
        self.assertEqual(evaluate_expression(document, {"$indexOfArray": ["$numbers", 9]}), -1)
        self.assertIsNone(evaluate_expression(document, {"$indexOfArray": ["$missing", 9]}))
        self.assertEqual(
            evaluate_expression(document, {"$sortArray": {"input": "$numbers", "sortBy": 1}}),
            [1, 2, 3, 4],
        )
        self.assertEqual(
            evaluate_expression(document, {"$sortArray": {"input": "$numbers", "sortBy": -1}}),
            [4, 3, 2, 1],
        )
        self.assertEqual(
            evaluate_expression(document, {"$sortArray": {"input": "$items", "sortBy": {"rank": 1}}}),
            [{"rank": 1, "name": "a"}, {"rank": 2, "name": "b"}, {"rank": 3, "name": "c"}],
        )
        self.assertIsNone(evaluate_expression(document, {"$sortArray": {"input": "$missing", "sortBy": 1}}))

    def test_group_bucket_and_window_support_last_and_add_to_set_accumulators(self):
        documents = [
            {"_id": "1", "group": "a", "value": 1, "tag": "x"},
            {"_id": "2", "group": "a", "value": 3, "tag": "x"},
            {"_id": "3", "group": "a", "value": 5, "tag": "y"},
            {"_id": "4", "group": "b", "value": 2, "tag": "z"},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "lastValue": {"$last": "$value"},
                        "uniqueTags": {"$addToSet": "$tag"},
                    }
                }
            ],
        )
        self.assertEqual(
            grouped,
            [
                {"_id": "a", "lastValue": 5, "uniqueTags": ["x", "y"]},
                {"_id": "b", "lastValue": 2, "uniqueTags": ["z"]},
            ],
        )

        bucketed = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$value",
                        "boundaries": [0, 4, 10],
                        "output": {
                            "lastTag": {"$last": "$tag"},
                            "uniqueTags": {"$addToSet": "$tag"},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            bucketed,
            [
                {"_id": 0, "lastTag": "z", "uniqueTags": ["x", "z"]},
                {"_id": 4, "lastTag": "y", "uniqueTags": ["y"]},
            ],
        )

        windowed = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"value": 1},
                        "output": {
                            "lastSeen": {"$last": "$tag", "window": {"documents": ["unbounded", "current"]}},
                            "seenTags": {"$addToSet": "$tag", "window": {"documents": ["unbounded", "current"]}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            windowed,
            [
                {"_id": "1", "group": "a", "value": 1, "tag": "x", "lastSeen": "x", "seenTags": ["x"]},
                {"_id": "2", "group": "a", "value": 3, "tag": "x", "lastSeen": "x", "seenTags": ["x"]},
                {"_id": "3", "group": "a", "value": 5, "tag": "y", "lastSeen": "y", "seenTags": ["x", "y"]},
                {"_id": "4", "group": "b", "value": 2, "tag": "z", "lastSeen": "z", "seenTags": ["z"]},
            ],
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)},
                {"$dateTrunc": {"date": "$created_at", "unit": "hour"}},
            ),
            datetime.datetime(2026, 3, 24, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)},
                {"$dateTrunc": {"date": "$created_at", "unit": "minute", "binSize": 15}},
            ),
            datetime.datetime(2026, 3, 24, 10, 30, 0),
        )

    def test_group_bucket_and_window_support_count_and_merge_objects_accumulators(self):
        documents = [
            {"_id": "1", "group": "a", "score": 1, "meta": {"x": 1}},
            {"_id": "2", "group": "a", "score": 2, "meta": {"y": 2}},
            {"_id": "3", "group": "b", "score": 3, "meta": None},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "count": {"$count": {}},
                        "merged": {"$mergeObjects": "$meta"},
                    }
                }
            ],
        )
        self.assertEqual(
            grouped,
            [
                {"_id": "a", "count": 2, "merged": {"x": 1, "y": 2}},
                {"_id": "b", "count": 1, "merged": {}},
            ],
        )

        bucketed = apply_pipeline(
            documents,
            [
                {
                    "$bucketAuto": {
                        "groupBy": "$score",
                        "buckets": 2,
                        "output": {
                            "count": {"$count": {}},
                            "merged": {"$mergeObjects": "$meta"},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            bucketed,
            [
                {"_id": {"min": 1, "max": 3}, "count": 2, "merged": {"x": 1, "y": 2}},
                {"_id": {"min": 3, "max": 3}, "count": 1, "merged": {}},
            ],
        )

        windowed = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"score": 1},
                        "output": {
                            "runningCount": {"$count": {}, "window": {"documents": ["unbounded", "current"]}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            windowed,
            [
                {"_id": "1", "group": "a", "score": 1, "meta": {"x": 1}, "runningCount": 1},
                {"_id": "2", "group": "a", "score": 2, "meta": {"y": 2}, "runningCount": 2},
                {"_id": "3", "group": "b", "score": 3, "meta": None, "runningCount": 1},
            ],
        )

    def test_set_window_fields_uses_window_support_hook_when_initializing_state(self):
        class _WindowOnlyLastDialect(MongoDialect):
            def supports_group_accumulator(self, name: str) -> bool:
                return False if name == "$last" else super().supports_group_accumulator(name)

            def supports_window_accumulator(self, name: str) -> bool:
                return True if name == "$last" else super().supports_window_accumulator(name)

        result = apply_pipeline(
            [{"_id": "1", "group": "a", "value": 1}, {"_id": "2", "group": "a", "value": 2}],
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"value": 1},
                        "output": {
                            "lastSeen": {"$last": "$value", "window": {"documents": ["unbounded", "current"]}},
                        },
                    }
                }
            ],
            dialect=_WindowOnlyLastDialect(
                key="test",
                server_version="test",
                label="Window Only Last",
            ),
        )

    def test_require_projection_rejects_mixed_exclusion_with_id_inclusion(self):
        with self.assertRaises(OperationFailure):
            _require_projection({"_id": 1, "age": 0})

    def test_apply_unwind_keeps_null_include_array_index_for_scalar_values(self):
        result = apply_pipeline(
            [{"_id": "1", "value": "scalar"}],
            [{"$unwind": {"path": "$value", "includeArrayIndex": "idx"}}],
        )

        self.assertEqual(result, [{"_id": "1", "value": "scalar", "idx": None}])

    def test_accumulators_reject_invalid_count_and_merge_objects_operands(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$group": {"_id": None, "count": {"$count": 1}}}],
            )
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$group": {"_id": None, "merged": {"$mergeObjects": "$value"}}}],
            )
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [],
                [{"$group": {"_id": None, "median": {"$median": "$value"}}}],
            )
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$bucketAuto": {"groupBy": "$value", "buckets": 1, "output": {"merged": {"$mergeObjects": "$value"}}}}],
            )

    def test_sum_accumulator_sums_numeric_array_elements(self):
        result = apply_pipeline(
            [{"scores": [10, 20, 30]}, {"scores": [5, "x"]}],
            [{"$group": {"_id": None, "total": {"$sum": "$scores"}}}],
        )

        self.assertEqual(result, [{"_id": None, "total": 65}])

    def test_evaluate_expression_supports_date_trunc_across_units(self):
        value = datetime.datetime(2026, 3, 24, 10, 37, 52, 123456)

        self.assertEqual(
            evaluate_expression({"created_at": value}, {"$dateTrunc": {"date": "$created_at", "unit": "year"}}),
            datetime.datetime(2026, 1, 1, 0, 0, 0),
        )
        self.assertEqual(
            evaluate_expression({"created_at": value}, {"$dateTrunc": {"date": "$created_at", "unit": "quarter"}}),
            datetime.datetime(2026, 1, 1, 0, 0, 0),
        )
        self.assertEqual(
            evaluate_expression({"created_at": value}, {"$dateTrunc": {"date": "$created_at", "unit": "month"}}),
            datetime.datetime(2026, 3, 1, 0, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": value},
                {"$dateTrunc": {"date": "$created_at", "unit": "week", "startOfWeek": "monday"}},
            ),
            datetime.datetime(2026, 3, 23, 0, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": value},
                {"$dateTrunc": {"date": "$created_at", "unit": "day", "binSize": 2}},
            ),
            datetime.datetime(2026, 3, 24, 0, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": datetime.datetime(2026, 1, 15, 12, 0, 0)},
                {"$dateTrunc": {"date": "$created_at", "unit": "day", "binSize": 30}},
            ),
            datetime.datetime(2026, 1, 7, 0, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": value},
                {"$dateTrunc": {"date": "$created_at", "unit": "second", "binSize": 10, "timezone": "UTC"}},
            ),
            datetime.datetime(2026, 3, 24, 10, 37, 50),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": datetime.datetime(2026, 3, 24, 7, 0, 0)},
                {"$dateTrunc": {"date": "$created_at", "unit": "hour", "binSize": 5}},
            ),
            datetime.datetime(2026, 3, 24, 6, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": datetime.datetime(2026, 3, 24, 1, 45, 0)},
                {"$dateTrunc": {"date": "$created_at", "unit": "minute", "binSize": 7}},
            ),
            datetime.datetime(2026, 3, 24, 1, 41, 0),
        )
        self.assertEqual(
            evaluate_expression(
                {"created_at": datetime.datetime(2026, 3, 24, 1, 0, 5)},
                {"$dateTrunc": {"date": "$created_at", "unit": "second", "binSize": 7}},
            ),
            datetime.datetime(2026, 3, 24, 1, 0, 3),
        )
        self.assertIsNone(
            evaluate_expression({"created_at": None}, {"$dateTrunc": {"date": "$created_at", "unit": "hour"}})
        )

    def test_evaluate_expression_rejects_invalid_operator_payloads(self):
        document = {"score": 10, "tags": ["a"]}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$eq": 1})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$add": [1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$subtract": [1, 2, 3]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$in": ["x", "not-a-list"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$cond": {"if": True, "then": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$cond": "bad"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$size": "$score"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$arrayElemAt": ["$score", 0]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$arrayElemAt": ["$tags", "0"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$let": {"vars": [], "in": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$unknown": 1})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$divide": [1, 0]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$mod": [1, 0]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$mergeObjects": [1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$getField": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$getField": 1})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$getField": {"field": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$concatArrays": [1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$setUnion": [1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$map": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$map": {"input": 1, "in": "$$this"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$map": {"input": [], "as": 1, "in": "$$this"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$filter": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$filter": {"input": 1, "cond": True}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$filter": {"input": [], "as": 1, "cond": True}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$reduce": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$reduce": {"input": 1, "initialValue": 0, "in": "$$value"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$arrayToObject": 1})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$arrayToObject": [[[1, "x"]]]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$arrayToObject": [["a", 1, 2]]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfArray": [1, "x"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfArray": ["$tags", "a", "0"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$indexOfArray": ["$tags", "a", 0, "1"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {"input": 1, "sortBy": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {"input": "$tags", "sortBy": 0}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {"input": "$tags", "sortBy": True}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {"input": "$tags", "sortBy": {"rank": 0}}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {"input": "$tags", "sortBy": {"rank": True}}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$sortArray": {"input": "$tags", "sortBy": {1: 1}}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$strcasecmp": [1, "a"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": [1, 0, 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": ["$tags", "0", 1]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$substr": ["$tags", 0, "1"]})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": "$score", "unit": "hour"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": "hour", "binSize": True}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": "hour", "binSize": 0}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": "hour", "timezone": "Europe/Madrid"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": "week", "startOfWeek": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": "week", "startOfWeek": "noday"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateTrunc": {"date": datetime.datetime(2026, 3, 24, 10, 0), "unit": "millisecond"}})

    def test_apply_pipeline_project_treats_bool_flags_as_inclusion_and_exclusion(self):
        documents = [{"_id": "1", "name": "Ada", "role": "admin"}]

        included = apply_pipeline(documents, [{"$project": {"name": True, "_id": False}}])
        excluded = apply_pipeline(documents, [{"$project": {"role": False}}])

        self.assertEqual(included, [{"name": "Ada"}])
        self.assertEqual(excluded, [{"_id": "1", "name": "Ada"}])

        pushdown = split_pushdown_pipeline([{"$project": {"name": True, "_id": False}}])
        self.assertEqual(pushdown.projection, {"name": True, "_id": False})
        self.assertEqual(pushdown.remaining_pipeline, [])

    def test_apply_pipeline_project_supports_pure_exclusion(self):
        documents = [{"_id": "1", "name": "Ada", "role": "admin"}]

        result = apply_pipeline(documents, [{"$project": {"role": 0}}])

        self.assertEqual(result, [{"_id": "1", "name": "Ada"}])

    def test_apply_pipeline_unset_supports_string_and_list_specs(self):
        documents = [{"_id": "1", "name": "Ada", "role": "admin", "profile": {"city": "Madrid", "zip": 28001}}]

        single = apply_pipeline(documents, [{"$unset": "role"}])
        multiple = apply_pipeline(documents, [{"$unset": ["role", "profile.zip"]}])

        self.assertEqual(single, [{"_id": "1", "name": "Ada", "profile": {"city": "Madrid", "zip": 28001}}])
        self.assertEqual(multiple, [{"_id": "1", "name": "Ada", "profile": {"city": "Madrid"}}])

    def test_apply_pipeline_project_computed_only_keeps_id_by_default(self):
        documents = [{"_id": "1", "score": 10}]

        result = apply_pipeline(documents, [{"$project": {"label": {"$toString": "$score"}}}])

        self.assertEqual(result, [{"_id": "1", "label": "10"}])

    def test_apply_pipeline_project_rejects_mixed_include_exclude(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1, "b": 2}], [{"$project": {"a": 1, "b": 0}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1", "a": 1, "c": 3}], [{"$project": {"a": 0, "b": {"$literal": 1}}}])

    def test_apply_pipeline_add_fields_evaluates_against_original_document(self):
        documents = [{"_id": "1", "a": 1, "b": 2}]

        result = apply_pipeline(documents, [{"$addFields": {"a": "$b", "b": "$a"}}])

        self.assertEqual(result, [{"_id": "1", "a": 2, "b": 1}])

    def test_pipeline_supports_match_project_sort_skip_and_limit(self):
        documents = [
            {"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}},
            {"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}},
            {"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}},
            {"_id": "4", "kind": "view", "rank": 4, "payload": {"city": "Valencia"}},
        ]

        result = apply_pipeline(
            documents,
            [
                {"$match": {"kind": "view"}},
                {"$sort": {"rank": 1}},
                {"$skip": 1},
                {"$limit": 1},
                {"$project": {"payload.city": 1, "_id": 0}},
            ],
        )

        self.assertEqual(result, [{"payload": {"city": "Sevilla"}}])

    def test_pipeline_supports_unwind_string_path(self):
        documents = [
            {"_id": "1", "tags": ["python", "mongodb"]},
            {"_id": "2", "tags": ["sqlite"]},
        ]

        result = apply_pipeline(documents, [{"$unwind": "$tags"}])

        self.assertEqual(
            result,
            [
                {"_id": "1", "tags": "python"},
                {"_id": "1", "tags": "mongodb"},
                {"_id": "2", "tags": "sqlite"},
            ],
        )

    def test_pipeline_supports_unwind_document_spec_with_preserve_and_index(self):
        documents = [
            {"_id": "1", "tags": ["python", "mongodb"]},
            {"_id": "2", "tags": []},
            {"_id": "3", "tags": None},
            {"_id": "4"},
            {"_id": "5", "tags": "sqlite"},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$unwind": {
                        "path": "$tags",
                        "preserveNullAndEmptyArrays": True,
                        "includeArrayIndex": "index",
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tags": "python", "index": 0},
                {"_id": "1", "tags": "mongodb", "index": 1},
                {"_id": "2", "tags": [], "index": None},
                {"_id": "3", "tags": None, "index": None},
                {"_id": "4", "index": None},
                {"_id": "5", "tags": "sqlite", "index": None},
            ],
        )

    def test_pipeline_supports_unset_string_and_list_specs(self):
        documents = [{"_id": "1", "secret": "x", "profile": {"city": "Madrid", "zip": 28001}}]

        single = apply_pipeline(documents, [{"$unset": "secret"}])
        multiple = apply_pipeline(documents, [{"$unset": ["secret", "profile.zip"]}])

        self.assertEqual(single, [{"_id": "1", "profile": {"city": "Madrid", "zip": 28001}}])
        self.assertEqual(multiple, [{"_id": "1", "profile": {"city": "Madrid"}}])

    def test_pipeline_supports_add_fields_project_expr_and_match_expr(self):
        documents = [
            {"_id": "1", "kind": "view", "score": 10, "bonus": None, "tags": ["a", "b"]},
            {"_id": "2", "kind": "click", "score": 4, "bonus": 3, "tags": ["x"]},
        ]

        result = apply_pipeline(
            documents,
            [
                {"$addFields": {"effective_score": {"$add": ["$score", {"$ifNull": ["$bonus", 0]}]}}},
                {"$match": {"$expr": {"$gt": ["$effective_score", 7]}}},
                {
                    "$project": {
                        "_id": 0,
                        "kind": 1,
                        "passed": {"$cond": [{"$gte": ["$effective_score", 10]}, "yes", "no"]},
                        "first_tag": {"$arrayElemAt": ["$tags", 0]},
                    }
                },
            ],
        )

        self.assertEqual(
            result,
            [{"kind": "view", "passed": "yes", "first_tag": "a"}],
        )

    def test_pipeline_match_honors_custom_dialect(self):
        class _CaseInsensitiveDialect(MongoDialect):
            def values_equal(self, left, right):
                if isinstance(left, str) and isinstance(right, str):
                    return left.lower() == right.lower()
                return super().values_equal(left, right)

        result = apply_pipeline(
            [{"name": "Ada"}, {"name": "Grace"}],
            [{"$match": {"name": "ada"}}],
            dialect=_CaseInsensitiveDialect(
                key="test",
                server_version="test",
                label="Case Insensitive",
            ),
        )

        self.assertEqual(result, [{"name": "Ada"}])

    def test_pipeline_supports_group_with_common_accumulators(self):
        documents = [
            {"_id": "1", "kind": "view", "amount": 10, "user": "ada"},
            {"_id": "2", "kind": "view", "amount": 7, "user": "grace"},
            {"_id": "3", "kind": "click", "amount": 3, "user": "alan"},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$kind",
                        "total": {"$sum": "$amount"},
                        "minimum": {"$min": "$amount"},
                        "maximum": {"$max": "$amount"},
                        "average": {"$avg": "$amount"},
                        "users": {"$push": "$user"},
                        "first_user": {"$first": "$user"},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
        )

        self.assertEqual(
            result,
            [
                {
                    "_id": "click",
                    "total": 3,
                    "minimum": 3,
                    "maximum": 3,
                    "average": 3.0,
                    "users": ["alan"],
                    "first_user": "alan",
                },
                {
                    "_id": "view",
                    "total": 17,
                    "minimum": 7,
                    "maximum": 10,
                    "average": 8.5,
                    "users": ["ada", "grace"],
                    "first_user": "ada",
                },
            ],
        )

    def test_pipeline_supports_group_with_null_keys_and_missing_values(self):
        documents = [
            {"_id": "1", "amount": None},
            {"_id": "2"},
            {"_id": "3", "amount": 5},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": None,
                        "total": {"$sum": "$amount"},
                        "average": {"$avg": "$amount"},
                        "minimum": {"$min": "$amount"},
                        "maximum": {"$max": "$amount"},
                        "first_amount": {"$first": "$amount"},
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [{"_id": None, "total": 5, "average": 5.0, "minimum": 5, "maximum": 5, "first_amount": None}],
        )

    def test_pipeline_group_ignores_non_numeric_values_for_sum_and_avg(self):
        documents = [
            {"_id": "1", "kind": "view", "amount": 10},
            {"_id": "2", "kind": "view", "amount": "oops"},
            {"_id": "3", "kind": "view", "amount": None},
            {"_id": "4", "kind": "view", "amount": 6},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$kind",
                        "total": {"$sum": "$amount"},
                        "average": {"$avg": "$amount"},
                    }
                }
            ],
        )

        self.assertEqual(result, [{"_id": "view", "total": 16, "average": 8.0}])

    def test_pipeline_group_distinguishes_bool_and_int_keys(self):
        result = apply_pipeline(
            [
                {"_id": "1", "kind": True, "amount": 10},
                {"_id": "2", "kind": 1, "amount": 7},
            ],
            [{"$group": {"_id": "$kind", "total": {"$sum": "$amount"}}}],
        )

        self.assertEqual(
            sorted(result, key=lambda item: (type(item["_id"]).__name__, item["_id"])),
            [{"_id": True, "total": 10}, {"_id": 1, "total": 7}],
        )

    def test_pipeline_supports_lookup_replace_root_and_replace_with(self):
        documents = [
            {"_id": "1", "user_id": "u1", "kind": "view"},
            {"_id": "2", "user_id": "u2", "kind": "click"},
        ]
        foreign = {
            "users": [
                {"_id": "u1", "name": "Ada", "city": "Sevilla"},
                {"_id": "u2", "name": "Grace", "city": "Madrid"},
            ]
        }

        result = apply_pipeline(
            documents,
            [
                {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "_id", "as": "user"}},
                {"$addFields": {"user": {"$arrayElemAt": ["$user", 0]}}},
                {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", "$user"]}}},
                {"$project": {"user": 0, "user_id": 0}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [
                {"_id": "u1", "kind": "view", "name": "Ada", "city": "Sevilla"},
                {"_id": "u2", "kind": "click", "name": "Grace", "city": "Madrid"},
            ],
        )

        replaced = apply_pipeline(
            [{"_id": "1", "profile": {"name": "Ada"}}],
            [{"$replaceWith": "$profile"}],
            collection_resolver=foreign.get,
        )
        self.assertEqual(replaced, [{"name": "Ada"}])

    def test_pipeline_supports_union_with_pipeline_only_using_current_collection(self):
        documents = [
            {"_id": "1", "kind": "event", "rank": 2},
            {"_id": "2", "kind": "event", "rank": 1},
            {"_id": "3", "kind": "archive", "rank": 0},
        ]

        result = apply_pipeline(
            documents,
            [
                {"$match": {"kind": "event"}},
                {"$unionWith": {"pipeline": [{"$match": {"kind": "archive"}}]}},
                {"$sort": {"rank": 1}},
                {"$project": {"_id": 1, "kind": 1}},
            ],
            collection_resolver=lambda name: documents if name == "__mongoeco_current_collection__" else None,
        )

        self.assertEqual(
            result,
            [
                {"_id": "3", "kind": "archive"},
                {"_id": "2", "kind": "event"},
                {"_id": "1", "kind": "event"},
            ],
        )

    def test_pipeline_supports_lookup_with_multiple_and_missing_matches(self):
        documents = [
            {"_id": "1", "tenant": "a"},
            {"_id": "2", "tenant": "missing"},
            {"_id": "3"},
        ]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a"},
                {"_id": "u2", "tenant": "a"},
                {"_id": "u3"},
            ]
        }

        result = apply_pipeline(
            documents,
            [{"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}}],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tenant": "a", "users": [{"_id": "u1", "tenant": "a"}, {"_id": "u2", "tenant": "a"}]},
                {"_id": "2", "tenant": "missing", "users": []},
                {"_id": "3", "users": [{"_id": "u3"}]},
            ],
        )

    def test_pipeline_supports_lookup_with_dotted_variable_paths(self):
        documents = [{"_id": "1", "tenant": {"id": "a"}}, {"_id": "2", "tenant": {"id": "b"}}]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a", "profile": {"name": "Ada"}},
                {"_id": "u2", "tenant": "b", "profile": {"name": "Linus"}},
            ]
        }

        result = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ctx": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ctx.id"]}}},
                            {"$project": {"_id": 0, "name": "$$ROOT.profile.name", "city": "$$ctx.id"}},
                        ],
                        "as": "users",
                    }
                }
            ],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tenant": {"id": "a"}, "users": [{"name": "Ada", "city": "a"}]},
                {"_id": "2", "tenant": {"id": "b"}, "users": [{"name": "Linus", "city": "b"}]},
            ],
        )

    def test_pipeline_supports_lookup_with_let_and_pipeline(self):
        documents = [{"_id": "1", "tenant": "a"}, {"_id": "2", "tenant": "b"}]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a", "name": "Ada"},
                {"_id": "u2", "tenant": "a", "name": "Grace"},
                {"_id": "u3", "tenant": "b", "name": "Linus"},
            ]
        }

        result = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"tenantId": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                }
            ],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                {"_id": "2", "tenant": "b", "users": [{"name": "Linus"}]},
            ],
        )

    def test_pipeline_supports_lookup_with_local_foreign_and_pipeline(self):
        documents = [{"_id": "1", "tenant": "a"}, {"_id": "2", "tenant": "b"}]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a", "name": "Ada"},
                {"_id": "u2", "tenant": "a", "name": "Grace"},
                {"_id": "u3", "tenant": "b", "name": "Linus"},
                {"_id": "u4", "tenant": "c", "name": "Nope"},
            ]
        }

        result = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "localField": "tenant",
                        "foreignField": "tenant",
                        "let": {"tenantId": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                {"_id": "2", "tenant": "b", "users": [{"name": "Linus"}]},
            ],
        )

    def test_pipeline_supports_lookup_with_missing_foreign_collection(self):
        result = apply_pipeline(
            [{"_id": "1", "tenant": "a"}],
            [{"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}}],
            collection_resolver=lambda name: None,
        )

        self.assertEqual(result, [{"_id": "1", "tenant": "a", "users": []}])

    def test_pipeline_lookup_without_filters_does_not_alias_joined_arrays(self):
        result = apply_pipeline(
            [{"_id": "1"}, {"_id": "2"}],
            [{"$lookup": {"from": "users", "as": "users", "pipeline": [], "let": {}}}],
            collection_resolver=lambda name: [{"_id": "u1", "name": "Ada"}] if name == "users" else None,
        )

        result[0]["users"][0]["name"] = "Changed"

        self.assertEqual(result[1]["users"], [{"_id": "u1", "name": "Ada"}])

    def test_pipeline_supports_nested_lookup_inside_lookup_pipeline(self):
        documents = [{"_id": "1", "tenant": "a"}]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a", "role_id": "r1", "name": "Ada"},
                {"_id": "u2", "tenant": "a", "role_id": "r2", "name": "Grace"},
            ],
            "roles": [
                {"_id": "r1", "label": "admin"},
                {"_id": "r2", "label": "staff"},
            ],
        }

        result = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"tenantId": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                            {"$lookup": {"from": "roles", "localField": "role_id", "foreignField": "_id", "as": "roles"}},
                            {"$project": {"_id": 0, "name": 1, "roles": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                }
            ],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [
                {
                    "_id": "1",
                    "tenant": "a",
                    "users": [
                        {"name": "Ada", "roles": [{"_id": "r1", "label": "admin"}]},
                        {"name": "Grace", "roles": [{"_id": "r2", "label": "staff"}]},
                    ],
                }
            ],
        )

    def test_pipeline_supports_join_operator_combinations(self):
        documents = [
            {"_id": "e1", "tenant": "a", "user_id": "u1", "kind": "view"},
            {"_id": "e2", "tenant": "b", "user_id": "u3", "kind": "click"},
            {"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"},
        ]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a", "name": "Ada", "role": "admin"},
                {"_id": "u2", "tenant": "a", "name": "Grace", "role": "staff"},
                {"_id": "u3", "tenant": "b", "name": "Linus", "role": "owner"},
            ]
        }

        no_join = apply_pipeline(
            documents,
            [
                {"$match": {"$expr": {"$eq": ["$tenant", "a"]}}},
                {"$project": {"_id": 1, "tenant": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(no_join, [{"_id": "e1", "tenant": "a"}])

        inner_join = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$match": {"$expr": {"$gt": [{"$size": "$users"}, 0]}}},
                {"$project": {"_id": 1, "tenant": 1, "users": 1}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(
            inner_join,
            [
                {"_id": "e1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                {"_id": "e2", "tenant": "b", "users": [{"name": "Linus"}]},
            ],
        )

        document_join = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$addFields": {"joined_user": {"$mergeObjects": [{"tenant": "$tenant"}, {"$first": "$users"}]}}},
                {"$project": {"_id": 1, "joined_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(
            document_join,
            [
                {"_id": "e1", "joined_user": {"tenant": "a", "name": "Ada"}},
                {"_id": "e2", "joined_user": {"tenant": "b", "name": "Linus"}},
                {"_id": "e3", "joined_user": {"tenant": "missing"}},
            ],
        )

        left_join = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {
                    "$addFields": {
                        "joined_user": {
                            "$cond": [
                                {"$gt": [{"$size": "$users"}, 0]},
                                {"$arrayElemAt": ["$users", 0]},
                                {"name": "unknown"},
                            ]
                        }
                    }
                },
                {"$project": {"_id": 1, "joined_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(
            left_join,
            [
                {"_id": "e1", "joined_user": {"name": "Ada"}},
                {"_id": "e2", "joined_user": {"name": "Linus"}},
                {"_id": "e3", "joined_user": {"name": "unknown"}},
            ],
        )

        count_join = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$set": {"user_count": {"$size": "$users"}}},
                {"$project": {"_id": 1, "user_count": 1}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(
            count_join,
            [
                {"_id": "e1", "user_count": 2},
                {"_id": "e2", "user_count": 1},
                {"_id": "e3", "user_count": 0},
            ],
        )

        aggregated_join = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$set": {"primary_user": {"$ifNull": [{"$first": "$users"}, {"name": "unknown"}]}}},
                {"$project": {"_id": 1, "primary_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(
            aggregated_join,
            [
                {"_id": "e1", "primary_user": {"name": "Ada"}},
                {"_id": "e2", "primary_user": {"name": "Linus"}},
                {"_id": "e3", "primary_user": {"name": "unknown"}},
            ],
        )

        merged = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$user_id"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$_id", "$$ref_key"]}}},
                            {"$project": {"name": 1, "role": 1}},
                        ],
                        "as": "user_doc",
                    }
                },
                {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", {"$arrayElemAt": ["$user_doc", 0]}]}}},
                {"$project": {"user_doc": 0}},
                {"$sort": {"_id": 1}},
            ],
            collection_resolver=foreign.get,
        )
        self.assertEqual(
            merged,
            [
                {"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"},
                {"_id": "u1", "tenant": "a", "user_id": "u1", "kind": "view", "name": "Ada", "role": "admin"},
                {"_id": "u3", "tenant": "b", "user_id": "u3", "kind": "click", "name": "Linus", "role": "owner"},
            ],
        )

    def test_pipeline_supports_array_expression_transformations(self):
        documents = [
            {"_id": "1", "tags": ["a", "b", "c"], "other_tags": ["b", "d"], "numbers": [1, 2, 3, 4]},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$project": {
                        "_id": 0,
                        "mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}},
                        "filtered": {"$filter": {"input": "$numbers", "as": "n", "cond": {"$gt": ["$$n", 2]}}},
                        "reduced": {"$reduce": {"input": "$numbers", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}},
                        "concatenated": {"$concatArrays": ["$tags", "$other_tags"]},
                        "unioned": {"$setUnion": ["$tags", "$other_tags"]},
                    }
                }
            ],
        )

    def test_pipeline_supports_array_expression_transformations_with_empty_arrays(self):
        documents = [{"_id": "1", "tags": [], "other_tags": [], "numbers": []}]

        result = apply_pipeline(
            documents,
            [
                {
                    "$project": {
                        "_id": 0,
                        "mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}},
                        "filtered": {"$filter": {"input": "$numbers", "as": "n", "cond": {"$gt": ["$$n", 2]}}},
                        "reduced": {"$reduce": {"input": "$numbers", "initialValue": 99, "in": {"$add": ["$$value", "$$this"]}}},
                        "concatenated": {"$concatArrays": ["$tags", "$other_tags"]},
                        "unioned": {"$setUnion": ["$tags", "$other_tags"]},
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [{"mapped": [], "filtered": [], "reduced": 99, "concatenated": [], "unioned": []}],
        )

    def test_pipeline_array_expression_transformations_return_null_for_missing_inputs(self):
        result = apply_pipeline(
            [{"_id": "1"}],
            [
                {
                    "$project": {
                        "_id": 0,
                        "mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}},
                        "filtered": {"$filter": {"input": "$numbers", "as": "n", "cond": {"$gt": ["$$n", 2]}}},
                        "reduced": {"$reduce": {"input": "$numbers", "initialValue": 99, "in": {"$add": ["$$value", "$$this"]}}},
                    }
                }
            ],
        )

        self.assertEqual(result, [{"mapped": None, "filtered": None, "reduced": None}])

    def test_pipeline_supports_set_union_with_embedded_documents(self):
        documents = [
            {
                "_id": "1",
                "left": [{"kind": "a", "qty": 1}, {"kind": "b", "qty": 2}],
                "right": [{"qty": 1, "kind": "a"}, {"kind": "c", "qty": 3}],
            }
        ]

        result = apply_pipeline(
            documents,
            [{"$project": {"_id": 0, "unioned": {"$setUnion": ["$left", "$right"]}}}],
        )

        self.assertEqual(
            result,
            [
                {
                    "unioned": [
                        {"kind": "a", "qty": 1},
                        {"kind": "b", "qty": 2},
                        {"qty": 1, "kind": "a"},
                        {"kind": "c", "qty": 3},
                    ]
                }
            ],
        )

    def test_expression_eq_and_in_respect_embedded_document_key_order(self):
        document = {"value": {"b": 2, "a": 1}}

        self.assertFalse(evaluate_expression(document, {"$eq": ["$value", {"a": 1, "b": 2}]}))
        self.assertFalse(evaluate_expression(document, {"$in": ["$value", [{"a": 1, "b": 2}]]}))

    def test_pipeline_supports_get_field_and_merge_objects_in_public_pipeline(self):
        documents = [{"_id": "1", "profile": {"name": "Ada"}, "fallback": {"city": "Sevilla"}}]

        result = apply_pipeline(
            documents,
            [
                {
                    "$project": {
                        "_id": 0,
                        "merged": {
                            "$mergeObjects": [
                                "$fallback",
                                {"name": {"$getField": {"field": "name", "input": "$profile"}}},
                            ]
                        },
                    }
                }
            ],
        )

        self.assertEqual(result, [{"merged": {"city": "Sevilla", "name": "Ada"}}])

    def test_pipeline_supports_array_to_object_index_of_array_and_sort_array(self):
        documents = [
            {
                "_id": "1",
                "pairs": [["a", 1], ["b", 2]],
                "numbers": [4, 1, 3, 2],
                "items": [{"rank": 3, "name": "c"}, {"rank": 1, "name": "a"}, {"rank": 2, "name": "b"}],
            }
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$project": {
                        "_id": 0,
                        "mapped": {"$arrayToObject": "$pairs"},
                        "position": {"$indexOfArray": ["$numbers", 3]},
                        "sorted_numbers": {"$sortArray": {"input": "$numbers", "sortBy": 1}},
                        "sorted_items": {"$sortArray": {"input": "$items", "sortBy": {"rank": 1}}},
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {
                    "mapped": {"a": 1, "b": 2},
                    "position": 2,
                    "sorted_numbers": [1, 2, 3, 4],
                    "sorted_items": [
                        {"rank": 1, "name": "a"},
                        {"rank": 2, "name": "b"},
                        {"rank": 3, "name": "c"},
                    ],
                }
            ],
        )

    def test_pipeline_supports_facet_and_date_trunc(self):
        documents = [
            {"_id": "1", "kind": "view", "score": 5, "created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)},
            {"_id": "2", "kind": "view", "score": 7, "created_at": datetime.datetime(2026, 3, 24, 10, 5, 0)},
            {"_id": "3", "kind": "click", "score": 3, "created_at": datetime.datetime(2026, 3, 24, 11, 10, 0)},
        ]

        result = apply_pipeline(
            documents,
            [
                {"$addFields": {"bucket": {"$dateTrunc": {"date": "$created_at", "unit": "hour"}}}},
                {
                    "$facet": {
                        "views": [
                            {"$match": {"kind": "view"}},
                            {"$project": {"_id": 0, "bucket": 1}},
                            {"$sort": {"bucket": 1}},
                        ],
                        "scores": [
                            {"$group": {"_id": "$kind", "total": {"$sum": "$score"}}},
                            {"$sort": {"_id": 1}},
                        ],
                    }
                },
            ],
        )

        self.assertEqual(
            result,
            [
                {
                    "views": [
                        {"bucket": datetime.datetime(2026, 3, 24, 10, 0, 0)},
                        {"bucket": datetime.datetime(2026, 3, 24, 10, 0, 0)},
                    ],
                    "scores": [{"_id": "click", "total": 3}, {"_id": "view", "total": 12}],
                }
            ],
        )

    def test_pipeline_supports_bucket_with_default_and_output(self):
        documents = [
            {"_id": "1", "score": 5, "kind": "view"},
            {"_id": "2", "score": 12, "kind": "view"},
            {"_id": "3", "score": 17, "kind": "click"},
            {"_id": "4", "score": 25, "kind": "view"},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$score",
                        "boundaries": [0, 10, 20],
                        "default": "other",
                        "output": {
                            "count": {"$sum": 1},
                            "kinds": {"$push": "$kind"},
                            "firstScore": {"$first": "$score"},
                            "maxScore": {"$max": "$score"},
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": 0, "count": 1, "kinds": ["view"], "firstScore": 5, "maxScore": 5},
                {"_id": 10, "count": 2, "kinds": ["view", "click"], "firstScore": 12, "maxScore": 17},
                {"_id": "other", "count": 1, "kinds": ["view"], "firstScore": 25, "maxScore": 25},
            ],
        )

    def test_pipeline_supports_bucket_default_count_output(self):
        documents = [{"_id": "1", "score": 5}, {"_id": "2", "score": 12}]

        result = apply_pipeline(
            documents,
            [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10, 20]}}],
        )

        self.assertEqual(result, [{"_id": 0, "count": 1}, {"_id": 10, "count": 1}])

    def test_pipeline_supports_bucket_with_avg_and_missing_values(self):
        documents = [
            {"_id": "1", "rank": 5},
            {"_id": "2", "rank": 6, "score": 4},
            {"_id": "3", "rank": 12},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$rank",
                        "boundaries": [0, 10, 20],
                        "output": {
                            "total": {"$sum": "$score"},
                            "minScore": {"$min": "$score"},
                            "maxScore": {"$max": "$score"},
                            "avgScore": {"$avg": "$score"},
                            "firstRank": {"$first": "$rank"},
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": 0, "total": 4, "minScore": 4, "maxScore": 4, "avgScore": 4.0, "firstRank": 5},
                {"_id": 10, "total": 0, "minScore": None, "maxScore": None, "avgScore": None, "firstRank": 12},
            ],
        )

    def test_pipeline_bucket_ignores_non_numeric_values_for_sum_and_avg(self):
        documents = [
            {"_id": "1", "score": 5, "amount": 10},
            {"_id": "2", "score": 6, "amount": "oops"},
            {"_id": "3", "score": 12, "amount": 4},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$score",
                        "boundaries": [0, 10, 20],
                        "output": {
                            "total": {"$sum": "$amount"},
                            "avgAmount": {"$avg": "$amount"},
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": 0, "total": 10, "avgAmount": 10.0},
                {"_id": 10, "total": 4, "avgAmount": 4.0},
            ],
        )

    def test_pipeline_supports_bucket_auto(self):
        documents = [
            {"_id": "1", "score": 5, "kind": "view"},
            {"_id": "2", "score": 12, "kind": "view"},
            {"_id": "3", "score": 17, "kind": "click"},
            {"_id": "4", "score": 25, "kind": "view"},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$bucketAuto": {
                        "groupBy": "$score",
                        "buckets": 2,
                        "output": {
                            "count": {"$sum": 1},
                            "kinds": {"$push": "$kind"},
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": {"min": 5, "max": 17}, "count": 2, "kinds": ["view", "view"]},
                {"_id": {"min": 17, "max": 25}, "count": 2, "kinds": ["click", "view"]},
            ],
        )
        self.assertEqual(
            apply_pipeline([], [{"$bucketAuto": {"groupBy": "$score", "buckets": 3}}]),
            [],
        )

    def test_pipeline_supports_set_window_fields(self):
        documents = [
            {"_id": "1", "tenant": "a", "rank": 1, "score": 5},
            {"_id": "2", "tenant": "a", "rank": 2, "score": 7},
            {"_id": "3", "tenant": "b", "rank": 1, "score": 3},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$tenant",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningTotal": {
                                "$sum": "$score",
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "allScores": {
                                "$push": "$score",
                                "window": {"documents": ["unbounded", "unbounded"]},
                            },
                            "currentAndNextMax": {
                                "$max": "$score",
                                "window": {"documents": ["current", 1]},
                            },
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tenant": "a", "rank": 1, "score": 5, "runningTotal": 5, "allScores": [5, 7], "currentAndNextMax": 7},
                {"_id": "2", "tenant": "a", "rank": 2, "score": 7, "runningTotal": 12, "allScores": [5, 7], "currentAndNextMax": 7},
                {"_id": "3", "tenant": "b", "rank": 1, "score": 3, "runningTotal": 3, "allScores": [3], "currentAndNextMax": 3},
            ],
        )

    def test_pipeline_supports_set_window_fields_without_explicit_window(self):
        result = apply_pipeline(
            [{"_id": "1", "tenant": "a", "rank": 2, "score": 7}, {"_id": "2", "tenant": "a", "rank": 1, "score": 5}],
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$tenant",
                        "sortBy": {"rank": 1},
                        "output": {"partitionMax": {"$max": "$score"}},
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "2", "tenant": "a", "rank": 1, "score": 5, "partitionMax": 7},
                {"_id": "1", "tenant": "a", "rank": 2, "score": 7, "partitionMax": 7},
            ],
        )

    def test_pipeline_set_window_fields_distinguishes_bool_and_int_partitions(self):
        result = apply_pipeline(
            [
                {"_id": "1", "tenant": True, "rank": 1, "score": 5},
                {"_id": "2", "tenant": 1, "rank": 1, "score": 7},
            ],
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$tenant",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningTotal": {
                                "$sum": "$score",
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            sorted(result, key=lambda item: (type(item["tenant"]).__name__, item["tenant"])),
            [
                {"_id": "1", "tenant": True, "rank": 1, "score": 5, "runningTotal": 5},
                {"_id": "2", "tenant": 1, "rank": 1, "score": 7, "runningTotal": 7},
            ],
        )

    def test_pipeline_supports_set_window_fields_with_numeric_range_window(self):
        documents = [
            {"_id": "1", "score": 5},
            {"_id": "2", "score": 7},
            {"_id": "3", "score": 12},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"score": 1},
                        "output": {
                            "nearbyTotal": {
                                "$sum": "$score",
                                "window": {"range": [-2, 2]},
                            }
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "score": 5, "nearbyTotal": 12},
                {"_id": "2", "score": 7, "nearbyTotal": 12},
                {"_id": "3", "score": 12, "nearbyTotal": 12},
            ],
        )

    def test_pipeline_set_window_fields_ignores_non_numeric_values_for_sum(self):
        documents = [
            {"_id": "1", "tenant": "a", "rank": 1, "score": 5},
            {"_id": "2", "tenant": "a", "rank": 2, "score": "oops"},
            {"_id": "3", "tenant": "a", "rank": 3, "score": 7},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$tenant",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningTotal": {
                                "$sum": "$score",
                                "window": {"documents": ["unbounded", "current"]},
                            }
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "tenant": "a", "rank": 1, "score": 5, "runningTotal": 5},
                {"_id": "2", "tenant": "a", "rank": 2, "score": "oops", "runningTotal": 5},
                {"_id": "3", "tenant": "a", "rank": 3, "score": 7, "runningTotal": 12},
            ],
        )

    def test_pipeline_supports_set_window_fields_with_current_and_unbounded_range(self):
        result = apply_pipeline(
            [{"_id": "1", "score": 5}, {"_id": "2", "score": 7}],
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"score": 1},
                        "output": {
                            "fromCurrent": {
                                "$sum": "$score",
                                "window": {"range": ["current", "unbounded"]},
                            }
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "score": 5, "fromCurrent": 12},
                {"_id": "2", "score": 7, "fromCurrent": 7},
            ],
        )

    def test_pipeline_supports_count_and_sort_by_count(self):
        documents = [
            {"_id": "1", "kind": "view"},
            {"_id": "2", "kind": "view"},
            {"_id": "3", "kind": "click"},
            {"_id": "4", "kind": "click"},
        ]

        counted = apply_pipeline(documents, [{"$count": "total"}])
        sorted_counts = apply_pipeline(documents, [{"$sortByCount": "$kind"}])

        self.assertEqual(counted, [{"total": 4}])
        self.assertEqual(sorted_counts, [{"_id": "click", "count": 2}, {"_id": "view", "count": 2}])

    def test_pipeline_supports_sample(self):
        documents = [
            {"_id": "1"},
            {"_id": "2"},
            {"_id": "3"},
        ]

        sampled = apply_pipeline(documents, [{"$sample": {"size": 2}}])
        oversampled = apply_pipeline(documents, [{"$sample": {"size": 10}}])
        empty = apply_pipeline(documents, [{"$sample": {"size": 0}}])

        self.assertEqual(len(sampled), 2)
        self.assertEqual(len({item["_id"] for item in sampled}), 2)
        self.assertTrue(all(item in documents for item in sampled))
        self.assertCountEqual(oversampled, documents)
        self.assertEqual(empty, [])

    def test_pipeline_supports_union_with_string_and_pipeline_spec(self):
        documents = [
            {"_id": "e1", "kind": "event", "tenant": "a"},
            {"_id": "e2", "kind": "event", "tenant": "b"},
        ]
        collections = {
            "archived_events": [
                {"_id": "a1", "kind": "archive", "tenant": "a", "rank": 2},
                {"_id": "a2", "kind": "archive", "tenant": "b", "rank": 1},
            ]
        }

        plain = apply_pipeline(
            documents,
            [{"$unionWith": "archived_events"}],
            collection_resolver=collections.get,
        )
        filtered = apply_pipeline(
            documents,
            [
                {
                    "$unionWith": {
                        "coll": "archived_events",
                        "pipeline": [
                            {"$match": {"tenant": "b"}},
                            {"$project": {"_id": 1, "kind": 1, "rank": 1}},
                        ],
                    }
                }
            ],
            collection_resolver=collections.get,
        )

        self.assertEqual(plain, documents + collections["archived_events"])
        self.assertEqual(
            filtered,
            documents + [{"_id": "a2", "kind": "archive", "rank": 1}],
        )

    def test_pipeline_count_returns_empty_result_for_empty_input(self):
        self.assertEqual(apply_pipeline([], [{"$count": "total"}]), [{"total": 0}])

    def test_pipeline_rejects_count_field_names_with_dots(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$count": "a.b"}])

    def test_expression_and_or_accept_empty_arrays(self):
        document = {"_id": "1"}

        self.assertTrue(evaluate_expression(document, {"$and": []}))
        self.assertFalse(evaluate_expression(document, {"$or": []}))

    def test_pipeline_group_preserves_user_document_that_looks_like_avg_state(self):
        result = apply_pipeline(
            [{"_id": "1", "payload": {"total": 5, "count": 2}}],
            [{"$group": {"_id": None, "firstPayload": {"$first": "$payload"}}}],
        )

        self.assertEqual(result, [{"_id": None, "firstPayload": {"total": 5, "count": 2}}])

    def test_pipeline_union_with_current_collection_keeps_empty_resolved_collection_empty(self):
        result = apply_pipeline(
            [{"_id": "seed"}],
            [{"$unionWith": {"pipeline": []}}],
            collection_resolver=lambda name: [] if name == "__mongoeco_current_collection__" else None,
        )

        self.assertEqual(result, [{"_id": "seed"}])

    def test_pipeline_union_with_current_collection_falls_back_to_input_when_resolver_returns_none(self):
        result = apply_pipeline(
            [{"_id": "seed"}],
            [{"$unionWith": {"pipeline": []}}],
            collection_resolver=lambda name: None,
        )

        self.assertEqual(result, [{"_id": "seed"}, {"_id": "seed"}])

    def test_pipeline_set_window_fields_preserves_user_document_that_looks_like_avg_state(self):
        result = apply_pipeline(
            [{"_id": "1", "rank": 1, "payload": {"total": 5, "count": 2}}],
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"rank": 1},
                        "output": {
                            "firstPayload": {
                                "$first": "$payload",
                                "window": {"documents": ["unbounded", "current"]},
                            }
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [{"_id": "1", "rank": 1, "payload": {"total": 5, "count": 2}, "firstPayload": {"total": 5, "count": 2}}],
        )

    def test_pipeline_rejects_project_exclusion_with_computed_fields(self):
        documents = [{"_id": "1", "kind": "view", "score": 10, "secret": "x"}]

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                documents,
                [{"$project": {"secret": 0, "label": {"$toString": "$score"}}}],
            )

    def test_pipeline_rejects_invalid_stage_shape(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$match": {}, "$limit": 1}])

    def test_pipeline_rejects_stage_without_dollar_operator(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"match": {}}])

    def test_pipeline_rejects_unsupported_stage(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$lookup": {"from": "other"}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$densify": {}}])

    def test_pipeline_rejects_invalid_match_payload(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$match": []}])

    def test_pipeline_rejects_invalid_project_payload(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$project": []}])

    def test_pipeline_rejects_invalid_unwind_payload(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$unwind": []}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$unwind": "tags"}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$unwind": {"path": "$tags", "includeArrayIndex": 1}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$unwind": {"path": "$tags", "includeArrayIndex": "$idx"}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$unset": {}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$unset": ["ok", ""]}])

    def test_pipeline_rejects_invalid_sort_direction(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sort": {"rank": 2}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sort": {"rank": True}}])

    def test_pipeline_rejects_invalid_sort_payload_and_field(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sort": []}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sort": {1: 1}}])

    def test_pipeline_rejects_invalid_skip_and_limit(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$skip": -1}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$limit": True}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sample": []}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sample": {"size": True}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$sample": {"size": 1, "extra": 2}}])

    def test_pipeline_rejects_invalid_group_and_expression_payloads(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$group": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$group": {"total": {"$sum": 1}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$group": {"_id": None, "total": 1}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$group": {"_id": None, "total": {"$unknown": 1}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$match": {"$expr": {"$divide": [1, "x"]}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$addFields": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$addFields": {1: "bad"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$project": {1: "$_id"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": "user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": 1}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "", "localField": "x", "foreignField": "_id", "as": "user"}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "$users", "localField": "x", "foreignField": "_id", "as": "user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "", "foreignField": "_id", "as": "user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": ""}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "$x", "foreignField": "_id", "as": "user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "$_id", "as": "user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": "$user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "let": []}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": {}, "let": {}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "localField": "x"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "let": {"x": 1}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": 1, "foreignField": "_id", "as": "user"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1"}],
                [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "localField": 1, "foreignField": "_id"}}],
            )

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1"}],
                [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "localField": "x"}}],
            )

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1"}],
                [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "localField": "$x", "foreignField": "_id"}}],
            )

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1"}],
                [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "foreignField": "_id"}}],
            )

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"_id": "1"}],
                [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": "user", "let": {"x": 1}}}],
            )

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": {}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": {"coll": "", "pipeline": []}}])
        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": {"coll": "$users", "pipeline": []}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": ""}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": {"coll": "users", "pipeline": {}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": {"coll": "users", "extra": 1}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$unionWith": "users"}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$replaceRoot": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$replaceWith": 1}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$facet": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$facet": {1: []}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$facet": {"bad": {}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$bucket": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [10]}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [10, 5]}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10], "output": []}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10], "output": {"count": 1}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 50}], [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10]}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucketAuto": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucketAuto": {"groupBy": "$score", "buckets": 0}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucketAuto": {"groupBy": "$score", "buckets": 1, "granularity": "R5"}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$bucketAuto": {"groupBy": "$score", "buckets": 1, "output": []}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": []}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"output": []}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"x": {"$sum": 1, "window": []}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"output": {1: {"$sum": 1}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": 1}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": {"$sum": 1, "$max": 2}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": {"$sum": 1, "window": {"documents": [0]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": {"$sum": 1, "window": {"documents": [0, "later"]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$setWindowFields": {"output": {"x": {"$sum": "$score", "window": {"range": [0, 1]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1, "_id": 1}, "output": {"x": {"$sum": "$score", "window": {"range": [0, 1]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": "$score", "window": {"documents": [0, 0], "range": [0, 1]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": "$score", "window": {"range": [0]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": "x"}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [0, 1]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [True, 1]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"score": 1}, {"score": "x"}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [0, "current"]}}}}}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$count": ""}])

        with self.assertRaises(OperationFailure):
            apply_pipeline([{"_id": "1"}], [{"$count": "$total"}])

    def test_split_pushdown_pipeline_extracts_safe_prefix(self):
        pushdown = split_pushdown_pipeline(
            [
                {"$match": {"kind": "view"}},
                {"$match": {"rank": {"$gte": 2}}},
                {"$sort": {"rank": 1}},
                {"$skip": 1},
                {"$limit": 2},
                {"$project": {"rank": 1, "_id": 0}},
            ]
        )

        self.assertEqual(
            pushdown.filter_spec,
            {"$and": [{"kind": "view"}, {"rank": {"$gte": 2}}]},
        )
        self.assertEqual(pushdown.sort, [("rank", 1)])
        self.assertEqual(pushdown.skip, 1)
        self.assertEqual(pushdown.limit, 2)
        self.assertEqual(pushdown.projection, {"rank": 1, "_id": 0})
        self.assertEqual(pushdown.remaining_pipeline, [])

    def test_split_pushdown_pipeline_stops_on_unsafe_order(self):
        pushdown = split_pushdown_pipeline(
            [
                {"$match": {"kind": "view"}},
                {"$project": {"kind": 1, "_id": 0}},
                {"$sort": {"kind": 1}},
            ]
        )

        self.assertEqual(pushdown.filter_spec, {"kind": "view"})
        self.assertEqual(pushdown.projection, {"kind": 1, "_id": 0})
        self.assertIsNone(pushdown.sort)
        self.assertEqual(pushdown.remaining_pipeline, [{"$sort": {"kind": 1}}])

    def test_split_pushdown_pipeline_rejects_invalid_match_payload(self):
        with self.assertRaises(OperationFailure):
            split_pushdown_pipeline([{"$match": []}])

    def test_split_pushdown_pipeline_keeps_match_with_nor_expr_in_core(self):
        pushdown = split_pushdown_pipeline(
            [
                {"$match": {"$nor": [{"$expr": {"$gt": ["$a", 5]}}]}},
                {"$project": {"a": 1, "_id": 0}},
            ]
        )

        self.assertEqual(pushdown.filter_spec, {})
        self.assertEqual(
            pushdown.remaining_pipeline,
            [
                {"$match": {"$nor": [{"$expr": {"$gt": ["$a", 5]}}]}},
                {"$project": {"a": 1, "_id": 0}},
            ],
        )

    def test_is_simple_projection_helper(self):
        self.assertFalse(_is_simple_projection([]))
        self.assertTrue(_is_simple_projection({"name": 1, "_id": 0}))
        self.assertFalse(_is_simple_projection({"name": {"$toString": "$score"}}))
