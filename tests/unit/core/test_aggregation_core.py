import datetime
import decimal
import math
import re
import unittest
import uuid
from copy import deepcopy
from unittest.mock import ANY, patch

from mongoeco.compat import MongoDialect
import mongoeco.core.aggregation.accumulators as accumulators_module
import mongoeco.core.aggregation.grouping_stages as grouping_stages
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.aggregation.compiled_aggregation import CompiledGroup
from mongoeco.core.collation import CollationSpec
from mongoeco.core.aggregation.accumulators import _AccumulatorBucket, _OrderedAccumulator
from mongoeco.core.aggregation.runtime import (
    _bson_value_size,
    _mongo_mod,
    _normalize_numeric_place,
    _round_numeric,
    _stringify_aggregation_value,
    _subtract_values,
    _trunc_numeric,
)
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
    AggregationSpillPolicy,
    CompiledPipelinePlan,
    apply_pipeline,
    compile_pipeline,
    evaluate_expression,
    register_aggregation_expression_operator,
    register_aggregation_stage,
    split_pushdown_pipeline,
    unregister_aggregation_expression_operator,
    unregister_aggregation_stage,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Binary, Decimal128, ObjectId, Regex, Timestamp, UNDEFINED




class AggregationCoreTests(unittest.TestCase):
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

        with self.assertRaisesRegex(OperationFailure, "BSON-compatible values"):
            _aggregation_key(_Unhashable())

    def test_match_spec_contains_expr_and_field_path_resolution_cover_edge_cases(self):
        self.assertFalse(_match_spec_contains_expr("invalid"))
        self.assertTrue(_match_spec_contains_expr({"$and": [{"a": 1}, {"$expr": True}]}))
        self.assertTrue(_match_spec_contains_expr({"$nor": [{"a": 1}, {"$expr": True}]}))
        self.assertEqual(_resolve_aggregation_field_path([{"name": "Ada"}], "0.name"), "Ada")
        self.assertIs(_resolve_aggregation_field_path([{"name": "Ada"}], "-1.name"), _MISSING)
        self.assertIs(_resolve_aggregation_field_path([{"name": "Ada"}], "2.name"), _MISSING)
        self.assertEqual(_resolve_aggregation_field_path([{"name": "Ada"}, 1], "name"), ["Ada"])

    def test_aggregation_scalar_expressions_support_public_bson_classes(self):
        documents = apply_pipeline(
            [
                {
                    "blob": Binary(b"\x00\x01\x02", subtype=4),
                    "amount": Decimal128("10.25"),
                    "pattern": Regex("^ad", "i"),
                    "ts": Timestamp(1234567890, 7),
                }
            ],
            [
                {
                    "$project": {
                        "_id": 0,
                        "blobType": {"$type": "$blob"},
                        "blobSize": {"$binarySize": "$blob"},
                        "amountType": {"$type": "$amount"},
                        "amountString": {"$toString": "$amount"},
                        "patternType": {"$type": "$pattern"},
                        "patternString": {"$toString": "$pattern"},
                        "timestampType": {"$type": "$ts"},
                        "timestampString": {"$toString": "$ts"},
                    }
                }
            ],
        )

        self.assertEqual(
            documents,
            [
                {
                    "blobType": "binData",
                    "blobSize": 3,
                    "amountType": "decimal",
                    "amountString": "10.25",
                    "patternType": "regex",
                    "patternString": "/^ad/i",
                    "timestampType": "timestamp",
                    "timestampString": "Timestamp(1234567890, 7)",
                }
            ],
        )

    def test_apply_pipeline_does_not_mutate_input_documents(self):
        documents = [
            {"_id": "1", "name": "Ada", "nested": {"city": "London"}, "scores": [3, 1, 2]},
            {"_id": "2", "name": "Grace", "nested": {"city": "Paris"}, "scores": [5, 4]},
        ]
        original = deepcopy(documents)

        result = apply_pipeline(
            documents,
            [
                {"$addFields": {"city": "$nested.city"}},
                {"$sort": {"name": 1}},
                {"$project": {"_id": 1, "city": 1, "scores": 1}},
            ],
        )

        self.assertEqual(
            result,
            [
                {"_id": "1", "city": "London", "scores": [3, 1, 2]},
                {"_id": "2", "city": "Paris", "scores": [5, 4]},
            ],
        )
        self.assertEqual(documents, original)
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

    def test_evaluate_expression_preserves_bson_numeric_wrappers_in_add_and_subtract(self):
        self.assertEqual(
            evaluate_expression({"left": BsonInt32(2), "right": BsonInt32(3)}, {"$add": ["$left", "$right"]}),
            BsonInt32(5),
        )
        self.assertEqual(
            evaluate_expression({"left": BsonInt64(1 << 40), "right": 1}, {"$subtract": ["$left", "$right"]}),
            BsonInt64((1 << 40) - 1),
        )

    def test_group_accumulators_accept_bson_numeric_wrappers(self):
        documents = [
            {"group": "a", "score": BsonInt32(2)},
            {"group": "a", "score": BsonInt32(3)},
            {"group": "b", "score": BsonDecimal128(decimal.Decimal("1.5"))},
            {"group": "b", "score": BsonDecimal128(decimal.Decimal("2.5"))},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "total": {"$sum": "$score"},
                        "avg": {"$avg": "$score"},
                    }
                }
            ],
        )

        self.assertEqual(
            grouped,
            [
                {"_id": "a", "total": BsonInt32(5), "avg": 2.5},
                {"_id": "b", "total": BsonDecimal128(decimal.Decimal("4.0")), "avg": decimal.Decimal("2.0")},
            ],
        )

    def test_scalar_conversions_preserve_bson_numeric_wrappers_when_input_is_wrapped(self):
        document = {
            "i32": BsonInt32(2),
            "i64": BsonInt64(1 << 40),
            "double": BsonDouble(3.5),
            "decimal": BsonDecimal128(decimal.Decimal("1.25")),
        }

        self.assertTrue(evaluate_expression(document, {"$isNumber": "$i32"}))
        self.assertEqual(evaluate_expression(document, {"$type": "$i64"}), "long")
        self.assertEqual(evaluate_expression(document, {"$toLong": "$i32"}), BsonInt64(2))
        self.assertEqual(evaluate_expression(document, {"$toInt": "$i32"}), BsonInt32(2))
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$i32"}), BsonDouble(2.0))
        self.assertEqual(
            evaluate_expression(document, {"$toDecimal": "$double"}),
            BsonDecimal128(decimal.Decimal("3.5")),
        )
        self.assertEqual(evaluate_expression(document, {"$toString": "$decimal"}), "1.25")
        document = {
            "score": 10,
            "bonus": 2,
            "tags": ["a", "b", "c"],
            "nested": {"value": 3},
            "other_tags": ["b", "d"],
        }
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

    def test_evaluate_expression_can_use_registered_extension_operator_without_custom_dialect(self):
        def _handler(document, spec, variables, _dialect, context):
            args = context.require_expression_args("$echo", spec, 1, 1)
            return context.evaluate_expression(document, args[0], variables)

        register_aggregation_expression_operator("$echo", _handler)
        try:
            self.assertEqual(
                evaluate_expression(
                    {"value": 7},
                    {"$echo": ["$value"]},
                ),
                7,
            )
        finally:
            unregister_aggregation_expression_operator("$echo")

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

    def test_compiled_group_supports_only_implemented_accumulators(self):
        self.assertTrue(
            CompiledGroup.supports(
                {"_id": "$kind", "total": {"$sum": "$value"}, "first": {"$first": "$value"}}
            )
        )
        self.assertTrue(CompiledGroup.supports({"_id": "$kind", "roles": {"$addToSet": "$role"}}))
        self.assertFalse(CompiledGroup.supports({"_id": "$kind", "items": {"$push": "$role"}}))

    def test_apply_group_compiles_add_to_set_without_changing_uniqueness_or_order(self):
        grouped = _apply_group(
            [
                {"kind": "a", "tag": {"code": 1}},
                {"kind": "a", "tag": {"code": 1}},
                {"kind": "a", "tag": {"code": 2}},
            ],
            {
                "_id": "$kind",
                "tags": {"$addToSet": "$tag"},
            },
        )

        self.assertEqual(
            grouped,
            [{"_id": "a", "tags": [{"code": 1}, {"code": 2}]}],
        )

    def test_apply_group_add_to_set_respects_collation(self):
        grouped = _apply_group(
            [
                {"kind": "a", "tag": "Ada"},
                {"kind": "a", "tag": "ada"},
                {"kind": "a", "tag": "Grace"},
            ],
            {
                "_id": "$kind",
                "tags": {"$addToSet": "$tag"},
            },
            collation=CollationSpec(locale="en", strength=2),
        )

        self.assertEqual(grouped, [{"_id": "a", "tags": ["Ada", "Grace"]}])

    def test_compiled_group_add_to_set_respects_collation(self):
        grouped = CompiledGroup(
            {"_id": "$kind", "tags": {"$addToSet": "$tag"}}
        ).apply(
            [
                {"kind": "a", "tag": "Ada"},
                {"kind": "a", "tag": "ada"},
                {"kind": "a", "tag": "Grace"},
            ],
            collation=CollationSpec(locale="en", strength=2),
        )

        self.assertEqual(grouped, [{"_id": "a", "tags": ["Ada", "Grace"]}])

    def test_apply_group_falls_back_for_accumulators_not_supported_by_compiler(self):
        grouped = _apply_group(
            [
                {"kind": "a", "role": "admin"},
                {"kind": "a", "role": "user"},
            ],
            {
                "_id": "$kind",
                "roles": {"$addToSet": "$role"},
                "items": {"$push": "$role"},
            },
        )

        self.assertEqual(
            grouped,
            [{"_id": "a", "roles": ["admin", "user"], "items": ["admin", "user"]}],
        )

    def test_apply_group_falls_back_when_compiler_raises(self):
        with patch("mongoeco.core.aggregation.grouping_stages.CompiledGroup") as compiled_group:
            compiled_group.supports.return_value = True
            compiled_group.side_effect = OperationFailure("boom")

            grouped = _apply_group(
                [
                    {"kind": "a", "value": 1},
                    {"kind": "a", "value": 2},
                ],
                {"_id": "$kind", "total": {"$sum": "$value"}},
            )

        self.assertEqual(grouped, [{"_id": "a", "total": 3}])

    def test_apply_group_propagates_unexpected_compiler_errors(self):
        with patch("mongoeco.core.aggregation.grouping_stages.CompiledGroup") as compiled_group:
            compiled_group.supports.return_value = True
            compiled_group.side_effect = RuntimeError("boom")

            with self.assertRaises(RuntimeError):
                _apply_group(
                    [
                        {"kind": "a", "value": 1},
                        {"kind": "a", "value": 2},
                    ],
                    {"_id": "$kind", "total": {"$sum": "$value"}},
                )

    def test_set_window_fields_does_not_mutate_input_documents(self):
        documents = [{"_id": "1", "partition": "a", "value": 2}, {"_id": "2", "partition": "a", "value": 3}]
        original = deepcopy(documents)

        result = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$partition",
                        "sortBy": {"_id": 1},
                        "output": {
                            "running": {
                                "$sum": "$value",
                                "window": {"documents": ["unbounded", "current"]},
                            }
                        },
                    }
                }
            ],
        )

        self.assertEqual(documents, original)
        self.assertEqual([document["running"] for document in result], [2, 5])

    def test_apply_accumulators_does_not_pre_evaluate_internal_pick_n_expressions(self):
        expression = {"input": "$score", "n": 2}
        bucket = _AccumulatorBucket(
            bucket_id=None,
            values=_initialize_accumulators({"firstTwo": {"$firstN": expression}}),
            include_bucket_id=False,
        )
        evaluated: list[object] = []
        missing_sentinel = object()

        def _evaluate(current_document, current_expression, current_variables=None):
            evaluated.append(current_expression)
            if current_expression == "$score":
                return current_document["score"]
            if current_expression == 2:
                return 2
            raise AssertionError(f"unexpected expression: {current_expression!r}")

        def _evaluate_with_missing(current_document, current_expression, current_variables=None):
            evaluated.append(("missing", current_expression))
            if current_expression == "$score":
                return current_document["score"]
            return missing_sentinel

        _apply_accumulators(
            bucket,
            {"firstTwo": {"$firstN": expression}},
            {"score": 7},
            {},
            evaluate_expression=_evaluate,
            evaluate_expression_with_missing=_evaluate_with_missing,
            append_unique_values=lambda target, values: target.extend(values),
            require_sort=lambda _spec: [],
            resolve_aggregation_field_path=lambda _document, _path: missing_sentinel,
            missing_sentinel=missing_sentinel,
        )

        self.assertNotIn(expression, evaluated)
        self.assertEqual(bucket.values["firstTwo"].items, [7])

    def test_apply_accumulators_first_n_skips_input_evaluation_once_full_but_still_checks_n(self):
        expression = {"input": "$score", "n": "$limit"}
        bucket = _AccumulatorBucket(
            bucket_id=None,
            values={"firstOne": accumulators_module._PickNAccumulator(items=[1], n=1)},
            include_bucket_id=False,
        )
        evaluated: list[object] = []

        def _evaluate(current_document, current_expression, current_variables=None):
            evaluated.append(current_expression)
            if current_expression == "$limit":
                return current_document["limit"]
            raise AssertionError(f"unexpected expression: {current_expression!r}")

        def _evaluate_with_missing(current_document, current_expression, current_variables=None):
            raise AssertionError(f"unexpected missing expression: {current_expression!r}")

        _apply_accumulators(
            bucket,
            {"firstOne": {"$firstN": expression}},
            {"score": 7, "limit": 1},
            {},
            evaluate_expression=_evaluate,
            evaluate_expression_with_missing=_evaluate_with_missing,
            append_unique_values=lambda target, values: target.extend(values),
            require_sort=lambda _spec: [],
            resolve_aggregation_field_path=lambda _document, _path: None,
            missing_sentinel=object(),
        )

        self.assertEqual(evaluated, ["$limit"])
        self.assertEqual(bucket.values["firstOne"].items, [1])

    def test_apply_group_validates_slow_path_accumulator_specs_once(self):
        spec = {"_id": "$kind", "items": {"$push": "$value"}}

        with patch(
            "mongoeco.core.aggregation.accumulators._validate_accumulator_expression",
            wraps=accumulators_module._validate_accumulator_expression,
        ) as validate_expression:
            result = _apply_group(
                [
                    {"kind": "a", "value": 1},
                    {"kind": "b", "value": 2},
                    {"kind": "a", "value": 3},
                ],
                spec,
            )

        self.assertEqual(result, [{"_id": "a", "items": [1, 3]}, {"_id": "b", "items": [2]}])
        self.assertEqual(validate_expression.call_count, 1)

    def test_set_window_fields_validates_accumulator_spec_once_per_field(self):
        with patch(
            "mongoeco.core.aggregation.accumulators._validate_accumulator_expression",
            wraps=accumulators_module._validate_accumulator_expression,
        ) as validate_expression:
            result = apply_pipeline(
                [
                    {"_id": "1", "group": "a", "rank": 1, "score": 2},
                    {"_id": "2", "group": "a", "rank": 2, "score": 3},
                ],
                [
                    {
                        "$setWindowFields": {
                            "partitionBy": "$group",
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

        self.assertEqual([document["runningTotal"] for document in result], [2, 5])
        self.assertEqual(validate_expression.call_count, 1)

    def test_compiled_group_preserves_sum_add_null_semantics(self):
        spec = {"_id": "$kind", "total": {"$sum": {"$add": ["$left", "$right"]}}}
        documents = [
            {"kind": "a", "left": 2, "right": 3},
            {"kind": "a", "left": 4, "right": 5},
            {"kind": "b", "left": None, "right": 1},
        ]

        self.assertTrue(CompiledGroup.supports(spec))
        self.assertEqual(
            CompiledGroup(spec).apply(documents),
            apply_pipeline(documents, [{"$group": spec}]),
        )

    def test_compiled_group_compiles_common_arithmetic_expressions(self):
        created_at = datetime.datetime(2026, 3, 29, 12, 0, 0)
        spec = {
            "_id": "$kind",
            "sumValue": {"$sum": {"$multiply": [{"$add": ["$left", "$right"]}, "$factor"]}},
            "shiftedDate": {"$first": {"$subtract": [{"$add": ["$createdAt", 1000]}, 500]}},
        }
        documents = [
            {"kind": "a", "left": 2, "right": 3, "factor": 2, "createdAt": created_at},
            {"kind": "a", "left": 1, "right": 4, "factor": 3, "createdAt": created_at},
        ]

        self.assertEqual(
            CompiledGroup(spec).apply(documents),
            apply_pipeline(documents, [{"$group": spec}]),
        )

    def test_compiled_group_ifnull_evaluates_fallback_expression_once(self):
        spec = {"_id": "$kind", "total": {"$sum": {"$ifNull": [{"$unsupported": "$value"}, 1]}}}
        documents = [{"kind": "a", "value": 7}]
        calls: list[object] = []

        def fake_evaluate_expression(document, expression, variables=None, *, dialect):
            calls.append(expression)
            if expression == {"$unsupported": "$value"}:
                return None
            raise AssertionError(f"unexpected expression: {expression!r}")

        with patch(
            "mongoeco.core.aggregation.compiled_aggregation.evaluate_expression",
            side_effect=fake_evaluate_expression,
        ):
            self.assertEqual(CompiledGroup(spec).apply(documents), [{"_id": "a", "total": 1}])

        self.assertEqual(calls, [{"$unsupported": "$value"}])

    def test_compiled_group_cond_uses_mongo_truthiness(self):
        spec = {"_id": "$kind", "total": {"$sum": {"$cond": ["$flag", 1, 0]}}}
        documents = [{"kind": "a", "flag": ""}]

        self.assertEqual(
            CompiledGroup(spec).apply(documents),
            apply_pipeline(documents, [{"$group": spec}]),
        )

    def test_compiled_group_supports_cond_dict_form(self):
        spec = {"_id": "$kind", "total": {"$sum": {"$cond": {"if": "$flag", "then": "$value", "else": 0}}}}
        documents = [
            {"kind": "a", "flag": True, "value": 3},
            {"kind": "a", "flag": False, "value": 5},
        ]

        self.assertEqual(
            CompiledGroup(spec).apply(documents),
            apply_pipeline(documents, [{"$group": spec}]),
        )

    def test_compiled_group_uses_distinct_context_keys_for_variables(self):
        spec = {"_id": "$kind", "chosen": {"$first": {"$cond": ["$$flag", "$$yes", "$$no"]}}}
        documents = [{"kind": "a"}]
        variables = {"flag": True, "yes": "A", "no": "B"}

        self.assertEqual(
            CompiledGroup(spec).apply(documents, variables),
            apply_pipeline(documents, [{"$group": spec}], variables=variables),
        )

    def test_compiled_group_uses_distinct_context_keys_for_dotted_paths(self):
        spec = {"_id": "$kind", "chosen": {"$first": {"$cond": ["$left.flag", "$left.value", "$right.value"]}}}
        documents = [
            {"kind": "a", "left": {"flag": True, "value": "L"}, "right": {"value": "R"}},
        ]

        self.assertEqual(
            CompiledGroup(spec).apply(documents),
            apply_pipeline(documents, [{"$group": spec}]),
        )

    def test_compiled_group_reuses_cached_compilation_for_same_spec_and_dialect(self):
        CompiledGroup._COMPILED_FUNCTION_CACHE.clear()
        spec = {"_id": "$kind", "cache_probe_total": {"$sum": "$value"}}

        with patch.object(CompiledGroup, "_compile", autospec=True, wraps=CompiledGroup._compile) as compile_method:
            first = CompiledGroup(spec)
            second = CompiledGroup(spec)

        self.assertEqual(compile_method.call_count, 1)
        self.assertIs(first._aggregate_func, second._aggregate_func)

    def test_finalize_accumulators_preserves_user_fields_with_has_prefix(self):
        bucket = _initialize_accumulators({"__has_total": {"$first": "$value"}})
        _apply_accumulators(bucket, {"__has_total": {"$first": "$value"}}, {"value": 7})

        self.assertEqual(_finalize_accumulators(bucket), {"__has_total": 7})

    def test_apply_accumulators_accepts_prepared_specs(self):
        prepared_specs = accumulators_module._prepare_accumulator_specs({"total": {"$sum": "$value"}})
        bucket = _AccumulatorBucket(bucket_id=None, values={"total": 0}, include_bucket_id=False)

        _apply_accumulators(bucket, prepared_specs, {"value": 2})
        _apply_accumulators(bucket, prepared_specs, {"value": 3})

        self.assertEqual(_finalize_accumulators(bucket), {"total": 5})

    def test_finalize_accumulators_reuses_pick_n_and_ordered_outputs_without_extra_deepcopy(self):
        payload = {"nested": ["value"]}
        bucket = {
            "pick": accumulators_module._PickNAccumulator(items=[payload]),
            "top": _OrderedAccumulator(items=[([], payload, 0)]),
            "topn": _OrderedAccumulator(items=[([], payload, 0), ([], {"other": 1}, 1)], n=2),
        }

        with patch("mongoeco.core.aggregation.accumulators.deepcopy", side_effect=AssertionError("unexpected deepcopy")):
            self.assertEqual(
                _finalize_accumulators(bucket),
                {
                    "pick": [payload],
                    "top": payload,
                    "topn": [payload, {"other": 1}],
                },
            )

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

    def test_bucket_auto_handles_even_bucket_sizes_without_index_error(self):
        self.assertEqual(
            apply_pipeline(
                [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}],
                [{"$bucketAuto": {"groupBy": "$value", "buckets": 2}}],
            ),
            [
                {"_id": {"min": 1, "max": 3}, "count": 2},
                {"_id": {"min": 3, "max": 4}, "count": 2},
            ],
        )

    def test_bucket_supports_single_bucket_range(self):
        self.assertEqual(
            apply_pipeline(
                [{"value": 10}, {"value": 30}, {"value": 90}],
                [{"$bucket": {"groupBy": "$value", "boundaries": [0, 100]}}],
            ),
            [{"_id": 0, "count": 3}],
        )

    def test_bucket_auto_does_not_deepcopy_scalar_boundaries(self):
        original_deepcopy = grouping_stages.deepcopy

        def guarded_deepcopy(value):
            if isinstance(value, int) and not isinstance(value, bool):
                raise AssertionError("unexpected scalar deepcopy")
            return original_deepcopy(value)

        with patch("mongoeco.core.aggregation.grouping_stages.deepcopy", side_effect=guarded_deepcopy):
            self.assertEqual(
                apply_pipeline(
                    [{"value": 1}, {"value": 2}, {"value": 3}, {"value": 4}],
                    [{"$bucketAuto": {"groupBy": "$value", "buckets": 2}}],
                ),
                [
                    {"_id": {"min": 1, "max": 3}, "count": 2},
                    {"_id": {"min": 3, "max": 4}, "count": 2},
                ],
            )

    def test_finalize_accumulators_returns_empty_list_for_empty_topn(self):
        bucket = _AccumulatorBucket(
            bucket_id=None,
            values={"top": _OrderedAccumulator(n=2)},
            include_bucket_id=False,
        )

        self.assertEqual(_finalize_accumulators(bucket), {"top": []})

    def test_initialize_accumulators_directly_rejects_unsupported_accumulator_under_default_dialect(self):
        with self.assertRaises(OperationFailure):
            _initialize_accumulators({"value": {"$futureAccumulator": "$value"}})

    def test_set_window_fields_rejects_supported_but_unimplemented_accumulator(self):
        class _FutureWindowDialect(MongoDialect):
            def supports_window_accumulator(self, name: str) -> bool:
                return True if name == "$futureWindow" else super().supports_window_accumulator(name)

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"kind": "a", "value": 1}],
                [
                    {
                        "$setWindowFields": {
                            "partitionBy": "$kind",
                            "sortBy": {"value": 1},
                            "output": {"rank": {"$futureWindow": "$value"}},
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

    def test_apply_pipeline_can_use_registered_extension_stage_without_custom_dialect(self):
        def _handler(documents, spec, _context):
            return [
                {**document, "tag": spec.get("tag")}
                for document in documents
            ]

        register_aggregation_stage("$annotate", _handler)
        try:
            self.assertEqual(
                apply_pipeline(
                    [{"_id": "1"}],
                    [{"$annotate": {"tag": "ext"}}],
                ),
                [{"_id": "1", "tag": "ext"}],
            )
        finally:
            unregister_aggregation_stage("$annotate")

    def test_compile_pipeline_executes_streamable_pipeline_with_same_result(self):
        documents = [
            {"_id": "1", "name": "Ada", "city": "London", "active": True, "score": 7},
            {"_id": "2", "name": "Grace", "city": "Paris", "active": False, "score": 8},
            {"_id": "3", "name": "Linus", "city": "Madrid", "active": True, "score": 5},
        ]
        pipeline = [
            {"$match": {"active": True}},
            {"$addFields": {"label": {"$concat": ["$name", "-", "$city"]}}},
            {"$unset": "city"},
            {"$project": {"_id": 1, "label": 1, "score": 1}},
        ]

        plan = compile_pipeline(pipeline)

        self.assertIsInstance(plan, CompiledPipelinePlan)
        self.assertEqual(plan.execute(documents), apply_pipeline(documents, pipeline))
        self.assertEqual(
            plan.explain()["nodes"],
            [
                {
                    "kind": "streamable_block",
                    "operators": ["$match", "$addFields", "$unset", "$project"],
                }
            ],
        )

    def test_compile_pipeline_executes_sort_window_pipeline_with_same_result(self):
        documents = [
            {"_id": "1", "active": True, "score": 3},
            {"_id": "2", "active": False, "score": 9},
            {"_id": "3", "active": True, "score": 8},
            {"_id": "4", "active": True, "score": 5},
        ]
        pipeline = [
            {"$match": {"active": True}},
            {"$sort": {"score": -1}},
            {"$skip": 1},
            {"$limit": 1},
        ]

        plan = compile_pipeline(pipeline)

        self.assertIsInstance(plan, CompiledPipelinePlan)
        self.assertEqual(plan.execute(documents), apply_pipeline(documents, pipeline))
        self.assertEqual(plan.explain()["nodes"][1]["window"], 2)

    def test_compile_pipeline_returns_none_for_unsupported_stages_extensions_and_collation(self):
        self.assertIsNone(compile_pipeline([{"$lookup": {"from": "users", "localField": "a", "foreignField": "b", "as": "hits"}}]))
        self.assertIsNone(compile_pipeline([{"$group": {"_id": "$kind", "count": {"$sum": 1}}}]))
        self.assertIsNone(compile_pipeline([{"$match": {"name": "ada"}}], collation=CollationSpec(locale="en")))

        register_aggregation_stage("$annotate", lambda documents, spec, _context: documents)
        try:
            self.assertIsNone(compile_pipeline([{"$annotate": {"tag": "x"}}]))
        finally:
            unregister_aggregation_stage("$annotate")

    def test_compile_pipeline_validates_add_fields_and_set_specs_before_execution(self):
        with self.assertRaisesRegex(OperationFailure, "field names must be strings"):
            compile_pipeline([{"$addFields": {1: "x"}}])  # type: ignore[dict-item]
        with self.assertRaisesRegex(OperationFailure, "field names must be strings"):
            compile_pipeline([{"$set": {1: "x"}}])  # type: ignore[dict-item]

    def test_apply_pipeline_uses_compiled_pipeline_when_available(self):
        class _StubPlan:
            def execute(self, documents, *, variables=None, collection_resolver=None, spill_policy=None):
                del variables
                del collection_resolver
                del spill_policy
                return [{"count": len(list(documents))}]

        with patch("mongoeco.core.aggregation.stages.compile_pipeline", return_value=_StubPlan()):
            self.assertEqual(
                apply_pipeline([{"_id": "1"}, {"_id": "2"}], [{"$match": {"_id": "1"}}]),
                [{"count": 2}],
            )

    def test_apply_pipeline_spills_after_blocking_stage_when_threshold_is_exceeded(self):
        class _CountingPolicy:
            def __init__(self) -> None:
                self.calls = 0

            def maybe_spill(self, operator, documents):
                if operator == "$sort" and len(documents) > 1:
                    self.calls += 1
                return documents

        policy = _CountingPolicy()
        result = apply_pipeline(
            [{"_id": "2", "score": 2}, {"_id": "1", "score": 1}],
            [{"$sort": {"score": 1}}],
            spill_policy=policy,
        )

        self.assertEqual(result, [{"_id": "1", "score": 1}, {"_id": "2", "score": 2}])
        self.assertEqual(policy.calls, 1)

    def test_apply_pipeline_does_not_spill_after_non_blocking_stage(self):
        class _CountingPolicy:
            def __init__(self) -> None:
                self.calls = 0

            def maybe_spill(self, operator, documents):
                if operator == "$sort" and len(documents) > 1:
                    self.calls += 1
                return documents

        policy = _CountingPolicy()
        result = apply_pipeline(
            [{"_id": "1", "score": 1}, {"_id": "2", "score": 2}],
            [{"$project": {"_id": 1}}],
            spill_policy=policy,
        )

        self.assertEqual(result, [{"_id": "1"}, {"_id": "2"}])
        self.assertEqual(policy.calls, 0)

    def test_apply_pipeline_propagates_spill_policy_into_facet_pipelines(self):
        class _CountingPolicy:
            def __init__(self) -> None:
                self.calls = 0

            def maybe_spill(self, operator, documents):
                if operator == "$sort" and len(documents) > 1:
                    self.calls += 1
                return documents

        policy = _CountingPolicy()
        result = apply_pipeline(
            [{"_id": "2", "score": 2}, {"_id": "1", "score": 1}],
            [{"$facet": {"ordered": [{"$sort": {"score": 1}}]}}],
            spill_policy=policy,
        )

        self.assertEqual(result, [{"ordered": [{"_id": "1", "score": 1}, {"_id": "2", "score": 2}]}])
        self.assertEqual(policy.calls, 1)

    def test_aggregation_spill_policy_round_trips_documents_with_bson_wrappers(self):
        policy = AggregationSpillPolicy(threshold=1)

        result = policy.maybe_spill(
            "$sort",
            [{"_id": "1", "score": BsonInt32(1)}, {"_id": "2", "score": BsonInt32(2)}],
        )

        self.assertEqual(
            result,
            [{"_id": "1", "score": BsonInt32(1)}, {"_id": "2", "score": BsonInt32(2)}],
        )

    def test_aggregation_spill_policy_can_external_sort_large_blocking_stage(self):
        policy = AggregationSpillPolicy(threshold=2)

        result = policy.sort_with_spill(
            [
                {"_id": "4", "score": BsonInt32(4)},
                {"_id": "1", "score": BsonInt32(1)},
                {"_id": "3", "score": BsonInt32(3)},
                {"_id": "2", "score": BsonInt32(2)},
            ],
            [("score", 1)],
        )

        self.assertEqual([document["_id"] for document in result], ["1", "2", "3", "4"])

    def test_runtime_numeric_and_string_helpers_cover_edge_cases(self):
        aware = datetime.datetime(
            2026,
            3,
            25,
            10,
            5,
            6,
            789000,
            tzinfo=datetime.timezone(datetime.timedelta(hours=2)),
        )

        self.assertEqual(_stringify_aggregation_value(decimal.Decimal("10.50")), "10.50")
        self.assertEqual(_stringify_aggregation_value(float("inf")), "Infinity")
        self.assertEqual(_stringify_aggregation_value(float("-inf")), "-Infinity")
        self.assertEqual(_stringify_aggregation_value(aware), "2026-03-25T08:05:06.789Z")
        self.assertEqual(_stringify_aggregation_value(Binary(b"\x00\x01", subtype=4)), "AAE=")
        self.assertEqual(_stringify_aggregation_value(Regex("^a", "im")), "/^a/im")
        self.assertEqual(_stringify_aggregation_value(Timestamp(10, 2)), "Timestamp(10, 2)")
        self.assertTrue(math.isnan(_mongo_mod(float("inf"), 2)))
        self.assertEqual(_mongo_mod(-5, 3), -2)
        self.assertEqual(_normalize_numeric_place("$round", 2), 2)
        with self.assertRaises(OperationFailure):
            _normalize_numeric_place("$round", True)
        with self.assertRaises(OperationFailure):
            _normalize_numeric_place("$round", 101)
        self.assertEqual(_round_numeric(12, 2), 12)
        self.assertEqual(_round_numeric(12.345, 2), 12.34)
        self.assertEqual(_round_numeric(125, -1), 120)
        self.assertEqual(_trunc_numeric(12, 2), 12)
        self.assertEqual(_trunc_numeric(12.987, 2), 12.98)
        self.assertEqual(_trunc_numeric(127, -1), 120)

    def test_runtime_bson_size_and_subtract_helpers_cover_special_types(self):
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        object_id = ObjectId("0123456789abcdef01234567")
        created_at = datetime.datetime(2026, 3, 25, 10, 0, 0)

        self.assertEqual(_bson_value_size(1.5), 8)
        self.assertEqual(_bson_value_size("Ada"), 8)
        self.assertEqual(_bson_value_size({"name": "Ada"}), 19)
        self.assertEqual(_bson_value_size([1, "x"]), 21)
        self.assertEqual(_bson_value_size(Binary(b"ab", subtype=4)), 7)
        self.assertEqual(_bson_value_size(session_id), 21)
        self.assertEqual(_bson_value_size(object_id), 12)
        self.assertEqual(_bson_value_size(True), 1)
        self.assertEqual(_bson_value_size(created_at), 8)
        self.assertEqual(_bson_value_size(None), 0)
        self.assertEqual(_bson_value_size(UNDEFINED), 0)
        self.assertEqual(_bson_value_size(re.compile("^a")), 4)
        self.assertEqual(_bson_value_size(Regex("^a", "im")), 6)
        self.assertEqual(_bson_value_size(Timestamp(1, 2)), 8)
        self.assertEqual(_bson_value_size(1 << 40), 8)
        with self.assertRaises(OperationFailure):
            _bson_value_size(decimal.Decimal("1.5"))

        earlier = datetime.datetime(2026, 3, 25, 9, 59, 30)
        self.assertEqual(_subtract_values(created_at, earlier), 30000)
        self.assertEqual(
            _subtract_values(created_at, 2500),
            datetime.datetime(2026, 3, 25, 9, 59, 57, 500000),
        )
        self.assertEqual(_subtract_values(10, 3), 7)
        with self.assertRaises(OperationFailure):
            _subtract_values(1, created_at)
