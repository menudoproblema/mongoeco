import datetime
import decimal
import math
import re
import unittest
import uuid
from copy import deepcopy
from unittest.mock import ANY, patch

from mongoeco.compat import MongoDialect, MONGODB_DIALECT_70
import mongoeco.core.aggregation.accumulators as accumulators_module
import mongoeco.core.aggregation.grouping_stages as grouping_stages
from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt32, BsonInt64
from mongoeco.core.aggregation.compiled_aggregation import CompiledGroup
from mongoeco.core.collation import CollationSpec
from mongoeco.core.aggregation.accumulators import _AccumulatorBucket, _OrderedAccumulator
from mongoeco.core.aggregation.stages import (
    _apply_fill_output,
    _densify_datetime_delta,
    _require_densify_value,
    _resolve_densify_bounds,
    _sort_window_for_following_slices,
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




class AggregationPipelineCoreTests(unittest.TestCase):
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

    def test_pipeline_supports_densify_for_numeric_values(self):
        documents = [{"x": 1}, {"x": 3}]

        result = apply_pipeline(
            documents,
            [{"$densify": {"field": "x", "range": {"step": 1, "bounds": "full"}}}],
        )

        self.assertEqual(result, [{"x": 1}, {"x": 2}, {"x": 3}])

    def test_pipeline_supports_fill_locf_and_linear(self):
        documents = [
            {"order": 1, "temp": 10, "qty": 1},
            {"order": 2, "temp": None, "qty": None},
            {"order": 3, "temp": 16, "qty": None},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$fill": {
                        "sortBy": {"order": 1},
                        "output": {
                            "temp": {"method": "linear"},
                            "qty": {"method": "locf"},
                        },
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {"order": 1, "temp": 10, "qty": 1},
                {"order": 2, "temp": 13.0, "qty": 1},
                {"order": 3, "temp": 16, "qty": 1},
            ],
        )

    def test_densify_and_fill_cover_validation_and_helper_paths(self):
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            apply_pipeline([{"x": 1}], [{"$densify": []}])
        with self.assertRaisesRegex(OperationFailure, "field must be a non-empty string"):
            apply_pipeline([{"x": 1}], [{"$densify": {"field": "", "range": {"step": 1, "bounds": "full"}}}])
        with self.assertRaisesRegex(OperationFailure, "partitionByFields must be a list"):
            apply_pipeline([{"x": 1}], [{"$densify": {"field": "x", "partitionByFields": [""], "range": {"step": 1, "bounds": "full"}}}])
        with self.assertRaisesRegex(OperationFailure, "range must be a document"):
            apply_pipeline([{"x": 1}], [{"$densify": {"field": "x", "range": []}}])
        with self.assertRaisesRegex(OperationFailure, "positive number"):
            apply_pipeline([{"x": 1}], [{"$densify": {"field": "x", "range": {"step": 0, "bounds": "full"}}}])
        with self.assertRaisesRegex(OperationFailure, "range.unit must be a string"):
            apply_pipeline([{"x": datetime.datetime.now(datetime.UTC)}], [{"$densify": {"field": "x", "range": {"step": 1, "bounds": "full", "unit": 1}}}])

        with self.assertRaisesRegex(OperationFailure, "sortBy must be a single-field document"):
            apply_pipeline([{"order": 1}], [{"$fill": {"sortBy": {}, "output": {"x": {"value": 1}}}}])
        with self.assertRaisesRegex(OperationFailure, "sortBy directions must be 1 or -1"):
            apply_pipeline([{"order": 1}], [{"$fill": {"sortBy": {"order": 0}, "output": {"x": {"value": 1}}}}])
        with self.assertRaisesRegex(OperationFailure, "partitionByFields must be a list"):
            apply_pipeline([{"order": 1}], [{"$fill": {"sortBy": {"order": 1}, "partitionByFields": [""], "output": {"x": {"value": 1}}}}])
        with self.assertRaisesRegex(OperationFailure, "output must be a non-empty document"):
            apply_pipeline([{"order": 1}], [{"$fill": {"sortBy": {"order": 1}, "output": {}}}])

        with self.assertRaisesRegex(OperationFailure, "densified field to exist"):
            _require_densify_value({}, "x")
        with self.assertRaisesRegex(OperationFailure, "numeric or date values only"):
            _require_densify_value({"x": "bad"}, "x")
        self.assertEqual(_resolve_densify_bounds("full", [1, 3]), (1, 3))
        self.assertEqual(_resolve_densify_bounds([1, 5], [2, 4]), (1, 5))
        self.assertEqual(_densify_datetime_delta(2, "hour"), datetime.timedelta(hours=2))
        with self.assertRaisesRegex(OperationFailure, "require a unit"):
            _densify_datetime_delta(1, None)
        with self.assertRaisesRegex(OperationFailure, "millisecond/second/minute/hour/day units"):
            _densify_datetime_delta(1, "month")

        linear_docs = [
            {"order": 1, "value": datetime.datetime(2024, 1, 1, 0, 0, 0)},
            {"order": 2, "value": None},
            {"order": 3, "value": datetime.datetime(2024, 1, 1, 0, 0, 10)},
        ]
        _apply_fill_output(linear_docs, "value", {"method": "linear"})
        self.assertEqual(linear_docs[1]["value"], datetime.datetime(2024, 1, 1, 0, 0, 5))
        with self.assertRaisesRegex(OperationFailure, "supports only value, locf or linear"):
            _apply_fill_output([{"x": None}], "x", {"method": "future"})
        with self.assertRaisesRegex(OperationFailure, "output fields must be documents"):
            _apply_fill_output([{"x": None}], "x", 1)
        with self.assertRaisesRegex(OperationFailure, "supports only numeric or date values"):
            _apply_fill_output([{"x": "a"}, {"x": None}, {"x": "b"}], "x", {"method": "linear"})

    def test_densify_and_fill_cover_none_partition_fields_and_gap_skips(self):
        result = apply_pipeline(
            [{"x": 1}, {"x": 2}],
            [{"$densify": {"field": "x", "partitionByFields": None, "range": {"step": 1, "bounds": "full"}}}],
        )
        self.assertEqual(result, [{"x": 1}, {"x": 2}])

        filled = apply_pipeline(
            [{"order": 1, "value": 1}, {"order": 2, "value": None}],
            [{"$fill": {"sortBy": {"order": 1}, "partitionByFields": None, "output": {"value": {"method": "locf"}}}}],
        )
        self.assertEqual(filled[1]["value"], 1)

    def test_fill_and_densify_helpers_cover_remaining_scalar_paths(self):
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            apply_pipeline([{"order": 1}], [{"$fill": []}])

        with self.assertRaisesRegex(OperationFailure, "full' or a \\[lower, upper\\] pair"):
            _resolve_densify_bounds("range", [1, 2])

        self.assertEqual(_densify_datetime_delta(1, "second"), datetime.timedelta(seconds=1))
        self.assertEqual(_densify_datetime_delta(2, "minute"), datetime.timedelta(minutes=2))
        self.assertEqual(_densify_datetime_delta(3, "day"), datetime.timedelta(days=3))

        docs = [{"value": None}, {"value": 2}, {}]
        _apply_fill_output(docs, "value", {"value": 7})
        self.assertEqual(docs, [{"value": 7}, {"value": 2}, {"value": 7}])

        adjacent = [{"value": 1}, {"value": 2}]
        _apply_fill_output(adjacent, "value", {"method": "linear"})
        self.assertEqual(adjacent, [{"value": 1}, {"value": 2}])

    def test_pipeline_supports_coll_stats_stage_with_count_and_storage_stats(self):
        result = apply_pipeline(
            [],
            [{"$collStats": {"count": {}, "storageStats": {"scale": 2}}}],
            collection_stats_resolver=lambda scale: {
                "ns": "db.events",
                "count": 3,
                "size": 10,
                "storageSize": 10,
                "scaleFactor": scale,
                "ok": 1.0,
            },
        )

        self.assertEqual(
            result,
            [
                {
                    "ns": "db.events",
                    "count": {"count": 3},
                    "storageStats": {
                        "ns": "db.events",
                        "count": 3,
                        "size": 10,
                        "storageSize": 10,
                        "scaleFactor": 2,
                    },
                }
            ],
        )

    def test_coll_stats_stage_requires_first_position_and_supported_options(self):
        resolver = lambda scale: {"ns": "db.events", "count": 1, "size": 1, "storageSize": 1, "scaleFactor": scale}

        with self.assertRaisesRegex(OperationFailure, "\\$collStats is only valid as the first pipeline stage"):
            apply_pipeline(
                [{"_id": 1}],
                [{"$match": {"_id": 1}}, {"$collStats": {"count": {}}}],
                collection_stats_resolver=resolver,
            )

        with self.assertRaisesRegex(OperationFailure, "supports only count and storageStats"):
            apply_pipeline(
                [],
                [{"$collStats": {"latencyStats": {}}}],
                collection_stats_resolver=resolver,
            )

        with self.assertRaisesRegex(OperationFailure, "storageStats.scale must be a positive integer"):
            apply_pipeline(
                [],
                [{"$collStats": {"storageStats": {"scale": 0}}}],
                collection_stats_resolver=resolver,
            )
        with self.assertRaisesRegex(OperationFailure, "requires a collection stats resolver"):
            apply_pipeline([], [{"$collStats": {"count": {}}}])
        with self.assertRaisesRegex(OperationFailure, "document specification"):
            apply_pipeline([], [{"$collStats": []}], collection_stats_resolver=resolver)
        with self.assertRaisesRegex(OperationFailure, "requires at least one of count or storageStats"):
            apply_pipeline([], [{"$collStats": {}}], collection_stats_resolver=resolver)
        with self.assertRaisesRegex(OperationFailure, "count must be an empty document"):
            apply_pipeline([], [{"$collStats": {"count": 1}}], collection_stats_resolver=resolver)
        with self.assertRaisesRegex(OperationFailure, "storageStats must be a document"):
            apply_pipeline([], [{"$collStats": {"storageStats": 1}}], collection_stats_resolver=resolver)
        with self.assertRaisesRegex(OperationFailure, "supports only scale"):
            apply_pipeline([], [{"$collStats": {"storageStats": {"future": True}}}], collection_stats_resolver=resolver)

    def test_pipeline_supports_geo_near_for_local_points(self):
        documents = [
            {"_id": "a", "name": "Ada", "location": {"type": "Point", "coordinates": [0, 0]}, "active": True},
            {"_id": "b", "name": "Grace", "location": [2, 0], "active": False},
            {"_id": "c", "name": "Linus", "location": {"type": "Point", "coordinates": [1, 0]}, "active": True},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [0, 0]},
                        "key": "location",
                        "distanceField": "dist",
                        "includeLocs": "matched",
                        "query": {"active": True},
                    }
                }
            ],
        )

        self.assertEqual(
            result,
            [
                {
                    "_id": "a",
                    "name": "Ada",
                    "location": {"type": "Point", "coordinates": [0, 0]},
                    "active": True,
                    "dist": 0.0,
                    "matched": {"type": "Point", "coordinates": [0, 0]},
                },
                {
                    "_id": "c",
                    "name": "Linus",
                    "location": {"type": "Point", "coordinates": [1, 0]},
                    "active": True,
                    "dist": 1.0,
                    "matched": {"type": "Point", "coordinates": [1, 0]},
                },
            ],
        )

    def test_geo_near_stage_covers_validation_and_skip_paths(self):
        documents = [{"_id": "a", "location": {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}}]

        with self.assertRaisesRegex(OperationFailure, "document specification"):
            apply_pipeline(documents, [{"$geoNear": []}])
        with self.assertRaisesRegex(OperationFailure, "requires near"):
            apply_pipeline(documents, [{"$geoNear": {"key": "location", "distanceField": "dist"}}])
        with self.assertRaisesRegex(OperationFailure, "distanceField must be a non-empty string"):
            apply_pipeline(documents, [{"$geoNear": {"near": [0, 0], "key": "location", "distanceField": ""}}])
        with self.assertRaisesRegex(OperationFailure, "key must be a non-empty string"):
            apply_pipeline(documents, [{"$geoNear": {"near": [0, 0], "key": "", "distanceField": "dist"}}])
        with self.assertRaisesRegex(OperationFailure, "query must be a document"):
            apply_pipeline(documents, [{"$geoNear": {"near": [0, 0], "key": "location", "distanceField": "dist", "query": []}}])
        with self.assertRaisesRegex(OperationFailure, "includeLocs must be a non-empty string"):
            apply_pipeline(documents, [{"$geoNear": {"near": [0, 0], "key": "location", "distanceField": "dist", "includeLocs": ""}}])
        with self.assertRaisesRegex(OperationFailure, "minDistance must be a non-negative number"):
            apply_pipeline(documents, [{"$geoNear": {"near": [0, 0], "key": "location", "distanceField": "dist", "minDistance": -1}}])
        with self.assertRaisesRegex(OperationFailure, "maxDistance must be a non-negative number"):
            apply_pipeline(documents, [{"$geoNear": {"near": [0, 0], "key": "location", "distanceField": "dist", "maxDistance": -1}}])

        result = apply_pipeline(
            [{"_id": "a"}, {"_id": "b", "location": "bad"}],
            [{"$geoNear": {"near": [0, 0], "key": "location", "distanceField": "dist"}}],
        )
        self.assertEqual(result, [])

        distance_filtered = apply_pipeline(
            [
                {"_id": "a", "location": {"type": "Point", "coordinates": [0, 0]}},
                {"_id": "b", "location": {"type": "Point", "coordinates": [10, 0]}},
            ],
            [
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [0, 0]},
                        "key": "location",
                        "distanceField": "dist",
                        "minDistance": 1,
                        "maxDistance": 9,
                    }
                }
            ],
        )
        self.assertEqual(distance_filtered, [])

    def test_pipeline_supports_geo_near_for_non_point_planar_geometries(self):
        documents = [
            {
                "_id": "a",
                "location": {"type": "LineString", "coordinates": [[0, 0], [3, 0]]},
                "active": True,
            },
            {
                "_id": "b",
                "location": {
                    "type": "Polygon",
                    "coordinates": [[[4, -1], [6, -1], [6, 1], [4, 1], [4, -1]]],
                },
                "active": True,
            },
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$geoNear": {
                        "near": {"type": "Point", "coordinates": [1, 1]},
                        "key": "location",
                        "distanceField": "dist",
                        "query": {"active": True},
                    }
                }
            ],
        )

        self.assertEqual([document["_id"] for document in result], ["a", "b"])
        self.assertAlmostEqual(result[0]["dist"], 1.0)
        self.assertGreater(result[1]["dist"], result[0]["dist"])

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

    def test_pipeline_supports_field_bound_logical_expr_conditions_inside_lookup(self):
        documents = [{"_id": "1", "tenant": "a"}]
        foreign = {
            "users": [
                {"_id": "u1", "tenant": "a", "role": "admin", "score": 3},
                {"_id": "u2", "tenant": "a", "role": "staff", "score": 4},
                {"_id": "u3", "tenant": "a", "role": "guest", "score": 4},
                {"_id": "u4", "tenant": "a", "role": "admin", "score": 7},
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
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$tenant", "$$tenantId"]},
                                            {"$or": ["$role", [{"$eq": "admin"}, {"$eq": "staff"}]]},
                                            {"$and": ["$score", [{"$gte": 3}, {"$lt": 5}]]},
                                        ]
                                    }
                                }
                            },
                            {"$project": {"_id": 0, "role": 1, "score": 1}},
                            {"$sort": {"role": 1}},
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
                        {"role": "admin", "score": 3},
                        {"role": "staff", "score": 4},
                    ],
                }
            ],
        )

    def test_pipeline_supports_field_bound_query_filter_expr_conditions_inside_lookup(self):
        documents = [{"_id": "1", "tenant": "a"}]
        foreign = {
            "users": [
                {
                    "_id": "u1",
                    "tenant": "a",
                    "tags": ["a", "b"],
                    "status": "active",
                    "items": [{"kind": "x", "qty": 1}, {"kind": "y", "qty": 3}],
                },
                {
                    "_id": "u2",
                    "tenant": "a",
                    "tags": ["a"],
                    "status": "archived",
                    "items": [{"kind": "y", "qty": 1}],
                },
                {"_id": "u3", "tenant": "a", "status": "active"},
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
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {"$eq": ["$tenant", "$$tenantId"]},
                                            {"$exists": ["$tags", True]},
                                            {"$all": ["$tags", ["a", "b"]]},
                                            {"$nin": ["$status", ["archived"]]},
                                            {"$elemMatch": ["$items", {"kind": "y", "qty": {"$gte": 2}}]},
                                        ]
                                    }
                                }
                            },
                            {"$project": {"_id": 1}},
                        ],
                        "as": "users",
                    }
                }
            ],
            collection_resolver=foreign.get,
        )

        self.assertEqual(
            result,
            [{"_id": "1", "tenant": "a", "users": [{"_id": "u1"}]}],
        )

    def test_pipeline_supports_correlated_list_lookup_with_in_and_dotted_variable_path(self):
        documents = [
            {"_id": "1", "links": [{"id": "u1"}, {"id": "u3"}]},
            {"_id": "2", "links": []},
            {"_id": "3"},
        ]
        foreign = {
            "users": [
                {"_id": "u1", "name": "Ada"},
                {"_id": "u2", "name": "Grace"},
                {"_id": "u3", "name": "Linus"},
            ]
        }

        result = apply_pipeline(
            documents,
            [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$links"},
                        "pipeline": [
                            {
                                "$match": {
                                    "$expr": {
                                        "$and": [
                                            {"$gt": [{"$size": {"$ifNull": ["$$ref_key.id", []]}}, 0]},
                                            {"$in": ["$_id", "$$ref_key.id"]},
                                        ]
                                    }
                                }
                            },
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
                {"_id": "1", "links": [{"id": "u1"}, {"id": "u3"}], "users": [{"name": "Ada"}, {"name": "Linus"}]},
                {"_id": "2", "links": [], "users": []},
                {"_id": "3", "users": []},
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
        foreign_documents = [{"_id": "u1", "name": "Ada"}]
        result = apply_pipeline(
            [{"_id": "1"}, {"_id": "2"}],
            [{"$lookup": {"from": "users", "as": "users", "pipeline": [], "let": {}}}],
            collection_resolver=lambda name: foreign_documents if name == "users" else None,
        )

        result[0]["users"][0]["name"] = "Changed"

        self.assertEqual(result[1]["users"], [{"_id": "u1", "name": "Ada"}])
        self.assertEqual(foreign_documents, [{"_id": "u1", "name": "Ada"}])

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

    def test_pipeline_supports_merge_objects_with_single_array_operand(self):
        documents = [
            {
                "_id": "1",
                "profile_list": [
                    {"name": "Ada"},
                    None,
                    {"city": "Sevilla"},
                ],
            }
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$project": {
                        "_id": 0,
                        "merged": {"$mergeObjects": "$profile_list"},
                    }
                }
            ],
        )

        self.assertEqual(result, [{"merged": {"name": "Ada", "city": "Sevilla"}}])

    def test_interpreted_pipeline_covers_sample_unset_skip_limit_and_sort_window(self):
        documents = [
            {"_id": "3", "score": 3, "secret": "c"},
            {"_id": "1", "score": 1, "secret": "a"},
            {"_id": "2", "score": 2, "secret": "b"},
        ]

        class _Policy:
            def __init__(self) -> None:
                self.operators: list[str] = []

            def maybe_spill(self, operator, documents):
                self.operators.append(operator)
                return documents

        policy = _Policy()
        with patch("mongoeco.core.aggregation.stages.compile_pipeline", return_value=None):
            result = apply_pipeline(
                documents,
                [
                    {"$sample": {"size": 3}},
                    {"$unset": "secret"},
                    {"$sort": {"score": 1}},
                    {"$skip": 1},
                    {"$limit": 1},
                ],
                spill_policy=policy,
            )

        self.assertEqual(result, [{"_id": "2", "score": 2}])
        self.assertEqual(policy.operators, ["$sample", "$unset", "$sort", "$skip", "$limit"])

    def test_interpreted_sort_uses_spill_policy_sort_with_spill(self):
        documents = [{"_id": "2", "score": 2}, {"_id": "1", "score": 1}]

        class _Policy:
            def __init__(self) -> None:
                self.sort_calls: list[list[tuple[str, int]]] = []

            def sort_with_spill(self, documents, sort_spec, *, dialect, collation):
                del dialect
                del collation
                self.sort_calls.append(sort_spec)
                return sorted(documents, key=lambda document: document["score"])

            def maybe_spill(self, operator, documents):
                del operator
                return documents

        policy = _Policy()
        with patch("mongoeco.core.aggregation.stages.compile_pipeline", return_value=None):
            result = apply_pipeline(
                documents,
                [{"$sort": {"score": 1}}],
                spill_policy=policy,
            )

        self.assertEqual(result, [{"_id": "1", "score": 1}, {"_id": "2", "score": 2}])
        self.assertEqual(policy.sort_calls, [[("score", 1)]])

    def test_sort_window_for_following_slices_requires_contiguous_skip_and_limit(self):
        self.assertEqual(
            _sort_window_for_following_slices(
                [
                    {"$sort": {"score": 1}},
                    {"$skip": 2},
                    {"$limit": 5},
                    {"$limit": 3},
                ],
                0,
            ),
            5,
        )
        self.assertIsNone(
            _sort_window_for_following_slices(
                [
                    {"$sort": {"score": 1}},
                    {"$limit": 5},
                    {"$skip": 1},
                ],
                0,
            )
        )
        self.assertIsNone(
            _sort_window_for_following_slices(
                [{"$sort": {"score": 1}}],
                0,
            )
        )

    def test_compiled_pipeline_helpers_cover_registered_stage_and_guard_paths(self):
        def _registered_stage(documents, spec, context):
            del spec, context
            return list(documents)

        register_aggregation_stage("$testRegistered", _registered_stage)
        try:
            self.assertIsNone(compile_pipeline([{"$testRegistered": {}}]))
        finally:
            unregister_aggregation_stage("$testRegistered")

        with patch("mongoeco.core.aggregation.compiled_pipeline.compile_pipeline", side_effect=RuntimeError("boom")):
            self.assertFalse(CompiledPipelinePlan.supports([{"$match": {"x": 1}}]))

        with self.assertRaisesRegex(AssertionError, "unsupported compiled document step"):
            from mongoeco.core.aggregation.compiled_pipeline import _compile_document_step

            _compile_document_step("$skip", 1, dialect=MONGODB_DIALECT_70)

        with self.assertRaisesRegex(OperationFailure, "\\$match requires a document specification"):
            compile_pipeline([{"$match": []}])

        self.assertIsNone(
            _sort_window_for_following_slices(
                [
                    {"$sort": {"score": 1}},
                    {"$limit": 2},
                    {"$skip": 1},
                ],
                0,
            )
        )
