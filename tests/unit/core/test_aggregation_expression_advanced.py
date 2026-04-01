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
from mongoeco.core.collation import CollationSpec, normalize_collation
from mongoeco.core.aggregation.accumulators import _AccumulatorBucket, _OrderedAccumulator
from mongoeco.core.aggregation.scalar_expressions import (
    _aggregation_type_name,
    _convert_aggregation_scalar,
    evaluate_scalar_expression,
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




class AggregationExpressionAdvancedTests(unittest.TestCase):
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
            [document for document in documents if document["_id"] != "4"],
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

    def test_group_bucket_and_window_support_stddev_accumulators(self):
        documents = [
            {"_id": "1", "group": "a", "rank": 1, "score": 2},
            {"_id": "2", "group": "a", "rank": 2, "score": 4},
            {"_id": "3", "group": "a", "rank": 3, "score": 4},
            {"_id": "4", "group": "a", "rank": 4, "score": "x"},
            {"_id": "5", "group": "b", "rank": 1, "score": 10},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "pop": {"$stdDevPop": "$score"},
                        "samp": {"$stdDevSamp": "$score"},
                    }
                }
            ],
        )
        self.assertEqual(grouped[0]["_id"], "a")
        self.assertAlmostEqual(grouped[0]["pop"], 0.94280904158, places=10)
        self.assertAlmostEqual(grouped[0]["samp"], 1.15470053838, places=10)
        self.assertEqual(grouped[1], {"_id": "b", "pop": 0.0, "samp": None})

        bucketed = apply_pipeline(
            [document for document in documents if document["_id"] != "4"],
            [
                {
                    "$bucket": {
                        "groupBy": "$score",
                        "boundaries": [0, 5, 20],
                        "output": {
                            "pop": {"$stdDevPop": "$score"},
                            "samp": {"$stdDevSamp": "$score"},
                        },
                    }
                }
            ],
        )
        self.assertAlmostEqual(bucketed[0]["pop"], 0.94280904158, places=10)
        self.assertAlmostEqual(bucketed[0]["samp"], 1.15470053838, places=10)
        self.assertEqual(bucketed[1]["pop"], 0.0)
        self.assertIsNone(bucketed[1]["samp"])

    def test_bucket_and_bucket_auto_honour_pipeline_collation(self):
        documents = [
            {"_id": "1", "name": "alice"},
            {"_id": "2", "name": "Bob"},
            {"_id": "3", "name": "carol"},
        ]
        collation = normalize_collation({"locale": "en", "strength": 2})

        bucketed = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$name",
                        "boundaries": ["a", "d", "z"],
                        "default": "other",
                        "output": {"names": {"$push": "$name"}},
                    }
                }
            ],
            collation=collation,
        )
        bucketed_auto = apply_pipeline(
            documents,
            [
                {
                    "$bucketAuto": {
                        "groupBy": "$name",
                        "buckets": 2,
                        "output": {"names": {"$push": "$name"}},
                    }
                }
            ],
            collation=collation,
        )

        self.assertEqual(bucketed[0]["names"], ["alice", "Bob", "carol"])
        self.assertEqual(bucketed_auto[0]["names"], ["alice", "Bob"])

    def test_group_bucket_and_window_support_first_n_and_last_n_accumulators(self):
        documents = [
            {"_id": "1", "group": "a", "rank": 1, "score": 2},
            {"_id": "2", "group": "a", "rank": 2, "score": 4},
            {"_id": "3", "group": "a", "rank": 3, "score": 6},
            {"_id": "4", "group": "b", "rank": 1, "score": 9},
            {"_id": "5", "group": "b", "rank": 2},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "firstTwo": {"$firstN": {"input": "$score", "n": 2}},
                        "lastTwo": {"$lastN": {"input": "$score", "n": 2}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
        )
        self.assertEqual(
            grouped,
            [
                {"_id": "a", "firstTwo": [2, 4], "lastTwo": [4, 6]},
                {"_id": "b", "firstTwo": [9, None], "lastTwo": [9, None]},
            ],
        )

        bucketed = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$rank",
                        "boundaries": [0, 2, 4],
                        "output": {
                            "firstTwo": {"$firstN": {"input": "$score", "n": 2}},
                            "lastTwo": {"$lastN": {"input": "$score", "n": 2}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            bucketed,
            [
                {"_id": 0, "firstTwo": [2, 9], "lastTwo": [2, 9]},
                {"_id": 2, "firstTwo": [4, 6], "lastTwo": [6, None]},
            ],
        )

        bucketed_auto = apply_pipeline(
            documents,
            [
                {
                    "$bucketAuto": {
                        "groupBy": "$rank",
                        "buckets": 2,
                        "output": {
                            "firstTwo": {"$firstN": {"input": "$score", "n": 2}},
                            "lastTwo": {"$lastN": {"input": "$score", "n": 2}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            bucketed_auto,
            [
                {"_id": {"min": 1, "max": 2}, "firstTwo": [2, 9], "lastTwo": [9, 4]},
                {"_id": {"min": 2, "max": 3}, "firstTwo": [None, 6], "lastTwo": [None, 6]},
            ],
        )

        windowed = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningFirstTwo": {
                                "$firstN": {"input": "$score", "n": 2},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "runningLastTwo": {
                                "$lastN": {"input": "$score", "n": 2},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                }
            ],
        )
        self.assertEqual(windowed[0]["runningFirstTwo"], [2])
        self.assertEqual(windowed[0]["runningLastTwo"], [2])
        self.assertEqual(windowed[1]["runningFirstTwo"], [2, 4])
        self.assertEqual(windowed[1]["runningLastTwo"], [2, 4])
        self.assertEqual(windowed[2]["runningFirstTwo"], [2, 4])
        self.assertEqual(windowed[2]["runningLastTwo"], [4, 6])
        self.assertEqual(windowed[3]["runningFirstTwo"], [9])
        self.assertEqual(windowed[3]["runningLastTwo"], [9])
        self.assertEqual(windowed[4]["runningFirstTwo"], [9, None])
        self.assertEqual(windowed[4]["runningLastTwo"], [9, None])

    def test_group_bucket_and_window_support_min_n_and_max_n_accumulators(self):
        documents = [
            {"_id": "1", "group": "a", "rank": 1, "score": 2},
            {"_id": "2", "group": "a", "rank": 2, "score": None},
            {"_id": "3", "group": "a", "rank": 3, "score": 6},
            {"_id": "4", "group": "b", "rank": 1, "score": 9},
            {"_id": "5", "group": "b", "rank": 2, "score": 1},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "topTwo": {"$maxN": {"input": "$score", "n": 2}},
                        "bottomTwo": {"$minN": {"input": "$score", "n": 2}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
        )
        self.assertEqual(
            grouped,
            [
                {"_id": "a", "topTwo": [6, 2], "bottomTwo": [2, 6]},
                {"_id": "b", "topTwo": [9, 1], "bottomTwo": [1, 9]},
            ],
        )

        bucketed = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$rank",
                        "boundaries": [0, 2, 4],
                        "output": {
                            "topTwo": {"$maxN": {"input": "$score", "n": 2}},
                            "bottomTwo": {"$minN": {"input": "$score", "n": 2}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            bucketed,
            [
                {"_id": 0, "topTwo": [9, 2], "bottomTwo": [2, 9]},
                {"_id": 2, "topTwo": [6, 1], "bottomTwo": [1, 6]},
            ],
        )

        windowed = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningTopTwo": {
                                "$maxN": {"input": "$score", "n": 2},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "runningBottomTwo": {
                                "$minN": {"input": "$score", "n": 2},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                }
            ],
        )
        self.assertEqual(windowed[0]["runningTopTwo"], [2])
        self.assertEqual(windowed[0]["runningBottomTwo"], [2])
        self.assertEqual(windowed[1]["runningTopTwo"], [2])
        self.assertEqual(windowed[1]["runningBottomTwo"], [2])
        self.assertEqual(windowed[2]["runningTopTwo"], [6, 2])
        self.assertEqual(windowed[2]["runningBottomTwo"], [2, 6])

    def test_group_bucket_and_window_support_top_and_bottom_accumulators(self):
        documents = [
            {"_id": "1", "group": "a", "rank": 1, "score": 2, "label": "a1"},
            {"_id": "2", "group": "a", "rank": 2, "score": 4, "label": "a2"},
            {"_id": "3", "group": "a", "rank": 3, "score": 4, "label": "a3"},
            {"_id": "4", "group": "b", "rank": 1, "score": None, "label": "b1"},
            {"_id": "5", "group": "b", "rank": 2, "label": "b2"},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "topLabel": {"$top": {"sortBy": {"score": -1}, "output": "$label"}},
                        "bottomLabel": {"$bottom": {"sortBy": {"score": -1}, "output": "$label"}},
                        "topTwo": {"$topN": {"sortBy": {"score": -1}, "output": "$label", "n": 2}},
                        "bottomTwo": {"$bottomN": {"sortBy": {"score": -1}, "output": "$label", "n": 2}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
        )
        self.assertEqual(
            grouped,
            [
                {"_id": "a", "topLabel": "a2", "bottomLabel": "a1", "topTwo": ["a2", "a3"], "bottomTwo": ["a2", "a1"]},
                {"_id": "b", "topLabel": "b1", "bottomLabel": "b1", "topTwo": ["b1", "b2"], "bottomTwo": ["b1", "b2"]},
            ],
        )

        bucketed = apply_pipeline(
            documents,
            [
                {
                    "$bucket": {
                        "groupBy": "$rank",
                        "boundaries": [0, 2, 4],
                        "output": {
                            "topLabel": {"$top": {"sortBy": {"score": -1}, "output": "$label"}},
                            "bottomLabel": {"$bottom": {"sortBy": {"score": -1}, "output": "$label"}},
                        },
                    }
                }
            ],
        )
        self.assertEqual(
            bucketed,
            [
                {"_id": 0, "topLabel": "a1", "bottomLabel": "b1"},
                {"_id": 2, "topLabel": "a2", "bottomLabel": "b2"},
            ],
        )

        windowed = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningTop": {
                                "$top": {"sortBy": {"score": -1}, "output": "$label"},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "runningBottomTwo": {
                                "$bottomN": {"sortBy": {"score": -1}, "output": "$label", "n": 2},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                }
            ],
        )
        self.assertEqual(windowed[0]["runningTop"], "a1")
        self.assertEqual(windowed[1]["runningTop"], "a2")
        self.assertEqual(windowed[2]["runningTop"], "a2")
        self.assertEqual(windowed[0]["runningBottomTwo"], ["a1"])
        self.assertEqual(windowed[1]["runningBottomTwo"], ["a2", "a1"])
        self.assertEqual(windowed[2]["runningBottomTwo"], ["a2", "a1"])
        self.assertEqual(windowed[3]["runningTop"], "b1")
        self.assertEqual(windowed[4]["runningBottomTwo"], ["b1", "b2"])

    def test_group_window_and_expression_support_percentile_and_median(self):
        documents = [
            {"_id": "1", "group": "a", "rank": 1, "score": 1, "scores": [1, 2, 3, 4]},
            {"_id": "2", "group": "a", "rank": 2, "score": 5, "scores": [10, 20, 30, 40]},
            {"_id": "3", "group": "a", "rank": 3, "score": "x", "scores": [7, "x", 9]},
            {"_id": "4", "group": "b", "rank": 1, "score": 2, "scores": None},
        ]

        grouped = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "medianScore": {"$median": {"input": "$score", "method": "approximate"}},
                        "percentiles": {"$percentile": {"input": "$score", "p": [0.0, 0.5, 1.0], "method": "approximate"}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
        )
        self.assertEqual(
            grouped,
            [
                {"_id": "a", "medianScore": 1, "percentiles": [1, 1, 5]},
                {"_id": "b", "medianScore": 2, "percentiles": [2, 2, 2]},
            ],
        )

        projected = apply_pipeline(
            documents,
            [
                {
                    "$project": {
                        "_id": 0,
                        "medianScores": {"$median": {"input": "$scores", "method": "approximate"}},
                        "percentileScores": {"$percentile": {"input": "$scores", "p": [0.25, 0.5, 1.0], "method": "approximate"}},
                    }
                },
                {"$limit": 2},
            ],
        )
        self.assertEqual(
            projected,
            [
                {"medianScores": 2, "percentileScores": [1, 2, 4]},
                {"medianScores": 20, "percentileScores": [10, 20, 40]},
            ],
        )

        windowed = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "partitionBy": "$group",
                        "sortBy": {"rank": 1},
                        "output": {
                            "runningMedian": {
                                "$median": {"input": "$score", "method": "approximate"},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                            "runningPercentiles": {
                                "$percentile": {"input": "$score", "p": [0.0, 1.0], "method": "approximate"},
                                "window": {"documents": ["unbounded", "current"]},
                            },
                        },
                    }
                }
            ],
        )
        self.assertEqual(windowed[0]["runningMedian"], 1)
        self.assertEqual(windowed[1]["runningMedian"], 1)
        self.assertEqual(windowed[2]["runningMedian"], 1)
        self.assertEqual(windowed[0]["runningPercentiles"], [1, 1])
        self.assertEqual(windowed[1]["runningPercentiles"], [1, 5])
        self.assertEqual(windowed[2]["runningPercentiles"], [1, 5])

    def test_evaluate_expression_supports_stddev_expression_forms(self):
        document = {
            "scores": [2, 4, 4, "x"],
            "a": 2,
            "b": 4,
            "c": 4,
            "bad": "x",
        }

        self.assertAlmostEqual(
            evaluate_expression(document, {"$stdDevPop": "$scores"}),
            0.94280904158,
            places=10,
        )
        self.assertAlmostEqual(
            evaluate_expression(document, {"$stdDevSamp": "$scores"}),
            1.15470053838,
            places=10,
        )
        self.assertAlmostEqual(
            evaluate_expression(document, {"$stdDevPop": ["$a", "$b", "$c", "$bad"]}),
            0.94280904158,
            places=10,
        )
        self.assertAlmostEqual(
            evaluate_expression(document, {"$stdDevSamp": ["$a", "$b", "$c", "$bad"]}),
            1.15470053838,
            places=10,
        )
        self.assertEqual(
            evaluate_expression({"only": 5}, {"$stdDevPop": "$only"}),
            0.0,
        )
        self.assertIsNone(
            evaluate_expression({"only": 5}, {"$stdDevSamp": "$only"})
        )
        self.assertIsNone(
            evaluate_expression({"bad": "x"}, {"$stdDevPop": "$bad"})
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
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$group": {"_id": None, "percentiles": {"$percentile": {"input": "$value", "p": [0.5], "method": "exact"}}}}],
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

    def test_evaluate_expression_supports_extended_scalar_conversion_variants(self):
        created_at = datetime.datetime(2026, 3, 25, 10, 5, 6, 789000, tzinfo=datetime.timezone.utc)
        oid = ObjectId("65f0a1000000000000000000")
        uuid_value = uuid.UUID("12345678-1234-5678-1234-567812345678")
        document = {
            "flag": True,
            "neg": -1,
            "long_text": str(1 << 40),
            "float_text": "10.5",
            "created_at": created_at,
            "oid": oid,
            "uuid_bytes": uuid_value.bytes,
            "uuid_value": uuid_value,
            "decimal_text": "10.25",
            "decimal_value": decimal.Decimal("9.5"),
            "regex": re.compile("^a"),
            "undefined": UNDEFINED,
            "nested": {"a": 1},
        }

        self.assertTrue(evaluate_expression(document, {"$toBool": "$neg"}))
        self.assertTrue(evaluate_expression(document, {"$toBool": "$nested"}))
        self.assertEqual(evaluate_expression(document, {"$toInt": "$flag"}), 1)
        self.assertEqual(evaluate_expression(document, {"$toLong": "$long_text"}), 1 << 40)
        self.assertEqual(
            evaluate_expression(document, {"$toLong": "$created_at"}),
            int(created_at.timestamp() * 1000),
        )
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$flag"}), 1.0)
        self.assertEqual(evaluate_expression(document, {"$toDouble": "$float_text"}), 10.5)
        self.assertEqual(
            evaluate_expression(document, {"$toDouble": "$created_at"}),
            float(int(created_at.timestamp() * 1000)),
        )
        self.assertEqual(evaluate_expression(document, {"$toUUID": "$uuid_bytes"}), uuid_value)
        self.assertEqual(evaluate_expression(document, {"$toUUID": "$uuid_value"}), uuid_value)
        self.assertEqual(evaluate_expression(document, {"$toDecimal": "$flag"}), decimal.Decimal("1"))
        self.assertEqual(
            evaluate_expression(document, {"$toDecimal": "$decimal_value"}),
            decimal.Decimal("9.5"),
        )
        self.assertEqual(
            evaluate_expression(document, {"$convert": {"input": "$created_at", "to": "long"}}),
            int(created_at.timestamp() * 1000),
        )
        self.assertEqual(
            evaluate_expression(document, {"$convert": {"input": "$oid", "to": "objectId"}}),
            oid,
        )
        self.assertEqual(
            evaluate_expression(document, {"$convert": {"input": "$decimal_text", "to": "double"}}),
            10.25,
        )
        self.assertEqual(evaluate_expression(document, {"$type": "$regex"}), "regex")
        self.assertEqual(evaluate_expression(document, {"$type": "$uuid_bytes"}), "binData")
        self.assertEqual(evaluate_expression(document, {"$type": "$undefined"}), "undefined")
        self.assertEqual(evaluate_expression(document, {"$type": "$missing"}), "missing")

    def test_evaluate_expression_preserves_bson_numeric_wrappers_in_less_common_numeric_paths(self):
        document = {
            "i32": BsonInt32(7),
            "i32neg": BsonInt32(-7),
            "dbl": BsonDouble(9.25),
            "dec": BsonDecimal128(decimal.Decimal("12.50")),
        }

        self.assertEqual(evaluate_expression(document, {"$divide": ["$i32", 2]}), BsonDouble(3.5))
        self.assertEqual(evaluate_expression(document, {"$mod": ["$i32", 3]}), BsonInt32(1))
        self.assertEqual(evaluate_expression(document, {"$abs": "$i32neg"}), BsonInt32(7))
        self.assertEqual(evaluate_expression(document, {"$bitNot": "$i32"}), BsonInt32(-8))
        self.assertEqual(evaluate_expression(document, {"$floor": ["$dbl"]}), BsonDouble(9.0))
        self.assertEqual(evaluate_expression(document, {"$ceil": ["$dbl"]}), BsonDouble(10.0))
        self.assertEqual(evaluate_expression(document, {"$round": ["$dbl", 1]}), BsonDouble(9.2))
        self.assertEqual(evaluate_expression(document, {"$trunc": ["$dec"]}), BsonDecimal128(decimal.Decimal("12")))
        self.assertEqual(evaluate_expression(document, {"$pow": ["$i32", 2]}), BsonDouble(49.0))

    def test_evaluate_expression_scalar_conversion_edge_cases_and_errors(self):
        huge_decimal = "9" * 7000
        document = {
            "blank": "   ",
            "fractional": 10.5,
            "nan_value": float("nan"),
            "bad_uuid": b"short",
            "huge_decimal": huge_decimal,
            "text": "Ada",
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {"$convert": {"input": "$text", "to": "unknown", "onError": "fallback"}},
            ),
            "fallback",
        )
        self.assertIsNone(
            evaluate_expression(
                document,
                {"$convert": {"input": "$missing", "to": "double"}},
            )
        )
        self.assertEqual(evaluate_expression(document, {"$toInt": "$fractional"}), 10)
        self.assertEqual(evaluate_expression(document, {"$toLong": "$fractional"}), 10)
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDouble": "$blank"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDate": "$nan_value"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toUUID": "$bad_uuid"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toDecimal": "$huge_decimal"})

    def test_evaluate_expression_scalar_conversion_additional_type_and_error_paths(self):
        class CustomValue:
            pass

        oid = ObjectId("65f0a1000000000000000000")
        aware = datetime.datetime(2026, 3, 25, 10, 5, 6, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        document = {
            "zero": 0,
            "empty_list": [],
            "empty_dict": {},
            "regex": re.compile("^a"),
            "none_value": None,
            "bool_value": False,
            "decimal_value": decimal.Decimal("1.25"),
            "dict_value": {"a": 1},
            "array_value": [1, 2],
            "oid": oid,
            "aware": aware,
            "custom": CustomValue(),
            "too_big_int": (1 << 31),
            "too_big_long": (1 << 63),
            "uuid_text": "12345678-1234-5678-1234-567812345678",
        }

        self.assertFalse(evaluate_expression(document, {"$toBool": "$zero"}))
        self.assertTrue(evaluate_expression(document, {"$toBool": "$empty_list"}))
        self.assertTrue(evaluate_expression(document, {"$toBool": "$empty_dict"}))
        self.assertEqual(evaluate_expression(document, {"$type": "$none_value"}), "null")
        self.assertEqual(evaluate_expression(document, {"$type": "$bool_value"}), "bool")
        self.assertEqual(evaluate_expression(document, {"$type": "$decimal_value"}), "decimal")
        self.assertEqual(evaluate_expression(document, {"$type": "$dict_value"}), "object")
        self.assertEqual(evaluate_expression(document, {"$type": "$array_value"}), "array")
        self.assertEqual(evaluate_expression(document, {"$type": "$oid"}), "objectId")
        self.assertEqual(evaluate_expression(document, {"$type": "$aware"}), "date")
        self.assertEqual(evaluate_expression(document, {"$type": "$custom"}), "CustomValue")
        self.assertEqual(evaluate_expression(document, {"$toDate": "$aware"}), datetime.datetime(2026, 3, 25, 8, 5, 6))
        self.assertEqual(evaluate_expression(document, {"$toObjectId": "$oid"}), oid)
        self.assertEqual(
            evaluate_expression(document, {"$convert": {"input": "$uuid_text", "to": "string"}}),
            "12345678-1234-5678-1234-567812345678",
        )
        self.assertEqual(
            evaluate_expression(document, {"$convert": {"input": 3.9, "to": "int"}}),
            3,
        )
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$convert": {"input": "$regex", "to": "bool"}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toInt": "$too_big_int"})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$toLong": "$too_big_long"})

    def test_convert_on_error_catches_non_operationfailure_conversion_errors(self):
        missing_sentinel = object()
        document = {"uuid_text": "12345678-1234-5678-1234-567812345678"}

        with patch(
            "mongoeco.core.aggregation.scalar_expressions._convert_aggregation_scalar",
            side_effect=ValueError("boom"),
        ):
            result = evaluate_scalar_expression(
                "$convert",
                {},
                {"input": 1, "to": "int", "onError": "fallback"},
                None,
                evaluate_expression=lambda document, expression, variables: expression,
                evaluate_expression_with_missing=lambda document, expression, variables: expression,
                require_expression_args=lambda operator, spec, minimum, maximum: spec,
                stringify_value=str,
                bson_document_size=lambda document: 0,
                missing_sentinel=missing_sentinel,
            )

        self.assertEqual(result, "fallback")
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$convert": {"input": "$uuid_text", "to": "unknown"}})

    def test_evaluate_expression_supports_additional_date_string_and_calendar_variants(self):
        aware = datetime.datetime(2026, 3, 25, 10, 5, 6, 789000, tzinfo=datetime.timezone.utc)
        document = {
            "created_at": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
            "aware": aware,
            "start": datetime.datetime(2026, 1, 31, 10, 0, 0),
            "end": datetime.datetime(2027, 3, 1, 9, 0, 0),
        }

        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateToString": {"date": "$created_at", "timezone": "+02:00"}},
            ),
            "2026-03-25T12:05:06.789",
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromString": {"dateString": None, "onNull": "missing"}},
            ),
            "missing",
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromString": {"dateString": "bad-date", "onError": "bad"}},
            ),
            "bad",
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromString": {"dateString": "2026-03-25T12:05:06+02:00", "timezone": "Europe/Madrid"}},
            ),
            datetime.datetime(2026, 3, 25, 10, 5, 6),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateFromParts": {"year": 2026, "month": 1, "day": 31, "hour": 23, "minute": 59, "second": 58, "millisecond": 7}},
            ),
            datetime.datetime(2026, 1, 31, 23, 59, 58, 7000),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$start", "unit": "month", "amount": 1}},
            ),
            datetime.datetime(2026, 2, 28, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$start", "unit": "quarter", "amount": 1}},
            ),
            datetime.datetime(2026, 4, 30, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateSubtract": {"startDate": "$start", "unit": "year", "amount": 1}},
            ),
            datetime.datetime(2025, 1, 31, 10, 0, 0),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateAdd": {"startDate": "$aware", "unit": "hour", "amount": 1, "timezone": "Europe/Madrid"}},
            ),
            datetime.datetime(2026, 3, 25, 11, 5, 6, 789000, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "second"}},
            ),
            int((document["end"] - document["start"]).total_seconds()),
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "month"}},
            ),
            14,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "quarter"}},
            ),
            4,
        )
        self.assertEqual(
            evaluate_expression(
                document,
                {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "year"}},
            ),
            1,
        )

    def test_evaluate_expression_date_math_and_timezone_reject_additional_invalid_values(self):
        document = {"created_at": datetime.datetime(2026, 3, 25, 10, 5, 6), "text": "Ada"}

        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateAdd": {"startDate": "$created_at", "unit": "month", "amount": True}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateAdd": {"startDate": "$created_at", "unit": "bad", "amount": 1}})
        with self.assertRaises(OperationFailure):
            evaluate_expression(document, {"$dateDiff": {"startDate": "$created_at", "endDate": "$text", "unit": "day"}})

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
        self.assertEqual(
            evaluate_expression(
                {"meta": {"name": "Ada"}},
                {"$mergeObjects": "$meta"},
            ),
            {"name": "Ada"},
        )
        self.assertEqual(
            evaluate_expression(
                {"meta": [{"name": "Ada"}, {"city": "Sevilla"}]},
                {"$mergeObjects": "$meta"},
            ),
            {"name": "Ada", "city": "Sevilla"},
        )
        self.assertEqual(
            evaluate_expression(
                {"meta": [{"name": "Ada"}, None, {"city": "Sevilla"}]},
                {"$mergeObjects": "$meta"},
            ),
            {"name": "Ada", "city": "Sevilla"},
        )
        with self.assertRaises(OperationFailure):
            evaluate_expression(
                {"meta": [{"name": "Ada"}, 1]},
                {"$mergeObjects": "$meta"},
            )
        with self.assertRaises(OperationFailure):
            evaluate_expression(
                {"meta": [{"name": "Ada"}]},
                {"$mergeObjects": ["$meta", {"city": "Sevilla"}]},
            )
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

    def test_scalar_helpers_cover_direct_type_conversion_and_error_paths(self):
        missing_sentinel = object()
        object_id = ObjectId("0123456789abcdef01234567")
        created_at = datetime.datetime(2026, 3, 25, 10, 5, 6, tzinfo=datetime.timezone.utc)

        self.assertEqual(_aggregation_type_name(BsonInt32(1), missing_sentinel=missing_sentinel), "int")
        self.assertEqual(_aggregation_type_name(BsonInt64(1 << 40), missing_sentinel=missing_sentinel), "long")
        self.assertEqual(_aggregation_type_name(BsonDouble(1.5), missing_sentinel=missing_sentinel), "double")
        self.assertEqual(
            _aggregation_type_name(BsonDecimal128(decimal.Decimal("1.5")), missing_sentinel=missing_sentinel),
            "decimal",
        )
        self.assertEqual(_aggregation_type_name(Decimal128("2.5"), missing_sentinel=missing_sentinel), "decimal")

        self.assertTrue(_convert_aggregation_scalar("$convert", object_id, "bool", stringify_value=str))
        self.assertEqual(
            _convert_aggregation_scalar("$convert", created_at, "long", stringify_value=str),
            int(created_at.timestamp() * 1000),
        )
        self.assertEqual(
            _convert_aggregation_scalar("$convert", created_at, "double", stringify_value=str),
            float(int(created_at.timestamp() * 1000)),
        )
        self.assertEqual(
            _convert_aggregation_scalar("$convert", object_id, "date", stringify_value=str),
            datetime.datetime.fromtimestamp(object_id.generation_time, tz=datetime.UTC).replace(tzinfo=None),
        )
        self.assertEqual(
            _convert_aggregation_scalar("$convert", Decimal128("10.25"), "decimal", stringify_value=str),
            decimal.Decimal("10.25"),
        )
        self.assertEqual(
            _convert_aggregation_scalar(
                "$convert",
                "12345678-1234-5678-1234-567812345678",
                "uuid",
                stringify_value=str,
            ),
            uuid.UUID("12345678-1234-5678-1234-567812345678"),
        )
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", float(1 << 40), "int", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", float("inf"), "date", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", "bad", "objectId", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", "   ", "decimal", stringify_value=str)

    def test_evaluate_scalar_expression_rejects_unknown_operator(self):
        with self.assertRaises(OperationFailure):
            evaluate_scalar_expression(
                "$unsupported",
                {},
                None,
                None,
                evaluate_expression=lambda document, expression, variables: expression,
                evaluate_expression_with_missing=lambda document, expression, variables: expression,
                require_expression_args=lambda operator, spec, minimum, maximum: [],
                stringify_value=str,
                bson_document_size=lambda document: 0,
                missing_sentinel=object(),
            )

    def test_scalar_helpers_cover_direct_expression_and_conversion_edges(self):
        missing_sentinel = object()

        with self.assertRaises(OperationFailure):
            evaluate_scalar_expression(
                "$convert",
                {},
                {"input": 1},
                None,
                evaluate_expression=lambda document, expression, variables: expression,
                evaluate_expression_with_missing=lambda document, expression, variables: expression,
                require_expression_args=lambda operator, spec, minimum, maximum: spec,
                stringify_value=str,
                bson_document_size=lambda document: 0,
                missing_sentinel=missing_sentinel,
            )
        self.assertEqual(
            evaluate_scalar_expression(
                "$convert",
                {},
                {"input": 5, "to": {"type": "string"}},
                None,
                evaluate_expression=lambda document, expression, variables: expression,
                evaluate_expression_with_missing=lambda document, expression, variables: expression,
                require_expression_args=lambda operator, spec, minimum, maximum: spec,
                stringify_value=str,
                bson_document_size=lambda document: 0,
                missing_sentinel=missing_sentinel,
            ),
            "5",
        )
        self.assertIsNone(
            evaluate_scalar_expression(
                "$bsonSize",
                {},
                "$missing",
                None,
                evaluate_expression=lambda document, expression, variables: expression,
                evaluate_expression_with_missing=lambda document, expression, variables: missing_sentinel,
                require_expression_args=lambda operator, spec, minimum, maximum: spec,
                stringify_value=str,
                bson_document_size=lambda document: 0,
                missing_sentinel=missing_sentinel,
            )
        )

        self.assertTrue(_convert_aggregation_scalar("$convert", True, "bool", stringify_value=str))
        self.assertEqual(_convert_aggregation_scalar("$convert", 12.0, "int", stringify_value=str), 12)
        self.assertEqual(_convert_aggregation_scalar("$convert", 12.0, "long", stringify_value=str), 12)
        self.assertEqual(_convert_aggregation_scalar("$convert", 7, "decimal", stringify_value=str), decimal.Decimal("7"))
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", str(1 << 40), "int", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", str(1 << 80), "long", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", {"a": 1}, "int", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", {"a": 1}, "long", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", "bad-float", "double", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", {"a": 1}, "date", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", 1, "objectId", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", 1, "uuid", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", float("inf"), "decimal", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", [], "decimal", stringify_value=str)
        with self.assertRaises(OperationFailure):
            _convert_aggregation_scalar("$convert", 1, "unsupported", stringify_value=str)
