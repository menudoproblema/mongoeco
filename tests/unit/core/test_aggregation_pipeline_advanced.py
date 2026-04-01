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




class AggregationPipelineAdvancedTests(unittest.TestCase):
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
        for pipeline in (
            [{"$lookup": {"from": "other"}}],
            [{"$densify": {}}],
        ):
            with self.subTest(pipeline=pipeline):
                with self.assertRaises(OperationFailure):
                    apply_pipeline([], pipeline)

    def test_pipeline_rejects_invalid_match_payload(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$match": []}])

    def test_pipeline_rejects_invalid_project_payload(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline([], [{"$project": []}])

    def test_pipeline_rejects_invalid_unwind_payload(self):
        for pipeline in (
            [{"$unwind": []}],
            [{"$unwind": "tags"}],
            [{"$unwind": {"path": "$tags", "includeArrayIndex": 1}}],
            [{"$unwind": {"path": "$tags", "includeArrayIndex": "$idx"}}],
            [{"$unset": {}}],
            [{"$unset": ["ok", ""]}],
        ):
            with self.subTest(pipeline=pipeline):
                with self.assertRaises(OperationFailure):
                    apply_pipeline([], pipeline)

    def test_pipeline_rejects_invalid_sort_direction(self):
        for pipeline in (
            [{"$sort": {"rank": 2}}],
            [{"$sort": {"rank": True}}],
        ):
            with self.subTest(pipeline=pipeline):
                with self.assertRaises(OperationFailure):
                    apply_pipeline([], pipeline)

    def test_pipeline_rejects_invalid_sort_payload_and_field(self):
        for pipeline in (
            [{"$sort": []}],
            [{"$sort": {1: 1}}],
        ):
            with self.subTest(pipeline=pipeline):
                with self.assertRaises(OperationFailure):
                    apply_pipeline([], pipeline)

    def test_pipeline_rejects_invalid_skip_and_limit(self):
        for pipeline in (
            [{"$skip": -1}],
            [{"$limit": True}],
            [{"$sample": []}],
            [{"$sample": {"size": True}}],
            [{"$sample": {"size": 1, "extra": 2}}],
        ):
            with self.subTest(pipeline=pipeline):
                with self.assertRaises(OperationFailure):
                    apply_pipeline([], pipeline)

    def test_pipeline_rejects_invalid_group_and_expression_payloads(self):
        cases = (
            ([], [{"$group": []}]),
            ([], [{"$group": {"total": {"$sum": 1}}}]),
            ([{"_id": "1"}], [{"$group": {"_id": None, "total": 1}}]),
            ([{"_id": "1"}], [{"$group": {"_id": None, "total": {"$unknown": 1}}}]),
            ([{"_id": "1"}], [{"$match": {"$expr": {"$divide": [1, "x"]}}}]),
            ([{"_id": "1"}], [{"$addFields": []}]),
            ([{"_id": "1"}], [{"$addFields": {1: "bad"}}]),
            ([{"_id": "1"}], [{"$project": {1: "$_id"}}]),
            ([{"_id": "1"}], [{"$lookup": []}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": 1}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "", "localField": "x", "foreignField": "_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "$users", "localField": "x", "foreignField": "_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "", "foreignField": "_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": ""}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "$x", "foreignField": "_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "$_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": "$user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "let": []}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": {}, "let": {}}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "localField": "x"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "let": {"x": 1}}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": 1, "foreignField": "_id", "as": "user"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "localField": 1, "foreignField": "_id"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "localField": "x"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "localField": "$x", "foreignField": "_id"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "foreignField": "_id"}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "localField": "x", "foreignField": "_id", "as": "user", "let": {"x": 1}}}]),
            ([{"_id": "1"}], [{"$lookup": {"from": "users", "as": "user", "pipeline": [], "let": {"ROOT": "$_id"}}}]),
            ([{"_id": "1"}], [{"$unionWith": []}]),
            ([{"_id": "1"}], [{"$unionWith": {}}]),
            ([{"_id": "1"}], [{"$unionWith": {"coll": "", "pipeline": []}}]),
            ([{"_id": "1"}], [{"$unionWith": {"coll": "$users", "pipeline": []}}]),
            ([{"_id": "1"}], [{"$unionWith": ""}]),
            ([{"_id": "1"}], [{"$unionWith": {"coll": "users", "pipeline": {}}}]),
            ([{"_id": "1"}], [{"$unionWith": {"coll": "users", "extra": 1}}]),
            ([{"_id": "1"}], [{"$unionWith": "users"}]),
            ([{"_id": "1"}], [{"$replaceRoot": []}]),
            ([{"_id": "1"}], [{"$replaceRoot": {"newRoot": None}}]),
            ([{"_id": "1"}], [{"$replaceRoot": {"newRoot": 5}}]),
            ([{"_id": "1"}], [{"$replaceWith": 1}]),
            ([{"_id": "1"}], [{"$facet": []}]),
            ([{"_id": "1"}], [{"$facet": {1: []}}]),
            ([{"_id": "1"}], [{"$facet": {"bad": {}}}]),
            ([{"_id": "1"}], [{"$match": {}}, {"$documents": [{"_id": "2"}]}]),
            ([{"_id": "1"}], [{"$documents": [1]}]),
            ([{"_id": "1"}], [{"$bucket": []}]),
            ([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [10]}}]),
            ([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [10, 5]}}]),
            ([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10], "output": []}}]),
            ([{"score": 1}], [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10], "output": {"count": 1}}}]),
            ([{"score": 50}], [{"$bucket": {"groupBy": "$score", "boundaries": [0, 10]}}]),
            ([{"score": 1}], [{"$bucketAuto": []}]),
            ([{"score": 1}], [{"$bucketAuto": {"groupBy": "$score", "buckets": 0}}]),
            ([{"score": 1}], [{"$bucketAuto": {"groupBy": "$score", "buckets": 1, "granularity": "R5"}}]),
            ([{"score": 1}], [{"$bucketAuto": {"groupBy": "$score", "buckets": 1, "output": []}}]),
            ([{"_id": "1"}], [{"$setWindowFields": []}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"output": []}}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"sortBy": {"_id": 1}, "output": {"x": {"$sum": 1, "window": []}}}}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"output": {1: {"$sum": 1}}}}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": 1}}}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": {"$sum": 1, "$max": 2}}}}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": {"$sum": 1, "window": {"documents": [0]}}}}}]),
            ([{"_id": "1"}], [{"$setWindowFields": {"output": {"x": {"$sum": 1, "window": {"documents": [0, "later"]}}}}}]),
            ([{"score": 1}], [{"$setWindowFields": {"output": {"x": {"$sum": "$score", "window": {"range": [0, 1]}}}}}]),
            ([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1, "_id": 1}, "output": {"x": {"$sum": "$score", "window": {"range": [0, 1]}}}}}]),
            ([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": "$score", "window": {"documents": [0, 0], "range": [0, 1]}}}}}]),
            ([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": "$score", "window": {"range": [0]}}}}}]),
            ([{"score": "x"}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [0, 1]}}}}}]),
            ([{"score": 1}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [True, 1]}}}}}]),
            ([{"score": 1}, {"score": "x"}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [0, "current"]}}}}}]),
            ([{"score": float("nan")}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [0, 1]}}}}}]),
            ([{"score": 1}, {"score": float("inf")}], [{"$setWindowFields": {"sortBy": {"score": 1}, "output": {"x": {"$sum": 1, "window": {"range": [0, 1]}}}}}]),
            ([{"_id": "1"}], [{"$count": ""}]),
            ([{"_id": "1"}], [{"$count": "$total"}]),
        )

        for documents, pipeline in cases:
            with self.subTest(pipeline=pipeline):
                with self.assertRaises(OperationFailure):
                    apply_pipeline(documents, pipeline)

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

    def test_remove_variable_with_path_suffix_returns_null_not_sentinel(self):
        documents = [{"_id": "1", "value": 1}]

        self.assertEqual(
            apply_pipeline(documents, [{"$addFields": {"extra": "$$REMOVE.field"}}]),
            [{"_id": "1", "value": 1, "extra": None}],
        )

    def test_bucket_rejects_non_strictly_increasing_boundaries(self):
        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$bucket": {"groupBy": "$value", "boundaries": [0, 5, 5, 10]}}],
            )

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                [{"value": 1}],
                [{"$bucket": {"groupBy": "$value", "boundaries": [0, 10, 5]}}],
            )

    def test_first_n_raises_when_n_evaluates_to_different_value_within_group(self):
        documents = [
            {"_id": "1", "group": "a", "score": 1, "n": 2},
            {"_id": "2", "group": "a", "score": 2, "n": 3},
        ]

        with self.assertRaises(OperationFailure):
            apply_pipeline(
                documents,
                [{"$group": {"_id": "$group", "result": {"$firstN": {"input": "$score", "n": "$n"}}}}],
            )

    def test_top_uses_insertion_order_as_tiebreaker_for_equal_sort_values(self):
        documents = [
            {"_id": "1", "group": "a", "score": 5, "label": "first"},
            {"_id": "2", "group": "a", "score": 5, "label": "second"},
            {"_id": "3", "group": "a", "score": 5, "label": "third"},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$group": {
                        "_id": "$group",
                        "top_one": {"$top": {"sortBy": {"score": 1}, "output": "$label"}},
                        "top_two": {"$topN": {"sortBy": {"score": 1}, "output": "$label", "n": 2}},
                    }
                }
            ],
        )

        self.assertEqual(result[0]["top_one"], "first")
        self.assertEqual(result[0]["top_two"], ["first", "second"])

    def test_compiled_group_matches_slow_path_when_add_operand_is_missing(self):
        spec = {"_id": "$kind", "total": {"$sum": {"$add": ["$a", "$missing_field"]}}}
        documents = [
            {"kind": "x", "a": 5},
            {"kind": "x", "a": 3, "missing_field": 2},
        ]

        self.assertEqual(
            CompiledGroup(spec).apply(documents),
            apply_pipeline(documents, [{"$group": spec}]),
        )

    def test_compiled_group_matches_slow_path_for_constant_numeric_sum_literals(self):
        documents = [{"kind": "x"}, {"kind": "x"}]

        for literal in (2, 2.5):
            with self.subTest(literal=literal):
                spec = {"_id": "$kind", "total": {"$sum": literal}}
                self.assertEqual(
                    CompiledGroup(spec).apply(documents),
                    apply_pipeline(documents, [{"$group": spec}]),
                )

    def test_setwindowfields_range_window_includes_only_docs_within_numeric_bounds(self):
        documents = [
            {"_id": "1", "value": 1},
            {"_id": "2", "value": 4},
            {"_id": "3", "value": 7},
            {"_id": "4", "value": 10},
        ]

        result = apply_pipeline(
            documents,
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"value": 1},
                        "output": {
                            "rangeSum": {
                                "$sum": "$value",
                                "window": {"range": [-3, 3]},
                            }
                        },
                    }
                }
            ],
        )

        self.assertEqual(result[0]["rangeSum"], 1 + 4)
        self.assertEqual(result[1]["rangeSum"], 1 + 4 + 7)
        self.assertEqual(result[2]["rangeSum"], 4 + 7 + 10)
        self.assertEqual(result[3]["rangeSum"], 7 + 10)

    def test_resolve_aggregation_field_path_returns_missing_for_unknown_key_at_any_depth(self):
        from mongoeco.core.aggregation.runtime import _MISSING, _resolve_aggregation_field_path

        self.assertIs(_resolve_aggregation_field_path({"a": 1}, "b"), _MISSING)
        self.assertIs(_resolve_aggregation_field_path({"a": {"b": 1}}, "a.c"), _MISSING)
        self.assertIs(_resolve_aggregation_field_path([], "name"), _MISSING)
        self.assertIs(_resolve_aggregation_field_path([{}], "name"), _MISSING)

    def test_pow_expression_returns_double_wrapper_for_integral_bson_inputs(self):
        from mongoeco.core.bson_scalars import BsonDouble, BsonInt32

        result = evaluate_expression(
            {"base": BsonInt32(2), "exp": BsonInt32(10)},
            {"$pow": ["$base", "$exp"]},
        )

        self.assertIsInstance(result, BsonDouble)
        self.assertEqual(result.value, 1024.0)
