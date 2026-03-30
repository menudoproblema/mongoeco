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

