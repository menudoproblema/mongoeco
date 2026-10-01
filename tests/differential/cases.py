import datetime
import re

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


RealParityAction = Callable[[Any], Any]


@dataclass(frozen=True, slots=True)
class RealParityCase:
    name: str
    seed_documents: list[dict[str, Any]]
    action: RealParityAction
    min_version: tuple[int, int] = (7, 0)

    def supports(self, target_version: tuple[int, int]) -> bool:
        return target_version >= self.min_version

    def to_manifest(self) -> dict[str, Any]:
        """Return the stable, serializable reproduction envelope."""
        return {
            "name": self.name,
            "min_version": list(self.min_version),
            "seed_documents": self.seed_documents,
            "selector": self.name,
        }


def _write_error_code(action: Callable[[], Any]) -> tuple[str, int | None]:
    try:
        action()
    except Exception as exc:
        return ("error", getattr(exc, "code", None))
    return ("ok", None)


def _write_error_flag(action: Callable[[], Any]) -> str:
    try:
        action()
    except Exception:
        return "error"
    return "ok"


_BOOLEAN_FILTER_MATRIX_DOCUMENTS = [
    {
        "_id": "active-work-open",
        "planning_status": "active",
        "completed_at": None,
        "task_type": "work_unit",
        "score": 8,
        "items": [
            {
                "enrollment_task_id": "soft",
                "exposed_at": datetime.datetime(2024, 1, 1, 12, 0),
            },
        ],
    },
    {
        "_id": "missing-status-open",
        "completed_at": None,
        "task_type": "content_update",
        "score": 3,
        "items": [{"enrollment_task_id": "soft"}],
    },
    {
        "_id": "retired-work-open",
        "planning_status": "retired",
        "completed_at": None,
        "task_type": "work_unit",
        "score": 9,
        "items": [
            {
                "enrollment_task_id": "soft",
                "exposed_at": datetime.datetime(2024, 1, 2, 12, 0),
            },
        ],
    },
    {
        "_id": "active-work-done",
        "planning_status": "active",
        "completed_at": datetime.datetime(2024, 1, 3, 12, 0),
        "task_type": "work_unit",
        "score": 5,
        "items": [
            {
                "enrollment_task_id": "soft",
                "exposed_at": datetime.datetime(2024, 1, 4, 12, 0),
            },
        ],
    },
    {
        "_id": "pending-quiz-open",
        "planning_status": "pending",
        "completed_at": None,
        "task_type": "quiz",
        "score": 7,
        "items": [],
    },
    {
        "_id": "active-update-open",
        "planning_status": "active",
        "completed_at": None,
        "task_type": "content_update",
        "score": 6,
        "items": [{"enrollment_task_id": "soft", "exposed_at": None}],
    },
]


def _ids_for_find(collection: Any, filter_spec: dict[str, Any]) -> list[Any]:
    return [
        document["_id"] for document in collection.find(filter_spec, sort=[("_id", 1)])
    ]


def _ids_for_aggregate(
    collection: Any,
    pipeline: list[dict[str, Any]],
) -> list[Any]:
    return [
        document["_id"]
        for document in collection.aggregate(
            [*pipeline, {"$sort": {"_id": 1}}, {"$project": {"_id": 1}}],
        )
    ]


def _boolean_filter_matrix_action(
    collection: Any,
) -> dict[str, list[Any]]:
    or_with_siblings = {
        "$or": [
            {"planning_status": {"$exists": False}},
            {"planning_status": "active"},
        ],
        "completed_at": None,
        "task_type": {"$in": ["work_unit", "content_update"]},
    }
    expr_with_or_and_siblings = {
        "$expr": {
            "$or": [
                {"$eq": ["$task_type", "work_unit"]},
                {"$gt": ["$score", 6]},
            ],
        },
        "$or": [
            {"planning_status": "active"},
            {"planning_status": "pending"},
        ],
        "completed_at": None,
    }
    return {
        "find_or_with_siblings": _ids_for_find(collection, or_with_siblings),
        "find_nested_and_or": _ids_for_find(
            collection,
            {
                "$and": [
                    {"completed_at": None},
                    {
                        "$or": [
                            {"task_type": "work_unit"},
                            {"task_type": "content_update"},
                        ],
                    },
                    {
                        "$or": [
                            {"planning_status": "active"},
                            {"planning_status": {"$exists": False}},
                        ],
                    },
                ],
            },
        ),
        "find_nor_with_sibling": _ids_for_find(
            collection,
            {
                "$nor": [
                    {"planning_status": "retired"},
                    {"completed_at": {"$ne": None}},
                ],
                "task_type": {"$in": ["work_unit", "content_update"]},
            },
        ),
        "find_dotted_array_or": _ids_for_find(
            collection,
            {
                "items.exposed_at": {"$exists": True, "$ne": None},
                "$or": [
                    {"task_type": "work_unit"},
                    {"planning_status": "pending"},
                ],
            },
        ),
        "find_expr_with_or_and_siblings": _ids_for_find(
            collection,
            expr_with_or_and_siblings,
        ),
        "aggregate_match_or_with_siblings": _ids_for_aggregate(
            collection,
            [{"$match": or_with_siblings}],
        ),
        "aggregate_match_expr_with_or_and_siblings": _ids_for_aggregate(
            collection,
            [{"$match": expr_with_or_and_siblings}],
        ),
        "aggregate_match_dotted_array_or": _ids_for_aggregate(
            collection,
            [
                {
                    "$match": {
                        "items.exposed_at": {"$exists": True, "$ne": None},
                        "$or": [
                            {"task_type": "work_unit"},
                            {"planning_status": "pending"},
                        ],
                    },
                },
            ],
        ),
    }


def _aggregate_project_shapes(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {
                    "$project": {
                        "_id": 1,
                        "nestedValue": "$nested.value",
                        "arrayValues": "$items.value",
                        "missing": "$missing.value",
                        "searchHighlights": 1,
                        "__mongoeco_user_data": 1,
                    },
                },
                {"$sort": {"_id": 1}},
            ],
        ),
    )


def _aggregate_set_add_fields(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {"$set": {"derived": "$nested.value"}},
                {"$addFields": {"copied": "$derived"}},
                {"$project": {"_id": 1, "derived": 1, "copied": 1}},
                {"$sort": {"_id": 1}},
            ],
        ),
    )


def _aggregate_unset_shapes(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {"$unset": ["obsolete", "nested.remove"]},
                {"$sort": {"_id": 1}},
            ],
        ),
    )


def _aggregate_replace_root(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {"$replaceRoot": {"newRoot": "$nested"}},
                {"$sort": {"value": 1}},
            ],
        ),
    )


def _aggregate_replace_with(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {"$replaceWith": {"value": "$nested.value", "source": "$_id"}},
                {"$sort": {"source": 1}},
            ],
        ),
    )


def _aggregate_unwind_fanout(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {
                    "$unwind": {
                        "path": "$items",
                        "includeArrayIndex": "itemIndex",
                        "preserveNullAndEmptyArrays": True,
                    },
                },
                {
                    "$project": {
                        "_id": 1,
                        "itemIndex": 1,
                        "itemValue": "$items.value",
                    },
                },
                {"$sort": {"_id": 1, "itemIndex": 1}},
            ],
        ),
    )


def _aggregate_group_shapes(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {
                    "$group": {
                        "_id": "$kind",
                        "count": {"$sum": 1},
                        "values": {"$push": "$nested.value"},
                    },
                },
                {"$sort": {"_id": 1}},
            ],
        ),
    )


def _aggregate_facet_shapes(collection: Any) -> list[dict[str, Any]]:
    return list(
        collection.aggregate(
            [
                {
                    "$facet": {
                        "kept": [
                            {"$match": {"kind": "keep"}},
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ],
                        "summary": [
                            {"$group": {"_id": None, "count": {"$sum": 1}}},
                            {"$project": {"_id": 0, "count": 1}},
                        ],
                    },
                },
            ],
        ),
    )


def _seed_foreign_collection(collection: Any) -> Any:
    foreign = collection.database.get_collection("foreign")
    foreign.insert_many(
        [
            {"_id": "foreign-1", "kind": "keep", "label": "first"},
            {"_id": "foreign-2", "kind": "other", "label": "second"},
        ],
    )
    return foreign


def _aggregate_lookup_shapes(collection: Any) -> list[dict[str, Any]]:
    _seed_foreign_collection(collection)
    return list(
        collection.aggregate(
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": "kind",
                        "foreignField": "kind",
                        "as": "joined",
                    },
                },
                {"$project": {"_id": 1, "joined._id": 1, "joined.label": 1}},
                {"$sort": {"_id": 1}},
            ],
        ),
    )


def _aggregate_union_with_shapes(collection: Any) -> list[dict[str, Any]]:
    _seed_foreign_collection(collection)
    return list(
        collection.aggregate(
            [
                {"$project": {"_id": 1, "kind": 1}},
                {
                    "$unionWith": {
                        "coll": "foreign",
                        "pipeline": [{"$project": {"_id": 1, "kind": 1}}],
                    },
                },
                {"$sort": {"_id": 1}},
            ],
        ),
    )


def _aggregate_group_collection_references(collection: Any) -> dict[str, Any]:
    _seed_foreign_collection(collection)
    grouped = [{"$group": {"_id": "$kind"}}]
    lookup = {
        "$lookup": {
            "from": "foreign",
            "localField": "_id",
            "foreignField": "kind",
            "as": "profile.joined",
        }
    }
    correlated = {
        "$lookup": {
            "from": "foreign",
            "let": {"kind": "$_id"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$kind", "$$kind"]}}}],
            "as": "joined",
        }
    }
    return {
        "fields": list(
            collection.aggregate(
                [
                    *grouped,
                    lookup,
                    {"$project": {"_id": 1, "profile.joined._id": 1}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
        "pipeline": list(
            collection.aggregate(
                [
                    *grouped,
                    correlated,
                    {"$project": {"_id": 1, "joined._id": 1}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
        "union": list(
            collection.aggregate(
                [
                    *grouped,
                    {
                        "$unionWith": {
                            "coll": "foreign",
                            "pipeline": [{"$project": {"_id": "$kind"}}],
                        }
                    },
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    }


def _aggregate_semantic_collation(collection: Any) -> dict[str, Any]:
    collation = {"locale": "en", "strength": 2}
    result = {
        "group": list(
            collection.aggregate(
                [
                    {"$group": {"_id": "$key", "n": {"$sum": 1}}},
                    {"$project": {"_id": {"$toLower": "$_id"}, "n": 1}},
                    {"$sort": {"_id": 1}},
                ],
                collation=collation,
            )
        ),
        "numeric": list(
            collection.aggregate(
                [
                    {"$group": {"_id": "$value", "n": {"$sum": 1}}},
                    {"$project": {"_id": {"$toDouble": "$_id"}, "n": 1}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
        "expr": list(
            collection.aggregate(
                [
                    {"$match": {"$expr": {"$eq": ["$key", "ADA"]}}},
                    {"$project": {"_id": 1}},
                    {"$sort": {"_id": 1}},
                ],
                collation=collation,
            )
        ),
        "nested_expr": list(
            collection.aggregate(
                [
                    {"$match": {"$expr": {"$eq": [{"x": ["$key"]}, {"x": ["ADA"]}]}}},
                    {"$project": {"_id": 1}},
                    {"$sort": {"_id": 1}},
                ],
                collation=collation,
            )
        ),
        "window": list(
            collection.aggregate(
                [
                    {
                        "$setWindowFields": {
                            "partitionBy": "$value",
                            "output": {
                                "n": {
                                    "$sum": 1,
                                    "window": {"documents": ["unbounded", "unbounded"]},
                                }
                            },
                        }
                    },
                    {"$project": {"_id": 1, "n": 1}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    }

    result["rank"] = [
        [row["_id"], row["r"], row["d"]]
        for row in collection.aggregate(
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"key": 1},
                        "output": {"r": {"$rank": {}}, "d": {"$denseRank": {}}},
                    }
                },
                {"$sort": {"_id": 1}},
            ],
            collation=collation,
        )
    ]
    selected_names = ("lo", "hi", "lowN", "highN", "top", "bottom", "topN", "bottomN")
    selectors = {
        "_id": None,
        "lo": {"$min": "$mixed"},
        "hi": {"$max": "$mixed"},
        "lowN": {"$minN": {"input": "$mixed", "n": 1}},
        "highN": {"$maxN": {"input": "$mixed", "n": 1}},
        "top": {"$top": {"sortBy": {"mixed": 1}, "output": "$mixed"}},
        "bottom": {"$bottom": {"sortBy": {"mixed": 1}, "output": "$mixed"}},
        "topN": {"$topN": {"sortBy": {"mixed": 1}, "output": "$mixed", "n": 1}},
        "bottomN": {"$bottomN": {"sortBy": {"mixed": 1}, "output": "$mixed", "n": 1}},
    }
    result["selectors"] = [
        [row[name] for name in selected_names]
        for row in collection.aggregate(
            [
                {"$set": {"mixed": {"$cond": [{"$eq": ["$value", 2]}, "Z", "a"]}}},
                {"$group": selectors},
            ],
            collation=collation,
        )
    ]

    for name in ("group", "numeric", "window"):
        result[name] = [[row["_id"], row["n"]] for row in result[name]]
    return result


def _aggregate_pipeline_validation(collection: Any) -> list[str]:
    pipelines = [
        [{"$match": {"_id": "absent"}}, {"$group": {"_id": None}}, {"$lookup": {}}],
        [
            {
                "$lookup": {
                    "from": "foreign",
                    "let": {"Invalid": 1},
                    "pipeline": [],
                    "as": "e",
                }
            }
        ],
        [
            {
                "$lookup": {
                    "from": "foreign",
                    "let": {"bad\n": 1},
                    "pipeline": [],
                    "as": "e",
                }
            }
        ],
        [{"$match": {}}, {"$indexStats": {}}],
        [
            {
                "$lookup": {
                    "from": "foreign",
                    "localField": "kind",
                    "foreignField": "kind",
                    "as": "e",
                    "unexpected": 1,
                }
            }
        ],
        [{"$lookup": {"pipeline": [{"$documents": [{"x": 1}]}], "as": "e"}}],
        [{"$unionWith": {"pipeline": [{"$documents": [{"x": 1}]}]}}],
        [{"$unionWith": {"coll": "foreign", "pipeline": [{"$documents": []}]}}],
        [{"$facet": {"x": [{"$facet": {"y": []}}]}}],
        [{"$facet": {"x": [{"$lookup": {
            "from": "foreign", "pipeline": [{"$indexStats": {}}], "as": "e",
        }}]}}],
        [{"$facet": {"x": [{"$unionWith": {
            "coll": "foreign", "pipeline": [{"$indexStats": {}}],
        }}]}}],
        [{"$match": {"_id": "absent"}}, {"$facet": {"x": [{"$lookup": {
            "from": "foreign", "pipeline": [{"$unionWith": {
                "coll": "foreign", "pipeline": [{"$planCacheStats": {}}],
            }}], "as": "e",
        }}]}}],
        [{"$facet": {"x": [{"$lookup": {
            "pipeline": [{"$documents": [{"seed": 1}]}], "as": "e",
        }}]}}],
        [{"$facet": {"x": [{"$unionWith": {
            "pipeline": [{"$documents": [{"seed": 1}]}],
        }}]}}],
        [{"$facet": {"x": [{"$lookup": {
            "from": "foreign", "pipeline": [{"$match": {}}], "as": "e",
        }}]}}],
    ]
    return [
        _write_error_flag(
            lambda pipeline=pipeline: list(collection.aggregate(pipeline))
        )
        for pipeline in pipelines
    ]


def _aggregate_foreign_stats(collection: Any) -> dict[str, Any]:
    foreign = _seed_foreign_collection(collection)
    foreign.create_index("kind", name="foreign_only")

    def run(stage):
        return list(
            collection.aggregate(
                [
                    {"$limit": 1},
                    {
                        "$lookup": {
                            "from": "foreign",
                            "pipeline": [
                                stage,
                                {"$project": {"_id": 0, "name": 1, "count": 1}},
                                {"$sort": {"name": 1}},
                            ],
                            "as": "stats",
                        }
                    },
                    {"$project": {"_id": 0, "stats": 1}},
                ]
            )
        )

    return {
        "indexes": run({"$indexStats": {}}),
        "count": run({"$collStats": {"count": {}}}),
    }


def _aggregate_merge_writeback(collection: Any) -> list[dict[str, Any]]:
    list(
        collection.aggregate(
            [
                {"$match": {"kind": "keep"}},
                {
                    "$project": {
                        "_id": 1,
                        "kind": 1,
                        "nested": 1,
                        "searchHighlights": 1,
                        "__mongoeco_user_data": 1,
                    },
                },
                {"$merge": {"into": "archive"}},
            ],
        ),
    )
    return list(collection.database.archive.find({}, sort=[("_id", 1)]))


_AGGREGATION_STAGE_DOCUMENTS = [
    {
        "_id": "local-1",
        "kind": "keep",
        "nested": {"value": 1, "remove": "legacy"},
        "items": [{"value": "a"}, {"value": "b"}],
        "obsolete": True,
        "searchHighlights": {"caller": 1},
        "__mongoeco_user_data": {"kept": True},
    },
    {
        "_id": "local-2",
        "kind": "drop",
        "nested": {"value": 2, "remove": "legacy"},
        "items": [],
        "obsolete": True,
        "searchHighlights": {"caller": 2},
        "__mongoeco_user_data": {"kept": True},
    },
    {
        "_id": "local-3",
        "kind": "keep",
        "nested": {"value": 3, "remove": "legacy"},
        "obsolete": True,
        "searchHighlights": {"caller": 3},
        "__mongoeco_user_data": {"kept": True},
    },
]


REAL_PARITY_CASES: tuple[RealParityCase, ...] = (
    RealParityCase(
        name="boolean_filter_matrix",
        seed_documents=_BOOLEAN_FILTER_MATRIX_DOCUMENTS,
        action=_boolean_filter_matrix_action,
    ),
    RealParityCase(
        name="find_expr_compare_fields",
        seed_documents=[
            {"_id": "1", "spent": 12, "budget": 10},
            {"_id": "2", "spent": 8, "budget": 10},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {"$expr": {"$gt": ["$spent", "$budget"]}},
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="find_subdocument_order_sensitive_equality",
        seed_documents=[
            {"_id": "ordered", "value": {"a": 1, "b": 2}},
            {"_id": "reordered", "value": {"b": 2, "a": 1}},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {"value": {"a": 1, "b": 2}},
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="find_all_with_multiple_elem_match",
        seed_documents=[
            {
                "_id": "1",
                "items": [
                    {"kind": "a", "qty": 1},
                    {"kind": "b", "qty": 2},
                    {"kind": "b", "qty": 5},
                ],
            },
            {
                "_id": "2",
                "items": [{"kind": "a", "qty": 1}, {"kind": "b", "qty": 2}],
            },
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {
                    "items": {
                        "$all": [
                            {"$elemMatch": {"kind": "a"}},
                            {"$elemMatch": {"kind": "b", "qty": {"$gte": 5}}},
                        ],
                    },
                },
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="find_implicit_regex_literal",
        seed_documents=[
            {"_id": "1", "name": "MongoDB"},
            {"_id": "2", "name": "Postgres"},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {"name": re.compile("^mongo", re.IGNORECASE)},
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="find_in_with_regex_literals",
        seed_documents=[
            {"_id": "1", "tags": ["beta", "stable"]},
            {"_id": "2", "tags": ["alpha", "stable"]},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {"tags": {"$in": [re.compile("^be"), re.compile("^zz")]}},
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="update_add_to_set_document_order_sensitive",
        seed_documents=[{"_id": "1", "items": [{"kind": "a", "qty": 1}]}],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                collection.find_one({"_id": "1"}),
            )
        )(
            collection.update_one(
                {"_id": "1"},
                {"$addToSet": {"items": {"qty": 1, "kind": "a"}}},
            ),
        ),
    ),
    RealParityCase(
        name="update_set_on_insert_id_matches_filter",
        seed_documents=[],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                result.upserted_id,
                collection.find_one({"_id": "seed"}),
            )
        )(
            collection.update_one(
                {"_id": "seed"},
                {"$setOnInsert": {"_id": "seed", "state": "new"}},
                upsert=True,
            ),
        ),
    ),
    RealParityCase(
        name="update_pipeline_project_preserves_id",
        seed_documents=[{"_id": "1", "name": "Ada", "legacy": True}],
        action=lambda collection: (
            lambda result: (
                result.matched_count,
                result.modified_count,
                collection.find_one({"_id": "1"}),
            )
        )(
            collection.update_one(
                {"_id": "1"},
                [{"$project": {"_id": 0, "name": 1}}],
            ),
        ),
    ),
    RealParityCase(
        name="update_pipeline_rejects_id_change",
        seed_documents=[{"_id": "1", "name": "Ada"}],
        action=lambda collection: _write_error_code(
            lambda: collection.update_one(
                {"_id": "1"},
                [{"$set": {"_id": "2"}}],
            ),
        ),
    ),
    RealParityCase(
        name="insert_rejects_root_array_id",
        seed_documents=[],
        action=lambda collection: _write_error_flag(
            lambda: collection.insert_one({"_id": [1]}),
        ),
    ),
    RealParityCase(
        name="find_expr_truthiness_array",
        seed_documents=[
            {"_id": "array", "flag": []},
            {"_id": "zero", "flag": 0},
            {"_id": "false", "flag": False},
            {"_id": "string", "flag": ""},
        ],
        action=lambda collection: [
            document["_id"]
            for document in collection.find(
                {"$expr": "$flag"},
                sort=[("_id", 1)],
            )
        ],
    ),
    RealParityCase(
        name="aggregate_project_array_traversal",
        seed_documents=[
            {"_id": "1", "items": [{"kind": "a"}, {"kind": "b"}]},
            {"_id": "2", "items": [{"kind": "c"}]},
        ],
        action=lambda collection: list(
            collection.aggregate(
                [
                    {"$project": {"_id": 1, "kinds": "$items.kind"}},
                    {"$sort": {"_id": 1}},
                ],
            ),
        ),
    ),
    RealParityCase(
        name="aggregate_get_field_literal_name",
        seed_documents=[{"_id": "1", "a.b.c": 1, "$price": 2, "x..y": 3}],
        action=lambda collection: list(
            collection.aggregate(
                [
                    {
                        "$project": {
                            "_id": 0,
                            "dotted": {
                                "$getField": {
                                    "field": "a.b.c",
                                    "input": "$$CURRENT",
                                },
                            },
                            "dollar": {
                                "$getField": {
                                    "field": {"$literal": "$price"},
                                    "input": "$$CURRENT",
                                },
                            },
                            "double_dot": {
                                "$getField": {
                                    "field": "x..y",
                                    "input": "$$CURRENT",
                                },
                            },
                        },
                    },
                ],
            ),
        ),
    ),
    RealParityCase(
        name="aggregate_project_shapes",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_project_shapes,
    ),
    RealParityCase(
        name="aggregate_set_add_fields",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_set_add_fields,
    ),
    RealParityCase(
        name="aggregate_unset_shapes",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_unset_shapes,
    ),
    RealParityCase(
        name="aggregate_replace_root",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_replace_root,
    ),
    RealParityCase(
        name="aggregate_replace_with",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_replace_with,
    ),
    RealParityCase(
        name="aggregate_unwind_fanout",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_unwind_fanout,
    ),
    RealParityCase(
        name="aggregate_group_shapes",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_group_shapes,
    ),
    RealParityCase(
        name="aggregate_facet_shapes",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_facet_shapes,
    ),
    RealParityCase(
        name="aggregate_lookup_shapes",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_lookup_shapes,
    ),
    RealParityCase(
        name="aggregate_union_with_shapes",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_union_with_shapes,
    ),
    RealParityCase(
        name="aggregate_group_collection_references",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_group_collection_references,
    ),
    RealParityCase(
        name="aggregate_semantic_collation",
        seed_documents=[
            {"_id": "a", "key": "Ada", "value": 1},
            {"_id": "b", "key": "other", "value": 2},
            {"_id": "c", "key": "ada", "value": 1.0},
        ],
        action=_aggregate_semantic_collation,
    ),
    RealParityCase(
        name="aggregate_pipeline_validation",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_pipeline_validation,
    ),
    RealParityCase(
        name="aggregate_foreign_stats",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_foreign_stats,
    ),
    RealParityCase(
        name="aggregate_merge_writeback",
        seed_documents=_AGGREGATION_STAGE_DOCUMENTS,
        action=_aggregate_merge_writeback,
    ),
)


# Keep this boundary explicit whenever executable cases precede a real-server
# replay capture. Names may leave it only with their checked-in golden.
REAL_CAPTURE_PENDING_CASES: frozenset[str] = frozenset()


def get_real_parity_case(name: str) -> RealParityCase:
    for case in REAL_PARITY_CASES:
        if case.name == name:
            return case
    raise KeyError(name)
