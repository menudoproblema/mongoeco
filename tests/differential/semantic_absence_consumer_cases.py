"""Absence consumers: terminal fallbacks, reductions, replacement and writes."""

from functools import partial

from bson import Decimal128, ObjectId
from pymongo import ReturnDocument, UpdateOne


SEED = [{"_id": 1, "keep": 7}, {"_id": 2, "keep": 7}]
REDUCE_MISSING = {"$reduce": {"input": [{}], "initialValue": 0, "in": "$$this.absent"}}
IFNULL_MISSING = {"$ifNull": ["$absent", "$also_absent"]}


def project(collection, expression):
    return list(
        collection.aggregate(
            [
                {"$project": {"value": expression}},
                {"$sort": {"_id": 1}},
            ]
        )
    )


def replacement(collection, stage):
    value = {
        "kept": "$_id",
        "absent": "$absent",
        "remove": "$$REMOVE",
        "items": [{"$getField": "absent"}, "$$REMOVE"],
    }
    spec = {"newRoot": value} if stage == "$replaceRoot" else value
    return list(collection.aggregate([{stage: spec}, {"$sort": {"kept": 1}}]))


def write(collection, method, expression):
    pipeline = [{"$set": {"keep": expression}}]
    query = {"_id": 1}
    if method.startswith("bulk_"):
        result = collection.bulk_write(
            [
                UpdateOne({"_id": 1}, pipeline),
                UpdateOne({"_id": 2}, pipeline),
            ],
            ordered=method == "bulk_ordered",
        )
        counters = {"matched": result.matched_count, "modified": result.modified_count}
    elif method == "command":
        result = collection.database.command(
            {
                "update": collection.name,
                "updates": [{"q": query, "u": pipeline}],
            }
        )
        counters = {"matched": result["n"], "modified": result["nModified"]}
    elif method == "find_one_and_update":
        counters = {
            "returned": collection.find_one_and_update(
                query,
                pipeline,
                return_document=ReturnDocument.AFTER,
            )
        }
    else:
        result = getattr(collection, method)(
            query if method == "update_one" else {}, pipeline
        )
        counters = {"matched": result.matched_count, "modified": result.modified_count}
    return {
        "result": counters,
        "documents": list(collection.aggregate([{"$sort": {"_id": 1}}])),
    }


def lookup_scope(collection):
    pipeline = [
        {
            "$project": {
                "_id": 0,
                "foreignRoot": "$$ROOT._id",
                "implicit": "$_id",
                "outerType": {"$type": "$$missing"},
                "nullType": {"$type": "$$n"},
                "nullPathType": {"$type": "$$n.field"},
                "removePathType": {"$type": "$$REMOVE.path"},
                "same": {"$eq": ["$absent", "$$missing"]},
                "equalsNull": {"$eq": [None, "$$missing"]},
            }
        },
        {"$sort": {"foreignRoot": 1}},
    ]
    return list(
        collection.aggregate(
            [
                {"$match": {"_id": 1}},
                {
                    "$lookup": {
                        "from": collection.name,
                        "let": {"missing": "$absent", "n": None},
                        "pipeline": pipeline,
                        "as": "rows",
                    }
                },
                {"$project": {"_id": 0, "rows": 1}},
            ]
        )
    )


def subpipeline_groups(collection):
    group = {
        "$group": {
            "_id": None,
            "value": {"$sum": True},
            "decimal": {"$first": {"$literal": Decimal128("1.25")}},
            "oid": {"$first": {"$literal": ObjectId("000000000000000000000001")}},
        }
    }
    return {
        "lookup": list(
            collection.aggregate(
                [
                    {"$match": {"_id": 1}},
                    {
                        "$lookup": {
                            "from": collection.name,
                            "pipeline": [group],
                            "as": "rows",
                        }
                    },
                    {"$project": {"_id": 0, "rows": 1}},
                ]
            )
        ),
        "union": list(
            collection.aggregate(
                [
                    {"$match": {"_id": -1}},
                    {"$unionWith": {"coll": collection.name, "pipeline": [group]}},
                ]
            )
        ),
    }


SEMANTIC_ABSENCE_CONSUMER_CASES = (
    ("semantic_absence_lookup_scope", lookup_scope),
    ("semantic_absence_subpipeline_groups", subpipeline_groups),
    (
        "semantic_absence_reduce",
        partial(
            project,
            expression={
                "value": REDUCE_MISSING,
                "type": {"$type": REDUCE_MISSING},
                "initial": {
                    "$reduce": {
                        "input": [{}],
                        "initialValue": "$absent",
                        "in": {"$type": "$$value"},
                    }
                },
                "empty": {"$reduce": {"input": [], "initialValue": "$absent", "in": 0}},
            },
        ),
    ),
    (
        "semantic_absence_ifnull",
        partial(
            project,
            expression={
                "value": IFNULL_MISSING,
                "type": {"$type": IFNULL_MISSING},
                "null": {"$ifNull": ["$absent", None]},
                "zero": {"$ifNull": [0, "$absent"]},
                "false": {"$ifNull": [False, "$absent"]},
            },
        ),
    ),
    *(
        (f"semantic_absence_replace_{stage[1:]}", partial(replacement, stage=stage))
        for stage in ("$replaceRoot", "$replaceWith")
    ),
    *(
        (
            f"semantic_absence_write_{method}_{kind}",
            partial(write, method=method, expression=value),
        )
        for method in (
            "update_one",
            "update_many",
            "find_one_and_update",
            "bulk_ordered",
            "bulk_unordered",
            "command",
        )
        for kind, value in (
            ("field", "$absent"),
            ("fallback", IFNULL_MISSING),
            ("array", [{"$getField": "absent"}, "$$REMOVE"]),
            ("object", {"absent": "$absent", "remove": "$$REMOVE"}),
        )
    ),
)
