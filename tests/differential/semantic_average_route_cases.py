"""Native average finalization through pipeline shapes and expression contexts."""

from bson import Int64


SEED = [
    {"_id": i, "g": "g", "v": value, "items": [1]}
    for i, value in enumerate([Int64(2**63 - 1), 1, Int64(-(2**63 - 1))])
]
GROUP = {"$group": {"_id": "$g", "s": {"$sum": "$v"}, "a": {"$avg": "$v"}}}
PREFIXES = {
    "root": [],
    "match": [{"$match": {"g": "g"}}],
    "sort": [{"$sort": {"_id": 1}}],
    "project": [{"$project": {"v": 1, "g": 1}}],
    "computed_project": [{"$project": {"g": 1, "v": {"$add": ["$v", 0]}}}],
    "exclusion_project": [{"$project": {"items": 0}}],
    "dotted_project": [{"$project": {"g": 1, "v": 1, "foo.bar": 1}}],
    "set": [{"$set": {"x": 1}}],
    "addFields": [{"$addFields": {"x": 1}}],
    "unset": [{"$unset": "x"}],
    "replaceRoot": [{"$replaceRoot": {"newRoot": "$$ROOT"}}],
    "replaceWith": [{"$replaceWith": "$$ROOT"}],
    "limit": [{"$limit": 10}],
    "skip": [{"$skip": 0}],
    "unwind": [{"$unwind": "$items"}],
    "lookup": [
        {
            "$lookup": {
                "from": "average_route_empty",
                "localField": "g",
                "foreignField": "g",
                "as": "x",
            }
        }
    ],
    "lookup_pipeline": [
        {
            "$lookup": {
                "from": "average_route_empty",
                "pipeline": [],
                "as": "x",
            }
        }
    ],
    "union": [{"$unionWith": "average_route_empty"}],
    "window": [
        {
            "$setWindowFields": {
                "sortBy": {"_id": 1},
                "output": {"p": {"$sum": "$v"}},
            }
        }
    ],
    "group": [
        {"$group": {"_id": "$_id", "g": {"$first": "$g"}, "v": {"$first": "$v"}}},
        {"$sort": {"_id": 1}},
    ],
    "match_expr": [{"$match": {"$expr": {"$in": ["$g", ["g"]]}}}],
}
EXPRESSIONS = {
    "direct": "$v",
    "add": {"$add": ["$v", 0]},
    "ifNull": {"$ifNull": ["$v", 0]},
    "let": {"$let": {"vars": {"x": "$v"}, "in": "$$x"}},
    "getField": {"$getField": "v"},
    "arrayElemAt": {"$arrayElemAt": [["$v"], 0]},
    "toLong": {"$toLong": "$v"},
}


def pipelines(collection_name):
    result = {f"prefix_{name}": [*prefix, GROUP] for name, prefix in PREFIXES.items()}
    result.update(
        {
            f"expression_{name}": [{"$group": {"_id": "$g", "a": {"$avg": expression}}}]
            for name, expression in EXPRESSIONS.items()
        }
    )
    result["facet"] = [{"$facet": {"groups": [GROUP]}}]
    result["lookup_nested"] = [
        {"$lookup": {"from": collection_name, "pipeline": [GROUP], "as": "groups"}},
        {"$project": {"_id": 1, "groups": 1}},
        {"$sort": {"_id": 1}},
    ]
    result["union_nested"] = [
        {"$match": {"_id": "absent"}},
        {"$unionWith": {"coll": collection_name, "pipeline": [GROUP]}},
    ]
    return result


def average_routes(collection):
    return {
        name: list(collection.aggregate(pipeline))
        for name, pipeline in pipelines(collection.name).items()
    }


SEMANTIC_AVERAGE_ROUTE_CASES = (("semantic_average_routes", average_routes),)
