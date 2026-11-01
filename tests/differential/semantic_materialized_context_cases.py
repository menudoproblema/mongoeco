"""Materialized prefixes preserve source windows, grammar and bound context."""

from copy import deepcopy

from bson import Code, DBRef

from tests.differential.semantic_bson_grammar_context_cases import _aggregate


# Keep manifest seed neutral: Extended JSON may reconstruct DBRef shapes.
# Ordered witness documents are installed inside each action, before execution.
SEED = [{"_id": "initial"}]
DOCUMENTS = [
    {"_id": 1, "tag": "A", "v": 5, "ref": {"$ref": "c", "$id": 1}},
    {"_id": 2, "tag": "a", "v": 7, "ref": {"$id": 1, "$ref": "c"}},
    {"_id": 3, "tag": "A", "v": 9, "ref": {"$ref": "c", "$id": 1, "tenant": "t"}},
    {"_id": 4, "tag": "B", "v": 11, "ref": {"$ref": "c", "$id": 1}},
    {"_id": 5, "tag": "A", "v": 13, "ref": {"$ref": 1, "$id": 1}},
]
HEADS = ("all", "match", "window", "project")
REFERENCES = {
    "ordered": {"$ref": "c", "$id": 1},
    "reversed": {"$id": 1, "$ref": "c"},
    "extras": {"$ref": "c", "$id": 1, "tenant": "t"},
    "wrong-ref": {"$ref": 1, "$id": 1},
    "sdk": DBRef("c", 1),
}


def _head(name):
    stages = [] if name == "all" else [{"$match": {"tag": "A"}}]
    stages.append({"$sort": {"_id": 1}})
    if name in {"window", "project"}:
        stages.extend([{"$skip": 1}, {"$limit": 2}])
    if name == "project":
        stages.append({"$project": {"_id": 1, "tag": 1, "v": 1, "ref": 1, "code": 1}})
    return stages


def _reset(collection, *, rows):
    collection.delete_many({})
    if rows:
        collection.insert_many(deepcopy(DOCUMENTS))


def materialized_source_windows(collection):
    output = {}
    for populated in (False, True):
        _reset(collection, rows=populated)
        for head in HEADS:
            for collation in ({"locale": "simple"}, {"locale": "en", "strength": 2}):
                prefix = [
                    *_head(head),
                    {
                        "$facet": {
                            "rows": [
                                {
                                    "$project": {
                                        "_id": 1,
                                        "v": 1,
                                        "clock": "$$NOW",
                                        "echo": "$$arg",
                                    }
                                }
                            ]
                        }
                    },
                ]
                unfold = [{"$unwind": "$rows"}, {"$replaceWith": "$rows"}]
                transform = {"$set": {"sameClock": {"$eq": ["$clock", "$$NOW"]}}}
                for window, suffix in {
                    "skip-before": [
                        *unfold,
                        {"$skip": 1},
                        transform,
                        {"$project": {"clock": 0}},
                    ],
                    "limit-before": [
                        *unfold,
                        {"$limit": 2},
                        transform,
                        {"$project": {"clock": 0}},
                    ],
                    "trailing": [
                        *unfold,
                        transform,
                        {"$project": {"clock": 0}},
                        {"$skip": 1},
                        {"$limit": 2},
                    ],
                }.items():
                    key = f"{populated}/{head}/{collation['locale']}/{window}"
                    output[key] = _aggregate(
                        collection,
                        [*prefix, *suffix],
                        collation=collation,
                        let={"arg": {"$literal": "bound"}},
                    )
    return output


def materialized_grammar_contexts(collection):
    output = {}
    for populated in (False, True):
        _reset(collection, rows=False)
        if populated:
            documents = deepcopy(DOCUMENTS)
            for index, document in enumerate(documents):
                document["code"] = (
                    Code("$v") if index % 2 else Code("$$ghost", {"z": 1, "a": 2})
                )
            collection.insert_many(documents)
        for head in HEADS:
            for label, reference in REFERENCES.items():
                for collation in (
                    {"locale": "simple"},
                    {"locale": "en", "strength": 2},
                ):
                    pipeline = [
                        *_head(head),
                        {
                            "$facet": {
                                "rows": [
                                    {"$match": {"ref": reference}},
                                    {
                                        "$project": {
                                            "_id": 1,
                                            "v": 1,
                                            "stored": "$code",
                                            "literal": Code("$$ghost", {}),
                                            "echo": "$$arg",
                                            "clock": "$$NOW",
                                        }
                                    },
                                ],
                                "all": [
                                    {"$group": {"_id": None, "ids": {"$push": "$_id"}}}
                                ],
                            }
                        },
                        {"$set": {"clock": "$$NOW"}},
                        {
                            "$project": {
                                "_id": 0,
                                "ids": {"$arrayElemAt": ["$all.ids", 0]},
                                "rows": {
                                    "$map": {
                                        "input": "$rows",
                                        "as": "r",
                                        "in": {
                                            "id": "$$r._id",
                                            "v": "$$r.v",
                                            "stored": "$$r.stored",
                                            "literal": "$$r.literal",
                                            "echo": "$$r.echo",
                                            "sameClock": {
                                                "$eq": ["$$r.clock", "$clock"]
                                            },
                                        },
                                    }
                                },
                            }
                        },
                    ]
                    key = f"{populated}/{head}/{label}/{collation['locale']}"
                    output[key] = _aggregate(
                        collection,
                        pipeline,
                        collation=collation,
                        let={"arg": {"$literal": Code("$cmd", {})}},
                    )
    return output


SEMANTIC_MATERIALIZED_CONTEXT_CASES = (
    ("semantic_materialized_source_windows", materialized_source_windows),
    ("semantic_materialized_grammar_contexts", materialized_grammar_contexts),
)
