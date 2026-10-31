"""Native witnesses for absence, expression composition and lexical scope."""

SEED = [
    {"_id": 1, "a": None, "arr": [1], "keep": 7},
    {"_id": 2, "arr": [], "keep": 7},
    {"_id": 3, "a": 5, "keep": 7},
    {"_id": 4, "a": [None], "keep": 7},
]
MISSING_FIELD = {"$getField": "absent"}


def _project(expression):
    def action(collection):
        return list(
            collection.aggregate(
                [
                    {"$project": {"value": expression}},
                    {"$sort": {"_id": 1}},
                ]
            )
        )

    return action


EXPRESSIONS = {
    "compare": {
        **{
            op[1:]: {op: ["$a", None]}
            for op in ("$eq", "$ne", "$in", "$cmp", "$gte", "$lt")
            if op != "$in"
        },
        "in": {"$in": ["$a", [None]]},
        "absent_equal": {"$eq": [MISSING_FIELD, "$absent"]},
        "getfield_cmp": {"$cmp": [MISSING_FIELD, None]},
    },
    "references": {
        "field": {"$type": "$absent"},
        "root": {"$type": "$$ROOT.absent"},
        "current": {"$type": "$$CURRENT.absent"},
        "lexical": {"$let": {"vars": {"x": "$absent"}, "in": {"$type": "$$x"}}},
        "getfield": {"$type": {"$getField": {"field": "x", "input": "$absent"}}},
    },
    "producers": {
        "index": {"$type": {"$arrayElemAt": ["$arr", 999]}},
        "first": {"$type": {"$first": {"$literal": []}}},
    },
    "composition": {
        "object": {"k": "$absent"},
        "cond": {"$cond": [True, "$absent", 9]},
        "switch": {
            "$switch": {"branches": [{"case": True, "then": "$absent"}], "default": 9}
        },
        "map": {"$map": {"input": [{}], "as": "item", "in": {"k": "$$item.x"}}},
        "merge": {"$mergeObjects": [{"k": 1}, {"k": "$absent"}]},
        "setfield": {
            "$setField": {
                "field": "k",
                "input": {"$literal": {"k": 1}},
                "value": "$absent",
            }
        },
    },
    "arrays": [MISSING_FIELD, "$$REMOVE", "$absent"],
    "remove_object": {"k": "$$REMOVE"},
    "truth": {
        "cond": {"$cond": [MISSING_FIELD, "yes", "no"]},
        "and": {"$and": [MISSING_FIELD, True]},
        "or": {"$or": [MISSING_FIELD, False]},
        "filter": {
            "$filter": {
                "input": [{}, {}],
                "as": "x",
                "cond": {"$getField": {"field": "a", "input": "$$x"}},
            }
        },
    },
    "current_scope": {
        "$let": {
            "vars": {"CURRENT": {"n": 8}},
            "in": {
                "implicit": "$n",
                "explicit": "$$CURRENT.n",
                "root": "$$ROOT._id",
            },
        }
    },
}

SEMANTIC_MISSING_CASES = tuple(
    (f"semantic_missing_{name}", _project(expression))
    for name, expression in EXPRESSIONS.items()
)
