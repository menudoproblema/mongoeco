"""DBRef literal grammar and raw document order through public APIs."""

from bson import DBRef


SEED = [{"_id": "initial"}]
VALUES = {
    "ordered": {"$ref": "c", "$id": 1},
    "reversed": {"$id": 1, "$ref": "c"},
    "extras": {"$ref": "c", "$id": 1, "tenant": "t"},
    "database": {"$ref": "c", "$id": 1, "$db": "d"},
    "wrong_ref": {"$ref": 1, "$id": 1},
    "ref_only": {"$ref": "c"},
    "id_only": {"$id": 1},
    "ordinary": {"x": 1, "$unknown": 2},
    "operator": {"$unknown": 2, "x": 1},
}


def _reset(collection, *, populated):
    collection.delete_many({})
    if populated:
        collection.insert_many(
            [{"_id": name, "v": value, "a": [value]} for name, value in VALUES.items()]
            + [{"_id": "sdk", "v": DBRef("c", 1), "a": [DBRef("c", 1)]}]
        )


def _observe(collection, query):
    phase = "open"
    try:
        cursor = collection.aggregate(
            [{"$match": query}, {"$project": {"_id": 1}}, {"$sort": {"_id": 1}}]
        )
        phase = "iterate"
        return {"ok": True, "ids": [row["_id"] for row in cursor]}
    except Exception as exc:
        return {
            "ok": False,
            "exception": type(exc).__name__,
            "code": getattr(exc, "code", None),
            "phase": phase,
        }


def field_literals(collection):
    output = {}
    for populated in (False, True):
        _reset(collection, populated=populated)
        for name, value in {**VALUES, "sdk": DBRef("c", 1)}.items():
            for kind, operand in (("implicit", value), ("eq", {"$eq": value})):
                output[f"{populated}-{name}-{kind}"] = _observe(
                    collection, {"v": operand}
                )
    return output


def membership(collection):
    _reset(collection, populated=True)
    return {
        f"{name}-{operator}": _observe(collection, {"v": {operator: [value]}})
        for name, value in {**VALUES, "sdk": DBRef("c", 1)}.items()
        for operator in ("$in", "$nin")
    }


def nested_grammar(collection):
    output = {}
    for populated in (False, True):
        _reset(collection, populated=populated)
        for name, value in VALUES.items():
            output[f"{populated}-{name}-elem"] = _observe(
                collection, {"a": {"$elemMatch": value}}
            )
            output[f"{populated}-{name}-not"] = _observe(
                collection, {"v": {"$not": value}}
            )
        output[f"{populated}-top"] = _observe(collection, {"$ref": "c", "$id": 1})
    return output


def collation_contexts(collection):
    collection.delete_many({})
    values = [
        {"$ref": "c", "$id": "abc", "$db": "d"},
        {"$ref": "C", "$id": "abc", "$db": "d"},
        {"$ref": "c", "$id": "ABC", "$db": "d"},
        {"$ref": "c", "$id": "abc", "$db": "D"},
        DBRef("c", "abc", "d"),
    ]
    collection.insert_many(
        [{"_id": i, "v": value, "a": [value]} for i, value in enumerate(values, 1)]
    )
    queries = {
        "literal": {"v": {"$ref": "C", "$id": "ABC", "$db": "D"}},
        "sdk": {"v": DBRef("C", "ABC", "D")},
        "elem-ref": {"a": {"$elemMatch": {"$ref": "C"}}},
        "elem-id": {"a": {"$elemMatch": {"$id": "ABC"}}},
        "elem-db": {"a": {"$elemMatch": {"$db": "D"}}},
    }
    return {
        name: [
            row["_id"]
            for row in collection.aggregate(
                [{"$match": query}, {"$sort": {"_id": 1}}],
                collation={"locale": "en", "strength": 2},
            )
        ]
        for name, query in queries.items()
    }


def mutation_literals(collection):
    output = {}
    for label, value in (
        ("ordered", VALUES["ordered"]),
        ("reversed", VALUES["reversed"]),
        ("ordinary", VALUES["ordinary"]),
        ("sdk", DBRef("c", 1)),
    ):
        collection.delete_many({})
        result = collection.update_one(
            {"_id": 1, "v": value}, {"$set": {"done": True}}, upsert=True
        )
        output["upsert-" + label] = {
            "counts": [result.matched_count, result.modified_count],
            "matches": collection.count_documents({"v": {"$eq": value}}),
        }
        collection.delete_many({})
        collection.insert_one(
            {
                "_id": 1,
                "a": [
                    {"$ref": "c", "$id": 1, "tag": "ordered"},
                    {"$id": 1, "$ref": "c", "tag": "reversed"},
                    {"$ref": "c", "$id": 2, "tag": "other"},
                ],
            }
        )
        try:
            result = collection.update_one({"_id": 1}, {"$pull": {"a": value}})
            output["pull-" + label] = {"modified": result.modified_count}
        except Exception as exc:
            output["pull-" + label] = {"code": getattr(exc, "code", None)}
        output["pull-" + label]["remaining"] = len(collection.find_one({"_id": 1})["a"])
    return output


SEMANTIC_DBREF_GRAMMAR_CASES = (
    ("semantic_dbref_field_literals", field_literals),
    ("semantic_dbref_membership", membership),
    ("semantic_dbref_nested_grammar", nested_grammar),
    ("semantic_dbref_collation_contexts", collation_contexts),
    ("semantic_dbref_mutation_literals", mutation_literals),
)
