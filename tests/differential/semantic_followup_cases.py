"""Bounded F00-F10 witnesses through the existing native capture/replay."""

import datetime

from copy import deepcopy
from functools import partial

from bson import BSON, Binary, Decimal128, Int64, ObjectId, Regex, Timestamp
from pymongo import IndexModel, InsertOne, ReplaceOne, UpdateOne

from tests.differential.semantic_bson_grammar_context_cases import _aggregate
from tests.differential.semantic_update_event_cases import _drain, _single
from tests.support_write_parity import BSONObservation, public_error


SEED = [{"_id": 0}]


def reset(collection, rows):
    collection.delete_many({})
    if rows:
        collection.insert_many(deepcopy(rows))


def outcome(invoke, *, write=False):
    """Observe public error identity; wording remains in existing strict corpora."""
    try:
        return {"ok": True, "value": invoke()}
    except Exception as error:
        if write:
            # Local subclasses retain the same public WriteError contract.
            observation = public_error(error, BSONObservation([]))
            return {
                "ok": False,
                "family": observation["family"],
                "code": observation["code"],
            }
        return {
            "ok": False,
            "family": type(error).__name__,
            "code": getattr(error, "code", None),
        }


def expression(collection, *, value, rows=None):
    result = {}
    for state, documents in (
        ("empty", []),
        ("rows", rows if rows is not None else [{"_id": 1}]),
    ):
        reset(collection, documents)
        result[state] = _aggregate(collection, [{"$project": {"_id": 0, "r": value}}])
    return result


def pipeline(collection, *, stages, rows):
    result = {}
    for state, documents in (("empty", []), ("rows", rows)):
        reset(collection, documents)
        result[state] = _aggregate(collection, stages)
    return result


def query(collection, *, spec, rows=None):
    reset(
        collection,
        rows
        if rows is not None
        else [
            {"_id": 1, "a": Regex("x")},
            {"_id": 2, "a": "x"},
            {"_id": 3, "a": [1]},
            {"_id": 4, "a": [[1]]},
            {"_id": 5, "a": [{"b": 1}]},
            {"_id": 6, "a": [{"c": 1}]},
            {"_id": 7, "a": []},
            {"_id": 8},
        ],
    )

    def run():
        ids = [r["_id"] for r in collection.find(spec, sort=[("_id", 1)])]
        count = collection.count_documents(spec)
        values = sorted(collection.distinct("_id", spec))
        matched = collection.update_many(spec, {"$set": {"done": True}}).matched_count
        deleted = collection.delete_many(spec).deleted_count
        return {
            "ids": ids,
            "count": count,
            "distinct": values,
            "matched": matched,
            "deleted": deleted,
        }

    return outcome(run)


def regex_values(collection):
    rows = [
        {"_id": 1, "a": Regex("x", "i")},
        {"_id": 2, "a": Regex("x")},
        {"_id": 3, "a": "X"},
        {"_id": 4, "a": [Regex("x", "i"), "X"]},
        {"_id": 5, "a": [Regex("other")]},
        {"_id": 6, "a": None},
        {"_id": 7},
    ]
    return {
        name: query(collection, spec=spec, rows=rows)
        for name, spec in {
            "eq": {"a": {"$eq": Regex("x", "i")}},
            "implicit": {"a": Regex("x", "i")},
            "in": {"a": {"$in": [Regex("x", "i")]}},
            "not": {"a": {"$not": Regex("x", "i")}},
            "regex": {"a": {"$regex": "x", "$options": "i"}},
        }.items()
    }


def distinct_arrays(collection):
    reset(
        collection,
        [
            {"_id": 1, "a": [{"b": 1}, {"b": [2, 3]}]},
            {"_id": 2, "a": {"b": [4]}},
            {"_id": 3},
        ],
    )
    return sorted(collection.distinct("a.b"), key=lambda v: BSON.encode({"v": v}))


def path_contexts(collection):
    reset(
        collection,
        [
            {"_id": 1, "a": [{"0": {"b": 1}, "b": 3}, {"b": 4}, [{"0": 9}], None]},
            {"_id": 2, "a": {"0": {"b": 5}}},
            {"_id": 3, "a": []},
            {"_id": 4},
            {"_id": 5, "a": None},
        ],
    )
    project = {
        "_id": 1,
        "r": "$a.0",
        "nested": "$a.0.b",
        "b": "$a.b",
        "root": "$$ROOT.a.0",
        "current": "$$CURRENT.a.0",
        "variable": {"$let": {"vars": {"x": "$$ROOT"}, "in": "$$x.a.0"}},
    }
    return {
        "aggregate": _aggregate(
            collection, [{"$sort": {"_id": 1}}, {"$project": project}]
        ),
        "find": outcome(lambda: list(collection.find({}, project, sort=[("_id", 1)]))),
        "materialized": _aggregate(
            collection,
            [
                {"$sort": {"_id": 1}},
                {"$facet": {"rows": [{"$project": project}]}},
                {"$unwind": "$rows"},
                {"$replaceWith": "$rows"},
            ],
        ),
    }


def distinct_paths(collection):
    reset(
        collection,
        [
            {"_id": 1, "a": [{"b": [1, [2], []]}, {"b": 3}, {}]},
            {"_id": 2, "a": {"b": []}},
            {"_id": 3, "a": {"b": None}},
            {"_id": 4, "a": [[{"b": 4}]]},
            {"_id": 5},
        ],
    )
    return {
        path: sorted(collection.distinct(path), key=lambda v: BSON.encode({"v": v}))
        for path in ["a", "a.b", "a.0", "a.0.b", "a.b.0"]
    }


def lookup_paths(collection):
    rows = [
        {"_id": 1, "a": []},
        {"_id": 2},
        {"_id": 3, "a": None},
        {"_id": 4, "a": [None, 1]},
        {"_id": 5, "a": [[1]]},
        {"_id": 6, "a": [{"b": []}, {"b": 1}]},
        {"_id": 7, "a": [1]},
        {"_id": 8, "a": {"b": [1]}},
    ]
    reset(collection, rows)
    reset(collection.database["foreign"], rows)
    return {
        f"{path}/{subpipeline}": _aggregate(
            collection,
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": path,
                        "foreignField": path,
                        "as": "matches",
                        **(
                            {"pipeline": [{"$sort": {"_id": 1}}]} if subpipeline else {}
                        ),
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "matches": {
                            "$sortArray": {"input": "$matches._id", "sortBy": 1}
                        },
                    }
                },
                {"$sort": {"_id": 1}},
            ],
        )
        for path in ["a", "a.b"]
        for subpipeline in [False, True]
    }


def index_rule(collection, *, keys, options, populated):
    collection.drop()
    if populated:
        collection.insert_one({"_id": 1, "a": 1, "b": 2})
    result = outcome(
        lambda keys=keys, options=options: collection.create_index(keys, **options)
    )
    result["indexes"] = sorted(collection.index_information())
    result["ids"] = [r["_id"] for r in collection.find({}, sort=[("_id", 1)])]
    return result


def unique_null(collection):
    collection.drop()
    collection.create_index("a", unique=True)
    collection.insert_one({"_id": 1, "a": None})
    first = outcome(lambda: collection.insert_one({"_id": 2}).inserted_id)
    result = outcome(
        lambda: collection.update_one({"_id": 1}, {"$unset": {"a": 1}}).modified_count
    )
    return {
        "insert_missing": first,
        "unset": result,
        "rows": list(collection.find({}, sort=[("_id", 1)])),
    }


def default_collation(collection):
    db = collection.database
    collection.drop()
    collection = db.create_collection(
        collection.name, collation={"locale": "en", "strength": 2}
    )
    collection.insert_many(
        [{"_id": 1, "a": "A"}, {"_id": 2, "a": "a"}, {"_id": 3, "a": "b"}]
    )
    result = {
        "ids": [r["_id"] for r in collection.find({"a": "a"}, sort=[("_id", 1)])],
        "distinct": sorted(collection.distinct("a")),
        "groups": list(
            collection.aggregate(
                [{"$group": {"_id": "$a", "n": {"$sum": 1}}}, {"$sort": {"_id": 1}}]
            )
        ),
        "override": [
            r["_id"]
            for r in collection.find({"a": "a"}, collation={"locale": "simple"})
        ],
    }
    result["updated"] = collection.update_many(
        {"a": "a"}, {"$set": {"done": True}}
    ).matched_count
    result["deleted"] = collection.delete_many({"a": "a"}).deleted_count
    return result


def invalid_collation(collection, *, spec):
    reset(collection, [{"_id": 1, "a": "a"}])
    return outcome(lambda: list(collection.find({}, collation=spec)))


def collation_consumers(collection, *, spec):
    db = collection.database
    collection.drop()
    collection = db.create_collection(collection.name, collation=spec)
    collection.insert_many(
        [
            {"_id": 1, "a": "E"},
            {"_id": 2, "a": "e"},
            {"_id": 3, "a": "é"},
            {"_id": 4, "a": "10"},
            {"_id": 5, "a": "2"},
        ]
    )
    result = {
        label: [
            r["_id"] for r in collection.find({"a": "e"}, sort=[("_id", 1)], **options)
        ]
        for label, options in [
            ("default", {}),
            ("empty", {"collation": {}}),
            ("simple", {"collation": {"locale": "simple"}}),
        ]
    }
    result["range"] = [
        r["_id"]
        for r in collection.find({"a": {"$gte": "e"}}, sort=[("a", 1), ("_id", 1)])
    ]
    result["sort"] = [
        r["_id"] for r in collection.find({}, sort=[("a", 1), ("_id", 1)])
    ]
    result["groups"] = list(
        collection.aggregate(
            [
                {"$sort": {"_id": 1}},
                {"$group": {"_id": "$a", "n": {"$sum": 1}}},
                {"$sort": {"_id": 1}},
            ]
        )
    )
    foreign = db["foreign"]
    foreign.drop()
    foreign = db.create_collection("foreign", collation={"locale": "en", "strength": 3})
    foreign.insert_many([{"_id": 1, "a": "E"}, {"_id": 2, "a": "e"}])
    result["lookup"] = list(
        collection.aggregate(
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": "a",
                        "foreignField": "a",
                        "as": "found",
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "ids": {"$sortArray": {"input": "$found._id", "sortBy": 1}},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    )
    result["update"] = collection.update_many(
        {"a": "e"}, [{"$set": {"same": {"$eq": ["$a", "e"]}}}]
    ).matched_count
    result["updated_rows"] = list(
        collection.find({"same": {"$exists": True}}, sort=[("_id", 1)])
    )
    valid = db["validated"]
    valid.drop()
    valid = db.create_collection("validated", collation=spec, validator={"a": "e"})

    def insert_valid():
        valid.insert_one({"_id": 1, "a": "E"})
        return valid.count_documents({})

    result["validator"] = outcome(insert_valid, write=True)
    result["delete"] = collection.delete_many({"a": "e"}).deleted_count
    return result


def setup_collation_persisted(collection):
    db = collection.database
    collection.drop()
    created = db.create_collection(
        collection.name, collation={"locale": "en", "strength": 2}
    )
    created.insert_many(
        [
            {"_id": 1, "a": "A", "title": "ada", "embedding": [1.0, 0.0]},
            {"_id": 2, "a": "a", "title": "ada", "embedding": [1.0, 0.0]},
            {"_id": 3, "a": "B", "title": "ada", "embedding": [1.0, 0.0]},
        ]
    )
    created.create_index("a", name="a_index")


def read_collation_persisted(collection):
    return {
        "ids": [r["_id"] for r in collection.find({"a": "a"}, sort=[("_id", 1)])],
        "hinted": [
            r["_id"]
            for r in collection.find({"a": "a"}, hint="a_index", sort=[("_id", 1)])
        ],
        "count": collection.count_documents({"a": "a"}),
        "distinct": sorted(collection.distinct("a")),
    }


def collation_persisted(collection):
    setup_collation_persisted(collection)
    return {
        "cold": read_collation_persisted(collection),
        "warm": read_collation_persisted(collection),
    }


def collation_namespaces(collection):
    db = collection.database
    result = []
    cursor = collection.find({"a": "a"}, sort=[("_id", 1)])
    for spec in [
        {"locale": "en", "strength": 2},
        {"locale": "simple"},
        {"locale": "en", "strength": 1},
    ]:
        collection.drop()
        created = db.create_collection(collection.name, collation=spec)
        created.insert_many([{"_id": 1, "a": "A"}, {"_id": 2, "a": "a"}])
        # Reuse the original handle after namespace recreation; do not cache absence.
        result.append(
            [r["_id"] for r in collection.find({"a": "a"}, sort=[("_id", 1)])]
        )
        result.append([r["_id"] for r in cursor.rewind()])
        result.append([r["_id"] for r in cursor.clone()])
        result.append(
            [r["_id"] for r in db[collection.name].find({"a": "a"}, sort=[("_id", 1)])]
        )
    target = db["renamed_collation"]
    target.drop()
    collection.rename(target.name)
    result.append([r["_id"] for r in target.find({"a": "a"}, sort=[("_id", 1)])])
    return result


def collation_partial_index(collection):
    collection.drop()
    collection.database.create_collection(
        collection.name, collation={"locale": "en", "strength": 2}
    )
    collection.create_index(
        "a", name="partial", unique=True, partialFilterExpression={"p": "a"}
    )
    collection.insert_one({"_id": 1, "p": "A", "a": "K"})

    def duplicate():
        collection.insert_one({"_id": 2, "p": "a", "a": "k"})

    result = {"insert": outcome(duplicate)}
    collection.insert_one({"_id": 3, "p": "B", "a": "k"})
    result["update"] = outcome(
        lambda: collection.update_one({"_id": 3}, {"$set": {"p": "A"}}).modified_count
    )
    result["rows"] = list(collection.find({}, sort=[("_id", 1)]))
    result["hinted"] = [
        r["_id"] for r in collection.find({"p": "a", "a": "k"}, hint="partial")
    ]
    return result


def collation_case_level(collection):
    collection.drop()
    collection.database.create_collection(
        collection.name, collation={"locale": "en", "strength": 1, "caseLevel": True}
    )
    reset(
        collection,
        [
            {"_id": n, "a": value}
            for n, value in enumerate(
                [
                    "e",
                    "é",
                    "E",
                    "É",
                    "ß",
                    "ss",
                    "ẞ",
                    "SS",
                    "\uff45",
                    "\uff25",
                    "æ",
                    "Æ",
                    "ae",
                ]
            )
        ],
    )
    return {
        "groups": list(
            collection.aggregate(
                [
                    {"$sort": {"_id": 1}},
                    {"$group": {"_id": "$a", "ids": {"$push": "$_id"}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
        "ids": [r["_id"] for r in collection.find({"a": "é"}, sort=[("_id", 1)])],
    }


def collation_numeric_levels(collection, *, strength, case_level):
    collection.drop()
    collection.database.create_collection(
        collection.name,
        collation={
            "locale": "en",
            "strength": strength,
            "caseLevel": case_level,
            "numericOrdering": True,
        },
    )
    reset(
        collection,
        [
            {"_id": n, "a": value}
            for n, value in enumerate(
                ["a2B", "A10b", "a10b", "á2b", "a002B", "²2", "2", "10", "a٢B"]
            )
        ],
    )
    return {
        "sort": [r["_id"] for r in collection.find({}, sort=[("a", 1), ("_id", 1)])],
        "groups": list(
            collection.aggregate(
                [
                    {"$sort": {"_id": 1}},
                    {"$group": {"_id": "$a", "ids": {"$push": "$_id"}}},
                    {"$sort": {"_id": 1}},
                ]
            )
        ),
    }


def collation_raw(collection):
    db = collection.database
    result = {}
    for label, value in [
        ("null", None),
        ("empty", {}),
        ("array", []),
        ("bool", False),
        ("required", {"strength": 2}),
        ("unknown", {"locale": "en", "foo": 1}),
        ("simple", {"locale": "simple"}),
    ]:
        for role in [
            "find",
            "aggregate",
            "count",
            "distinct",
            "update",
            "delete",
            "findAndModify",
            "createIndexes",
        ]:
            collection.drop()
            db.create_collection(
                collection.name, collation={"locale": "en", "strength": 2}
            )
            collection.insert_many([{"_id": 1, "a": "A"}, {"_id": 2, "a": "a"}])
            commands = {
                "find": {
                    "find": collection.name,
                    "filter": {"a": "a"},
                    "sort": {"_id": 1},
                    "collation": value,
                },
                "aggregate": {
                    "aggregate": collection.name,
                    "pipeline": [{"$match": {"a": "a"}}, {"$sort": {"_id": 1}}],
                    "cursor": {},
                    "collation": value,
                },
                "count": {
                    "count": collection.name,
                    "query": {"a": "a"},
                    "collation": value,
                },
                "distinct": {
                    "distinct": collection.name,
                    "key": "_id",
                    "query": {"a": "a"},
                    "collation": value,
                },
                "update": {
                    "update": collection.name,
                    "updates": [
                        {
                            "q": {"a": "a"},
                            "u": {"$set": {"done": True}},
                            "multi": True,
                            "collation": value,
                        }
                    ],
                },
                "delete": {
                    "delete": collection.name,
                    "deletes": [{"q": {"a": "a"}, "limit": 0, "collation": value}],
                },
                "findAndModify": {
                    "findAndModify": collection.name,
                    "query": {"a": "a"},
                    "update": {"$set": {"done": True}},
                    "sort": {"_id": 1},
                    "collation": value,
                },
                "createIndexes": {
                    "createIndexes": collection.name,
                    "indexes": [{"key": {"b": 1}, "name": "b", "collation": value}],
                },
            }

            def run(command=commands[role], role=role):
                reply = db.command(command)
                if "cursor" in reply:
                    return [r["_id"] for r in reply["cursor"]["firstBatch"]]
                if "values" in reply:
                    return sorted(reply["values"])
                if role == "findAndModify":
                    return None if reply["value"] is None else reply["value"]["_id"]
                return {
                    key: reply[key]
                    for key in ["n", "nModified", "numIndexesAfter"]
                    if key in reply
                } | {
                    "errors": [entry["code"] for entry in reply.get("writeErrors", [])]
                }

            result[f"{role}/{label}"] = outcome(run)
    return result


def ordered_write(collection, *, kind):
    reset(collection, [{"_id": 1, "a": 1}])
    if kind == "replace":
        collection.replace_one({"_id": 1}, {"a": 2, "_id": 1, "b": 3})
    elif kind == "upsert":
        collection.update_one({"z": 7}, {"$set": {"b": 3}}, upsert=True)
    elif kind == "modify":
        collection.find_one_and_update({"z": 7}, {"$set": {"b": 3}}, upsert=True)
    return [list(r) for r in collection.find({}, sort=[("a", 1)])]


def update_value(collection, *, update, rows):
    reset(collection, rows)
    result = outcome(lambda: collection.update_many({}, update).modified_count)
    result["rows"] = list(collection.find({}, sort=[("_id", 1)]))
    return result


def empty_timestamp(collection):
    reset(collection, [])

    collection.insert_one(
        {"_id": 1, "t": Timestamp(0, 0), "nested": {"t": Timestamp(0, 0)}}
    )
    row = collection.find_one()
    return {
        "root_replaced": row["t"] != Timestamp(0, 0),
        "nested_unchanged": row["nested"]["t"] == Timestamp(0, 0),
    }


def write_field_contexts(collection):
    result = {}
    for kind in ["insert", "replace", "set", "pipeline"]:
        for matched in [True, False]:
            for label, fields in [
                ("root_dollar", {"$a": 2}),
                ("nested_dollar", {"o": {"$a": 2}}),
                ("dotted_nested", {"o.$a": 2}),
                ("id_dollar", {"_id": {"$a": 2}}),
                ("id_nested_dollar", {"_id": {"a": {"$b": 2}}}),
                ("id_dotted", {"_id": {"a.b": 2}}),
            ]:
                collection.drop()
                if matched:
                    collection.insert_one({"_id": 1, "o": {"keep": 1}})

                def invoke(fields=fields, kind=kind, matched=matched):
                    doc = {"_id": 1, **fields}
                    if kind == "insert":
                        collection.insert_one(doc)
                        return {"inserted": True}
                    if kind == "replace":
                        response = collection.replace_one(
                            {"_id": 1}, doc, upsert=not matched
                        )
                    else:
                        update = (
                            {"$set": fields}
                            if kind == "set"
                            else [{"$replaceWith": {"$literal": doc}}]
                        )
                        response = collection.update_one(
                            {"_id": 1}, update, upsert=not matched
                        )
                    return {
                        "matched": response.matched_count,
                        "modified": response.modified_count,
                        "upserted": response.upserted_id is not None,
                    }

                entry = outcome(invoke, write=True)
                entry["state"] = [
                    BSON.encode(row).hex()
                    for row in collection.find({}, sort=[("_id", 1)])
                ]
                result[f"{kind}/{matched}/{label}"] = entry
    return result


def timestamp_contexts(collection):
    result = {}
    for kind in [
        "insert",
        "replace",
        "set",
        "pipeline",
        "replace_upsert",
        "set_upsert",
        "pipeline_upsert",
    ]:
        collection.drop()
        matched = kind != "insert" and not kind.endswith("upsert")
        if matched:
            collection.insert_one({"_id": 1})
        fields = {
            "t": Timestamp(0, 0),
            "u": Timestamp(0, 0),
            "fixed": Timestamp(7, 3),
            "nested": {"t": Timestamp(0, 0)},
            "array": [Timestamp(0, 0)],
        }
        if kind == "insert":
            collection.insert_one({"_id": 1, **fields})
        elif kind.startswith("replace"):
            collection.replace_one({"_id": 1}, {"_id": 1, **fields}, upsert=not matched)
        else:
            update = (
                {"$set": fields}
                if kind.startswith("set")
                else [{"$set": {k: {"$literal": v} for k, v in fields.items()}}]
            )
            collection.update_one({"_id": 1}, update, upsert=not matched)
        row = collection.find_one()
        result[kind] = {
            "t_zero": row["t"] == Timestamp(0, 0),
            "u_zero": row["u"] == Timestamp(0, 0),
            "same_second": row["t"].time == row["u"].time,
            "increasing": row["t"].inc < row["u"].inc,
            "fixed": row["fixed"],
            "nested": row["nested"],
            "array": row["array"],
        }
    collection.drop()
    collection.insert_one({"_id": Timestamp(0, 0)})
    result["id"] = collection.find_one()["_id"]
    return result


def natural_order(collection, *, direction, hint=False):
    reset(collection, [{"_id": n, "v": n} for n in (30, 10, 20)])

    def run():
        cursor = collection.find()
        cursor = (
            cursor.hint([("$natural", direction)])
            if hint
            else cursor.sort("$natural", direction)
        )
        return [row["_id"] for row in cursor]

    return outcome(run)


def sort_documents(collection, *, rows, sort):
    reset(collection, rows)
    return outcome(lambda: [r["_id"] for r in collection.find({}, sort=sort)])


def namespace_metadata(collection):
    collection.drop()
    absent = collection.index_information()
    collection.insert_one({"_id": 1})
    return {"absent": absent, "present": collection.index_information()}


D = datetime.datetime
DT = D(2021, 1, 3, 4, 5, 6)
DATE = {"_id": 1, "d": D(2021, 3, 27, 12), "v": 3, "n": None}
EXPRESSIONS = {
    "f03_numeric_path": ("$a.0", [{"_id": 1, "a": [7, 8]}]),
    "f03_numeric_object": ("$a.0", [{"_id": 1, "a": {"0": 7}}]),
    "f03_numeric_nested": ("$a.0.b", [{"_id": 1, "a": [{"b": 1}, {"b": 2}]}]),
    "f03_root_numeric": ("$$ROOT.a.0", [{"_id": 1, "a": [7, 8]}]),
    "f02_object_nul": ({"$arrayToObject": {"$literal": [["a\u0000b", 1]]}}, None),
    "f08_sortarray_scalar": (
        {"$sortArray": {"input": [{"x": 2}, 3, {"x": 1}], "sortBy": {"x": 1}}},
        None,
    ),
    "f08_sortarray_arrays": (
        {"$sortArray": {"input": [{"x": [0, 9]}, {"x": 1}], "sortBy": {"x": 1}}},
        None,
    ),
    "f09_add_empty": ({"$add": []}, None),
    "f09_add_one": ({"$add": [1]}, None),
    "f09_multiply_empty": ({"$multiply": []}, None),
    "f09_concat_empty": ({"$concat": []}, None),
    "f09_range_long": ({"$range": [Int64(0), Int64(3)]}, None),
    "f09_range_double": ({"$range": [0, 3.0]}, None),
    "f09_range_budget": ({"$range": [0, 2**31 - 1]}, None),
    "f09_slice_long": ({"$slice": [[1, 2, 3], Int64(2)]}, None),
    "f09_round_long": ({"$round": [12.34, Int64(1)]}, None),
    "f09_subtract_null_left": ({"$subtract": [None, 1]}, None),
    "f09_subtract_null_right": ({"$subtract": [1, None]}, None),
    "f09_date_trunc_hour": (
        {"$dateTrunc": {"date": DT, "unit": "hour", "binSize": 5}},
        None,
    ),
    "f09_date_trunc_days": (
        {"$dateTrunc": {"date": DT, "unit": "day", "binSize": 7}},
        None,
    ),
    "f09_date_add_hours": (
        {
            "$dateAdd": {
                "startDate": "$d",
                "unit": "hour",
                "amount": 24,
                "timezone": "Europe/Madrid",
            }
        },
        [DATE],
    ),
    "f09_date_add_days": (
        {
            "$dateAdd": {
                "startDate": "$d",
                "unit": "day",
                "amount": 1,
                "timezone": "Europe/Madrid",
            }
        },
        [DATE],
    ),
    "f09_date_add_long": (
        {"$dateAdd": {"startDate": DT, "unit": "day", "amount": Int64(1)}},
        None,
    ),
    "f09_date_parts_month": ({"$dateFromParts": {"year": 2021, "month": 13}}, None),
    "f09_date_parts_day": ({"$dateFromParts": {"year": 2021, "day": 0}}, None),
    "f09_date_diff_ignored": (
        {
            "$dateDiff": {
                "startDate": DT,
                "endDate": DT,
                "unit": "day",
                "startOfWeek": "bad",
            }
        },
        None,
    ),
    "f09_tostring_double": ({"$toString": 1.0}, None),
    "f09_tostring_decimal": ({"$toString": Decimal128("1.50")}, None),
    "f09_tostring_uuid": (
        {"$toString": {"$literal": Binary(bytes(range(16)), 4)}},
        None,
    ),
    "f09_upper_number": ({"$toUpper": 12.5}, None),
    "f09_upper_decimal": ({"$toUpper": Decimal128("1.50")}, None),
    "f09_upper_date": ({"$toUpper": DT}, None),
    "f09_upper_bool": ({"$toUpper": True}, None),
    "f09_upper_oid": ({"$toUpper": ObjectId("000000000000000000000001")}, None),
    "f09_strcasecmp_number": ({"$strcasecmp": [5, "5"]}, None),
    "f09_trim_0085": ({"$trim": {"input": "\u0085a\u0085"}}, None),
    "f09_let_bad_name": ({"$let": {"vars": {"A": 1}, "in": "$$A"}}, None),
}
for fmt in ("%j", "%U", "%w", "%Z", "%%", "%q", "%"):
    EXPRESSIONS["f09_format_" + fmt] = (
        {"$dateToString": {"date": DT, "format": fmt, "timezone": "+05:30"}},
        None,
    )

QUERIES = {
    "eq_regex": {"a": {"$eq": Regex("x")}},
    "not_regex": {"a": {"$not": Regex("x")}},
    "elem_array": {"a": {"$elemMatch": {"b": {"$exists": False}}}},
    "all_mixed": {"a": {"$all": [{"$elemMatch": {"b": 1}}, 1]}},
    "all_mixed_reverse": {"a": {"$all": [1, {"$elemMatch": {"b": 1}}]}},
    "all_elem": {"a": {"$all": [{"$elemMatch": {"b": 1}}]}},
    "implicit_regex": {"a": Regex("x")},
    "in_regex": {"a": {"$in": [Regex("x")]}},
    "regex_operator": {"a": {"$regex": "x"}},
    "eq_regex_flags": {"a": {"$eq": Regex("x", "i")}},
    "elem_array_numeric": {"a": {"$elemMatch": {"0": 1}}},
}
for label, size in [
    ("long", Int64(1)),
    ("double", 1.0),
    ("fraction", 1.5),
    ("decimal", Decimal128("1")),
    ("bool", True),
    ("negative", -1),
    ("zero", 0),
    ("limit", 2**31 - 1),
    ("overflow", Int64(2**31)),
    ("nan", float("nan")),
    ("inf", float("inf")),
    ("decimal_fraction", Decimal128("1.5")),
    ("decimal_nan", Decimal128("NaN")),
]:
    QUERIES["size_" + label] = {"a": {"$size": size}}

PIPELINES = {
    "f02_shift_missing": (
        [
            {
                "$setWindowFields": {
                    "sortBy": {"_id": 1},
                    "output": {"r": {"$shift": {"output": "$v", "by": 1}}},
                }
            }
        ],
        [{"_id": 1, "v": 3}, {"_id": 2}],
    ),
    "f02_linear_missing": (
        [
            {
                "$setWindowFields": {
                    "sortBy": {"_id": 1},
                    "output": {"r": {"$linearFill": "$v"}},
                }
            }
        ],
        [{"_id": 1, "v": 1}, {"_id": 2}, {"_id": 3, "v": 3}],
    ),
    "f03_unset_array": (
        [{"$unset": "a.b"}],
        [{"_id": 1, "a": [{"b": 1, "c": 2}, {"b": 3}]}],
    ),
    "f03_unwind_dotted": (
        [{"$unwind": {"path": "$a", "includeArrayIndex": "x.n"}}],
        [{"_id": 1, "a": [1, 2]}],
    ),
    "f03_unwind_overwrite": (
        [
            {
                "$unwind": {
                    "path": "$a",
                    "includeArrayIndex": "x.n",
                    "preserveNullAndEmptyArrays": True,
                }
            }
        ],
        [
            {"_id": 1, "a": [1], "x": 4},
            {"_id": 2, "a": [], "x": []},
            {"_id": 3, "a": None, "x": {"keep": 1}},
            {"_id": 4},
        ],
    ),
    "f03_unset_branches": (
        [{"$unset": ["a.b", "x.0"]}],
        [
            {
                "_id": 1,
                "a": [{"b": 1, "c": 2}, [{"b": 3, "c": 4}], None, 5],
                "x": [{"0": 1, "keep": 2}, {"other": 3}],
            }
        ],
    ),
    "f08_stddev_decimal": (
        [{"$group": {"_id": None, "s": {"$stdDevPop": "$v"}}}],
        [{"_id": 1, "v": Decimal128("1")}, {"_id": 2, "v": Decimal128("3")}],
    ),
    "f08_buckets_equal": (
        [{"$bucketAuto": {"groupBy": "$v", "buckets": 3}}],
        [{"_id": i, "v": v} for i, v in enumerate([1, 1, 1, 2, 2, 3])],
    ),
    "f08_top_array": (
        [
            {
                "$group": {
                    "_id": None,
                    "t": {"$topN": {"n": 2, "sortBy": {"v": 1}, "output": "$_id"}},
                }
            }
        ],
        [{"_id": 1, "v": [1, 9]}, {"_id": 2, "v": [3, 4]}, {"_id": 3, "v": [0, 8]}],
    ),
    "f08_push_order": (
        [
            {"$sort": {"_id": -1}},
            {
                "$group": {
                    "_id": None,
                    "p": {"$push": "$_id"},
                    "n": {"$firstN": {"input": "$_id", "n": 2}},
                }
            },
        ],
        [{"_id": n} for n in [30, 10, 20]],
    ),
    "f09_limit_long": ([{"$limit": Int64(1)}], [{"_id": 1}]),
    "f09_sample_double": ([{"$sample": {"size": 1.0}}, {"$count": "n"}], [{"_id": 1}]),
    "f09_sort_empty": ([{"$sort": {}}], [{"_id": 1}]),
    "f09_count_id": ([{"$count": "_id"}], [{"_id": 1}]),
    "f09_group_dotted": ([{"$group": {"_id.a": 1}}], [{"_id": 1}]),
    "f09_facet_name": ([{"$facet": {"a.b": []}}], [{"_id": 1}]),
    "f09_documents_collection": ([{"$documents": [{"x": 1}]}], [{"_id": 1}]),
}
INDEX_RULES = [
    ("dbref_nested", "a.$id", {}),
    ("dbref_top", "$id", {}),
    ("name_empty", "a", {"name": ""}),
    ("name_null", "a", {"name": None}),
    ("name_nul", "a", {"name": "n\x00x"}),
    ("name_id", "a", {"name": "_id_"}),
    ("trailing", "a.", {}),
    ("dollar", "$a", {}),
    ("fields33", [(f"a{i}", 1) for i in range(33)], {}),
    ("ttl_compound", [("a", 1), ("b", 1)], {"expireAfterSeconds": 10}),
    ("ttl_id", "_id", {"expireAfterSeconds": 10}),
    ("id_sparse", "_id", {"sparse": True}),
]
SEMANTIC_FOLLOWUP_CASES = (
    ("semantic_followup_f04_regex_values", regex_values),
    *(
        (f"semantic_followup_{name}", partial(expression, value=expr, rows=rows))
        for name, (expr, rows) in EXPRESSIONS.items()
    ),
    *(
        (f"semantic_followup_f04_{name}", partial(query, spec=spec))
        for name, spec in QUERIES.items()
    ),
    *(
        (
            f"semantic_followup_f04_match_{name}",
            partial(pipeline, stages=[{"$match": spec}], rows=[{"_id": 1, "a": [1]}]),
        )
        for name, spec in QUERIES.items()
    ),
    *(
        (f"semantic_followup_{name}", partial(pipeline, stages=stages, rows=rows))
        for name, (stages, rows) in PIPELINES.items()
    ),
    *(
        (
            f"semantic_followup_f06_{name}_{populated}",
            partial(index_rule, keys=keys, options=options, populated=populated),
        )
        for name, keys, options in INDEX_RULES
        for populated in (False, True)
    ),
    ("semantic_followup_f03_distinct", distinct_arrays),
    ("semantic_followup_f03_distinct_paths", distinct_paths),
    ("semantic_followup_f03_path_contexts", path_contexts),
    ("semantic_followup_f03_lookup_paths", lookup_paths),
    ("semantic_followup_f05_default", default_collation),
    ("semantic_followup_f05_persisted", collation_persisted),
    ("semantic_followup_f05_namespaces", collation_namespaces),
    ("semantic_followup_f05_partial", collation_partial_index),
    ("semantic_followup_f05_case_level", collation_case_level),
    ("semantic_followup_f05_raw", collation_raw),
    *(
        (
            f"semantic_followup_f05_numeric_levels_{strength}_{case_level}",
            partial(collation_numeric_levels, strength=strength, case_level=case_level),
        )
        for strength, case_level in [(1, False), (2, True), (3, False)]
    ),
    *(
        (
            f"semantic_followup_f05_consumers_{strength}_{case_level}_{numeric}",
            partial(
                collation_consumers,
                spec={
                    "locale": "en",
                    "strength": strength,
                    "caseLevel": case_level,
                    "numericOrdering": numeric,
                },
            ),
        )
        for strength, case_level, numeric in [
            (1, False, False),
            (2, False, False),
            (3, False, False),
            (1, True, False),
            (2, False, True),
        ]
    ),
    *(
        (f"semantic_followup_f05_invalid_{i}", partial(invalid_collation, spec=spec))
        for i, spec in enumerate([{}, {"strength": 2}, {"locale": "en", "foo": 1}])
    ),
    ("semantic_followup_f06_unique_null", unique_null),
    *(
        (f"semantic_followup_f07_order_{kind}", partial(ordered_write, kind=kind))
        for kind in ["replace", "upsert", "modify"]
    ),
    (
        "semantic_followup_f07_bit",
        partial(
            update_value,
            update={"$bit": {"a": {"and": Int64(7)}}},
            rows=[{"_id": 1, "a": 5}],
        ),
    ),
    (
        "semantic_followup_f07_pull_path",
        partial(
            update_value,
            update={"$pull": {"a.b": 1}},
            rows=[{"_id": 1, "a": [{"b": [1, 2]}]}],
        ),
    ),
    (
        "semantic_followup_f07_dollar",
        partial(update_value, update={"$set": {"$a": 1}}, rows=[{"_id": 1}]),
    ),
    ("semantic_followup_f07_timestamp", empty_timestamp),
    *(
        (
            f"semantic_followup_f08_natural_{direction}_{hint}",
            partial(natural_order, direction=direction, hint=hint),
        )
        for direction in (1, -1)
        for hint in (False, True)
    ),
    (
        "semantic_followup_f08_sort_empty",
        partial(
            sort_documents,
            sort=[("a", 1)],
            rows=[{"_id": 1, "a": []}, {"_id": 2, "a": None}, {"_id": 3, "a": 0}],
        ),
    ),
    (
        "semantic_followup_f08_parallel",
        partial(
            sort_documents,
            sort=[("a", 1), ("b", 1)],
            rows=[{"_id": 1, "a": [1], "b": [2]}],
        ),
    ),
    ("semantic_followup_f10_metadata", namespace_metadata),
)


def lookup_empty(collection):
    reset(collection, [{"_id": 1, "a": []}, {"_id": 2}, {"_id": 3, "a": None}])
    other = collection.database["foreign"]
    reset(other, [{"_id": 1, "a": None}, {"_id": 2}, {"_id": 3, "a": []}])
    return [
        {"_id": r["_id"], "matches": sorted(v["_id"] for v in r["matches"])}
        for r in collection.aggregate(
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": "a",
                        "foreignField": "a",
                        "as": "matches",
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
    ]


def index_conflict(collection, *, kind):
    collection.drop()
    collection.insert_one({"_id": 1, "a": 1, "b": 2})
    if kind == "second_text":
        collection.create_index([("a", "text")])
        call = partial(collection.create_index, [("b", "text")])
    elif kind == "same_keys":
        collection.create_index("a", name="first")
        call = partial(collection.create_index, "a", name="second")
    else:
        collection.create_index([("a", "hashed")])

        def call():
            return collection.insert_one({"_id": 2, "a": [1, 2]}).inserted_id

    result = outcome(call)
    result["indexes"] = sorted(collection.index_information())
    result["ids"] = [r["_id"] for r in collection.find({}, sort=[("_id", 1)])]
    return result


def index_catalog_matrix(collection):
    result = {}
    for label, first, second in [
        ("same_name", {}, {"name": "first"}),
        ("new_name", {}, {"name": "second"}),
        ("unique", {}, {"name": "second", "unique": True}),
        ("nonunique", {"unique": True}, {"name": "second"}),
        ("sparse", {}, {"name": "second", "sparse": True}),
        (
            "unique_sparse",
            {"unique": True},
            {"name": "second", "unique": True, "sparse": True},
        ),
        (
            "partial",
            {"partialFilterExpression": {"p": 1}},
            {"name": "second", "partialFilterExpression": {"p": 2}},
        ),
        (
            "collation",
            {},
            {"name": "second", "collation": {"locale": "en", "strength": 2}},
        ),
        (
            "equivalent_collation",
            {"collation": {"locale": "en"}},
            {"name": "second", "collation": {"locale": "en", "strength": 3}},
        ),
        ("hidden", {}, {"name": "second", "hidden": True}),
        ("same_name_unique", {}, {"name": "first", "unique": True}),
        ("same_name_hidden", {}, {"name": "first", "hidden": True}),
        (
            "same_name_ttl",
            {"expireAfterSeconds": 1},
            {"name": "first", "expireAfterSeconds": 2},
        ),
        ("same_name_sparse", {}, {"name": "first", "sparse": True}),
        (
            "same_name_partial",
            {"partialFilterExpression": {"p": 1}},
            {"name": "first", "partialFilterExpression": {"p": 2}},
        ),
        (
            "same_name_collation",
            {},
            {"name": "first", "collation": {"locale": "en", "strength": 2}},
        ),
        ("same_name_key", {}, {"name": "first", "keys": "b"}),
        ("ttl", {"expireAfterSeconds": 1}, {"name": "second", "expireAfterSeconds": 2}),
    ]:
        collection.drop()
        collection.create_index("a", name="first", **first)
        options = dict(second)
        keys = options.pop("keys", "a")
        entry = outcome(
            lambda keys=keys, options=options: collection.create_index(keys, **options)
        )
        entry["indexes"] = sorted(collection.index_information())
        result[label] = entry
    return result


def index_id_options(collection):
    result = {}
    for label, options in [
        ("default", {}),
        ("name", {"name": "custom"}),
        ("unique_false", {"unique": False}),
        ("unique_true", {"unique": True}),
        ("unique_none", {"unique": None}),
        ("unique_number", {"unique": 1}),
        ("unique_string", {"unique": "true"}),
        ("named_unique_true", {"name": "_id_", "unique": True}),
        ("named_unique_false", {"name": "_id_", "unique": False}),
        ("hidden", {"hidden": True}),
        ("hidden_false", {"hidden": False}),
        ("sparse_false", {"sparse": False}),
        ("partial", {"partialFilterExpression": {"p": 1}}),
        ("simple", {"collation": {"locale": "simple"}}),
        ("en", {"collation": {"locale": "en", "strength": 2}}),
    ]:
        collection.drop()
        result[label] = outcome(
            lambda options=options: collection.create_index("_id", **options)
        )
        result[label]["indexes"] = sorted(collection.index_information())
    collection.drop()
    collection.database.create_collection(
        collection.name, collation={"locale": "en", "strength": 2}
    )
    result["inherited"] = outcome(lambda: collection.create_index("_id"))
    collection.insert_one({"_id": "A"})
    result["collated_id"] = outcome(
        lambda: collection.insert_one({"_id": "a"}).inserted_id
    )
    return result


def index_id_models(collection, *, model_factory=IndexModel):
    result = {}
    for label, options in [
        ("omitted", {}),
        ("unique_true", {"unique": True}),
        ("unique_false", {"unique": False}),
        ("sparse_false", {"sparse": False}),
        ("hidden_false", {"hidden": False}),
    ]:
        collection.drop()
        result[label] = outcome(
            lambda options=options: collection.create_indexes(
                [model_factory("_id", **options)]
            )
        )
        result[label]["indexes"] = sorted(collection.index_information())
    return result


def index_names_raw(collection):
    result = {}
    for label, name in [
        ("null", None),
        ("empty", ""),
        ("nul", "n\x00x"),
        ("reserved", "_id_"),
        ("number", 1),
        ("bool", False),
    ]:
        collection.drop()
        result[label] = outcome(
            lambda name=name: collection.database.command(
                {
                    "createIndexes": collection.name,
                    "indexes": [{"key": {"a": 1}, "name": name}],
                }
            )["numIndexesAfter"]
        )
        result[label]["indexes"] = sorted(collection.index_information())
    return result


def unique_index_arrays(collection):
    pairs = [
        ("missing_null", "a", {}, {"a": None}),
        ("empty_null", "a", {"a": []}, {"a": None}),
        ("empty_missing", "a", {"a": []}, {}),
        ("empty_empty", "a", {"a": []}, {"a": []}),
        ("flat_nested", "a", {"a": [1, 2]}, {"a": [[1, 2]]}),
        ("nested_nested", "a", {"a": [[1, 2]]}, {"a": [[1, 2]]}),
        ("dot_empty_missing", "a.b", {"a": []}, {}),
        ("dot_scalar_missing", "a.b", {"a": [1, 2]}, {}),
        ("dot_leaf_null", "a.b", {"a": [{"b": []}]}, {"a": {"b": None}}),
        ("repeated_elements", "a", {"a": [1, 1]}, {"a": [2]}),
        ("numeric_scalar", "a", {"a": 1}, {"a": 1.0}),
        ("numeric_array", "a", {"a": [1]}, {"a": [1.0]}),
        ("numeric_nested", "a", {"a": [[1]]}, {"a": [[1.0]]}),
        ("bool_number", "a", {"a": [True]}, {"a": [1]}),
        (
            "correlated",
            [("a.b", 1), ("a.c", 1)],
            {"a": [{"b": 1, "c": 2}, {"b": 2, "c": 3}]},
            {"a": [{"b": 1, "c": 3}]},
        ),
        ("compound_empty", [("a.b", 1), ("a.c", 1)], {"a": []}, {}),
    ]
    result = {}
    for label, keys, first, second in pairs:
        for profile in ["plain", "sparse", "partial"]:
            collection.drop()
            options = {"sparse": True} if profile == "sparse" else {}
            if profile == "partial":
                options["partialFilterExpression"] = {"active": True}
            collection.create_index(keys, unique=True, **options)
            collection.insert_one({"_id": 1, "active": True, **first})
            entry = outcome(
                lambda second=second: (
                    collection.insert_one(
                        {"_id": 2, "active": True, **second}
                    ).inserted_id
                )
            )
            entry["rows"] = list(collection.find({}, sort=[("_id", 1)]))
            result[f"{label}/{profile}"] = entry
    return result


def index_write_flow(collection, *, kind, ordered=True, observe_events=False):
    collection.drop()
    hashed = kind.startswith("hashed")
    collection.create_index([("a", "hashed")] if hashed else "a", unique=not hashed)
    collection.insert_many([{"_id": 1, "a": None}, {"_id": 2, "a": 5}])
    stream = collection.watch(max_await_time_ms=50) if observe_events else None
    try:
        try:
            if kind in {"bulk", "hashed_bulk"}:
                bad = [1] if hashed else None
                result = collection.bulk_write(
                    [
                        InsertOne({"_id": 3, "a": 7}),
                        InsertOne({"_id": 4, "a": bad}),
                        UpdateOne({"_id": 2}, {"$set": {"done": True}}),
                    ],
                    ordered=ordered,
                )
                observed = {
                    "ok": True,
                    "inserted": result.inserted_count,
                    "modified": result.modified_count,
                }
            elif kind in {"insert_many", "hashed_insert_many"}:
                result = collection.insert_many(
                    [
                        {"_id": 3, "a": 7},
                        {"_id": 4, "a": [1] if hashed else None},
                        {"_id": 5, "a": 9},
                    ],
                    ordered=ordered,
                )
                observed = {"ok": True, "inserted": len(result.inserted_ids)}
            else:
                if kind == "unset":
                    result = collection.update_one({"_id": 2}, {"$unset": {"a": 1}})
                elif kind == "replacement":
                    result = collection.replace_one({"_id": 2}, {"_id": 2, "a": None})
                elif kind == "upsert":
                    result = collection.update_one(
                        {"_id": 3}, {"$set": {"a": None}}, upsert=True
                    )
                elif kind == "pipeline":
                    result = collection.update_one({"_id": 2}, [{"$set": {"a": None}}])
                else:
                    result = collection.update_one(
                        {"_id": 2}, {"$set": {"a": [1] if hashed else None}}
                    )
                observed = {
                    "ok": True,
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                    "upserted": result.upserted_id is not None,
                }
        except Exception as error:
            observed = {
                key: value
                for key, value in public_error(error, BSONObservation([])).items()
                if key in {"family", "code", "write_errors", "partial"}
            } | {"ok": False}
        if stream is not None:
            collection.insert_one({"_id": "__event_barrier", "a": "__barrier_value"})
            observed["events"] = _drain(stream, collection)
        observed["state"] = [
            BSON.encode(dict(sorted(row.items()))).hex()
            for row in collection.find(
                {"_id": {"$ne": "__event_barrier"}}, sort=[("_id", 1)]
            )
        ]
        return observed
    finally:
        if stream is not None:
            stream.close()


def index_build_documents(collection):
    result = {}
    for label, keys, options, rows in [
        ("unique_missing", "a", {"unique": True}, [{"_id": 1}, {"_id": 2, "a": None}]),
        (
            "unique_arrays",
            "a",
            {"unique": True},
            [{"_id": 1, "a": [1, 2]}, {"_id": 2, "a": [[1, 2]]}],
        ),
        ("hashed", [("a", "hashed")], {}, [{"_id": 1, "a": [1]}]),
        ("hashed_dotted", [("a.b", "hashed")], {}, [{"_id": 1, "a": [{"b": 1}]}]),
        ("parallel", [("a", 1), ("b", 1)], {}, [{"_id": 1, "a": [1], "b": [2]}]),
    ]:
        collection.drop()
        collection.insert_many(rows)
        entry = outcome(
            lambda keys=keys, options=options: collection.create_index(keys, **options)
        )
        entry["indexes"] = sorted(collection.index_information())
        entry["ids"] = [r["_id"] for r in collection.find({}, sort=[("_id", 1)])]
        result[label] = entry
    return result


SEMANTIC_FOLLOWUP_WRITE_CASES = tuple(
    (
        f"semantic_followup_f06_events_{kind}_{ordered}",
        partial(index_write_flow, kind=kind, ordered=ordered, observe_events=True),
    )
    for kind in [
        "set",
        "unset",
        "replacement",
        "upsert",
        "pipeline",
        "hashed_set",
        "bulk",
        "hashed_bulk",
        "insert_many",
        "hashed_insert_many",
    ]
    for ordered in (
        [True, False] if "bulk" in kind or "insert_many" in kind else [True]
    )
)


def facade(collection, *, kind):
    reset(collection, [{"_id": 1}])
    db = collection.database
    if kind == "positional_command":
        return outcome(lambda: db.command("count", collection.name)["n"])
    if kind == "rename":
        target = db["target"]
        reset(target, [{"_id": 2}])
        result = outcome(
            lambda: (collection.rename("target", dropTarget=True), True)[1]
        )
        result["target_ids"] = [r["_id"] for r in target.find({}, sort=[("_id", 1)])]
        return result
    raise AssertionError(kind)


SEMANTIC_FOLLOWUP_CASES += (
    ("semantic_followup_f06_catalog_matrix", index_catalog_matrix),
    ("semantic_followup_f06_id_options", index_id_options),
    ("semantic_followup_f06_id_models", index_id_models),
    ("semantic_followup_f06_names_raw", index_names_raw),
    ("semantic_followup_f06_unique_arrays", unique_index_arrays),
    ("semantic_followup_f06_build_documents", index_build_documents),
    *(
        (
            f"semantic_followup_f06_write_{kind}_{ordered}",
            partial(index_write_flow, kind=kind, ordered=ordered),
        )
        for kind in [
            "set",
            "unset",
            "replacement",
            "upsert",
            "pipeline",
            "hashed_set",
            "bulk",
            "hashed_bulk",
            "insert_many",
            "hashed_insert_many",
        ]
        for ordered in (
            [True, False] if "bulk" in kind or "insert_many" in kind else [True]
        )
    ),
    ("semantic_followup_f03_lookup_empty", lookup_empty),
    *(
        (f"semantic_followup_f06_conflict_{kind}", partial(index_conflict, kind=kind))
        for kind in ("second_text", "same_keys", "hashed_array")
    ),
    *(
        (f"semantic_followup_f10_{kind}", partial(facade, kind=kind))
        for kind in ("positional_command", "rename")
    ),
)


for label, value in [
    ("oid", ObjectId("5f0000000000000000000000")),
    ("timestamp", Timestamp(1600000000, 1)),
]:
    EXPRESSIONS["f09_date_input_" + label] = (
        {"$dateToString": {"date": {"$literal": value}, "format": "%Y-%m-%d"}},
        None,
    )
for op, value in [("$limit", 1.0), ("$sample", {"size": Int64(1)})]:
    PIPELINES["f09_numeric_" + op] = ([{op: value}, {"$count": "n"}], [{"_id": 1}])
SEMANTIC_FOLLOWUP_CASES += (
    *(
        (
            f"semantic_followup_f09_date_input_{label}",
            partial(expression, value=EXPRESSIONS["f09_date_input_" + label][0]),
        )
        for label in ("oid", "timestamp")
    ),
    *(
        (
            f"semantic_followup_f09_numeric_{op}",
            partial(
                pipeline, stages=PIPELINES["f09_numeric_" + op][0], rows=[{"_id": 1}]
            ),
        )
        for op in ("$limit", "$sample")
    ),
)


def invalid_id(collection):
    collection.drop()
    result = outcome(lambda: collection.insert_one({"_id": {"$a": 1}}).inserted_id)
    result["count"] = collection.count_documents({})
    return result


def bulk_replacement(collection):

    reset(collection, [{"_id": 1}])
    result = collection.bulk_write([ReplaceOne({"_id": 1}, {"a": 1, "_id": 1})])
    return {"modified": result.modified_count, "keys": list(collection.find_one())}


def default_unique(collection):
    collection.drop()
    c = collection.database.create_collection(
        collection.name, collation={"locale": "en", "strength": 2}
    )
    c.insert_many([{"_id": 1, "a": "A"}, {"_id": 2, "a": "a"}])
    return outcome(lambda: c.create_index("a", unique=True))


EXTRA_EXPRESSIONS = {
    "maxn_long": {"$maxN": {"input": [1, 2, 3], "n": Int64(2)}},
    "maxn_double": {"$maxN": {"input": [1, 2, 3], "n": 2.0}},
    "slice_position_long": {"$slice": [[1, 2, 3], Int64(0), Int64(2)]},
    "range_negative": {"$range": [3, 0, -1]},
    "range_fraction": {"$range": [0, 1.5]},
    "subtract_missing_right": {"$subtract": [1, "$absent"]},
    "subtract_bad_null": {"$subtract": ["x", None]},
    "add_null_bad": {"$add": [None, "x"]},
    "date_parts_negative": {"$dateFromParts": {"year": 2021, "millisecond": -1}},
    "date_parts_hour": {"$dateFromParts": {"year": 2021, "hour": 25}},
    "date_diff_week": {
        "$dateDiff": {
            "startDate": DT,
            "endDate": D(2021, 1, 5),
            "unit": "week",
            "startOfWeek": "mon",
        }
    },
    "date_diff_bad_week": {
        "$dateDiff": {
            "startDate": DT,
            "endDate": DT,
            "unit": "week",
            "startOfWeek": "bad",
        }
    },
    "year_timestamp": {"$year": {"$literal": Timestamp(1600000000, 1)}},
    "year_oid": {"$year": {"$literal": ObjectId("5f0000000000000000000000")}},
    "upper_long": {"$toUpper": Int64(5)},
    "lower_number": {"$toLower": 12.5},
    "upper_null": {"$toUpper": None},
    "upper_missing": {"$toUpper": "$absent"},
    "strcasecmp_reverse": {"$strcasecmp": ["5", 5]},
    "trim_nul": {"$trim": {"input": "\u0000a\u0000"}},
    "let_sibling": {"$let": {"vars": {"a": 1, "b": "$$a"}, "in": "$$b"}},
}
SEMANTIC_FOLLOWUP_CASES += (
    *(
        (f"semantic_followup_f09_{name}", partial(expression, value=value))
        for name, value in EXTRA_EXPRESSIONS.items()
    ),
    ("semantic_followup_f07_invalid_id", invalid_id),
    ("semantic_followup_f07_bulk_replace", bulk_replacement),
    ("semantic_followup_f05_default_unique", default_unique),
)

# F02 boundary controls: no other window-family expansion.
SEMANTIC_FOLLOWUP_CASES += (
    (
        "semantic_followup_f02_window_edges",
        partial(
            pipeline,
            stages=[
                {
                    "$setWindowFields": {
                        "sortBy": {"_id": 1},
                        "output": {
                            "carry": {"$locf": "$v"},
                            "linear": {"$linearFill": "$v"},
                            "shift": {
                                "$shift": {
                                    "output": "$v",
                                    "by": -1,
                                    "default": "outside",
                                }
                            },
                        },
                    }
                }
            ],
            rows=[
                {"_id": 0},
                {"_id": 1, "v": None},
                {"_id": 2, "v": 2},
                {"_id": 3},
                {"_id": 4, "v": 4},
                {"_id": 5},
            ],
        ),
    ),
    (
        "semantic_followup_f02_dynamic_nul",
        partial(
            expression,
            value={"$arrayToObject": [[["$key", 1]]]},
            rows=[{"_id": 1, "key": "a\u0000b"}],
        ),
    ),
    *(
        (
            f"semantic_followup_f02_no_sort_{name}",
            partial(
                pipeline,
                stages=[{"$setWindowFields": {"output": {"r": value}}}],
                rows=[{"_id": 1, "v": 1}],
            ),
        )
        for name, value in [
            ("shift", {"$shift": {"output": "$v", "by": 1}}),
            ("linear", {"$linearFill": "$v"}),
        ]
    ),
    (
        "semantic_followup_f02_duplicate_sort",
        partial(
            pipeline,
            stages=[
                {
                    "$setWindowFields": {
                        "sortBy": {"s": 1},
                        "output": {"r": {"$linearFill": "$v"}},
                    }
                }
            ],
            rows=[
                {"_id": 1, "s": 1, "v": 1},
                {"_id": 2, "s": 1, "v": None},
                {"_id": 3, "s": 2, "v": 3},
            ],
        ),
    ),
)

SEMANTIC_FOLLOWUP_CASES += (
    ("semantic_followup_f07_fields_contextual", write_field_contexts),
    ("semantic_followup_f07_timestamp_contextual", timestamp_contexts),
)


def write_finalization_events(collection):
    result = {}
    for label, seed, update, mode, options in [
        (
            "replacement",
            {"_id": 1, "v": {"b": 2, "a": 1}},
            {"v": {"b": 2, "a": 1}, "_id": 1, "done": True},
            "replacement",
            {},
        ),
        (
            "replacement_upsert",
            None,
            {"v": {"b": 2, "a": 1}, "_id": 1},
            "replacement",
            {"upsert": True},
        ),
        (
            "operator_upsert",
            None,
            {"$set": {"_id": 1, "v": {"b": 2, "a": 1}}},
            "classic",
            {"upsert": True},
        ),
        ("bit", {"_id": 1, "v": 5}, {"$bit": {"v": {"and": Int64(7)}}}, "classic", {}),
        (
            "bit_noop",
            {"_id": 1, "v": Int64(5)},
            {"$bit": {"v": {"and": Int64(7)}}},
            "classic",
            {},
        ),
        (
            "pull_blocked",
            {"_id": 1, "v": [{"x": [1]}]},
            {"$pull": {"v.x": 1}},
            "classic",
            {},
        ),
        ("dollar_rejected", {"_id": 1}, {"$set": {"$a": 2}}, "classic", {}),
    ]:
        result[label] = _single(collection, seed, update, mode=mode, options=options)
    return result


SEMANTIC_FOLLOWUP_WRITE_CASES += (
    ("semantic_followup_f07_finalization_events", write_finalization_events),
)
