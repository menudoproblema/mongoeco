"""BSON equivalence through grouping, sets and lookup, observed on the server."""

from bson import Binary, Code, DBRef


SEED = [{"_id": 0}]
COLLATION = {"locale": "en", "strength": 2}
REF = {"$ref": "items", "$id": "abc"}
VALUES = [
    REF,
    DBRef("items", "abc"),
    {"$id": "abc", "$ref": "items"},
    {"$ref": "ITEMS", "$id": "ABC"},
    {"$ref": "items", "$id": "abc", "tag": "t"},
    DBRef("items", "abc", tag="t"),
    {"$ref": "items", "$id": "abc", "$db": None},
    {"$ref": "items", "$id": {"n": 1}},
    DBRef("items", {"n": 1}),
    [REF],
    [DBRef("items", "abc")],
    {"nested": REF},
    {"nested": DBRef("items", "abc")},
    b"x",
    Binary(b"x", 0),
    Binary(b"x", 5),
    Code("$a"),
    Code("$a", {}),
    Code("$a", {"n": 1}),
    Code("$a", {"n": 1.0}),
    Code("$a", {"s": "a"}),
    Code("$a", {"s": "A"}),
    "a",
    "A",
    True,
    1,
    1.0,
    None,
]
PAIRS = {
    "bytes-binary0": (b"x", Binary(b"x", 0)),
    "binary0-binary5": (Binary(b"x", 0), Binary(b"x", 5)),
    "code-string": (Code("x"), "x"),
    "code-empty-scope": (Code("x"), Code("x", {})),
    "code-numeric-scope": (Code("x", {"n": 1}), Code("x", {"n": 1.0})),
    "code-scope-case": (Code("x", {"s": "a"}), Code("x", {"s": "A"})),
    "dbref-document": (DBRef("items", "abc"), REF),
    "dbref-reversed": (DBRef("items", "abc"), {"$id": "abc", "$ref": "items"}),
    "dbref-collation": (DBRef("items", "abc"), {"$ref": "ITEMS", "$id": "ABC"}),
    "dbref-null-db": (DBRef("items", "abc"), {**REF, "$db": None}),
    "nested-dbref": ({"v": DBRef("items", "abc")}, {"v": REF}),
    "numeric": (1, 1.0),
    "bool-number": (True, 1),
    "string-case": ("a", "A"),
}


def _reset(collection):
    collection.delete_many({})
    collection.insert_many(
        [{"_id": index, "v": value} for index, value in enumerate(VALUES, 1)]
        + [{"_id": len(VALUES) + 1}]
    )


def _observe(collection, pipeline, *, collation, allow_disk_use=False, batch_size=1):
    cursor = None
    phase = "open"
    try:
        cursor = collection.aggregate(
            pipeline,
            collation=collation,
            allowDiskUse=allow_disk_use,
            batchSize=batch_size,
        )
        phase = "iterate"
        return {"ok": True, "rows": list(cursor)}
    except Exception as error:
        return {
            "ok": False,
            "phase": phase,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
        }
    finally:
        if cursor is not None:
            cursor.close()


def bson_group_consumers(collection, *, allow_disk_use=False, batch_size=1):
    _reset(collection)
    group = {
        "$group": {
            "_id": "$v",
            "members": {"$push": "$_id"},
            "count": {"$sum": 1},
            "first": {"$min": "$_id"},
        }
    }
    pipeline = [
        {"$sort": {"_id": 1}},
        group,
        {
            "$project": {
                "_id": 0,
                "members": 1,
                "count": 1,
                "first": 1,
                "kind": {"$type": "$_id"},
            }
        },
        {"$sort": {"first": 1}},
    ]
    result = {}
    for label, collation in (("simple", None), ("collated", COLLATION)):
        for route in ("top", "facet"):
            result[f"{label}-{route}"] = _observe(
                collection,
                pipeline if route == "top" else [{"$facet": {"rows": pipeline}}],
                collation=collation,
                allow_disk_use=allow_disk_use,
                batch_size=batch_size,
            )
    return result


def bson_set_consumers(collection):
    collection.delete_many({})
    collection.insert_many(
        [
            {"_id": name, "l": [left], "r": [right]}
            for name, (left, right) in PAIRS.items()
        ]
    )
    project = {"_id": 1}
    for name in ("$setUnion", "$setIntersection", "$setDifference"):
        expression = {name: ["$l", "$r"]}
        project[name[1:]] = {"$size": expression}
        project[name[1:] + "Members"] = [
            {"$in": [{"$literal": value}, expression]} for value in VALUES
        ]
    for name in ("$setEquals", "$setIsSubset"):
        project[name[1:]] = {name: ["$l", "$r"]}
    pipeline = [{"$project": project}, {"$sort": {"_id": 1}}]
    return {
        label: _observe(collection, pipeline, collation=collation)
        for label, collation in (("simple", None), ("collated", COLLATION))
    }


def bson_add_to_set_consumers(collection):
    _reset(collection)
    pipeline = [
        {"$group": {"_id": None, "values": {"$addToSet": "$v"}}},
        {
            "$project": {
                "_id": 0,
                "size": {"$size": "$values"},
                "members": [
                    {"$in": [{"$literal": value}, "$values"]} for value in VALUES
                ],
            }
        },
    ]
    return {
        label: _observe(collection, pipeline, collation=collation)
        for label, collation in (("simple", None), ("collated", COLLATION))
    }


def bson_lookup_consumers(collection):
    _reset(collection)
    pipeline = [
        {
            "$lookup": {
                "from": collection.name,
                "localField": "v",
                "foreignField": "v",
                "as": "matches",
            }
        },
        {
            "$project": {
                "_id": 1,
                "matches": {"$sortArray": {"input": "$matches._id", "sortBy": 1}},
            }
        },
        {"$sort": {"_id": 1}},
    ]
    return {
        label: _observe(collection, pipeline, collation=collation)
        for label, collation in (("simple", None), ("collated", COLLATION))
    }


def bson_group_representatives(collection, *, allow_disk_use=False):
    """Observe physical IDs without the SDK's DBRef decoding/canonicalization."""
    _reset(collection)
    group = {"$group": {"_id": "$v", "first": {"$min": "$_id"}}}
    tail = [group, {"$sort": {"first": 1}}, {"$project": {"_id": 1}}]
    routes = {
        "top": [{"$sort": {"_id": 1}}, *tail],
        "classic-unwind": [{"$sort": {"_id": 1}}, {"$unwind": "$_id"}, *tail],
        "set-prefix": [{"$sort": {"_id": 1}}, {"$set": {"marker": 1}}, *tail],
        "facet": [{"$facet": {"rows": [{"$sort": {"_id": 1}}, *tail]}}],
        "lookup": [
            {"$limit": 1},
            {
                "$lookup": {
                    "from": collection.name,
                    "pipeline": [{"$sort": {"_id": 1}}, *tail],
                    "as": "rows",
                }
            },
            {"$project": {"_id": 0, "rows": 1}},
        ],
        "union": [
            {"$match": {"_id": {"$lt": 0}}},
            {
                "$unionWith": {
                    "coll": collection.name,
                    "pipeline": [{"$sort": {"_id": 1}}, *tail],
                }
            },
        ],
    }
    result = {}
    for label, collation in (("simple", None), ("collated", COLLATION)):
        for route, pipeline in routes.items():
            cursor = None
            phase = "open"
            try:
                cursor = collection.aggregate_raw_batches(
                    pipeline,
                    collation=collation,
                    allowDiskUse=allow_disk_use,
                    batchSize=1,
                )
                phase = "iterate"
                result[f"{label}-{route}"] = {"ok": True, "bson": b"".join(cursor)}
            except Exception as error:
                result[f"{label}-{route}"] = {
                    "ok": False,
                    "phase": phase,
                    "exception": type(error).__name__,
                    "code": getattr(error, "code", None),
                }
            finally:
                if cursor is not None:
                    cursor.close()
    return result


SEMANTIC_BSON_CONSUMER_CASES = (
    ("semantic_bson_group_representatives", bson_group_representatives),
    ("semantic_bson_group_consumers", bson_group_consumers),
    ("semantic_bson_set_consumers", bson_set_consumers),
    ("semantic_bson_add_to_set_consumers", bson_add_to_set_consumers),
    ("semantic_bson_lookup_consumers", bson_lookup_consumers),
)
