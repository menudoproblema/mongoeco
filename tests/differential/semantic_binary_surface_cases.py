"""Decoded BSON reads and assigned versus server-returned binary identifiers."""

from bson import BSON, Binary, Code, DBRef
from pymongo import InsertOne, UpdateOne


SEED = [{"_id": "initial"}]


def binary_read_surfaces(collection):
    output = {}
    payload = b"0123456789abcdef"
    for name, value in (
        ("bytes", payload),
        ("zero", Binary(payload, 0)),
        ("five", Binary(payload, 5)),
    ):
        collection.delete_many({})
        collection.insert_one(
            {
                "_id": name,
                "v": value,
                "nested": [{"v": value}],
                "code": Code("return v", {"v": value}),
                "reference": DBRef("other", value, binary=value),
            }
        )
        output[name] = {
            "find": collection.find_one({"_id": name}),
            "aggregate": list(collection.aggregate([{"$match": {"_id": name}}])),
            "distinct": collection.distinct("v"),
            "findAndModify": collection.find_one_and_update(
                {"_id": name}, {"$set": {"seen": True}}, return_document=True
            ),
        }
    return output


def binary_identifier_surfaces(collection):
    return {
        str(subtype): _binary_identifier_case(collection, subtype) for subtype in (0, 5)
    }


def _binary_identifier_case(collection, subtype):
    collection.delete_many({})
    assigned = Binary(b"assigned", subtype)
    multiple = Binary(b"multiple", subtype)
    inserted = collection.insert_one({"_id": assigned})
    inserted_many = collection.insert_many([{"_id": multiple}])
    upserted = collection.update_one(
        {"_id": Binary(b"upserted", subtype)}, {"$set": {"v": 1}}, upsert=True
    )
    bulk = collection.bulk_write(
        [UpdateOne({"_id": Binary(b"bulk", subtype)}, {"$set": {"v": 1}}, upsert=True)]
    )
    return {
        "inserted": _typed_bson_value(inserted.inserted_id),
        "inserted_many": [
            _typed_bson_value(value) for value in inserted_many.inserted_ids
        ],
        "upserted": _typed_bson_value(upserted.upserted_id),
        "bulk": [
            [index, _typed_bson_value(value)]
            for index, value in bulk.upserted_ids.items()
        ],
        "stored": [
            _typed_bson_value(doc["_id"]) for doc in collection.find({}).sort("_id")
        ],
    }


def binary_metadata_surfaces(collection):
    output = {}
    for subtype in (0, 5):
        subject = collection.database[f"metadata_{subtype}"]
        subject.drop()
        subject = collection.database.create_collection(
            subject.name,
            validator={"v": {"$eq": Binary(b"0123456789abcdef", subtype)}},
        )
        listed = list(
            collection.database.list_collections(filter={"name": subject.name})
        )
        collection.database.command(
            {
                "createIndexes": subject.name,
                "indexes": [
                    {
                        "key": {"v": 1},
                        "name": "v_1",
                        "partialFilterExpression": {
                            "v": {"$eq": Binary(b"index", subtype)}
                        },
                    }
                ],
            }
        )
        index = next(row for row in subject.list_indexes() if row["name"] == "v_1")
        output[str(subtype)] = {
            "options": _typed_bson_value(subject.options()["validator"]["v"]["$eq"]),
            "listing": _typed_bson_value(listed[0]["options"]["validator"]["v"]["$eq"]),
            "index": _typed_bson_value(index["partialFilterExpression"]["v"]["$eq"]),
        }
    return output


def binary_partial_bulk_results(collection):
    output = {}
    for subtype in (0, 5):
        for ordered in (False, True):
            collection.delete_many({})
            assigned = Binary(b"assigned", subtype)
            collection.insert_one({"_id": assigned})
            try:
                collection.bulk_write(
                    [
                        UpdateOne(
                            {"_id": Binary(b"upserted", subtype)},
                            {"$set": {"v": 1}},
                            upsert=True,
                        ),
                        InsertOne({"_id": assigned}),
                        UpdateOne(
                            {"_id": Binary(b"after", subtype)},
                            {"$set": {"v": 2}},
                            upsert=True,
                        ),
                    ],
                    ordered=ordered,
                )
            except Exception as error:
                if type(error).__name__ != "BulkWriteError":
                    raise
                details = error.details
                output[f"{subtype}/{ordered}"] = {
                    "error": type(error).__name__,
                    "upserted": [
                        [entry["index"], _typed_bson_value(entry["_id"])]
                        for entry in details["upserted"]
                    ],
                    "rejected": [
                        [
                            entry["index"],
                            entry["code"],
                            _typed_bson_value(entry["op"]["_id"]),
                            _typed_bson_value(entry["keyValue"]["_id"]),
                        ]
                        for entry in details["writeErrors"]
                    ],
                    "stored": [
                        _typed_bson_value(doc["_id"])
                        for doc in collection.find({}).sort("_id")
                    ],
                }
            else:
                message = "duplicate-key bulk must report its partial result"
                raise AssertionError(message)
    return output


def binary_duplicate_errors(collection):
    output = {}
    for subtype in (0, 5):
        for field in ("_id", "v"):
            subject = collection.database["duplicate_values"]
            subject.drop()
            if field != "_id":
                subject.create_index(field, unique=True)
            value = Binary(b"0123456789abcdef", subtype)
            subject.insert_one({"_id": 1, field: value})
            try:
                subject.insert_one({"_id": 2, field: value})
            except Exception as error:
                if type(error).__name__ != "DuplicateKeyError":
                    raise
                output[f"{subtype}/{field}"] = {
                    "error": type(error).__name__,
                    "code": error.code,
                    "key": _typed_bson_value(error.details["keyValue"][field]),
                }
            else:
                message = "duplicate binary key must fail"
                raise AssertionError(message)
    return output


def binary_command_results(collection):
    collection.delete_many({})
    output = {}
    for subtype in (0, 5):
        value = Binary(b"raw_update", subtype)
        update = collection.database.command(
            {
                "update": collection.name,
                "updates": [
                    {"q": {"_id": value}, "u": {"$set": {"v": 1}}, "upsert": True}
                ],
            }
        )
        output[f"{subtype}/update"] = _typed_bson_value(update["upserted"][0]["_id"])
        for replace in (False, True):
            value = Binary(f"find_modify_{replace}".encode(), subtype)
            reply = collection.database.command(
                {
                    "findAndModify": collection.name,
                    "query": {"_id": value},
                    "update": {"v": 1} if replace else {"$set": {"v": 1}},
                    "upsert": True,
                    "new": True,
                }
            )
            output[f"{subtype}/findAndModify/{replace}"] = {
                "upserted": _typed_bson_value(reply["lastErrorObject"]["upserted"]),
                "document": _typed_bson_value(reply["value"]["_id"]),
            }
    return output


def _typed_bson_value(value):
    # EJSON decoding itself maps Binary subtype 0 to bytes. Record the actual
    # public Python type before that transport, alongside independent BSON bytes.
    return {
        "python_type": type(value).__module__ + "." + type(value).__qualname__,
        "bson": BSON.encode({"value": value}).hex(),
    }


SEMANTIC_BINARY_SURFACE_CASES = (
    ("semantic_binary_read_surfaces", binary_read_surfaces),
    ("semantic_binary_identifier_surfaces", binary_identifier_surfaces),
    ("semantic_binary_metadata_surfaces", binary_metadata_surfaces),
    ("semantic_binary_partial_bulk_results", binary_partial_bulk_results),
    ("semantic_binary_duplicate_errors", binary_duplicate_errors),
    ("semantic_binary_command_results", binary_command_results),
)
