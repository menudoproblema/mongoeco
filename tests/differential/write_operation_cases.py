"""Finite payload specimens; native PyMongo owns models and the error oracle."""

from copy import deepcopy
from datetime import UTC, datetime

from bson import Binary, DBRef, Decimal128, Int64, ObjectId, Regex
from pymongo import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne

from tests.differential.write_cases import WriteCase, WriteRequest


def bson_payload():
    return {
        "nested": {"second": Int64(2), "first": Decimal128("1.25")},
        "items": [Binary(b"payload", 128), ObjectId("000000000000000000000123")],
        "date": datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=UTC),
        "reference": DBRef("other", "id"),
        "regex": Regex("a.*", "i"),
    }


def specimens():  # noqa: PLR0912 - finite native specimen inventory
    cases = []
    validator = {"$jsonSchema": {"required": ["name"]}}
    for ordered in (True, False):
        cases.append(
            WriteCase(
                f"sort_batch_preflight_{ordered}",
                [{"_id": "seed", "count": 0}],
                [
                    WriteRequest(
                        "bulk_write",
                        [
                            [
                                InsertOne({"_id": "before"}),
                                UpdateOne({"_id": "seed"}, {"$inc": {"count": 1}}),
                                UpdateOne(
                                    {"_id": "seed"},
                                    {"$inc": {"count": "invalid"}},
                                    sort={"_id": 1},
                                ),
                                DeleteOne({"_id": "before"}),
                            ]
                        ],
                        {"ordered": ordered},
                    )
                ],
            )
        )
        for mode in ("duplicate", "schema", "invalid_id", "mixed", "shared"):
            first = {"_id": "a", "name": "Ada"}
            documents = [
                first,
                {"_id": "a", **bson_payload()},
                {"_id": "c", "name": "Cid"},
            ]
            options = {}
            if mode == "schema":
                documents[1].pop("_id")
                options = {"validator": deepcopy(validator)}
            elif mode == "invalid_id":
                documents[1]["_id"] = ["invalid"]
            elif mode == "mixed":
                options = {"validator": deepcopy(validator)}
                documents += [{"_id": ["invalid"]}, {"_id": "z", "name": "Zed"}]
            elif mode == "shared":
                first.pop("_id")
                documents = [first, first, {"name": "Cid"}]
            for method in ("insert_many", "bulk_write"):
                payload = deepcopy(documents)
                if method == "bulk_write":
                    payload = [InsertOne(doc) for doc in payload]
                cases.append(
                    WriteCase(
                        f"{method}_{mode}_{ordered}",
                        [],
                        [WriteRequest(method, [payload], {"ordered": ordered})],
                        options=deepcopy(options),
                    )
                )
        for model in (UpdateOne, UpdateMany, ReplaceOne, DeleteOne, DeleteMany):
            options = {
                "hint": "missing_index",
                "collation": {"locale": "en", "strength": 2},
            }
            query = {"name": "Ada", "literal": {"$ne": Int64(9)}}
            if model in (UpdateOne, ReplaceOne):
                options["sort"] = {"name": 1}
            if model in (UpdateOne, UpdateMany):
                options["array_filters"] = [{"item.key": "match"}]
                request = model(
                    query,
                    {"$set": {"items.$[item].value": Int64(2)}},
                    upsert=True,
                    **options,
                )
            elif model is ReplaceOne:
                request = model(
                    query,
                    {"name": "Replacement", **bson_payload()},
                    upsert=True,
                    **options,
                )
            else:
                request = model(query, **options)
            cases.append(
                WriteCase(
                    f"{model.__name__}_options_{ordered}",
                    [{"_id": "seed", "name": "Ada"}],
                    [
                        WriteRequest(
                            "bulk_write",
                            [
                                [
                                    InsertOne({"_id": "before"}),
                                    request,
                                    InsertOne({"_id": "after"}),
                                ]
                            ],
                            {"ordered": ordered},
                        )
                    ],
                )
            )
        cases.extend(
            WriteCase(
                f"{model.__name__}_type_error_{ordered}",
                [{"_id": "a", "count": 1}, {"_id": "b", "count": "invalid"}],
                [
                    WriteRequest(
                        "bulk_write",
                        [
                            [
                                model(
                                    {"_id": "b"}
                                    if model is UpdateOne
                                    else {"count": {"$exists": True}},
                                    {"$inc": {"count": 1}},
                                ),
                                InsertOne({"_id": "after"}),
                            ]
                        ],
                        {"ordered": ordered},
                    )
                ],
            )
            for model in (UpdateOne, UpdateMany)
        )
        cases.append(
            WriteCase(
                f"mixed_families_{ordered}",
                [{"_id": "a", "count": "invalid"}, {"_id": "delete"}],
                [
                    WriteRequest(
                        "bulk_write",
                        [
                            [
                                UpdateOne(
                                    {"_id": "upsert"},
                                    {"$set": {"count": 1}},
                                    upsert=True,
                                ),
                                InsertOne({"_id": "insert"}),
                                DeleteOne({"_id": "delete"}),
                                UpdateOne({"_id": "a"}, {"$inc": {"count": 1}}),
                                InsertOne({"_id": "a", **bson_payload()}),
                                DeleteMany({"_id": "absent"}),
                                InsertOne({"_id": "last"}),
                            ]
                        ],
                        {"ordered": ordered},
                    )
                ],
            )
        )
        cases.append(
            WriteCase(
                f"replace_identity_{ordered}",
                [{"_id": "a"}],
                [
                    WriteRequest(
                        "bulk_write",
                        [
                            [
                                ReplaceOne(
                                    {"_id": "a"}, {"_id": "changed", **bson_payload()}
                                ),
                                InsertOne({"_id": "after"}),
                            ]
                        ],
                        {"ordered": ordered},
                    )
                ],
            )
        )
        cases.append(
            WriteCase(
                f"pipeline_failure_{ordered}",
                [{"_id": "a"}],
                [
                    WriteRequest(
                        "bulk_write",
                        [
                            [
                                UpdateOne({"_id": "a"}, [{"$set": {"_id": "changed"}}]),
                                InsertOne({"_id": "after"}),
                            ]
                        ],
                        {"ordered": ordered},
                    )
                ],
            )
        )
    return cases


WRITE_OPERATION_CASES = specimens()
