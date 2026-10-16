"""Finite native write specimens, materialized from the approved planning corpus.

This catalog imports no Mongoeco code. PyMongo owns the request models and BSON.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bson import Int64
from pymongo import (
    DeleteMany,
    DeleteOne,
    InsertOne,
    ReplaceOne,
    ReturnDocument,
    UpdateMany,
    UpdateOne,
)

from tests.differential.positional_cases import (
    POSITIONAL_UPDATE_CASES,
    _arguments,
    _bulk_models,
)


@dataclass
class WriteRequest:
    method: str
    args: list[Any]
    kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class WriteCase:
    id: str
    documents: list[dict[str, Any]]
    requests: list[WriteRequest]
    options: dict[str, Any] = field(default_factory=dict)
    indexes: list[dict[str, Any]] = field(default_factory=list)
    probe: bool = False
    admissible_documents: list[Any] | None = None


def positional_write_case(case, identifier):
    return WriteCase(
        identifier,
        case["seed_documents"],
        [WriteRequest("bulk_write", [_bulk_models(case)], {"ordered": case["ordered"]})]
        if case["api"].startswith("bulk")
        else [
            WriteRequest(case["api"], [case["query"], case["update"]], _arguments(case))
        ],
    )


WRITE_CASES = [
    positional_write_case(case, f"P{index:03d}")
    for index, case in enumerate(POSITIONAL_UPDATE_CASES, 1)
]

WRITE_CASES.append(
    WriteCase(
        "I01",
        [],
        [WriteRequest("insert_one", [{"_id": "a", "value": 1}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I02",
        [],
        [WriteRequest("insert_one", [{"value": 1}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I03",
        [{"_id": "a", "value": 1}],
        [WriteRequest("insert_one", [{"_id": "a", "value": 9}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I04",
        [],
        [WriteRequest("insert_one", [{"_id": ["a"], "value": 1}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I05",
        [],
        [
            WriteRequest(
                "insert_many",
                [[{"_id": "a", "value": 1}, {"_id": "b", "value": 2}]],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I06",
        [{"_id": "dup"}],
        [
            WriteRequest(
                "insert_many",
                [[{"_id": "a", "value": 1}, {"_id": "dup"}, {"_id": "c"}]],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I07",
        [],
        [
            WriteRequest(
                "insert_many",
                [[{"_id": "a", "value": 1}, None, {"_id": "c"}]],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "I08",
        [],
        [
            WriteRequest(
                "insert_one",
                [{"_id": "a", "nested": {"value": Int64(1)}, "items": [1]}],
                {},
            )
        ],
        options={},
        indexes=[],
        probe=True,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M01",
        [{"_id": "a", "n": 1, "items": []}],
        [
            WriteRequest(
                "update_one",
                [
                    {"_id": "a"},
                    {
                        "$set": {"state": "done"},
                        "$inc": {"n": 1},
                        "$push": {"items": "x"},
                    },
                ],
                {},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M02",
        [{"_id": "a", "value": 1}],
        [WriteRequest("update_one", [{"_id": "a"}, {"$set": {"value": 1}}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M03",
        [{"_id": "a", "n": "bad", "untouched": 0}],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a"}, {"$set": {"untouched": 1}, "$inc": {"n": 1}}],
                {},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M04",
        [{"_id": "a", "items": [{"kind": "a", "qty": 1}]}],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a"}, {"$addToSet": {"items": {"qty": 1, "kind": "a"}}}],
                {},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M05",
        [{"_id": "a", "parent": 5, "untouched": 0}],
        [
            WriteRequest(
                "update_one", [{}, {"$set": {"untouched": 1, "parent.child": 2}}], {}
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M06",
        [{"_id": "a", "name": "Ada", "legacy": True}],
        [WriteRequest("update_one", [{}, [{"$project": {"_id": 0, "name": 1}}]], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M07",
        [{"_id": "a", "value": 1}],
        [WriteRequest("update_one", [{}, [{"$set": {"_id": "b", "value": 2}}]], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "M08",
        [{"_id": "a", "n": 1}, {"_id": "b", "n": "bad"}],
        [WriteRequest("update_many", [{}, {"$inc": {"n": 1}}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=[
            [{"_id": "a", "n": 1}, {"_id": "b", "n": "bad"}],
            [{"_id": "a", "n": 2}, {"_id": "b", "n": "bad"}],
        ],
    )
)

WRITE_CASES.append(
    WriteCase(
        "R01",
        [{"_id": "a", "value": 1}],
        [WriteRequest("replace_one", [{"_id": "a"}, {"value": 2}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R02",
        [{"_id": "a", "value": 1}],
        [WriteRequest("replace_one", [{"_id": "a"}, {"value": 1}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R03",
        [{"_id": "a", "value": 1}],
        [WriteRequest("replace_one", [{"_id": "a"}, {"_id": "b", "value": 2}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R04",
        [{"_id": "a", "value": 1}],
        [
            WriteRequest(
                "find_one_and_update",
                [{"_id": "a"}, {"$inc": {"value": 1}}],
                {"return_document": ReturnDocument.BEFORE},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R05",
        [{"_id": "a", "value": 1}],
        [
            WriteRequest(
                "find_one_and_update",
                [{"_id": "a"}, {"$inc": {"value": 1}}],
                {
                    "projection": {"value": 1, "_id": 0},
                    "return_document": ReturnDocument.AFTER,
                },
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R06",
        [{"_id": "a", "rank": 1}, {"_id": "b", "rank": 2}],
        [
            WriteRequest(
                "find_one_and_replace",
                [{}, {"value": 9}],
                {
                    "sort": [("rank", -1), ("_id", 1)],
                    "return_document": ReturnDocument.BEFORE,
                },
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R07",
        [{"_id": "a", "rank": 1}, {"_id": "b", "rank": 2}],
        [
            WriteRequest(
                "find_one_and_delete",
                [{}],
                {"sort": [("rank", 1), ("_id", 1)], "projection": {"_id": 1}},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "R08",
        [
            {"_id": "a", "kind": "keep"},
            {"_id": "b", "kind": "drop"},
            {"_id": "c", "kind": "drop"},
        ],
        [
            WriteRequest("delete_one", [{"_id": "missing"}], {}),
            WriteRequest("delete_one", [{"_id": "b"}], {}),
            WriteRequest("delete_many", [{"kind": "drop"}], {}),
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U01",
        [],
        [WriteRequest("update_one", [{"_id": "a"}, {"$set": {"value": 1}}], {})],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U02",
        [],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a"}, {"$setOnInsert": {"_id": "a", "state": "new"}}],
                {"upsert": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U03",
        [],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a", "profile.name": {"$eq": "Ada"}}, {"$set": {"value": 1}}],
                {"upsert": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U04",
        [],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a"}, {"$setOnInsert": {"_id": "b"}}],
                {"upsert": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U05",
        [{"_id": "a", "value": 1}],
        [
            WriteRequest(
                "update_one",
                [{"_id": "a"}, {"$setOnInsert": {"value": 9}}],
                {"upsert": True},
            ),
            WriteRequest(
                "update_one",
                [{"_id": "a"}, {"$setOnInsert": {"value": 9}}],
                {"upsert": True},
            ),
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U06",
        [],
        [
            WriteRequest(
                "update_one", [{"_id": "a"}, [{"$set": {"value": 1}}]], {"upsert": True}
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U07",
        [],
        [
            WriteRequest(
                "find_one_and_replace",
                [{"_id": "a"}, {"value": 1}],
                {"upsert": True, "return_document": ReturnDocument.AFTER},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "U08",
        [],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        UpdateOne(*[{"_id": "a"}, {"$set": {"n": 1}}], upsert=True),
                        UpdateMany(*[{"_id": "b"}, {"$set": {"n": 2}}], upsert=True),
                        ReplaceOne(*[{"_id": "c"}, {"n": 3}], upsert=True),
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N01",
        [{"_id": "a", "email": "same"}],
        [WriteRequest("insert_one", [{"_id": "b", "email": "same"}], {})],
        options={},
        indexes=[{"keys": [["email", 1]], "unique": True}],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N02",
        [{"_id": "a", "email": "a"}, {"_id": "b", "email": "b", "untouched": 0}],
        [
            WriteRequest(
                "update_one",
                [{"_id": "b"}, {"$set": {"email": "a", "untouched": 1}}],
                {},
            )
        ],
        options={},
        indexes=[{"keys": [["email", 1]], "unique": True}],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N03",
        [{"_id": "a", "email": "a"}, {"_id": "b", "email": "b"}],
        [WriteRequest("replace_one", [{"_id": "b"}, {"email": "a"}], {})],
        options={},
        indexes=[{"keys": [["email", 1]], "unique": True}],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N04",
        [{"_id": "a", "tenant": "t", "email": "e"}],
        [WriteRequest("insert_one", [{"_id": "b", "tenant": "t", "email": "e"}], {})],
        options={},
        indexes=[{"keys": [["tenant", 1], ["email", 1]], "unique": True}],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N05",
        [{"_id": "a"}, {"_id": "b", "email": None}],
        [
            WriteRequest("insert_one", [{"_id": "c"}], {}),
            WriteRequest("insert_one", [{"_id": "d", "email": None}], {}),
        ],
        options={},
        indexes=[{"keys": [["email", 1]], "unique": True, "sparse": True}],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N06",
        [
            {"_id": "a", "email": "same", "active": True},
            {"_id": "b", "email": "same", "active": False},
        ],
        [WriteRequest("update_one", [{"_id": "b"}, {"$set": {"active": True}}], {})],
        options={},
        indexes=[
            {
                "keys": [["email", 1]],
                "unique": True,
                "partial_filter_expression": {"active": True},
            }
        ],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N07",
        [{"_id": "a", "email": "ADA"}],
        [WriteRequest("insert_one", [{"_id": "b", "email": "ada"}], {})],
        options={},
        indexes=[
            {
                "keys": [["email", 1]],
                "unique": True,
                "collation": {"locale": "en", "strength": 2},
            }
        ],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "N08",
        [{"_id": "a", "value": 1}],
        [WriteRequest("insert_one", [{"_id": "b", "value": Int64(1)}], {})],
        options={},
        indexes=[{"keys": [["value", 1]], "unique": True}],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V01",
        [],
        [WriteRequest("insert_one", [{"_id": "a"}], {})],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V02",
        [{"_id": "a", "name": "Ada", "untouched": 0}],
        [
            WriteRequest(
                "update_one",
                [{}, {"$unset": {"name": ""}, "$set": {"untouched": 1}}],
                {},
            )
        ],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V03",
        [{"_id": "a", "name": "Ada"}],
        [WriteRequest("replace_one", [{}, {"value": 1}], {})],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V04",
        [],
        [
            WriteRequest(
                "insert_many",
                [
                    [
                        {"_id": "a", "name": "Ada"},
                        {"_id": "b"},
                        {"_id": "c", "name": "Cid"},
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V05",
        [],
        [
            WriteRequest(
                "insert_many",
                [
                    [
                        {"_id": "a", "name": "Ada"},
                        {"_id": "b"},
                        {"_id": "c", "name": "Cid"},
                    ]
                ],
                {"ordered": False},
            )
        ],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V06",
        [],
        [
            WriteRequest(
                "insert_one", [{"_id": "a"}], {"bypass_document_validation": True}
            )
        ],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V07",
        [],
        [WriteRequest("insert_one", [{"_id": "a"}], {})],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "warn",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "V08",
        [{"_id": "a", "value": 0}],
        [WriteRequest("update_one", [{}, {"$set": {"value": 1}}], {})],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "moderate",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B01",
        [{"_id": "a", "n": 0}, {"_id": "gone"}],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        InsertOne(*[{"_id": "new"}]),
                        UpdateOne(*[{"_id": "a"}, {"$inc": {"n": 1}}]),
                        ReplaceOne(*[{"_id": "new"}, {"value": 1}]),
                        DeleteOne(*[{"_id": "gone"}]),
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B02",
        [{"_id": "dup"}],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        InsertOne(*[{"_id": "a"}]),
                        InsertOne(*[{"_id": "dup"}]),
                        InsertOne(*[{"_id": "c"}]),
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B03",
        [{"_id": "dup"}],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        InsertOne(*[{"_id": "a"}]),
                        InsertOne(*[{"_id": "dup"}]),
                        InsertOne(*[{"_id": "c"}]),
                    ]
                ],
                {"ordered": False},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B04",
        [],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        InsertOne(*[{"_id": "a", "name": "Ada"}]),
                        InsertOne(*[{"_id": "b"}]),
                        InsertOne(*[{"_id": "c", "name": "Cid"}]),
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={
            "validator": {
                "$jsonSchema": {
                    "required": ["name"],
                    "properties": {"name": {"bsonType": "string"}},
                }
            },
            "validationLevel": "strict",
            "validationAction": "error",
        },
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B05",
        [{"_id": "dup"}, {"_id": "bad", "n": "text"}],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        InsertOne(*[{"_id": "a"}]),
                        InsertOne(*[{"_id": "dup"}]),
                        UpdateOne(*[{"_id": "bad"}, {"$inc": {"n": 1}}]),
                        InsertOne(*[{"_id": "c"}]),
                    ]
                ],
                {"ordered": False},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B06",
        [
            {"_id": "a", "kind": "x", "n": 0},
            {"_id": "b", "kind": "x", "n": 0},
            {"_id": "c", "kind": "keep"},
        ],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        UpdateMany(*[{"kind": "x"}, {"$inc": {"n": 1}}]),
                        DeleteOne(*[{"_id": "a"}]),
                        DeleteMany(*[{"kind": "x"}]),
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B07",
        [],
        [
            WriteRequest(
                "insert_many",
                [
                    [
                        {"_id": 0},
                        {"_id": 1},
                        {"_id": 2},
                        {"_id": 3},
                        {"_id": 4},
                        {"_id": 5},
                        {"_id": 6},
                        {"_id": 7},
                        {"_id": 8},
                        {"_id": 9},
                        {"_id": 10},
                        {"_id": 11},
                        {"_id": 12},
                        {"_id": 13},
                        {"_id": 14},
                        {"_id": 15},
                        {"_id": 16},
                        {"_id": 17},
                        {"_id": 18},
                        {"_id": 19},
                        {"_id": 20},
                        {"_id": 21},
                        {"_id": 22},
                        {"_id": 23},
                        {"_id": 24},
                        {"_id": 25},
                        {"_id": 26},
                        {"_id": 27},
                        {"_id": 28},
                        {"_id": 29},
                        {"_id": 30},
                        {"_id": 31},
                        {"_id": 32},
                        {"_id": 33},
                        {"_id": 34},
                        {"_id": 35},
                        {"_id": 36},
                        {"_id": 37},
                        {"_id": 38},
                        {"_id": 39},
                        {"_id": 40},
                        {"_id": 41},
                        {"_id": 42},
                        {"_id": 43},
                        {"_id": 44},
                        {"_id": 45},
                        {"_id": 46},
                        {"_id": 47},
                        {"_id": 48},
                        {"_id": 49},
                        {"_id": 50},
                        {"_id": 51},
                        {"_id": 52},
                        {"_id": 53},
                        {"_id": 54},
                        {"_id": 55},
                        {"_id": 56},
                        {"_id": 57},
                        {"_id": 58},
                        {"_id": 59},
                        {"_id": 60},
                        {"_id": 61},
                        {"_id": 62},
                        {"_id": 63},
                        {"_id": 64},
                        {"_id": 65},
                        {"_id": 66},
                        {"_id": 67},
                        {"_id": 68},
                        {"_id": 69},
                        {"_id": 70},
                        {"_id": 71},
                        {"_id": 72},
                        {"_id": 73},
                        {"_id": 74},
                        {"_id": 75},
                        {"_id": 76},
                        {"_id": 77},
                        {"_id": 78},
                        {"_id": 79},
                        {"_id": 80},
                        {"_id": 81},
                        {"_id": 82},
                        {"_id": 83},
                        {"_id": 84},
                        {"_id": 85},
                        {"_id": 86},
                        {"_id": 87},
                        {"_id": 88},
                        {"_id": 89},
                        {"_id": 90},
                        {"_id": 91},
                        {"_id": 92},
                        {"_id": 93},
                        {"_id": 94},
                        {"_id": 95},
                        {"_id": 96},
                        {"_id": 97},
                        {"_id": 98},
                        {"_id": 99},
                        {"_id": 100},
                        {"_id": 101},
                        {"_id": 102},
                        {"_id": 103},
                        {"_id": 104},
                        {"_id": 105},
                        {"_id": 106},
                        {"_id": 107},
                        {"_id": 108},
                        {"_id": 109},
                        {"_id": 110},
                        {"_id": 111},
                        {"_id": 112},
                        {"_id": 113},
                        {"_id": 114},
                        {"_id": 115},
                        {"_id": 116},
                        {"_id": 117},
                        {"_id": 118},
                        {"_id": 119},
                        {"_id": 120},
                        {"_id": 121},
                        {"_id": 122},
                        {"_id": 123},
                        {"_id": 124},
                        {"_id": 125},
                        {"_id": 126},
                        {"_id": 127},
                        {"_id": 128},
                        {"_id": 129},
                        {"_id": 130},
                        {"_id": 131},
                        {"_id": 132},
                        {"_id": 133},
                        {"_id": 134},
                        {"_id": 135},
                        {"_id": 136},
                        {"_id": 137},
                        {"_id": 138},
                        {"_id": 139},
                        {"_id": 140},
                        {"_id": 141},
                        {"_id": 142},
                        {"_id": 143},
                        {"_id": 144},
                        {"_id": 145},
                        {"_id": 146},
                        {"_id": 147},
                        {"_id": 148},
                        {"_id": 149},
                        {"_id": 150},
                        {"_id": 151},
                        {"_id": 152},
                        {"_id": 153},
                        {"_id": 154},
                        {"_id": 155},
                        {"_id": 156},
                        {"_id": 157},
                        {"_id": 158},
                        {"_id": 159},
                        {"_id": 160},
                        {"_id": 161},
                        {"_id": 162},
                        {"_id": 163},
                        {"_id": 164},
                        {"_id": 165},
                        {"_id": 166},
                        {"_id": 167},
                        {"_id": 168},
                        {"_id": 169},
                        {"_id": 170},
                        {"_id": 171},
                        {"_id": 172},
                        {"_id": 173},
                        {"_id": 174},
                        {"_id": 175},
                        {"_id": 176},
                        {"_id": 177},
                        {"_id": 178},
                        {"_id": 179},
                        {"_id": 180},
                        {"_id": 181},
                        {"_id": 182},
                        {"_id": 183},
                        {"_id": 184},
                        {"_id": 185},
                        {"_id": 186},
                        {"_id": 187},
                        {"_id": 188},
                        {"_id": 189},
                        {"_id": 190},
                        {"_id": 191},
                        {"_id": 192},
                        {"_id": 193},
                        {"_id": 194},
                        {"_id": 195},
                        {"_id": 196},
                        {"_id": 197},
                        {"_id": 198},
                        {"_id": 199},
                        {"_id": 200},
                        {"_id": 201},
                        {"_id": 202},
                        {"_id": 203},
                        {"_id": 204},
                        {"_id": 205},
                        {"_id": 206},
                        {"_id": 207},
                        {"_id": 208},
                        {"_id": 209},
                        {"_id": 210},
                        {"_id": 211},
                        {"_id": 212},
                        {"_id": 213},
                        {"_id": 214},
                        {"_id": 215},
                        {"_id": 216},
                        {"_id": 217},
                        {"_id": 218},
                        {"_id": 219},
                        {"_id": 220},
                        {"_id": 221},
                        {"_id": 222},
                        {"_id": 223},
                        {"_id": 224},
                        {"_id": 225},
                        {"_id": 226},
                        {"_id": 227},
                        {"_id": 228},
                        {"_id": 229},
                        {"_id": 230},
                        {"_id": 231},
                        {"_id": 232},
                        {"_id": 233},
                        {"_id": 234},
                        {"_id": 235},
                        {"_id": 236},
                        {"_id": 237},
                        {"_id": 238},
                        {"_id": 239},
                        {"_id": 240},
                        {"_id": 241},
                        {"_id": 242},
                        {"_id": 243},
                        {"_id": 244},
                        {"_id": 245},
                        {"_id": 246},
                        {"_id": 247},
                        {"_id": 248},
                        {"_id": 249},
                        {"_id": 250},
                        {"_id": 251},
                        {"_id": 252},
                        {"_id": 253},
                        {"_id": 254},
                        {"_id": 255},
                        {"_id": 256},
                        {"_id": 257},
                        {"_id": 258},
                        {"_id": 0},
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)

WRITE_CASES.append(
    WriteCase(
        "B08",
        [],
        [
            WriteRequest(
                "bulk_write",
                [
                    [
                        InsertOne(*[{"_id": "a"}]),
                        UpdateOne(*[{"_id": "b"}, {"replacement_without_operator": 1}]),
                    ]
                ],
                {"ordered": True},
            )
        ],
        options={},
        indexes=[],
        probe=False,
        admissible_documents=None,
    )
)
