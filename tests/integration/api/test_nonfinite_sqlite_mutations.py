"""Existing JSON payloads retain NaN across classic updates and reopenings."""

import asyncio
import sqlite3

from contextlib import closing
from itertools import product

import pytest

from bson import Code, DBRef, json_util

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.core.bson_scalars import BsonDouble
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.json_compat import json_dumps_compact
from mongoeco.engines import SQLiteEngine


CELLS = list(product(("7.0", "8.0", "9.0"), ("4.9", "4.19"), ("sync", "async")))
PAYLOADS = [
    pytest.param({"value": float("nan")}, id="raw-double"),
    pytest.param({"value": {"array": [float("nan")]}}, id="nested-array"),
    pytest.param({"value": BsonDouble(float("nan"))}, id="tagged-double"),
    pytest.param({"value": Code("x", {"n": float("nan")})}, id="code-scope"),
    pytest.param({"value": DBRef("items", {"n": float("nan")})}, id="dbref-id"),
]


def canonical(document):
    return json_util.dumps(
        DocumentCodec.to_public(document),
        json_options=json_util.CANONICAL_JSON_OPTIONS,
        sort_keys=True,
    )


def physical_identity(path):
    with closing(sqlite3.connect(path)) as connection:
        return (
            list(
                connection.execute("SELECT name,sql FROM sqlite_master ORDER BY name")
            ),
            list(
                connection.execute(
                    "SELECT storage_key FROM documents ORDER BY storage_key"
                )
            ),
        )


@pytest.mark.parametrize(["target", "profile", "surface"], CELLS)
@pytest.mark.parametrize("payload", PAYLOADS)
def test_existing_nonfinite_payloads_and_new_operands_survive_classic_writes(
    tmp_path, target, profile, surface, payload
):
    path = tmp_path / "existing.sqlite"
    with MongoClient(SQLiteEngine(str(path))) as client:
        client.existing.items.insert_many([{"_id": "a"}, {"_id": "b"}])
    # Both raw JSON NaN and tagged BSON doubles existed before this fix. Keep
    # their representation, physical keys and schema; no migration is required.
    with closing(sqlite3.connect(path)) as connection, connection:
        connection.execute(
            "UPDATE documents SET document=? WHERE json_extract(document,'$._id')='a'",
            (json_dumps_compact(DocumentCodec.encode({"_id": "a", **payload})),),
        )
    original_identity = physical_identity(path)

    def sync():
        with MongoClient(
            SQLiteEngine(str(path)), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            collection = client.existing.items
            result = collection.update_one({"_id": "b"}, {"$set": payload})
            assert result.matched_count == result.modified_count == 1
            for identifier in ("a", "b"):
                expected = {"_id": identifier, **payload}
                assert canonical(collection.find_one({"_id": identifier})) == canonical(
                    expected
                )
                for update in (
                    {"$set": {"unrelated": True}},
                    {"$unset": {"unrelated": ""}},
                ):
                    result = collection.update_one({"_id": identifier}, update)
                    assert result.matched_count == result.modified_count == 1
                assert canonical(collection.find_one({"_id": identifier})) == canonical(
                    expected
                )

    async def asynchronous():
        async with AsyncMongoClient(
            SQLiteEngine(str(path)), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            collection = client.existing.items
            result = await collection.update_one({"_id": "b"}, {"$set": payload})
            assert result.matched_count == result.modified_count == 1
            for identifier in ("a", "b"):
                expected = {"_id": identifier, **payload}
                assert canonical(
                    await collection.find_one({"_id": identifier})
                ) == canonical(expected)
                for update in (
                    {"$set": {"unrelated": True}},
                    {"$unset": {"unrelated": ""}},
                ):
                    result = await collection.update_one({"_id": identifier}, update)
                    assert result.matched_count == result.modified_count == 1
                assert canonical(
                    await collection.find_one({"_id": identifier})
                ) == canonical(expected)

    if surface == "sync":
        sync()
    else:
        asyncio.run(asynchronous())
    assert physical_identity(path) == original_identity
    with MongoClient(
        SQLiteEngine(str(path)), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        for identifier in ("a", "b"):
            assert canonical(
                client.existing.items.find_one({"_id": identifier})
            ) == canonical({"_id": identifier, **payload})
