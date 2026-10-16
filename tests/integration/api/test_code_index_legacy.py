"""Legacy SQLite scalar entries do not make BSON JavaScript a string."""

import asyncio
import sqlite3

from contextlib import closing
from itertools import product

import pytest

from bson import Code

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import SQLiteEngine
from mongoeco.errors import DuplicateKeyError


CELLS = list(product(("7.0", "8.0", "9.0"), ("4.9", "4.19"), ("sync", "async")))
DUPLICATE_KEY_CODE = 11000


def _legacy_file(path):
    with MongoClient(SQLiteEngine(str(path))) as client:
        client.legacy.items.create_index("key", unique=True)
        client.legacy.items.insert_one({"_id": "a", "key": Code("x", {"a": 1})})
    with closing(sqlite3.connect(path)) as connection, connection:
        (collection_id,) = connection.execute(
            "SELECT collection_id FROM collections WHERE db_name=? AND coll_name=?",
            ("legacy", "items"),
        ).fetchone()
        (storage_key,) = connection.execute(
            "SELECT storage_key FROM documents WHERE db_name=? AND coll_name=?",
            ("legacy", "items"),
        ).fetchone()
        # This is the physical representation written by releases which treated
        # Code as str. Reopen it without migrating payloads or storage keys.
        connection.execute(
            "INSERT INTO scalar_index_entries "
            "(collection_id,index_name,storage_key,element_type,type_score,"
            "element_key) "
            "VALUES(?,?,?,?,?,?)",
            (collection_id, "key_1", storage_key, "string", 3, "x"),
        )
        schema = list(
            connection.execute("SELECT name,sql FROM sqlite_master ORDER BY name")
        )
        document = connection.execute(
            "SELECT storage_key,document FROM documents WHERE storage_key=?",
            (storage_key,),
        ).fetchone()
    return schema, document


@pytest.mark.parametrize(["target", "profile", "surface"], CELLS)
def test_legacy_code_index_reopens_with_typed_queries_and_unique_writes(
    tmp_path, target, profile, surface
):
    path = tmp_path / "legacy.sqlite"
    schema, original = _legacy_file(path)
    options = {"mongodb_dialect": target, "pymongo_profile": profile}
    values = [Code("x", {"a": 1}), "x", Code("x", {"a": 2})]

    async def run_async():
        async with AsyncMongoClient(SQLiteEngine(str(path)), **options) as client:
            collection = client.legacy.items
            assert await collection.find_one({"key": "x"}) is None
            await collection.insert_one({"_id": "b", "key": values[1]})
            await collection.insert_one({"_id": "c", "key": values[2]})
            for identifier, value in zip("abc", values, strict=True):
                assert (await collection.find_one({"key": value}))["_id"] == identifier
                assert await collection.count_documents({"key": {"$in": [value]}}) == 1
            with pytest.raises(DuplicateKeyError) as caught:
                await collection.insert_one({"_id": "d", "key": values[0]})
            assert caught.value.code == DUPLICATE_KEY_CODE
            assert await collection.count_documents({}) == len(values)

    if surface == "async":
        asyncio.run(run_async())
    else:
        with MongoClient(SQLiteEngine(str(path)), **options) as client:
            collection = client.legacy.items
            assert collection.find_one({"key": "x"}) is None
            collection.insert_one({"_id": "b", "key": values[1]})
            collection.insert_one({"_id": "c", "key": values[2]})
            for identifier, value in zip("abc", values, strict=True):
                assert collection.find_one({"key": value})["_id"] == identifier
                assert collection.count_documents({"key": {"$in": [value]}}) == 1
            with pytest.raises(DuplicateKeyError) as caught:
                collection.insert_one({"_id": "d", "key": values[0]})
            assert caught.value.code == DUPLICATE_KEY_CODE
            assert collection.count_documents({}) == len(values)
    with closing(sqlite3.connect(path)) as connection, connection:
        assert (
            list(connection.execute("SELECT name,sql FROM sqlite_master ORDER BY name"))
            == schema
        )
        assert (
            connection.execute(
                "SELECT storage_key,document FROM documents WHERE storage_key=?",
                (original[0],),
            ).fetchone()
            == original
        )
        assert connection.execute("PRAGMA integrity_check").fetchone() == ("ok",)
