"""Logical identity equality must preserve physical rows and write boundaries."""

import asyncio
import hashlib
import json
import shutil
import sqlite3

from contextlib import closing
from pathlib import Path

import pytest

from bson import Decimal128, Int64

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, DuplicateKeyError, OperationFailure
from mongoeco.types import Decimal128 as InternalDecimal128

from tests.support import ENGINE_FACTORIES


DUPLICATE_KEY_CODE = 11000
FIXTURE = (
    Path(__file__).parents[2] / "fixtures/sqlite/numeric-identities-before-fix.sqlite"
)


def snapshot(database):
    with closing(sqlite3.connect(database)) as conn:
        return (
            conn.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall(),
            conn.execute(
                "SELECT coll_name,storage_key FROM documents ORDER BY coll_name"
            ).fetchall(),
        )


@pytest.mark.parametrize("surface", ["sync", "async"])
def test_legacy_sqlite_numeric_ids_keep_physical_keys_and_indexes(tmp_path, surface):
    metadata = json.loads(FIXTURE.with_suffix(".json").read_text())
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == metadata["sha256"]
    database = tmp_path / "legacy.sqlite"
    shutil.copyfile(FIXTURE, database)
    before = snapshot(database)
    queries = [Int64(1), 2.0, Decimal128("3"), 4, {"n": Int64(5)}]

    async def exercise():
        async with AsyncMongoClient(SQLiteEngine(str(database))) as client:
            for n, equivalent in enumerate(queries):
                collection = client.legacy[f"items{n}"]
                assert (await collection.find_one({"_id": equivalent}))["value"] == 0
                result = await collection.update_one(
                    {"_id": equivalent}, {"$set": {"value": 1}}, upsert=True
                )
                assert (
                    result.matched_count,
                    result.modified_count,
                    result.did_upsert,
                ) == (1, 1, False)
                with pytest.raises(DuplicateKeyError) as error:
                    await collection.insert_one({"_id": equivalent})
                assert error.value.code == DUPLICATE_KEY_CODE
                assert (
                    await collection.count_documents({"value": 1}, hint="value_1") == 1
                )

    if surface == "async":
        asyncio.run(exercise())
    else:
        with MongoClient(SQLiteEngine(str(database))) as client:
            for n, equivalent in enumerate(queries):
                collection = client.legacy[f"items{n}"]
                assert collection.find_one({"_id": equivalent})["value"] == 0
                result = collection.update_one(
                    {"_id": equivalent}, {"$set": {"value": 1}}, upsert=True
                )
                assert (
                    result.matched_count,
                    result.modified_count,
                    result.did_upsert,
                ) == (1, 1, False)
                with pytest.raises(DuplicateKeyError) as error:
                    collection.insert_one({"_id": equivalent})
                assert error.value.code == DUPLICATE_KEY_CODE
                assert collection.count_documents({"value": 1}, hint="value_1") == 1
    assert snapshot(database) == before
    with MongoClient(SQLiteEngine(str(database))) as reopened:
        for n, equivalent in enumerate(queries):
            assert (
                reopened.legacy[f"items{n}"].find_one({"_id": equivalent})["value"] == 1
            )
    assert snapshot(database) == before


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("ordered", [True, False])
def test_same_batch_numeric_duplicate_preserves_prefix_suffix_and_events(
    engine, ordered
):
    async def exercise():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            collection = client.batch.items
            stream = collection.watch(max_await_time_ms=5)
            try:
                with pytest.raises(BulkWriteError) as error:
                    await collection.insert_many(
                        [{"_id": 1}, {"_id": Int64(1)}, {"_id": "after"}],
                        ordered=ordered,
                    )
                details = error.value.details
                assert [(e["index"], e["code"]) for e in details["writeErrors"]] == [
                    (1, 11000)
                ]
                assert details["nInserted"] == (1 if ordered else 2)
                ids = [doc["_id"] for doc in await collection.find().to_list()]
                assert ids == ([1] if ordered else [1, "after"])
                events = []
                while (event := await stream.try_next()) is not None:
                    events.append(event["documentKey"]["_id"])
                assert events == ids
                await collection.insert_one({"_id": "recovery"})
                assert (await stream.try_next())["documentKey"] == {"_id": "recovery"}
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
def test_engine_id_lookup_and_delete_accept_numeric_equivalents(engine):
    async def exercise():
        backend = ENGINE_FACTORIES[engine]()
        await backend.connect()
        try:
            assert await backend.insert_document("db", "items", {"_id": 1, "value": 2})
            assert await backend.get_document("db", "items", Int64(1)) == {
                "_id": 1,
                "value": 2,
            }
            assert await backend.delete_document("db", "items", InternalDecimal128("1"))
            assert await backend.get_document("db", "items", 1) is None
        finally:
            await backend.disconnect()

    asyncio.run(exercise())


def test_two_sqlite_connections_enforce_numeric_uniqueness(tmp_path):
    async def exercise():
        database = str(tmp_path / "race.sqlite")
        async with (
            AsyncMongoClient(SQLiteEngine(database)) as left,
            AsyncMongoClient(SQLiteEngine(database)) as right,
        ):
            results = await asyncio.gather(
                left.race.items.insert_one({"_id": 1}),
                right.race.items.insert_one({"_id": Int64(1)}),
                return_exceptions=True,
            )
            failures = [result for result in results if isinstance(result, Exception)]
            assert len(failures) == 1
            assert isinstance(failures[0], DuplicateKeyError)
            assert failures[0].code == DUPLICATE_KEY_CODE
            assert await left.race.items.count_documents({}) == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("family", ["update", "delete"])
def test_missing_query_preflight_does_not_publish_valid_prefix(engine, family):
    async def exercise():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            collection = client.preflight.items
            await collection.insert_one({"_id": "a", "value": 0})
            stream = collection.watch(max_await_time_ms=5)
            try:
                spec = (
                    {"u": {"$set": {"value": 1}}}
                    if family == "update"
                    else {"limit": 0}
                )
                command = {
                    family: "items",
                    family + "s": [{"q": {}, **spec}, spec],
                    "ordered": False,
                }
                with pytest.raises(OperationFailure) as error:
                    await client.preflight.command(command)
                assert error.value.code == 40414  # noqa: PLR2004 - native IDLFailedToParse
                assert await collection.find_one() == {"_id": "a", "value": 0}
                assert await stream.try_next() is None
                await collection.insert_one({"_id": "recovery"})
                assert (await stream.try_next())["documentKey"] == {"_id": "recovery"}
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())
