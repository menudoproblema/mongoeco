"""BSON ID conversions preserve durable formats, row ownership and publication."""

import asyncio
import hashlib
import json
import shutil
import sqlite3

from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from bson import Decimal128, Int64, json_util

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import SearchIndexModel

from tests.support import ENGINE_FACTORIES


FIXTURE = (
    Path(__file__).parents[2] / "fixtures/sqlite/numeric-identities-before-fix.sqlite"
)
DUPLICATE_KEY_CODE = 11000


def bson_value(value):
    return json_util.dumps(value, json_options=json_util.CANONICAL_JSON_OPTIONS)


def schema_and_rows(database):
    with closing(sqlite3.connect(database)) as conn:
        return (
            conn.execute(
                "SELECT type,name,sql FROM sqlite_master ORDER BY type,name"
            ).fetchall(),
            conn.execute(
                "SELECT coll_name,rowid,storage_key FROM documents ORDER BY coll_name"
            ).fetchall(),
        )


@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("pipeline", [True, False])
def test_existing_sqlite_file_converts_only_target_rows_and_reopens(
    tmp_path, surface, pipeline
):
    original_hash = hashlib.sha256(FIXTURE.read_bytes()).hexdigest()
    assert (
        original_hash == json.loads(FIXTURE.with_suffix(".json").read_text())["sha256"]
    )
    database = tmp_path / "previous.sqlite"
    shutil.copyfile(FIXTURE, database)
    before_schema, before_rows = schema_and_rows(database)
    pairs = [
        (1, 1.0),
        (Int64(2), 2),
        (3.0, Int64(3)),
        (Decimal128("4"), 4.0),
        ({"n": 5}, {"n": Int64(5)}),
    ]

    def specification(converted):
        return (
            [{"$set": {"_id": {"$literal": converted}}}]
            if pipeline
            else {"_id": converted, "value": 0}
        )

    async def exercise():
        async with AsyncMongoClient(SQLiteEngine(str(database))) as client:
            for n, (original, converted) in enumerate(pairs):
                collection = client.legacy[f"items{n}"]
                method = collection.update_one if pipeline else collection.replace_one
                result = await method({"_id": original}, specification(converted))
                assert (result.matched_count, result.modified_count) == (1, 1)
                assert bson_value(
                    (await collection.find_one({"_id": original}))["_id"]
                ) == bson_value(converted)
                result = await method({"_id": converted}, specification(converted))
                assert result.modified_count == 0
                with pytest.raises(DuplicateKeyError) as error:
                    await collection.insert_one({"_id": original})
                assert error.value.code == DUPLICATE_KEY_CODE
                assert (
                    await collection.count_documents({"value": 0}, hint="value_1") == 1
                )

    if surface == "async":
        asyncio.run(exercise())
    else:
        with MongoClient(SQLiteEngine(str(database))) as client:
            for n, (original, converted) in enumerate(pairs):
                collection = client.legacy[f"items{n}"]
                method = collection.update_one if pipeline else collection.replace_one
                result = method({"_id": original}, specification(converted))
                assert (result.matched_count, result.modified_count) == (1, 1)
                assert bson_value(
                    collection.find_one({"_id": original})["_id"]
                ) == bson_value(converted)
                assert (
                    method({"_id": converted}, specification(converted)).modified_count
                    == 0
                )
                with pytest.raises(DuplicateKeyError) as error:
                    collection.insert_one({"_id": original})
                assert error.value.code == DUPLICATE_KEY_CODE
                assert collection.count_documents({"value": 0}, hint="value_1") == 1
    after_schema, after_rows = schema_and_rows(database)
    assert after_schema == before_schema
    assert [row[:2] for row in after_rows] == [row[:2] for row in before_rows]
    assert all(
        left[2] != right[2] for left, right in zip(before_rows, after_rows, strict=True)
    )
    with MongoClient(SQLiteEngine(str(database))) as client:
        for n, (original, converted) in enumerate(pairs):
            collection = client.legacy[f"items{n}"]
            assert bson_value(
                collection.find_one({"_id": original})["_id"]
            ) == bson_value(converted)
            result = collection.update_one(
                {"_id": original}, {"$set": {"value": 1}}, upsert=True
            )
            assert (result.matched_count, result.modified_count, result.did_upsert) == (
                1,
                1,
                False,
            )
            assert collection.delete_one({"_id": converted}).deleted_count == 1
            collection.insert_one({"_id": original})
    assert hashlib.sha256(FIXTURE.read_bytes()).hexdigest() == original_hash


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("pipeline", [True, False])
def test_conversion_preserves_indexes_search_ttl_and_exactly_one_event(
    engine, pipeline
):
    async def exercise():
        backend = ENGINE_FACTORIES[engine]()
        async with AsyncMongoClient(backend) as client:
            collection = client.convert.items
            expires = datetime.now(UTC) + timedelta(days=1)
            document = {
                "_id": 1,
                "value": 0,
                "tags": ["a", "b"],
                "title": "Ada bridge",
                "expires": expires,
                "embedding": [1.0, 0.0],
            }
            await collection.insert_one(document)
            await collection.create_index("value")
            await collection.create_index("tags")
            await collection.create_index("expires", expire_after_seconds=0)
            await collection.create_search_index(
                SearchIndexModel({"mappings": {"dynamic": True}}, name="text")
            )
            await collection.create_search_index(
                SearchIndexModel(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 2,
                                "similarity": "cosine",
                            }
                        ]
                    },
                    name="vectors",
                    type="vectorSearch",
                )
            )
            vector_query = [
                {
                    "$vectorSearch": {
                        "index": "vectors",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "numCandidates": 2,
                        "limit": 1,
                    }
                }
            ]
            stream = collection.watch(max_await_time_ms=5)
            try:
                if pipeline:
                    result = await collection.update_one({}, [{"$set": {"_id": 1.0}}])
                else:
                    result = await collection.replace_one({}, {**document, "_id": 1.0})
                assert result.modified_count == 1
                event = await stream.try_next()
                assert event["operationType"] == ("update" if pipeline else "replace")
                assert bson_value(event["documentKey"]) == bson_value({"_id": 1.0})
                assert await stream.try_next() is None
                assert (
                    await collection.count_documents({"value": 0}, hint="value_1") == 1
                )
                assert (
                    await collection.count_documents({"tags": "b"}, hint="tags_1") == 1
                )
                hits = await collection.aggregate(
                    [
                        {
                            "$search": {
                                "index": "text",
                                "text": {"query": "bridge", "path": "title"},
                            }
                        }
                    ]
                ).to_list()
                assert len(hits) == 1
                assert bson_value(hits[0]["_id"]) == bson_value(1.0)
                vectors = await collection.aggregate(vector_query).to_list()
                assert len(vectors) == 1
                assert bson_value(vectors[0]["_id"]) == bson_value(1.0)
                await collection.delete_one({"_id": 1})
                assert (
                    await collection.count_documents({"tags": "b"}, hint="tags_1") == 0
                )
                assert (
                    await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "text",
                                    "text": {"query": "bridge", "path": "title"},
                                }
                            }
                        ]
                    ).to_list()
                    == []
                )
                assert await collection.aggregate(vector_query).to_list() == []
                if isinstance(backend, SQLiteEngine):
                    connection = backend._require_connection()
                    for table in (
                        "multikey_entries",
                        "scalar_index_entries",
                        "ttl_index_entries",
                    ):
                        assert connection.execute(
                            f"SELECT COUNT(*) FROM {table}"  # noqa: S608 - fixed table names
                        ).fetchone() == (0,)
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("transaction", [True, False])
def test_memory_rekey_preserves_order_snapshot_and_rolls_back_failure(transaction):
    async def exercise():
        backend = MemoryEngine()
        async with AsyncMongoClient(backend) as client:
            collection = client.memory.items
            await collection.insert_many([{"_id": n, "key": "same"} for n in (0, 1, 2)])
            await collection.create_index("key")
            old = backend._storage["memory"]["items"].copy()
            session = client.start_session() if transaction else None
            if session is not None:
                session.start_transaction()
            try:
                await collection.update_one(
                    {"_id": 1}, [{"$set": {"_id": 1.0}}], session=session
                )
                rows = await collection.find({"key": "same"}, session=session).to_list()
                assert [bson_value(row["_id"]) for row in rows] == [
                    bson_value(v) for v in (0, 1.0, 2)
                ]
                assert backend._storage_key(1) in old
                assert backend._storage_key(1.0) not in old
                if session is not None:
                    session.abort_transaction()
                    assert bson_value(
                        (await collection.find_one({"_id": 1}))["_id"]
                    ) == bson_value(1)
            finally:
                if session is not None:
                    session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
def test_rekey_failure_restores_document_indexes_and_event_stream(engine):
    async def exercise():
        backend = ENGINE_FACTORIES[engine]()
        async with AsyncMongoClient(backend) as client:
            collection = client.rollback.items
            await collection.insert_one({"_id": 1, "key": "same"})
            await collection.create_index("key")
            stream = collection.watch(max_await_time_ms=5)
            method = (
                "_update_indexes_locked"
                if engine == "memory"
                else "_rebuild_scalar_entries_for_document"
            )
            original_method = getattr(backend, method)

            def fail_after_rekey(*args, **kwargs):
                original_method(*args, **kwargs)
                if engine == "sqlite" or kwargs.get("action") == "insert":
                    message = "injected rekey failure"
                    raise RuntimeError(message)

            try:
                with (
                    patch.object(backend, method, side_effect=fail_after_rekey),
                    pytest.raises(RuntimeError, match="injected rekey failure"),
                ):
                    await collection.update_one({}, [{"$set": {"_id": 1.0}}])
                assert bson_value(
                    (await collection.find_one({"key": "same"}, hint="key_1"))["_id"]
                ) == bson_value(1)
                assert await stream.try_next() is None
                result = await collection.update_one({}, [{"$set": {"_id": 1.0}}])
                assert result.modified_count == 1
                assert (await stream.try_next())["documentKey"] == {"_id": 1.0}
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("commit", [True, False])
def test_conversion_transaction_publishes_only_after_commit(tmp_path, engine, commit):
    async def exercise():
        backend = (
            MemoryEngine()
            if engine == "memory"
            else SQLiteEngine(str(tmp_path / "transaction.sqlite"))
        )
        async with AsyncMongoClient(backend) as client:
            collection = client.transaction.items
            await collection.insert_one({"_id": 1})
            stream = collection.watch(max_await_time_ms=5)
            session = client.start_session()
            session.start_transaction()
            try:
                result = await collection.update_one(
                    {}, [{"$set": {"_id": 1.0}}], session=session
                )
                assert result.modified_count == 1
                assert bson_value(
                    (await collection.find_one({"_id": 1}, session=session))["_id"]
                ) == bson_value(1.0)
                assert await stream.try_next() is None
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                expected = 1.0 if commit else 1
                assert bson_value(
                    (await collection.find_one({"_id": 1}))["_id"]
                ) == bson_value(expected)
                event = await stream.try_next()
                if commit:
                    assert bson_value(event["documentKey"]) == bson_value({"_id": 1.0})
                else:
                    assert event is None
                assert await stream.try_next() is None
            finally:
                if session.in_transaction:
                    session.abort_transaction()
                session.close()
                stream.close()

    asyncio.run(exercise())
