"""Index administration admits public frames once and preserves rejected state."""

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson.errors import InvalidDocument

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async import _collection_indexing
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError


ROUTES = ("list_indexes", "index_information", "drop_index", "drop_indexes")


async def _call(collection, route, **kwargs):
    if route == "list_indexes":
        return await collection.list_indexes(**kwargs).to_list()
    method = getattr(collection, route)
    return await method(*(["subject"] if route == "drop_index" else []), **kwargs)


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("route", ROUTES)
def test_rejected_index_admin_frame_preserves_engine_catalog_and_reopen(
    tmp_path, backend, route
):
    path = str(tmp_path / "indexes.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        implementation = engine()
        async with AsyncMongoClient(implementation) as client:
            collection = client.test.rows
            await collection.insert_one({"_id": 0, "v": 1})
            await collection.create_index("v", name="subject", unique=True)
            stream = collection.watch(max_await_time_ms=1)
            try:
                method = "list_indexes" if route == "index_information" else route
                with patch.object(
                    implementation, method, wraps=getattr(implementation, method)
                ) as execute:
                    with pytest.raises(InvalidDocument):
                        await _call(collection, route, comment=Decimal(1))
                    execute.assert_not_called()
                assert sorted(
                    index["name"] for index in await collection.list_indexes().to_list()
                ) == ["_id_", "subject"]
                await collection.insert_one({"_id": 1, "v": 2})
                event = await stream.try_next()
                assert event["operationType"] == "insert"
                assert event["documentKey"] == {"_id": 1}
                assert await stream.try_next() is None
                with pytest.raises(DuplicateKeyError):
                    await collection.insert_one({"_id": 2, "v": 1})
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert client.test.rows.count_documents({}) == 2  # noqa: PLR2004
            with pytest.raises(DuplicateKeyError):
                client.test.rows.insert_one({"_id": 3, "v": 1})


@pytest.mark.parametrize("backend", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("route", ROUTES)
def test_public_index_admin_encodes_once_and_prepared_commands_never_reencode(
    backend, route
):
    async def exercise():
        async with AsyncMongoClient(backend()) as client:
            collection = client.test.rows
            await collection.insert_one({"_id": 0, "v": 1})
            await collection.create_index("v", name="subject")
            with patch.object(
                _collection_indexing,
                "encode_bson_request",
                wraps=_collection_indexing.encode_bson_request,
            ) as admit:
                await _call(collection, route, comment={"trace": [1, True]})
                assert admit.call_count == 1
            await collection.create_index("v", name="subject")
            command = (
                {"listIndexes": collection.name}
                if route in {"list_indexes", "index_information"}
                else {
                    "dropIndexes": collection.name,
                    "index": "*" if route == "drop_indexes" else "subject",
                }
            )
            with patch.object(
                _collection_indexing,
                "encode_bson_request",
                wraps=_collection_indexing.encode_bson_request,
            ) as admit:
                await collection.database.command({**command, "comment": {"x": 1}})
                admit.assert_not_called()

    asyncio.run(exercise())
