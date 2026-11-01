"""Namespace admission and prepared DDL preserve state, options and delivery."""

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson import Code
from bson.errors import InvalidDocument
from pymongo.errors import InvalidName as DriverInvalidName

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async import _collection_namespace
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError, InvalidName, OperationFailure
from mongoeco.types import CodecOptions, UuidRepresentation


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize(
    "route", ["drop-scope", "rename-surrogate", "rename-nul", "rename-empty"]
)
def test_rejected_namespace_request_precedes_ddl_and_publication(
    tmp_path, backend, route
):
    path = str(tmp_path / "namespace.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        implementation = engine()
        async with AsyncMongoClient(implementation) as client:
            database = client.test
            await database.source.insert_one({"_id": 0, "v": 1})
            await database.source.create_index("v", unique=True)
            stream = client.watch(max_await_time_ms=1)
            error = {
                "drop-scope": InvalidDocument,
                "rename-surrogate": UnicodeEncodeError,
                "rename-nul": OperationFailure,
                "rename-empty": InvalidName,
            }[route]
            try:
                with (
                    patch.object(
                        implementation,
                        "drop_collection",
                        wraps=implementation.drop_collection,
                    ) as drop,
                    patch.object(
                        implementation,
                        "rename_collection",
                        wraps=implementation.rename_collection,
                    ) as rename,
                    patch.object(
                        _collection_namespace,
                        "encode_bson_request",
                        wraps=_collection_namespace.encode_bson_request,
                    ) as admit,
                ):

                    async def invoke():
                        if route == "drop-scope":
                            await database.get_collection(
                                Code("source", {"bad": Decimal(1)})
                            ).drop()
                        else:
                            target = {
                                "rename-surrogate": "bad\ud800",
                                "rename-nul": "bad\x00name",
                                "rename-empty": "",
                            }[route]
                            await database.source.rename(target)

                    with pytest.raises(error) as caught:
                        await invoke()
                    if route == "rename-nul":
                        assert caught.value.code == 73  # noqa: PLR2004
                    if route == "rename-empty":
                        assert isinstance(caught.value, DriverInvalidName)
                    drop.assert_not_called()
                    rename.assert_not_called()
                    assert admit.call_count == (0 if route == "rename-empty" else 1)
                assert await database.source.find({}).to_list() == [{"_id": 0, "v": 1}]
                with pytest.raises(DuplicateKeyError):
                    await database.source.insert_one({"_id": 1, "v": 1})
                await database.barrier.insert_one({"_id": "after"})
                event = await stream.try_next()
                assert event["operationType"] == "insert"
                assert event["ns"]["coll"] == "barrier"
                assert await stream.try_next() is None
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert client.test.source.find_one({"_id": 0}) == {"_id": 0, "v": 1}
            with pytest.raises(DuplicateKeyError):
                client.test.source.insert_one({"_id": 2, "v": 1})


@pytest.mark.parametrize("backend", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("route", ["drop", "rename"])
def test_namespace_helpers_encode_once_but_prepared_commands_do_not(backend, route):
    async def exercise():
        async with AsyncMongoClient(backend()) as client:
            database = client.test
            await database.source.insert_one({"_id": 0})
            with patch.object(
                _collection_namespace,
                "encode_bson_request",
                wraps=_collection_namespace.encode_bson_request,
            ) as admit:
                if route == "drop":
                    await database.source.drop()
                else:
                    await database.source.rename("target")
                assert admit.call_count == 1
            with patch.object(
                _collection_namespace,
                "encode_bson_request",
                wraps=_collection_namespace.encode_bson_request,
            ) as admit:
                if route == "drop":
                    await database.command({"drop": "source"})
                else:
                    await client.admin.command(
                        {"renameCollection": "test.target", "to": "test.source"}
                    )
                admit.assert_not_called()

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_rename_returns_canonical_collection_with_original_options(backend, surface):
    options = CodecOptions(
        uuid_representation=UuidRepresentation.STANDARD, tz_aware=True
    )
    name = Code("target", {"unused": Decimal(1)})
    if surface == "sync":
        with MongoClient(backend()) as client:
            source = client.test.source.with_options(codec_options=options)
            source.insert_one({"_id": 0})
            renamed = source.rename(name)
            assert type(renamed.name) is str
            assert renamed.name == "target"
            assert renamed.codec_options == options
            assert renamed.find_one({})["_id"] == 0
        return

    async def exercise():
        async with AsyncMongoClient(backend()) as client:
            source = client.test.source.with_options(codec_options=options)
            await source.insert_one({"_id": 0})
            renamed = await source.rename(name)
            assert type(renamed.name) is str
            assert renamed.name == "target"
            assert renamed.codec_options == options
            assert await renamed.find_one({}) == {"_id": 0}

    asyncio.run(exercise())
