"""Catalog client frames are admitted once before state or publication changes."""

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson import BSON
from bson.errors import InvalidDocument
from pymongo import IndexModel

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async import _collection_indexing, database_admin
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import CodecOptions


async def _create(db, route, value):
    if route == "create":
        return await db.create_collection("aux", validator={"v": value})
    if route == "index":
        return await db.aux.create_index("v", partialFilterExpression={"v": value})
    return await db.aux.create_indexes(
        [
            IndexModel("prefix"),
            IndexModel("v", partialFilterExpression={"v": value}),
        ]
    )


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("route", ["create", "index", "indexes"])
def test_rejected_catalog_frame_keeps_namespace_indexes_and_events(
    backend, route, tmp_path
):
    path = str(tmp_path / "catalog.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        implementation = engine()
        async with AsyncMongoClient(implementation) as client:
            db = client.test
            await db.rows.insert_one({"_id": 0, "v": 1})
            await db.rows.create_index("v", unique=True)
            stream = db.watch(max_await_time_ms=1)
            try:
                with (
                    patch.object(
                        implementation,
                        "create_collection",
                        wraps=implementation.create_collection,
                    ) as create_collection,
                    patch.object(
                        implementation,
                        "create_index",
                        wraps=implementation.create_index,
                    ) as create_index,
                ):
                    with pytest.raises(InvalidDocument):
                        await _create(db, route, Decimal(1))
                    create_collection.assert_not_called()
                    create_index.assert_not_called()
                assert "aux" not in await db.list_collection_names()
                await db.rows.update_one({"_id": 0}, {"$set": {"v": 2}})
                event = await stream.try_next()
                assert event["operationType"] == "update"
                assert event["documentKey"] == {"_id": 0}
                assert event["updateDescription"]["updatedFields"] == {"v": 2}
                assert await stream.try_next() is None
                with pytest.raises(DuplicateKeyError):
                    await db.rows.insert_one({"_id": 9, "v": 2})
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert "aux" not in client.test.list_collection_names()
            assert list(client.test.rows.find()) == [{"_id": 0, "v": 2}]
            with pytest.raises(DuplicateKeyError):
                client.test.rows.insert_one({"_id": 9, "v": 2})


@pytest.mark.parametrize("route", ["create", "index", "indexes"])
def test_public_catalog_admits_once_and_prepared_commands_do_not(route):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            db = client.test
            with (
                patch.object(
                    database_admin,
                    "encode_bson_request",
                    wraps=database_admin.encode_bson_request,
                ) as create_encode,
                patch.object(
                    _collection_indexing,
                    "encode_bson_request",
                    wraps=_collection_indexing.encode_bson_request,
                ) as index_encode,
            ):
                await _create(db, route, 1)
                assert create_encode.call_count + index_encode.call_count == 1
                create_encode.reset_mock()
                index_encode.reset_mock()
                await db._admin._commands.execute_document({"create": "raw"})
                await db._admin._commands.execute_document(
                    {
                        "createIndexes": "raw",
                        "indexes": [{"key": {"v": 1}, "name": "v_1"}],
                    }
                )
                create_encode.assert_not_called()
                index_encode.assert_not_called()

    asyncio.run(exercise())


@pytest.mark.parametrize("route", ["create", "index", "indexes"])
def test_arbitrary_catalog_values_are_not_copied_before_client_rejection(route):
    class Unsupported:
        def __deepcopy__(self, memo):
            message = "copy preceded BSON admission"
            raise AssertionError(message)

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            with pytest.raises(InvalidDocument):
                await _create(client.test, route, Unsupported())

    asyncio.run(exercise())


def test_create_reply_factory_failure_preserves_created_namespace_and_return_options():
    calls = []

    class RejectReply(dict):
        def __init__(self):
            calls.append("reply")
            message = "reply factory failed"
            raise RuntimeError(message)

    with MongoClient(MemoryEngine()) as client:
        db = client.test
        options = CodecOptions(document_class=RejectReply)
        with pytest.raises(RuntimeError, match="reply factory failed"):
            db.create_collection("created", codec_options=options)
        assert calls == ["reply"]
        assert "created" in db.list_collection_names()
        assert db.create_collection(
            "kept", codec_options=CodecOptions(tz_aware=True)
        ).codec_options.tz_aware


@pytest.mark.parametrize(
    ["alias", "canonical"],
    [
        ("partialFilterExpression", "partial_filter_expression"),
        ("maxTimeMS", "max_time_ms"),
    ],
)
def test_index_alias_conflicts_reject_before_admission(alias, canonical):
    with MongoClient(MemoryEngine()) as client:
        value = {"v": 1} if alias == "partialFilterExpression" else 1
        with patch.object(_collection_indexing, "encode_bson_request") as encode:
            with pytest.raises(TypeError, match="cannot pass both"):
                client.test.rows.create_index("v", **{alias: value, canonical: value})
            encode.assert_not_called()


@pytest.mark.parametrize("name", [Decimal(0), Decimal(1), object()])
def test_index_name_is_not_truthiness_normalized_before_bson_admission(name):
    model = IndexModel("v", name=name)
    with pytest.raises(InvalidDocument):
        BSON.encode({"createIndexes": "rows", "indexes": [model.document]})
    with MongoClient(MemoryEngine()) as client:
        with pytest.raises(InvalidDocument):
            client.test.rows.create_index("v", name=name)
        assert "rows" not in client.test.list_collection_names()
