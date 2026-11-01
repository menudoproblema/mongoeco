"""Administrative reads admit once and use prepared internal readers."""

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson.errors import InvalidDocument
from pymongo import MongoClient as DriverClient
from pymongo.errors import CollectionInvalid as DriverCollectionInvalid

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api import bson_input
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import CollectionInvalid


ROUTES = ("names", "collections", "validate")


async def _call(database, route, **kwargs):
    if route == "names":
        return await database.list_collection_names(**kwargs)
    if route == "collections":
        return await database.list_collections(**kwargs).to_list()
    return await database.validate_collection("rows", **kwargs)


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("slot", ["comment", "payload"])
def test_rejected_admin_frame_precedes_reads_and_preserves_state(
    tmp_path, backend, route, slot
):
    path = str(tmp_path / "admin.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        implementation = engine()
        async with AsyncMongoClient(implementation) as client:
            database = client.test
            await database.rows.insert_one({"_id": 0, "v": 1})
            stream = database.watch(max_await_time_ms=1)
            field = (
                "comment"
                if slot == "comment"
                else ("full" if route == "validate" else "filter")
            )
            value = Decimal(1) if field != "filter" else {"name": Decimal(1)}
            try:
                with patch.object(
                    implementation,
                    "list_collections",
                    wraps=implementation.list_collections,
                ) as listing:
                    with pytest.raises(InvalidDocument):
                        await _call(database, route, **{field: value})
                    listing.assert_not_called()
                assert await database.rows.find({}).to_list() == [{"_id": 0, "v": 1}]
                await database.rows.insert_one({"_id": 1, "v": 2})
                event = await stream.try_next()
                assert event["operationType"] == "insert"
                assert event["documentKey"] == {"_id": 1}
                assert await stream.try_next() is None
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert list(client.test.rows.find().sort("_id")) == [
                {"_id": 0, "v": 1},
                {"_id": 1, "v": 2},
            ]


@pytest.mark.parametrize("backend", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("route", ROUTES)
def test_admin_reads_encode_once_and_prepared_execution_encodes_zero(backend, route):
    async def exercise():
        async with AsyncMongoClient(backend()) as client:
            database = client.test
            await database.rows.insert_one({"_id": 0})
            with patch.object(
                bson_input,
                "_encode_bson_document",
                wraps=bson_input._encode_bson_document,
            ) as encoder:
                await _call(database, route, comment={"trace": [1, True]})
                assert encoder.call_count == 1
            command = (
                {"validate": "rows", "full": True}
                if route == "validate"
                else {"listCollections": 1}
            )
            admitted = bson_input.encode_bson_request(
                command,
                codec_options=database.codec_options,
            )
            with patch.object(
                bson_input,
                "_encode_bson_document",
                wraps=bson_input._encode_bson_document,
            ) as encoder:
                await database._admin._commands.execute_document(admitted)
                encoder.assert_not_called()

    asyncio.run(exercise())


def test_validate_invalid_reply_uses_driver_collection_invalid_contract():
    reply = {"ok": 1.0, "valid": False}
    with DriverClient(connect=False, serverSelectionTimeoutMS=1) as driver:
        database = driver.test
        with (
            patch.object(database, "command", return_value=reply),
            pytest.raises(DriverCollectionInvalid) as native,
        ):
            database.validate_collection("rows")

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            database = client.test
            with (
                patch.object(database._admin._commands, "command", return_value=reply),
                pytest.raises(CollectionInvalid) as local,
            ):
                await database.validate_collection("rows")
            assert str(local.value) == str(native.value)

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("route", ["names", "collections"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_listing_comments_survive_all_facades_into_session_metadata(
    backend, route, surface
):
    marker = "administrative listing comment"

    def check(session):
        recorded = [
            state["last_operation"]
            for state in session.engine_state.values()
            if isinstance(state, dict) and "last_operation" in state
        ]
        assert len(recorded) == 1
        assert recorded[0]["comment"] == marker

    if surface == "sync":
        with MongoClient(backend()) as client:
            database = client.test
            database.rows.insert_one({"_id": 0})
            session = client.start_session()
            try:
                if route == "names":
                    database.list_collection_names(comment=marker, session=session)
                else:
                    list(database.list_collections(comment=marker, session=session))
                check(session)
            finally:
                session.close()
        return

    async def exercise():
        async with AsyncMongoClient(backend()) as client:
            database = client.test
            await database.rows.insert_one({"_id": 0})
            session = client.start_session()
            try:
                await _call(database, route, comment=marker, session=session)
                check(session)
            finally:
                session.close()

    asyncio.run(exercise())
