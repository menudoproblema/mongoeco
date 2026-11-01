"""Client command admission precedes side effects and owns response decoding."""

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson import Int64
from bson.errors import InvalidDocument

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async import database_commands
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import CodecOptions


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("route", ["insert", "update", "create", "indexes"])
def test_rejected_command_keeps_state_indexes_catalog_and_events(
    backend, route, tmp_path
):
    path = str(tmp_path / "commands.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        async with AsyncMongoClient(engine()) as client:
            db = client.test
            c = db.rows
            await c.insert_one({"_id": 0, "v": 1})
            await c.create_index("v", unique=True)
            stream = c.watch(max_await_time_ms=1)
            commands = {
                "insert": {
                    "insert": "rows",
                    "documents": [{"_id": 1}, {"v": Decimal(1)}],
                },
                "update": {
                    "update": "rows",
                    "updates": [
                        {"q": {}, "u": {"$set": {"v": 9}}},
                        {"q": {}, "u": {"$set": {"v": Decimal(1)}}},
                    ],
                },
                "create": {"create": "aux", "validator": {"v": Decimal(1)}},
                "indexes": {
                    "createIndexes": "aux",
                    "indexes": [
                        {"key": {"v": 1}, "name": "v_1"},
                        {
                            "key": {"x": 1},
                            "name": "x_1",
                            "partialFilterExpression": {"v": Decimal(1)},
                        },
                    ],
                },
            }
            try:
                with pytest.raises(InvalidDocument):
                    await db.command(commands[route])
                assert "aux" not in await db.list_collection_names()
                # A committed barrier exposes any publication from a false prefix.
                await c.update_one({"_id": 0}, {"$set": {"v": 2}})
                event = await stream.try_next()
                assert event["operationType"] == "update"
                assert event["documentKey"] == {"_id": 0}
                assert event["updateDescription"]["updatedFields"] == {"v": 2}
                assert await stream.try_next() is None
                assert await c.find().to_list() == [{"_id": 0, "v": 2}]
                with pytest.raises(DuplicateKeyError):
                    await c.insert_one({"_id": 8, "v": 2})
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert list(client.test.rows.find()) == [{"_id": 0, "v": 2}]
            assert "aux" not in client.test.list_collection_names()
            with pytest.raises(DuplicateKeyError):
                client.test.rows.insert_one({"_id": 8, "v": 2})


def test_public_admits_once_and_internal_command_does_not_reencode():
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            service = client.test._admin._commands
            with patch.object(
                database_commands,
                "encode_bson_request",
                wraps=database_commands.encode_bson_request,
            ) as encode:
                assert (await client.test.command("ping"))["ok"] == 1
                assert encode.call_count == 1
                encode.reset_mock()
                assert (await service.execute_document({"ping": 1}))["ok"] == 1
                encode.assert_not_called()

    asyncio.run(exercise())


@pytest.mark.parametrize("route", ["find", "aggregate", "distinct"])
def test_command_ignores_database_user_codecs_and_explicit_decoders(route):
    calls = []

    class Document(dict):
        def __init__(self):
            calls.append("new")
            super().__init__()

    def decode(value):
        calls.append("decode")
        return value

    with MongoClient(MemoryEngine()) as client:
        db = client.test
        db.rows.insert_one({"_id": 0, "v": {"n": 1}})
        options = CodecOptions(document_class=Document, type_registry={int: decode})
        selected = db.with_options(codec_options=options)
        spec = {
            "find": {"find": "rows", "filter": {}},
            "aggregate": {"aggregate": "rows", "pipeline": [], "cursor": {}},
            "distinct": {"distinct": "rows", "key": "v"},
        }[route]
        assert type(selected.command(spec)) is dict
        assert calls == []
        result = selected.command(spec, codec_options=options)
        assert type(result) is Document
        assert calls == ["new"] * (2 if route == "distinct" else 4)
        if route != "distinct":
            assert type(result["cursor"]["id"]) is Int64
            assert list(result["cursor"]) == ["firstBatch", "id", "ns"]


def test_bson_admission_does_not_copy_unsupported_client_values():
    class Unsupported:
        def __deepcopy__(self, memo):
            message = "copy before admission"
            raise AssertionError(message)

    with MongoClient(MemoryEngine()) as client:
        spec = {"ping": 1}
        with pytest.raises(InvalidDocument):
            client.test.command(spec, comment=Unsupported())
        assert list(spec) == ["ping", "comment"]


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("spec", [{}, {1: 1}, 5])
def test_client_command_shape_errors_preserve_sync_async_exception_phase(
    engine_type, surface, spec
):
    empty = spec == {}
    expected = (
        (StopIteration if surface == "sync" else RuntimeError)
        if empty
        else (AttributeError if isinstance(spec, dict) else TypeError)
    )

    if surface == "sync":
        with MongoClient(engine_type()) as client, pytest.raises(expected):
            client.test.command(spec)
        return

    async def exercise():
        async with AsyncMongoClient(engine_type()) as client:
            with pytest.raises(expected) as caught:
                await client.test.command(spec)
            if empty:
                assert isinstance(caught.value.__cause__, StopIteration)

    asyncio.run(exercise())
