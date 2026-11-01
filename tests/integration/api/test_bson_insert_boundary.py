"""Rejected insert packets have no state/index/event prefix; server errors can."""

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson import BSON, Binary
from bson.errors import InvalidDocument

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async import _collection_insert, collection as collection_module
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import BulkWriteError, WriteError
from mongoeco.types import CodecOptions


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("many", [False, True])
def test_rejected_insert_packet_preserves_indexes_and_has_no_event(
    backend, many, tmp_path
):
    path = str(tmp_path / "inserts.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        async with AsyncMongoClient(engine()) as client:
            collection = client.test.records
            await collection.create_index("unique", unique=True)
            await collection.insert_one({"_id": 0, "unique": "seed"})
            stream = collection.watch(max_await_time_ms=1)
            try:
                if many:
                    operation = collection.insert_many(
                        [
                            {"_id": 1, "unique": "reusable"},
                            {"_id": 2, "v": Decimal(1)},
                        ]
                    )
                else:
                    operation = collection.insert_one({"_id": 2, "v": Decimal(1)})
                with pytest.raises(InvalidDocument):
                    await operation
                # The committed barrier proves absence of any rejected prefix.
                await collection.insert_one({"_id": 3, "unique": "reusable"})
                event = await stream.try_next()
                assert event["documentKey"] == {"_id": 3}
                assert event["fullDocument"] == {"_id": 3, "unique": "reusable"}
                assert await stream.try_next() is None
                assert [doc["_id"] async for doc in collection.find().sort("_id")] == [
                    0,
                    3,
                ]
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert [doc["_id"] for doc in client.test.records.find().sort("_id")] == [
                0,
                3,
            ]
            client.test.records.insert_one({"_id": 4, "unique": "not-reserved"})


@pytest.mark.parametrize("backend", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("ordered", [False, True])
def test_server_batch_failure_preserves_committed_prefix_and_event(backend, ordered):
    async def exercise():
        async with AsyncMongoClient(backend()) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 0})
            stream = collection.watch(max_await_time_ms=1)
            try:
                with pytest.raises(BulkWriteError) as caught:
                    await collection.insert_many(
                        [{"_id": 1}, {"_id": 0}, {"_id": 2}], ordered=ordered
                    )
                assert caught.value.details["nInserted"] == (1 if ordered else 2)
                assert caught.value.details["writeErrors"][0]["index"] == 1
                await collection.insert_one({"_id": 3})
                expected = [1, 3] if ordered else [1, 2, 3]
                assert [
                    (await stream.try_next())["documentKey"]["_id"] for _ in expected
                ] == expected
                assert await stream.try_next() is None
            finally:
                await stream.close()

    asyncio.run(exercise())


def test_single_and_many_encode_once_without_input_decoders():
    callbacks = []
    with MongoClient(
        MemoryEngine(),
        codec_options=CodecOptions(type_registry={int: callbacks.append}),
    ) as client:
        collection = client.test.records
        for owner, invoke in (
            (collection_module, lambda: collection.insert_one({"_id": 0})),
            (
                _collection_insert,
                lambda: collection.insert_many([{"_id": 1}, {"_id": 2}]),
            ),
        ):
            with patch.object(
                owner, "encode_bson_request", wraps=owner.encode_bson_request
            ) as encoder:
                invoke()
            assert encoder.call_count == 1
        assert callbacks == []


@pytest.mark.parametrize("many", [False, True])
def test_write_error_uses_builtin_codecs_and_preserves_original_bulk_operation(many):
    class ForbiddenDocument(dict):
        def __init__(self, *args, **kwargs):
            message = "a write error must not use the collection document factory"
            raise AssertionError(message)

    callbacks = []
    with MongoClient(MemoryEngine()) as client:
        base = client.test.create_collection(
            "records",
            validator={
                "$jsonSchema": {"properties": {"a": {"bsonType": "string"}}},
            },
        )
        collection = base.with_options(
            codec_options=CodecOptions(
                document_class=ForbiddenDocument,
                type_registry={bytes: callbacks.append},
            )
        )
        document = {"_id": 1, "a": Binary(b"x", 0)}
        invoke = (
            (lambda: collection.insert_many([document]))
            if many
            else (lambda: collection.insert_one(document))
        )
        with pytest.raises(WriteError) as caught:
            invoke()
        assert type(caught.value.details) is dict
        details = caught.value.details
        if many:
            details = details["writeErrors"][0]
            assert type(details["op"]["a"]) is Binary
        rules = details["errInfo"]["details"]["schemaRulesNotSatisfied"]
        rule = rules[0]["propertiesNotSatisfied"][0]["details"][0]
        assert type(rule["consideredValue"]) is bytes
        assert callbacks == []


@pytest.mark.parametrize("many", [False, True])
@pytest.mark.parametrize("placement", ["document", "comment"])
def test_bson_rejection_precedes_copying_an_unsupported_object(many, placement):
    class Unsupported:
        def __deepcopy__(self, memo):
            message = "unsupported input must reach the BSON encoder before copying"
            raise AssertionError(message)

    value = Unsupported()
    with pytest.raises(InvalidDocument):
        BSON.encode({"v": value})
    with MongoClient(MemoryEngine()) as client:
        collection = client.test.records
        document = {"_id": 2}
        options = {}
        if placement == "document":
            document["v"] = value
        else:
            options["comment"] = value
        invoke = (
            (lambda: collection.insert_many([{"_id": 1}, document], **options))
            if many
            else (lambda: collection.insert_one(document, **options))
        )
        with pytest.raises(InvalidDocument):
            invoke()
        assert collection.count_documents({}) == 0
