"""Exercise inherited 4.19 cursor, bulk and session APIs over the local proxy."""

import asyncio
import struct

import pymongo
import pytest

from bson.errors import InvalidBSON

from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer
from mongoeco.wire.protocol import OP_MSG, MessageHeader, decode_op_msg


pytestmark = pytest.mark.skipif(
    pymongo.version_tuple[:2] < (4, 19), reason="requires PyMongo 4.19"
)


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_released_driver_bulk_getmore_and_sessions_work_over_wire(factory, surface):
    document_count = 6
    updated_value = 2
    models = [
        pymongo.InsertOne({"_id": index, "value": index})
        for index in range(document_count)
    ]

    async def exercise():
        async with AsyncMongoEcoProxyServer(
            engine=factory(), pymongo_profile="4.19"
        ) as proxy:
            options = {"directConnection": True, "serverSelectionTimeoutMS": 3000}

            def check_sync():
                with (
                    pymongo.MongoClient(proxy.address.uri, **options) as client,
                    client.start_session() as session,
                ):
                    collection = client.test.driver419
                    result = collection.bulk_write(models, session=session)
                    assert result.inserted_count == document_count
                    assert [
                        doc["_id"]
                        for doc in collection.find(
                            {}, sort=[("_id", 1)], batch_size=2, session=session
                        )
                    ] == list(range(document_count))
                    result = collection.bulk_write(
                        [
                            pymongo.UpdateMany({}, {"$inc": {"value": 1}}),
                            pymongo.DeleteOne({"_id": 0}),
                        ],
                        session=session,
                    )
                    assert result.modified_count == document_count
                    assert result.deleted_count == 1
                    assert (
                        collection.find_one({"_id": 1}, session=session)["value"]
                        == updated_value
                    )

            async def check_async():
                async with (
                    pymongo.AsyncMongoClient(proxy.address.uri, **options) as client,
                    client.start_session() as session,
                ):
                    collection = client.test.driver419
                    result = await collection.bulk_write(models, session=session)
                    assert result.inserted_count == document_count
                    assert [
                        doc["_id"]
                        async for doc in collection.find(
                            {}, sort=[("_id", 1)], batch_size=2, session=session
                        )
                    ] == list(range(document_count))
                    result = await collection.bulk_write(
                        [
                            pymongo.UpdateMany({}, {"$inc": {"value": 1}}),
                            pymongo.DeleteOne({"_id": 0}),
                        ],
                        session=session,
                    )
                    assert result.modified_count == document_count
                    assert result.deleted_count == 1
                    assert (await collection.find_one({"_id": 1}, session=session))[
                        "value"
                    ] == updated_value

            if surface == "sync":
                await asyncio.to_thread(check_sync)
            else:
                await check_async()

    asyncio.run(exercise())


@pytest.mark.parametrize("value_type", [b"\x08", b"\x0b"])
def test_wire_bson_rejects_values_consuming_the_document_terminator(value_type):
    malformed = struct.pack("<i", 8) + value_type + b"a\0\0"
    payload = struct.pack("<iB", 0, 0) + malformed
    header = MessageHeader(16 + len(payload), 1, 0, OP_MSG)
    with pytest.raises(InvalidBSON):
        decode_op_msg(header, payload)
