"""BSON buffers around 4 KiB never become mutable storage aliases."""

import asyncio

import bson
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["async", "sync"])
@pytest.mark.parametrize("size", [4095, 4096, 4097, 8193])
@pytest.mark.parametrize("buffer_kind", [bytes, bytearray, memoryview])
def test_decoded_input_buffers_and_nested_reads_are_owned(
    backend, surface, size, buffer_kind
):
    expected = {"_id": "owned", "nested": {"text": "x" * size, "binary": b"y" * size}}
    mutable = bytearray(bson.encode(expected))
    buffer = buffer_kind(mutable)
    document = bson.decode(buffer)
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()

    def mutate_input():
        document["nested"]["text"] = "changed"
        mutable[:] = b"\0" * len(mutable)

    def check(result):
        assert result == expected
        result["nested"]["text"] = "changed output"

    if surface == "sync":
        with MongoClient(engine, pymongo_profile="4.18") as client:
            collection = client.test.buffers
            collection.insert_one(document)
            mutate_input()
            check(collection.find_one({"_id": "owned"}))
            assert list(collection.aggregate([])) == [expected]
            assert collection.find_one({"_id": "owned"}) == expected
    else:

        async def exercise():
            async with AsyncMongoClient(engine, pymongo_profile="4.18") as client:
                collection = client.test.buffers
                await collection.insert_one(document)
                mutate_input()
                check(await collection.find_one({"_id": "owned"}))
                assert await collection.aggregate([]).to_list() == [expected]
                assert await collection.find_one({"_id": "owned"}) == expected

        asyncio.run(exercise())
