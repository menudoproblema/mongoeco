"""Native-valid server inputs stay distinct from PyMongo client validation."""

import asyncio
import json

from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.support import ENGINE_FACTORIES


TARGETS = ("7.0", "8.0", "9.0")
REFERENCE = json.loads(
    (Path(__file__).parents[2] / "fixtures/empty_pipeline_commands.json").read_text()
)


def exercise_sync(client, target):
    database = client.server_inputs
    collection = database.items
    collection.insert_one({"_id": "a", "n": 1})
    with pytest.raises(ValueError, match="empty"):
        collection.update_one({"_id": "a"}, [])
    assert (
        database.command({"update": "items", "updates": [{"q": {"_id": "a"}, "u": []}]})
        == REFERENCE[target]["update"]
    )
    assert (
        database.command(
            {"findAndModify": "items", "query": {"_id": "a"}, "update": [], "new": True}
        )
        == REFERENCE[target]["findAndModify"]
    )
    assert collection.find_one({"_id": "a"}) == REFERENCE[target]["document"]
    assert collection.update_one({}, {"$set": {"recovered": True}}).modified_count == 1


async def exercise_async(client, target):
    database = client.server_inputs
    collection = database.items
    await collection.insert_one({"_id": "a", "n": 1})
    with pytest.raises(ValueError, match="empty"):
        await collection.update_one({"_id": "a"}, [])
    assert (
        await database.command(
            {"update": "items", "updates": [{"q": {"_id": "a"}, "u": []}]}
        )
        == REFERENCE[target]["update"]
    )
    assert (
        await database.command(
            {"findAndModify": "items", "query": {"_id": "a"}, "update": [], "new": True}
        )
        == REFERENCE[target]["findAndModify"]
    )
    assert await collection.find_one({"_id": "a"}) == REFERENCE[target]["document"]
    assert (
        await collection.update_one({}, {"$set": {"recovered": True}})
    ).modified_count == 1


@pytest.mark.parametrize(['target','engine','surface'],list(product(TARGETS,ENGINE_FACTORIES,['sync','async'])))
def test_native_valid_raw_empty_pipeline(target,engine,surface):
    if surface=='sync':
        with MongoClient(ENGINE_FACTORIES[engine](),mongodb_dialect=target)as client:
            exercise_sync(client,target)
    else:

        async def run():
            async with AsyncMongoClient(
                ENGINE_FACTORIES[engine](), mongodb_dialect=target
            ) as client:
                await exercise_async(client, target)

        asyncio.run(run())


@pytest.mark.parametrize(['target','engine','surface'],list(product(TARGETS,ENGINE_FACTORIES,['sync','async'])))
def test_wire_native_valid_raw_empty_pipeline(target,engine,surface):
    async def run():
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](), mongodb_dialect=target
        ) as proxy:

            def calls():
                if surface == "sync":
                    with pymongo.MongoClient(
                        proxy.address.uri, serverSelectionTimeoutMS=3000
                    ) as client:
                        exercise_sync(client, target)
                else:

                    async def ac():
                        async with pymongo.AsyncMongoClient(
                            proxy.address.uri, serverSelectionTimeoutMS=3000
                        ) as client:
                            await exercise_async(client, target)

                    asyncio.run(ac())
            await asyncio.to_thread(calls)
    asyncio.run(run())
