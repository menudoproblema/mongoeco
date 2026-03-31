import unittest

from mongoeco.api._async.client import AsyncDatabase, AsyncMongoClient
from mongoeco.api._async.collection import AsyncCollection
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import InvalidOperation


class WatchHelperTests(unittest.TestCase):
    def test_async_watch_rejects_explicit_sessions_across_client_database_and_collection(self):
        client = AsyncMongoClient(MemoryEngine())
        session = object()

        with self.assertRaisesRegex(InvalidOperation, "watch does not support explicit sessions"):
            client.watch(session=session)

        with self.assertRaisesRegex(InvalidOperation, "watch does not support explicit sessions"):
            client.get_database("db").watch(session=session)

        with self.assertRaisesRegex(InvalidOperation, "watch does not support explicit sessions"):
            client.get_database("db").get_collection("coll").watch(session=session)


class DirectWatchHubTests(unittest.IsolatedAsyncioTestCase):
    async def test_direct_collection_watch_shares_hub_with_local_writes(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            collection = AsyncCollection(engine, "db", "coll")
            stream = collection.watch(max_await_time_ms=25)
            await collection.insert_one({"_id": 1, "name": "Ada"})
            event = await stream.try_next()
        finally:
            await engine.disconnect()

        self.assertIsNotNone(event)
        self.assertEqual(event["operationType"], "insert")
        self.assertEqual(event["documentKey"], {"_id": 1})

    async def test_direct_database_watch_observes_existing_collection_instances(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            database = AsyncDatabase(engine, "db")
            collection = database.get_collection("coll")
            stream = database.watch(max_await_time_ms=25)
            await collection.insert_one({"_id": 1, "name": "Ada"})
            event = await stream.try_next()
        finally:
            await engine.disconnect()

        self.assertIsNotNone(event)
        self.assertEqual(event["operationType"], "insert")
        self.assertEqual(event["ns"]["db"], "db")
