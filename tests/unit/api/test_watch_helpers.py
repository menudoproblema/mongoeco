import os
import tempfile
import unittest

from mongoeco.api._async.client import AsyncDatabase, AsyncMongoClient
from mongoeco.api._async.collection import AsyncCollection
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import InvalidOperation, OperationFailure


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

    async def test_direct_collection_can_configure_retained_change_history(self):
        engine = MemoryEngine()
        await engine.connect()
        try:
            collection = AsyncCollection(engine, "db", "coll", change_stream_history_size=2)
            stream = collection.watch(resume_after={"_data": "1"}, max_await_time_ms=25)
            await collection.insert_one({"_id": 1})
            await collection.insert_one({"_id": 2})
            await collection.insert_one({"_id": 3})
            with self.assertRaisesRegex(OperationFailure, "history|resume token"):
                await stream.try_next()
        finally:
            await engine.disconnect()

    def test_async_client_exposes_configured_change_stream_history_size(self):
        client = AsyncMongoClient(MemoryEngine(), change_stream_history_size=123)
        self.assertEqual(client.change_stream_history_size, 123)
        self.assertEqual(client.with_options().change_stream_history_size, 123)

    def test_async_client_exposes_configured_change_stream_journal_path(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            client = AsyncMongoClient(MemoryEngine(), change_stream_journal_path=journal_path)
            self.assertEqual(client.change_stream_journal_path, journal_path)
            self.assertEqual(client.with_options().change_stream_journal_path, journal_path)

    async def test_direct_collection_can_resume_from_persisted_change_stream_journal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            engine = MemoryEngine()
            await engine.connect()
            try:
                writer = AsyncCollection(
                    engine,
                    "db",
                    "coll",
                    change_stream_journal_path=journal_path,
                )
                await writer.insert_one({"_id": 1, "name": "Ada"})
                await writer.insert_one({"_id": 2, "name": "Grace"})

                reader = AsyncCollection(
                    engine,
                    "db",
                    "coll",
                    change_stream_journal_path=journal_path,
                )
                stream = reader.watch(
                    resume_after={"_data": "1"},
                    max_await_time_ms=25,
                )
                event = await stream.try_next()
            finally:
                await engine.disconnect()

        self.assertIsNotNone(event)
        self.assertEqual(event["documentKey"], {"_id": 2})
