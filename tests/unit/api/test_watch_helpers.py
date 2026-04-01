import os
import tempfile
import unittest
from types import SimpleNamespace

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

    def test_async_client_exposes_journal_durability_settings(self):
        client = AsyncMongoClient(
            MemoryEngine(),
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=2048,
        )
        self.assertTrue(client.change_stream_journal_fsync)
        self.assertEqual(client.change_stream_journal_max_bytes, 2048)
        clone = client.with_options()
        self.assertTrue(clone.change_stream_journal_fsync)
        self.assertEqual(clone.change_stream_journal_max_bytes, 2048)

    def test_direct_collection_exposes_journal_durability_settings(self):
        collection = AsyncCollection(
            MemoryEngine(),
            "db",
            "coll",
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=4096,
        )
        self.assertTrue(collection.change_stream_journal_fsync)
        self.assertEqual(collection.change_stream_journal_max_bytes, 4096)
        clone = collection.with_options()
        self.assertTrue(clone.change_stream_journal_fsync)
        self.assertEqual(clone.change_stream_journal_max_bytes, 4096)

    def test_change_stream_state_is_exposed_on_client_and_direct_collection(self):
        client = AsyncMongoClient(MemoryEngine(), change_stream_history_size=5)
        client_state = client.change_stream_state()
        self.assertEqual(client_state["retainedEvents"], 0)
        self.assertEqual(client_state["currentOffset"], 0)

        collection = AsyncCollection(MemoryEngine(), "db", "coll")
        collection_state = collection.change_stream_state()
        self.assertEqual(collection_state["retainedEvents"], 0)
        self.assertTrue("journalEnabled" in collection_state)

    def test_change_stream_backend_info_is_exposed_on_client_and_direct_collection(self):
        client = AsyncMongoClient(
            MemoryEngine(),
            change_stream_history_size=5,
            change_stream_journal_path="/tmp/mongoeco-watch.json",
        )
        client_info = client.change_stream_backend_info()
        self.assertEqual(client_info["implementation"], "local")
        self.assertFalse(client_info["distributed"])
        self.assertTrue(client_info["persistent"])

        collection = AsyncCollection(MemoryEngine(), "db", "coll")
        collection_info = collection.change_stream_backend_info()
        self.assertEqual(collection_info["implementation"], "local")
        self.assertFalse(collection_info["distributed"])
        self.assertFalse(collection_info["persistent"])

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

    async def test_async_client_drop_database_falls_back_to_collection_iteration_without_fast_path(self):
        class EngineStub:
            def __init__(self):
                self.list_calls = []
                self.drop_calls = []

            def create_session_state(self, session):
                del session

            async def connect(self):
                return None

            async def disconnect(self):
                return None

            async def list_databases(self, *, context=None):
                del context
                return []

            async def list_collections(self, db_name, *, context=None):
                self.list_calls.append((db_name, context))
                return ["users"] if len(self.list_calls) == 1 else []

            async def drop_collection(self, db_name, coll_name, *, context=None):
                self.drop_calls.append((db_name, coll_name, context))

        client = AsyncMongoClient(EngineStub())
        await client.drop_database("alpha")

        self.assertEqual(client._engine.list_calls, [("alpha", None), ("alpha", None)])
        self.assertEqual(client._engine.drop_calls, [("alpha", "users", None)])

    def test_async_client_and_database_watch_validate_timeouts_and_expose_runtime_properties(self):
        client = AsyncMongoClient(MemoryEngine(), uri="mongodb://localhost:27017/")
        database = client.get_database("db")

        with self.assertRaisesRegex(TypeError, "max_await_time_ms must be a non-negative integer"):
            client.watch(max_await_time_ms=-1)
        with self.assertRaisesRegex(TypeError, "max_await_time_ms must be a non-negative integer"):
            database.watch(max_await_time_ms="bad")  # type: ignore[arg-type]

        self.assertIsNotNone(client.client_uri)
        self.assertIsNotNone(client.topology_description)
        self.assertEqual(client.sdam_capabilities()["fullSdam"], False)
        self.assertIsNotNone(client.effective_client_uri)
        self.assertIsNotNone(client.timeout_policy)
        self.assertIsNotNone(client.retry_policy)
        self.assertIsNotNone(client.selection_policy)
        self.assertIsNotNone(client.concern_policy)
        self.assertIsNotNone(client.auth_policy)

    def test_async_database_and_collection_public_properties_do_not_fall_through_getattr(self):
        client = AsyncMongoClient(
            MemoryEngine(),
            change_stream_history_size=9,
            change_stream_journal_path="/tmp/mongoeco-async-surface.json",
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=1234,
        )
        database = client.get_database("db")
        collection = database.get_collection("coll")

        self.assertEqual(database.change_stream_history_size, 9)
        self.assertEqual(database.change_stream_journal_path, "/tmp/mongoeco-async-surface.json")
        self.assertTrue(database.change_stream_journal_fsync)
        self.assertEqual(database.change_stream_journal_max_bytes, 1234)
        self.assertEqual(collection.change_stream_history_size, 9)
        self.assertEqual(collection.change_stream_journal_path, "/tmp/mongoeco-async-surface.json")
        self.assertTrue(collection.change_stream_journal_fsync)
        self.assertEqual(collection.change_stream_journal_max_bytes, 1234)
        self.assertIsNotNone(client.tls_policy)
        self.assertIsNone(client.srv_resolution)
        self.assertIsNotNone(client.driver_runtime)
        self.assertIsNotNone(client.driver_monitor)
        self.assertIsNotNone(client.network_transport)
        self.assertEqual(database.change_stream_state()["retainedEvents"], 0)
        self.assertEqual(database.change_stream_backend_info()["implementation"], "local")
