import unittest
import asyncio
import threading
import time
from datetime import UTC, datetime
from unittest.mock import patch

from mongoeco.compat import MongoDialect80, PyMongoProfile417
from mongoeco.api._sync.client import MongoClient, _SyncRunner
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import ExecutionTimeout, InvalidOperation, ServerSelectionTimeoutError


async def _noop() -> None:
    return None


async def _value() -> str:
    return "ok"


class SyncClientUnitTests(unittest.TestCase):
    def test_sync_client_exposes_and_propagates_injected_clock(self):
        fixed = datetime(2026, 1, 2, 3, 4, 5, 123_456, tzinfo=UTC)
        factory = lambda: fixed
        client = MongoClient(MemoryEngine(), now_factory=factory)
        try:
            derived = client.with_options()
            try:
                self.assertIs(client.now_factory, factory)
                self.assertIs(derived.now_factory, factory)
                database = client.clock.with_options()
                collection = database.values.with_options()
                self.assertIs(database.now_factory, factory)
                self.assertIs(collection.now_factory, factory)
                collection.insert_one({'_id': 1})
                document = list(collection.aggregate([{'$project': {'now': '$$NOW'}}]))[0]
                self.assertEqual(document['now'], datetime(2026, 1, 2, 3, 4, 5, 123_000))
            finally:
                derived.close()
        finally:
            client.close()

    def test_sync_client_with_transaction_accepts_async_callback(self):
        client = MongoClient(MemoryEngine())
        try:
            async def _run(active):
                del active
                return "ok"

            self.assertEqual(client.with_transaction(_run), "ok")
        finally:
            client.close()

    def test_sync_runner_helper_rethrows_helper_errors(self):
        with self.assertRaisesRegex(RuntimeError, "boom"):
            _SyncRunner._rethrow_helper_error({"error": RuntimeError("boom")})

    def test_sync_runner_cleans_up_pending_tasks_on_close(self):
        runner = _SyncRunner()

        async def _spawn_background():
            async def _background():
                await asyncio.sleep(3600)

            asyncio.create_task(_background())
            await asyncio.sleep(0)
            return "ok"

        try:
            self.assertEqual(runner.run(_spawn_background()), "ok")
            pending = [task for task in asyncio.all_tasks(runner._runner.get_loop()) if not task.done()]
            self.assertNotEqual(pending, [])
            runner.close()
            self.assertEqual(runner._closed, True)
        finally:
            if not runner._closed:
                runner.close()

    def test_sync_runner_rejects_use_after_close(self):
        runner = _SyncRunner()
        runner.close()
        coroutine = _noop()
        try:
            with self.assertRaises(InvalidOperation):
                runner.run(coroutine)
        finally:
            coroutine.close()

    def test_sync_runner_inline_completes_without_asyncio_runner(self):
        runner = _SyncRunner()
        try:
            with patch.object(runner._runner, "run", side_effect=AssertionError("runner used")):
                self.assertEqual(runner.run(_value(), inline=True), "ok")
        finally:
            runner.close()

    def test_sync_runner_inline_rejects_suspending_coroutine(self):
        runner = _SyncRunner()

        async def _suspends_once():
            await asyncio.sleep(0)
            return "late"

        try:
            with self.assertRaisesRegex(InvalidOperation, "inline operation suspended"):
                runner.run(_suspends_once(), inline=True)
        finally:
            runner.close()

    def test_sync_runner_cleanup_returns_early_when_closed(self):
        runner = _SyncRunner()
        runner._closed = True

        runner._cleanup_pending_tasks()

    def test_sync_runner_cleanup_returns_early_when_runner_has_no_get_loop(self):
        runner = _SyncRunner()

        class NoLoopRunner:
            pass

        runner._runner = NoLoopRunner()
        runner._cleanup_pending_tasks()

    def test_sync_runner_cleanup_returns_early_when_get_loop_fails(self):
        runner = _SyncRunner()

        class BrokenLoopRunner:
            def get_loop(self):
                raise RuntimeError("boom")

        runner._runner = BrokenLoopRunner()
        runner._cleanup_pending_tasks()

    def test_sync_runner_cleanup_returns_early_when_loop_is_closed(self):
        runner = _SyncRunner()
        loop = asyncio.new_event_loop()
        loop.close()

        class ClosedLoopRunner:
            def get_loop(self):
                return loop

        runner._runner = ClosedLoopRunner()
        runner._cleanup_pending_tasks()

    def test_sync_runner_cleanup_tolerates_timeout_while_draining_pending_tasks(self):
        runner = _SyncRunner()

        async def _spawn_background():
            async def _background():
                await asyncio.sleep(3600)

            asyncio.create_task(_background())
            await asyncio.sleep(0)

        try:
            runner.run(_spawn_background())

            async def _timeout(*_args, **_kwargs):
                raise TimeoutError

            with patch("mongoeco.api._sync.client.asyncio.wait_for", side_effect=_timeout):
                runner._cleanup_pending_tasks()
        finally:
            runner.close()

    def test_sync_runner_close_tolerates_get_loop_failure_inside_running_loop(self):
        runner = _SyncRunner()

        class BrokenLoopRunner:
            def get_loop(self):
                raise RuntimeError("boom")

        runner._runner = BrokenLoopRunner()

        async def _exercise():
            runner.close()

        asyncio.run(_exercise())
        self.assertTrue(runner._closed)

    def test_sync_runner_runs_inside_active_event_loop(self):
        runner = _SyncRunner()

        async def _exercise():
            self.assertEqual(runner.run(_noop()), None)

        try:
            asyncio.run(_exercise())
        finally:
            runner.close()

    def test_sync_runner_creates_persistent_helper_only_for_active_event_loop(self):
        runner = _SyncRunner()
        try:
            self.assertIsNone(runner._helper_thread)
            self.assertEqual(runner.run(_noop()), None)
            self.assertIsNone(runner._helper_thread)

            async def _exercise() -> None:
                self.assertEqual(runner.run(_noop()), None)
                helper_thread = runner._helper_thread
                self.assertIsNotNone(helper_thread)
                self.assertTrue(helper_thread.is_alive())
                self.assertEqual(runner.run(_noop()), None)
                self.assertIs(runner._helper_thread, helper_thread)

            asyncio.run(_exercise())
        finally:
            runner.close()
        self.assertIsNone(runner._helper_thread)

    def test_sync_runner_inline_uses_helper_inside_active_event_loop(self):
        runner = _SyncRunner()
        try:
            async def _exercise() -> None:
                self.assertEqual(runner.run(_value(), inline=True), "ok")
                self.assertIsNotNone(runner._helper_thread)

            asyncio.run(_exercise())
        finally:
            runner.close()

    def test_sync_runner_runs_inside_active_event_loop_from_secondary_thread(self):
        runner = _SyncRunner()
        captured: list[object] = []

        def _worker() -> None:
            async def _exercise() -> None:
                try:
                    captured.append(runner.run(_noop()))
                except BaseException as exc:  # pragma: no cover - assertion follows on captured type
                    captured.append(type(exc))

            asyncio.run(_exercise())

        try:
            worker = threading.Thread(target=_worker)
            worker.start()
            worker.join()
        finally:
            runner.close()
        self.assertEqual(captured, [None])

    def test_sync_database_change_stream_properties_delegate_to_async_database(self):
        client = MongoClient()
        try:
            database = client.get_database("db")
            state = database.change_stream_state()
            backend = database.change_stream_backend_info()
            self.assertIn("retainedEvents", state)
            self.assertIn("persistent", backend)
        finally:
            client.close()

    def test_memory_sync_collection_whitelisted_methods_use_inline(self):
        client = MongoClient(MemoryEngine())
        collection = client.test.hot
        try:
            collection.insert_one({"_id": 1, "count": 0})
            with patch.object(
                client._runner._runner,
                "run",
                side_effect=AssertionError("asyncio runner used"),
            ):
                self.assertEqual(collection.find_one({"_id": 1})["count"], 0)
                result = collection.update_one({"_id": 1}, {"$inc": {"count": 1}})
                self.assertEqual(result.matched_count, 1)
                self.assertEqual(collection.count_documents({"_id": 1}), 1)
        finally:
            client.close()

    def test_sqlite_sync_collection_never_uses_inline_fast_path(self):
        client = MongoClient(SQLiteEngine())
        collection = client.test.hot
        try:
            collection.insert_one({"_id": 1, "count": 0})
            with patch.object(
                client._runner,
                "_run_inline_direct",
                side_effect=AssertionError("inline used"),
            ):
                self.assertEqual(collection.find_one({"_id": 1})["count"], 0)
        finally:
            client.close()

    def test_memory_sync_collection_session_disables_inline_fast_path(self):
        client = MongoClient(MemoryEngine())
        collection = client.test.hot
        session = client.start_session()
        try:
            collection.insert_one({"_id": 1, "count": 0})
            with patch.object(
                client._runner,
                "_run_inline_direct",
                side_effect=AssertionError("inline used"),
            ):
                self.assertEqual(
                    collection.find_one({"_id": 1}, session=session)["count"],
                    0,
                )
        finally:
            session.close()
            client.close()

    def test_memory_sync_inline_serializes_shared_client_across_threads(self):
        client = MongoClient(MemoryEngine())
        collection = client.test.counters
        workers = 4
        iterations = 80
        errors: list[BaseException] = []
        start = threading.Event()
        collection.insert_one({"_id": "counter", "value": 0})

        def _worker() -> None:
            start.wait()
            try:
                for _ in range(iterations):
                    collection.update_one(
                        {"_id": "counter"},
                        {"$inc": {"value": 1}},
                    )
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        threads = [threading.Thread(target=_worker) for _ in range(workers)]
        try:
            for thread in threads:
                thread.start()
            start.set()
            for thread in threads:
                thread.join()

            self.assertEqual(errors, [])
            self.assertEqual(
                collection.find_one({"_id": "counter"})["value"],
                workers * iterations,
            )
        finally:
            client.close()

    def test_sync_runner_close_supports_active_event_loop(self):
        runner = _SyncRunner()

        async def _exercise() -> None:
            runner.run(_noop())
            runner.close()

        asyncio.run(_exercise())
        self.assertTrue(runner._closed)

    def test_sync_runner_defers_close_requested_from_helper(self):
        runner = _SyncRunner()

        async def _close_from_helper() -> str:
            runner.close()
            return "closed"

        async def _exercise() -> None:
            self.assertEqual(runner.run(_close_from_helper()), "closed")

        asyncio.run(_exercise())
        self.assertTrue(runner._closed)
        self.assertIsNone(runner._helper_thread)

    def test_sync_runner_marks_itself_closed_even_if_runner_close_fails(self):
        runner = _SyncRunner()

        class BrokenRunner:
            def close(self):
                raise RuntimeError("boom")

        runner._runner = BrokenRunner()

        with self.assertRaises(RuntimeError):
            runner.close()

        self.assertTrue(runner._closed)

    def test_sync_runner_rewraps_execution_timeout_with_sync_context(self):
        runner = _SyncRunner()
        awaitable = _noop()
        try:
            with patch.object(runner._runner, "run", side_effect=ExecutionTimeout("operation exceeded time limit")):
                with self.assertRaises(ExecutionTimeout) as raised:
                    runner.run(awaitable)
        finally:
            awaitable.close()
            runner.close()

        self.assertIn("sync operation timed out", str(raised.exception))
        self.assertEqual(raised.exception.code, 50)

    def test_sync_runner_rewraps_server_selection_timeout_with_sync_context(self):
        runner = _SyncRunner()
        awaitable = _noop()
        try:
            with patch.object(runner._runner, "run", side_effect=ServerSelectionTimeoutError("no suitable servers")):
                with self.assertRaises(ServerSelectionTimeoutError) as raised:
                    runner.run(awaitable)
        finally:
            awaitable.close()
            runner.close()

        self.assertIn("sync server selection timed out", str(raised.exception))

    def test_client_exit_after_manual_close_returns_false(self):
        client = MongoClient()
        client.close()

        self.assertFalse(client.__exit__(None, None, None))

    def test_client_preserves_configured_change_stream_history_size(self):
        client = MongoClient(MemoryEngine(), change_stream_history_size=321)
        try:
            self.assertEqual(client.change_stream_history_size, 321)
            self.assertEqual(client.with_options().change_stream_history_size, 321)
        finally:
            client.close()

    def test_client_preserves_configured_change_stream_journal_path(self):
        client = MongoClient(MemoryEngine(), change_stream_journal_path="/tmp/mongoeco-changes.json")
        try:
            self.assertEqual(client.change_stream_journal_path, "/tmp/mongoeco-changes.json")
            self.assertEqual(client.with_options().change_stream_journal_path, "/tmp/mongoeco-changes.json")
        finally:
            client.close()

    def test_client_preserves_journal_durability_settings(self):
        client = MongoClient(
            MemoryEngine(),
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=8192,
        )
        try:
            self.assertTrue(client.change_stream_journal_fsync)
            self.assertEqual(client.change_stream_journal_max_bytes, 8192)
            clone = client.with_options()
            self.assertTrue(clone.change_stream_journal_fsync)
            self.assertEqual(clone.change_stream_journal_max_bytes, 8192)
        finally:
            client.close()

    def test_database_exposes_client_change_stream_settings(self):
        client = MongoClient(
            MemoryEngine(),
            change_stream_history_size=321,
            change_stream_journal_path="/tmp/mongoeco-db-changes.json",
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=8192,
        )
        try:
            database = client.get_database("alpha")
            self.assertEqual(database.change_stream_history_size, 321)
            self.assertEqual(database.change_stream_journal_path, "/tmp/mongoeco-db-changes.json")
            self.assertTrue(database.change_stream_journal_fsync)
            self.assertEqual(database.change_stream_journal_max_bytes, 8192)
        finally:
            client.close()

    def test_collection_exposes_client_change_stream_settings(self):
        client = MongoClient(
            MemoryEngine(),
            change_stream_history_size=321,
            change_stream_journal_path="/tmp/mongoeco-collection-changes.json",
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=8192,
        )
        try:
            collection = client.get_database("alpha").get_collection("events")
            self.assertEqual(collection.change_stream_history_size, 321)
            self.assertEqual(collection.change_stream_journal_path, "/tmp/mongoeco-collection-changes.json")
            self.assertTrue(collection.change_stream_journal_fsync)
            self.assertEqual(collection.change_stream_journal_max_bytes, 8192)
        finally:
            client.close()

    def test_sync_database_and_collection_public_properties_do_not_fall_through_getattr(self):
        client = MongoClient(
            MemoryEngine(),
            change_stream_history_size=321,
            change_stream_journal_path="/tmp/mongoeco-sync-surface.json",
            change_stream_journal_fsync=True,
            change_stream_journal_max_bytes=8192,
        )
        try:
            database = client.get_database("alpha")
            collection = database.get_collection("events")
            self.assertEqual(database.change_stream_history_size, 321)
            self.assertEqual(database.change_stream_journal_path, "/tmp/mongoeco-sync-surface.json")
            self.assertTrue(database.change_stream_journal_fsync)
            self.assertEqual(database.change_stream_journal_max_bytes, 8192)
            self.assertEqual(collection.change_stream_history_size, 321)
            self.assertEqual(collection.change_stream_journal_path, "/tmp/mongoeco-sync-surface.json")
            self.assertTrue(collection.change_stream_journal_fsync)
            self.assertEqual(collection.change_stream_journal_max_bytes, 8192)
        finally:
            client.close()

    def test_client_exposes_change_stream_state(self):
        client = MongoClient(MemoryEngine(), change_stream_history_size=7)
        try:
            state = client.change_stream_state()
            self.assertEqual(state["retainedEvents"], 0)
            self.assertEqual(state["currentOffset"], 0)
        finally:
            client.close()

    def test_client_exposes_change_stream_backend_info_and_sdam_capabilities(self):
        client = MongoClient(
            MemoryEngine(),
            change_stream_journal_path="/tmp/mongoeco-sync-changes.json",
        )
        try:
            backend = client.change_stream_backend_info()
            capabilities = client.sdam_capabilities()
            self.assertEqual(backend["implementation"], "local")
            self.assertTrue(backend["persistent"])
            self.assertFalse(backend["distributed"])
            self.assertFalse(capabilities["fullSdam"])
            self.assertTrue(capabilities["helloMemberDiscovery"])
        finally:
            client.close()

    def test_sync_collection_exposes_subcollections_and_change_stream_helpers(self):
        client = MongoClient(MemoryEngine())
        try:
            collection = client.get_database("alpha").get_collection("events")
            self.assertEqual(collection.logs.name, "events.logs")
            self.assertEqual(collection["audit"].name, "events.audit")
            self.assertEqual(collection.change_stream_state()["retainedEvents"], 0)
            self.assertEqual(collection.change_stream_backend_info()["implementation"], "local")
            with self.assertRaises(AttributeError):
                _ = collection._private
            with self.assertRaises(TypeError):
                _ = collection[""]  # type: ignore[index]
        finally:
            client.close()

    def test_client_drop_database_prefers_engine_fast_path(self):
        class EngineStub:
            def __init__(self):
                self.fast_drop_calls = []
                self.fallback_calls = []

            def create_session_state(self, session):
                return None

            async def connect(self):
                return None

            async def disconnect(self):
                return None

            async def list_databases(self, *, context=None):
                return []

            async def list_collections(self, db_name, *, context=None):
                self.fallback_calls.append(("list", db_name, context))
                return ["users"]

            async def drop_collection(self, db_name, coll_name, *, context=None):
                self.fallback_calls.append(("drop", db_name, coll_name, context))

            async def drop_database(self, db_name, *, context=None):
                self.fast_drop_calls.append((db_name, context))

        client = MongoClient(EngineStub())
        try:
            client.drop_database("alpha")
        finally:
            client.close()

        self.assertEqual(client._async_client._engine.fast_drop_calls, [("alpha", None)])
        self.assertEqual(client._async_client._engine.fallback_calls, [])

    def test_client_del_suppresses_close_errors(self):
        client = MongoClient()

        def broken_close() -> None:
            raise RuntimeError("boom")

        client.close = broken_close
        client.__del__()

    def test_sync_runner_del_marks_closed_when_close_fails(self):
        runner = _SyncRunner()

        def broken_close() -> None:
            raise RuntimeError("boom")

        runner.close = broken_close
        runner.__del__()

        self.assertTrue(runner._closed)

    def test_sync_runner_close_waits_for_active_run_to_finish(self):
        runner = _SyncRunner()
        started = threading.Event()
        release = threading.Event()
        original_run = runner._runner.run

        def _wrapped(awaitable):
            async def _gate():
                started.set()
                while not release.is_set():
                    await asyncio.sleep(0.001)
                return await awaitable

            return original_run(_gate())

        runner._runner.run = _wrapped  # type: ignore[method-assign]
        result: list[str] = []

        def _invoke_run():
            result.append(runner.run(_noop()))

        worker = threading.Thread(target=_invoke_run)
        worker.start()
        self.assertTrue(started.wait(1))

        close_done = threading.Event()

        def _invoke_close():
            runner.close()
            close_done.set()

        closer = threading.Thread(target=_invoke_close)
        closer.start()
        time.sleep(0.05)
        self.assertFalse(close_done.is_set())
        release.set()
        worker.join()
        closer.join()

        self.assertEqual(result, [None])
        self.assertTrue(runner._closed)

    def test_client_exposes_resolved_dialect_and_profile(self):
        client = MongoClient(
            MemoryEngine(),
            mongodb_dialect='8.0',
            pymongo_profile='4.17',
        )

        self.assertEqual(client.mongodb_dialect, MongoDialect80())
        self.assertEqual(client.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(client.pymongo_profile, PyMongoProfile417())
        self.assertEqual(client.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(client.get_database('alpha').mongodb_dialect, MongoDialect80())
        self.assertEqual(
            client.get_database('alpha').mongodb_dialect_resolution.resolution_mode,
            'explicit-alias',
        )
        self.assertEqual(client.get_database('alpha').pymongo_profile, PyMongoProfile417())
        self.assertEqual(
            client.get_database('alpha').pymongo_profile_resolution.resolution_mode,
            'explicit-alias',
        )
        self.assertEqual(
            client.get_database('alpha').get_collection('users').mongodb_dialect,
            MongoDialect80(),
        )
        self.assertEqual(
            client.get_database('alpha').get_collection('users').mongodb_dialect_resolution.resolution_mode,
            'explicit-alias',
        )
        self.assertEqual(
            client.get_database('alpha').get_collection('users').pymongo_profile,
            PyMongoProfile417(),
        )
        self.assertEqual(
            client.get_database('alpha').get_collection('users').pymongo_profile_resolution.resolution_mode,
            'explicit-alias',
        )

        client.close()

    def test_sync_collection_resolution_metadata_does_not_force_connection(self):
        client = MongoClient(
            MemoryEngine(),
            mongodb_dialect='8.0',
            pymongo_profile='4.17',
        )
        collection = client.get_database('alpha').get_collection('users')

        self.assertFalse(client._connected)
        self.assertEqual(collection.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(collection.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
        self.assertFalse(client._connected)

        client.close()
        self.assertEqual(collection.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(collection.pymongo_profile_resolution.resolution_mode, 'explicit-alias')

    def test_sync_client_runtime_properties_and_wrappers_delegate_to_async_client(self):
        client = MongoClient(MemoryEngine(), uri="mongodb://localhost:27017/")
        try:
            self.assertIsNotNone(client.topology_description)
            self.assertIsNotNone(client.effective_client_uri)
            self.assertIsNotNone(client.timeout_policy)
            self.assertIsNotNone(client.retry_policy)
            self.assertIsNotNone(client.selection_policy)
            self.assertIsNotNone(client.concern_policy)
            self.assertIsNotNone(client.auth_policy)
            self.assertIsNotNone(client.tls_policy)
            self.assertIsNone(client.srv_resolution)
            self.assertIsNotNone(client.driver_runtime)
            self.assertIsNotNone(client.driver_monitor)
            self.assertIsNotNone(client.network_transport)

            async def _execute_driver_command(*args, **kwargs):
                return {"kind": "driver", "args": args, "kwargs": kwargs}

            async def _execute_network_command(*args, **kwargs):
                return {"kind": "network", "args": args, "kwargs": kwargs}

            async def _refresh_topology(*, transport=None):
                return {"transport": transport}

            async def _start_topology_monitoring(*, transport=None):
                return ("start", transport)

            async def _stop_topology_monitoring():
                return "stop"

            client._ensure_connected = lambda: None  # type: ignore[method-assign]
            client._async_client.execute_driver_command = _execute_driver_command  # type: ignore[method-assign]
            client._async_client.execute_network_command = _execute_network_command  # type: ignore[method-assign]
            client._async_client.refresh_topology = _refresh_topology  # type: ignore[method-assign]
            client._async_client.start_topology_monitoring = _start_topology_monitoring  # type: ignore[method-assign]
            client._async_client.stop_topology_monitoring = _stop_topology_monitoring  # type: ignore[method-assign]

            self.assertEqual(
                client.execute_driver_command("db", "ping", {"ping": 1})["kind"],
                "driver",
            )
            self.assertEqual(
                client.execute_network_command("db", "ping", {"ping": 1})["kind"],
                "network",
            )
            self.assertEqual(client.refresh_topology(), {"transport": None})
            self.assertIsNone(client.start_topology_monitoring())
            self.assertIsNone(client.stop_topology_monitoring())
        finally:
            client.close()
