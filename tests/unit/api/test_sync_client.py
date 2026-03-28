import unittest
import asyncio
import threading
import time
from unittest.mock import patch

from mongoeco import MongoDialect80, PyMongoProfile413
from mongoeco.api._sync.client import MongoClient, _SyncRunner
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import ExecutionTimeout, InvalidOperation, ServerSelectionTimeoutError


async def _noop() -> None:
    return None


class SyncClientUnitTests(unittest.TestCase):
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

    def test_sync_runner_rejects_running_inside_event_loop_and_closes_awaitable(self):
        runner = _SyncRunner()
        closed = False

        class _Awaitable:
            def close(self):
                nonlocal closed
                closed = True

            def __await__(self):
                if False:
                    yield None
                return None

        async def _exercise():
            with self.assertRaises(InvalidOperation):
                runner.run(_Awaitable())

        try:
            asyncio.run(_exercise())
        finally:
            runner.close()

        self.assertTrue(closed)

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
            pymongo_profile='4.13',
        )

        self.assertEqual(client.mongodb_dialect, MongoDialect80())
        self.assertEqual(client.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(client.pymongo_profile, PyMongoProfile413())
        self.assertEqual(client.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(client.get_database('alpha').mongodb_dialect, MongoDialect80())
        self.assertEqual(
            client.get_database('alpha').mongodb_dialect_resolution.resolution_mode,
            'explicit-alias',
        )
        self.assertEqual(client.get_database('alpha').pymongo_profile, PyMongoProfile413())
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
            PyMongoProfile413(),
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
            pymongo_profile='4.13',
        )
        collection = client.get_database('alpha').get_collection('users')

        self.assertFalse(client._connected)
        self.assertEqual(collection.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(collection.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
        self.assertFalse(client._connected)

        client.close()
        self.assertEqual(collection.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
        self.assertEqual(collection.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
