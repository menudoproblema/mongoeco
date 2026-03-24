import unittest

from mongoeco.api._sync.client import MongoClient, _SyncRunner
from mongoeco.errors import InvalidOperation


async def _noop() -> None:
    return None


class SyncClientUnitTests(unittest.TestCase):
    def test_sync_runner_rejects_use_after_close(self):
        runner = _SyncRunner()
        runner.close()
        coroutine = _noop()
        try:
            with self.assertRaises(InvalidOperation):
                runner.run(coroutine)
        finally:
            coroutine.close()

    def test_sync_runner_marks_itself_closed_even_if_runner_close_fails(self):
        runner = _SyncRunner()

        class BrokenRunner:
            def close(self):
                raise RuntimeError("boom")

        runner._runner = BrokenRunner()

        with self.assertRaises(RuntimeError):
            runner.close()

        self.assertTrue(runner._closed)

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
