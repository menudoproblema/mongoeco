"""Ownership and response boundaries for cursors retained between wire commands."""

import asyncio
import unittest

from mongoeco.errors import OperationFailure
from mongoeco.types import Binary
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.cursors import WireCursorStore


class Source:
    def __init__(self, rows=()):
        self.rows = list(rows)
        self._closed = False
        self.reads = []
        self.close_calls = 0
        self.error = None

    async def _read_response(self, maximum, *, initial=False):
        self.reads.append((maximum, initial))
        if self.error is not None:
            raise self.error
        maximum = len(self.rows) if maximum is None else maximum
        batch, self.rows = self.rows[:maximum], self.rows[maximum:]
        if maximum and not self.rows:
            self._closed = True
        return batch

    async def close(self):
        self.close_calls += 1
        self._closed = True


class LiveCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_get_more_zero_reads_an_unbounded_count_after_empty_open(self):
        source = Source([{"_id": n} for n in range(3)])
        cursor_id = await self.open(source)
        reply = await self.store.get_more_async(
            {"getMore": cursor_id, "collection": "rows", "batchSize": 0}, db_name="db"
        )
        self.assertEqual(reply["cursor"]["nextBatch"], [{"_id": n} for n in range(3)])
        self.assertEqual(reply["cursor"]["id"], 0)
        self.assertEqual(source.reads, [(0, True), (None, False)])

    async def asyncSetUp(self):
        self.store = WireCursorStore()
        self.addAsyncCleanup(self.store.aclear)

    async def open(self, source, **kwargs):
        reply = await self.store.open_stream(
            source, "db.rows", command_document={"cursor": {"batchSize": 0}}, **kwargs
        )
        self.assertEqual(reply["cursor"]["firstBatch"], [])
        self.assertGreater(reply["cursor"]["id"], 0)
        return reply["cursor"]["id"]

    async def test_get_more_checks_owner_before_consuming_and_accepts_pooled_connection(
        self,
    ):
        connection = WireConnectionContext(
            1, authenticated_users=[{"db": "admin", "user": "reader"}]
        )
        pooled = WireConnectionContext(
            2, authenticated_users=list(connection.authenticated_users)
        )
        lsid = {"id": Binary(b"a" * 16, 4)}
        source = Source([{"v": 1}, {"v": 2}])
        cursor_id = await self.open(source, connection=connection, lsid=lsid)
        command = {"getMore": cursor_id, "collection": "rows", "batchSize": 1}
        for override, expected_code in (
            ({"db_name": "other"}, 13),
            ({"lsid": None}, 50737),
            ({"lsid": {"id": Binary(b"b" * 16, 4)}}, 50738),
            ({"connection": WireConnectionContext(3)}, 13),
        ):
            kwargs = {"db_name": "db", "lsid": lsid, "connection": pooled} | override
            with self.assertRaises(OperationFailure) as failure:
                await self.store.get_more_async(command, **kwargs)
            self.assertEqual(failure.exception.code, expected_code)
            self.assertEqual(source.reads, [(0, True)])
            self.assertEqual(source.close_calls, 0)
        reply = await self.store.get_more_async(
            command, db_name="db", lsid=lsid, connection=pooled
        )
        self.assertEqual(reply["cursor"]["nextBatch"], [{"v": 1}])
        self.assertEqual(source.reads[-1], (1, False))
        reply = await self.store.get_more_async(
            command, db_name="db", lsid=lsid, connection=pooled
        )
        self.assertEqual(reply["cursor"]["id"], 0)
        self.assertEqual(source.close_calls, 1)
        self.assertEqual(len(self.store), 0)

    async def test_get_more_cannot_add_session_to_sessionless_cursor(self):
        source = Source([{"v": 1}])
        cursor_id = await self.open(source)
        with self.assertRaises(OperationFailure) as failure:
            await self.store.get_more_async(
                {"getMore": cursor_id, "collection": "rows"},
                db_name="db",
                lsid={"id": Binary(b"a" * 16, 4)},
            )
        self.assertEqual(failure.exception.code, 50736)
        self.assertEqual(source.reads, [(0, True)])
        self.assertEqual(source.close_calls, 0)

    async def test_invalid_kill_list_does_not_remove_or_leak_live_source(self):
        source = Source([{"v": 1}])
        cursor_id = await self.open(source)
        with self.assertRaises(TypeError):
            await self.store.kill_cursors_async(
                {"killCursors": "rows", "cursors": [cursor_id, []]}, db_name="db"
            )
        self.assertEqual(len(self.store), 1)
        self.assertEqual(source.close_calls, 0)
        reply = await self.store.kill_cursors_async(
            {"killCursors": "rows", "cursors": [cursor_id]}, db_name="db"
        )
        self.assertEqual(reply["cursorsKilled"], [cursor_id])
        self.assertEqual(source.close_calls, 1)

    async def test_failed_or_cancelled_response_releases_source(self):
        for error in (OperationFailure("evaluation failed"), asyncio.CancelledError()):
            with self.subTest(error=type(error).__name__):
                source = Source([{"v": 1}])
                cursor_id = await self.open(source)
                source.error = error
                with self.assertRaises(type(error)):
                    await self.store.get_more_async(
                        {"getMore": cursor_id, "collection": "rows"}, db_name="db"
                    )
                self.assertEqual(source.close_calls, 1)
                self.assertEqual(len(self.store), 0)

    async def test_teardown_closes_all_sources_even_if_one_close_fails(self):
        sources = [Source([{"v": i}]) for i in range(2)]
        for source in sources:
            await self.open(source)
        original_close = sources[0].close

        async def failed_close():
            await original_close()
            message = "cleanup failed"
            raise RuntimeError(message)

        sources[0].close = failed_close
        with self.assertRaises(ExceptionGroup):
            await self.store.aclear()
        self.assertEqual([source.close_calls for source in sources], [1, 1])
        self.assertEqual(len(self.store), 0)

    async def test_concurrent_get_more_reads_source_serially(self):
        source = Source([{"v": 1}, {"v": 2}])
        cursor_id = await self.open(source)
        entered = asyncio.Event()
        release = asyncio.Event()
        original_read = source._read_response
        active = 0

        async def blocked_read(maximum, *, initial=False):
            nonlocal active
            self.assertEqual(active, 0)
            active += 1
            entered.set()
            await release.wait()
            try:
                return await original_read(maximum, initial=initial)
            finally:
                active -= 1

        source._read_response = blocked_read
        command = {"getMore": cursor_id, "collection": "rows", "batchSize": 1}
        first = asyncio.create_task(self.store.get_more_async(command, db_name="db"))
        await entered.wait()
        second = asyncio.create_task(self.store.get_more_async(command, db_name="db"))
        release.set()
        replies = await asyncio.gather(first, second)
        self.assertEqual(
            [r["cursor"]["nextBatch"] for r in replies], [[{"v": 1}], [{"v": 2}]]
        )
        self.assertEqual(source.close_calls, 1)

    async def test_cancelled_kill_finishes_all_owned_cleanup(self):
        sources = [Source([{"v": i}]) for i in range(2)]
        ids = [await self.open(source) for source in sources]
        entered = asyncio.Event()
        release = asyncio.Event()
        original_close = sources[0].close

        async def blocked_close():
            entered.set()
            await release.wait()
            await original_close()

        sources[0].close = blocked_close
        task = asyncio.create_task(
            self.store.kill_cursors_async(
                {"killCursors": "rows", "cursors": ids}, db_name="db"
            )
        )
        await entered.wait()
        task.cancel()
        release.set()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual([source.close_calls for source in sources], [1, 1])
        self.assertEqual(len(self.store), 0)


class CleanupFailureTests(unittest.IsolatedAsyncioTestCase):
    async def test_clear_attempts_every_live_source_and_reports_all_failures(self):
        store = WireCursorStore()
        sources = []
        for index in range(2):
            source = Source([{"v": index}])

            async def fail_close(source=source):
                source.close_calls += 1
                message = "cleanup failed"
                raise ValueError(message)

            source.close = fail_close
            sources.append(source)
            await store.open_stream(
                source, "db.rows", command_document={"cursor": {"batchSize": 0}}
            )
        with self.assertRaises(ExceptionGroup) as failure:
            await store.aclear()
        self.assertEqual(len(failure.exception.exceptions), 2)
        self.assertEqual([item.close_calls for item in sources], [1, 1])
        self.assertEqual(len(store), 0)
        await store.aclear()

    async def test_response_failure_keeps_primary_error_when_cleanup_fails(self):
        for phase in ("open", "get_more"):
            with self.subTest(phase=phase):
                store = WireCursorStore()
                source = Source([{"v": 1}])
                cursor_id = None
                if phase == "get_more":
                    opened = await store.open_stream(
                        source, "db.rows", command_document={"cursor": {"batchSize": 0}}
                    )
                    cursor_id = opened["cursor"]["id"]
                source.error = OperationFailure("primary evaluation error")

                async def fail_close(source=source):
                    source.close_calls += 1
                    message = "secondary cleanup error"
                    raise ValueError(message)

                source.close = fail_close
                with self.assertRaisesRegex(
                    OperationFailure, "primary evaluation error"
                ) as failure:
                    if phase == "open":
                        await store.open_stream(
                            source,
                            "db.rows",
                            command_document={"cursor": {"batchSize": 1}},
                        )
                    else:
                        await store.get_more_async(
                            {"getMore": cursor_id, "collection": "rows"}, db_name="db"
                        )
                self.assertTrue(
                    any(
                        "secondary cleanup error" in note
                        for note in failure.exception.__notes__
                    )
                )
                self.assertEqual(source.close_calls, 1)
                self.assertEqual(len(store), 0)


class LegacyCursorOwnershipTests(unittest.IsolatedAsyncioTestCase):
    async def test_sync_clear_refuses_live_source_without_discarding_ownership(self):
        store = WireCursorStore()
        source = Source([{"v": 1}])
        await store.open_stream(
            source, "db.rows", command_document={"cursor": {"batchSize": 0}}
        )
        with self.assertRaisesRegex(RuntimeError, "require async cleanup"):
            store.clear()
        self.assertEqual(len(store), 1)
        self.assertEqual(source.close_calls, 0)
        await store.aclear()
        self.assertEqual(source.close_calls, 1)
        store.clear()
        self.assertEqual(len(store), 0)

    async def test_async_get_more_can_consume_a_retained_materialized_response(self):
        store = WireCursorStore()
        opened = store.materialize_command_result(
            {"cursor": {"batchSize": 1}},
            {"cursor": {"ns": "db.rows", "firstBatch": [{"v": 1}, {"v": 2}]}},
        )
        reply = await store.get_more_async(
            {"getMore": opened["cursor"]["id"], "collection": "rows"}, db_name="db"
        )
        self.assertEqual(reply["cursor"]["nextBatch"], [{"v": 2}])
        self.assertEqual(reply["cursor"]["id"], 0)
        self.assertEqual(len(store), 0)
