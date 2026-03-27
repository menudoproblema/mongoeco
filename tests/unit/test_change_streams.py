import unittest

from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
    compile_change_stream_pipeline,
)
from mongoeco.errors import OperationFailure


class ChangeStreamPipelineTests(unittest.TestCase):
    def test_compile_change_stream_pipeline_rejects_unsupported_stage(self):
        with self.assertRaises(OperationFailure):
            compile_change_stream_pipeline([{"$group": {"_id": "$operationType"}}])

    def test_compile_change_stream_pipeline_accepts_match_and_project(self):
        match_filter, projection = compile_change_stream_pipeline(
            [{"$match": {"operationType": "insert"}}, {"$project": {"operationType": 1}}]
        )
        self.assertEqual(match_filter, {"operationType": "insert"})
        self.assertEqual(projection, {"operationType": 1})


class AsyncChangeStreamCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_cursor_filters_by_scope_pipeline_and_timeout(self):
        hub = ChangeStreamHub()
        cursor = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(db_name="alpha", coll_name="users"),
            pipeline=[
                {"$match": {"operationType": "insert"}},
                {"$project": {"operationType": 1, "documentKey": 1}},
            ],
            max_await_time_ms=25,
        )

        self.assertIsNone(await cursor.try_next())

        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="other",
            document_key={"_id": 1},
            full_document={"_id": 1},
        )
        hub.publish(
            operation_type="update",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 2},
            full_document={"_id": 2},
        )
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 3},
            full_document={"_id": 3, "name": "Ada"},
        )

        event = await cursor.try_next()
        self.assertEqual(
            event,
            {"_id": {"_data": "3"}, "operationType": "insert", "documentKey": {"_id": 3}},
        )

        cursor.close()
        with self.assertRaises(OperationFailure):
            await cursor.try_next()


class ChangeStreamCursorTests(unittest.TestCase):
    def test_sync_cursor_delegates_to_async_cursor(self):
        class _FakeClient:
            def _run(self, awaitable):
                import asyncio

                return asyncio.run(awaitable)

        hub = ChangeStreamHub()
        async_cursor = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            max_await_time_ms=25,
        )
        cursor = ChangeStreamCursor(_FakeClient(), async_cursor)
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1},
        )

        document = cursor.try_next()
        self.assertEqual(document["operationType"], "insert")
        self.assertTrue(cursor.alive)
        cursor.close()
        self.assertFalse(cursor.alive)
