import asyncio
import json
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
    _parse_resume_token,
    _resolve_change_stream_offset,
    compile_change_stream_pipeline,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventSnapshot, encode_change_stream_token


class ChangeStreamPipelineTests(unittest.TestCase):
    def test_compile_change_stream_pipeline_accepts_none_and_combines_multiple_matches(self):
        self.assertIsNone(compile_change_stream_pipeline(None))
        pipeline = compile_change_stream_pipeline(
            [
                {"$match": {"operationType": "insert"}},
                {"$match": {"ns.coll": "users"}},
                {"$project": {"operationType": 1}},
            ]
        )
        self.assertEqual(
            pipeline,
            [
                {"$match": {"operationType": "insert"}},
                {"$match": {"ns.coll": "users"}},
                {"$project": {"operationType": 1}},
            ],
        )

    def test_compile_change_stream_pipeline_rejects_unsupported_stage(self):
        with self.assertRaises(OperationFailure):
            compile_change_stream_pipeline([{"$group": {"_id": "$operationType"}}])

    def test_compile_change_stream_pipeline_accepts_supported_transform_stages(self):
        pipeline = compile_change_stream_pipeline(
            [
                {"$match": {"operationType": "insert"}},
                {"$addFields": {"kind": "$operationType"}},
                {"$set": {"alias": "$kind"}},
                {"$unset": "kind"},
                {"$replaceRoot": {"newRoot": {"alias": "$alias"}}},
            ]
        )
        self.assertEqual(
            pipeline,
            [
                {"$match": {"operationType": "insert"}},
                {"$addFields": {"kind": "$operationType"}},
                {"$set": {"alias": "$kind"}},
                {"$unset": "kind"},
                {"$replaceRoot": {"newRoot": {"alias": "$alias"}}},
            ],
        )

    def test_compile_change_stream_pipeline_rejects_invalid_stage_shape(self):
        with self.assertRaises(TypeError):
            compile_change_stream_pipeline([{"$match": {}, "$project": {}}])
        with self.assertRaises(TypeError):
            compile_change_stream_pipeline({"$match": {}})
        with self.assertRaises(TypeError):
            compile_change_stream_pipeline([{"$match": []}])
        with self.assertRaises(TypeError):
            compile_change_stream_pipeline([{"$project": []}])


class ChangeStreamHubTests(unittest.TestCase):
    def test_scope_matches_requires_matching_collection_when_configured(self):
        event = ChangeEventSnapshot(
            token=1,
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
        )
        self.assertFalse(ChangeStreamScope(db_name="beta").matches(event))
        self.assertFalse(ChangeStreamScope(coll_name="orders").matches(event))

    def test_hub_offsets_and_wait_cover_empty_and_blocking_paths(self):
        hub = ChangeStreamHub()

        self.assertEqual(hub.current_offset(), 0)
        self.assertEqual(hub.offset_after_token(10), 0)
        self.assertEqual(hub.offset_at_or_after_cluster_time(10), 0)
        self.assertEqual(hub.wait_for_event(0, timeout_seconds=0), (0, None))

        result: list[tuple[int, object | None]] = []

        def _wait():
            result.append(hub.wait_for_event(0, timeout_seconds=None))

        waiter = threading.Thread(target=_wait)
        waiter.start()
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1},
        )
        waiter.join(timeout=1)

        self.assertFalse(waiter.is_alive())
        next_offset, event = result[0]
        self.assertEqual(next_offset, 1)
        self.assertEqual(event.token, 1)
        self.assertEqual(hub.offset_after_token(1), 1)
        self.assertEqual(hub.offset_at_or_after_cluster_time(1), 0)

    def test_hub_prunes_retained_history_and_rejects_stale_offsets(self):
        hub = ChangeStreamHub(max_retained_events=2)
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
        )
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 2},
        )
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 3},
        )

        self.assertEqual(hub.current_offset(), 3)
        self.assertEqual(hub.offset_after_token(2), 2)
        self.assertEqual(hub.offset_at_or_after_cluster_time(2), 1)
        with self.assertRaisesRegex(OperationFailure, "history is no longer available"):
            hub.wait_for_event(0, timeout_seconds=0)
        with self.assertRaisesRegex(OperationFailure, "resume token is no longer available"):
            hub.offset_after_token(1)
        with self.assertRaisesRegex(OperationFailure, "start_at_operation_time is no longer available"):
            hub.offset_at_or_after_cluster_time(1)

    def test_hub_persists_history_to_journal_and_rehydrates_resume_state(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            hub = ChangeStreamHub(max_retained_events=3, journal_path=journal_path)
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
                full_document={"_id": 1},
            )
            hub.publish(
                operation_type="update",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
                full_document={"_id": 1, "name": "Ada"},
                update_description={"updatedFields": {"name": "Ada"}},
            )

            reloaded = ChangeStreamHub(max_retained_events=3, journal_path=journal_path)

            self.assertEqual(reloaded.current_offset(), 2)
            self.assertEqual(reloaded.offset_after_token(1), 1)
            next_offset, event = reloaded.wait_for_event(1, timeout_seconds=0)
            self.assertEqual(next_offset, 2)
            self.assertEqual(event.operation_type, "update")

    def test_hub_rejects_invalid_journal_payload(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            with open(journal_path, "w", encoding="utf-8") as handle:
                handle.write("{bad json")

            with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
                ChangeStreamHub(journal_path=journal_path)

    def test_hub_replays_incremental_event_log_without_rewriting_snapshot_each_time(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)

            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 2},
            )

            self.assertTrue(os.path.exists(log_path))
            reloaded = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)
            self.assertEqual(reloaded.current_offset(), 2)
            self.assertEqual(reloaded.offset_after_token(1), 1)

    def test_hub_compacts_journal_after_pruning_or_explicit_request(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(max_retained_events=2, journal_path=journal_path)
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 2},
            )
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 3},
            )

            self.assertFalse(os.path.exists(log_path))
            reloaded = ChangeStreamHub(max_retained_events=2, journal_path=journal_path)
            self.assertEqual(reloaded.current_offset(), 3)
            self.assertEqual(reloaded.offset_after_token(2), 2)

    def test_hub_can_explicitly_compact_incremental_journal(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 2},
            )

            self.assertTrue(os.path.exists(log_path))
            hub.compact_journal()
            self.assertFalse(os.path.exists(log_path))

    def test_hub_ignores_truncated_tail_entry_in_incremental_log(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )

            with open(log_path, "a", encoding="utf-8") as handle:
                handle.write('{"version":1,"event":')

            reloaded = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)
            self.assertEqual(reloaded.current_offset(), 1)
            self.assertEqual(reloaded.offset_after_token(1), 1)

    def test_hub_rejects_corrupted_incremental_log_checksum(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )

            with open(log_path, "r+", encoding="utf-8") as handle:
                line = handle.readline().strip()
                entry = json.loads(line)
                entry["checksum"] = "bad"
                handle.seek(0)
                handle.write(json.dumps(entry, separators=(",", ":"), sort_keys=True))
                handle.write("\n")
                handle.truncate()

            with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
                ChangeStreamHub(max_retained_events=8, journal_path=journal_path)

    def test_hub_compacts_journal_when_incremental_log_exceeds_byte_limit(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(
                max_retained_events=64,
                journal_path=journal_path,
                journal_max_log_bytes=1,
            )
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )

            self.assertFalse(os.path.exists(log_path))
            reloaded = ChangeStreamHub(max_retained_events=64, journal_path=journal_path)
            self.assertEqual(reloaded.current_offset(), 1)

    def test_hub_fsyncs_snapshot_and_incremental_log_when_enabled(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            with patch("mongoeco.change_streams.os.fsync") as fsync_mock:
                hub = ChangeStreamHub(
                    max_retained_events=8,
                    journal_path=journal_path,
                    journal_fsync=True,
                )
                hub.publish(
                    operation_type="insert",
                    db_name="alpha",
                    coll_name="users",
                    document_key={"_id": 1},
                )
                hub.compact_journal()

            self.assertGreaterEqual(fsync_mock.call_count, 3)
            reloaded = ChangeStreamHub(max_retained_events=8, journal_path=journal_path)
            self.assertEqual(reloaded.current_offset(), 1)


class AsyncChangeStreamCursorTests(unittest.IsolatedAsyncioTestCase):
    async def test_cursor_filters_by_scope_pipeline_and_timeout(self):
        hub = ChangeStreamHub()
        cursor = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(db_name="alpha", coll_name="users"),
            pipeline=[
                {"$match": {"operationType": "insert"}},
                {"$addFields": {"kind": "$operationType"}},
                {"$project": {"operationType": 1, "documentKey": 1, "kind": 1}},
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
            {
                "_id": {"_data": encode_change_stream_token(3)},
                "operationType": "insert",
                "documentKey": {"_id": 3},
                "kind": "insert",
            },
        )

        cursor.close()
        with self.assertRaises(OperationFailure):
            await cursor.try_next()

    async def test_cursor_can_resume_after_token_and_start_at_operation_time(self):
        hub = ChangeStreamHub()
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1},
        )
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 2},
            full_document={"_id": 2},
        )

        resumed = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            resume_after={"_data": encode_change_stream_token(1)},
            max_await_time_ms=10,
        )
        started = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            start_at_operation_time=2,
            max_await_time_ms=10,
        )

        resumed_event = await resumed.try_next()
        started_event = await started.try_next()

        self.assertEqual(resumed_event["documentKey"], {"_id": 2})
        self.assertEqual(started_event["documentKey"], {"_id": 2})

        started_after = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            start_after={"_data": encode_change_stream_token(1)},
            max_await_time_ms=10,
        )
        started_after_event = await started_after.try_next()
        self.assertEqual(started_after_event["documentKey"], {"_id": 2})

    async def test_cursor_rejects_conflicting_resume_options(self):
        hub = ChangeStreamHub()
        with self.assertRaises(OperationFailure):
            AsyncChangeStreamCursor(
                hub,
                scope=ChangeStreamScope(),
                resume_after={"_data": encode_change_stream_token(1)},
                start_after={"_data": encode_change_stream_token(2)},
            )
        with self.assertRaises(OperationFailure):
            AsyncChangeStreamCursor(
                hub,
                scope=ChangeStreamScope(),
                resume_after={"_data": "x"},
            )
        with self.assertRaises(TypeError):
            AsyncChangeStreamCursor(
                hub,
                scope=ChangeStreamScope(),
                start_at_operation_time=-1,
            )

    async def test_cursor_next_skips_non_matching_events_and_async_iteration_stops_after_close(self):
        hub = ChangeStreamHub()
        cursor = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(db_name="alpha", coll_name="users"),
            pipeline=[{"$match": {"operationType": "insert"}}],
        )

        async def _publish_events():
            await asyncio.sleep(0.01)
            hub.publish(
                operation_type="update",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
                update_description={"updatedFields": {"name": "Ada"}},
            )
            hub.publish(
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 2},
                full_document={"_id": 2, "name": "Ada"},
            )

        publisher = asyncio.create_task(_publish_events())
        event = await cursor.next()
        await publisher

        self.assertEqual(event["documentKey"], {"_id": 2})

        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 3},
            full_document={"_id": 3},
        )
        iterator = cursor.__aiter__()
        iterated = await iterator.__anext__()
        self.assertEqual(iterated["documentKey"], {"_id": 3})
        cursor.close()
        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

    async def test_cursor_closes_after_invalidate_event(self):
        hub = ChangeStreamHub()
        cursor = AsyncChangeStreamCursor(hub, scope=ChangeStreamScope())

        hub.publish(
            operation_type="invalidate",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": "users"},
        )

        event = await cursor.try_next()

        self.assertEqual(event["operationType"], "invalidate")
        self.assertFalse(cursor.alive)
        with self.assertRaises(OperationFailure):
            await cursor.try_next()

    async def test_cursor_hides_update_full_document_by_default_and_can_require_it(self):
        hub = ChangeStreamHub()
        default_cursor = AsyncChangeStreamCursor(hub, scope=ChangeStreamScope(), max_await_time_ms=10)
        lookup_cursor = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            full_document="updateLookup",
            max_await_time_ms=10,
        )

        hub.publish(
            operation_type="update",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1, "name": "Ada"},
            update_description={"updatedFields": {"name": "Ada"}},
        )

        default_event = await default_cursor.try_next()
        lookup_event = await lookup_cursor.try_next()

        self.assertNotIn("fullDocument", default_event)
        self.assertEqual(lookup_event["fullDocument"], {"_id": 1, "name": "Ada"})

    async def test_cursor_next_ignores_none_events_returned_by_wait_helper(self):
        hub = ChangeStreamHub()
        cursor = AsyncChangeStreamCursor(hub, scope=ChangeStreamScope())
        event = ChangeEventSnapshot(
            token=1,
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1},
        )

        with patch(
            "mongoeco.change_streams.asyncio.to_thread",
            side_effect=[(0, None), (1, event)],
        ):
            document = await cursor.next()

        self.assertEqual(document["documentKey"], {"_id": 1})


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
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 2},
            full_document={"_id": 2},
        )

        document = cursor.try_next()
        self.assertEqual(document["operationType"], "insert")
        self.assertEqual(cursor.next()["documentKey"], {"_id": 2})
        self.assertTrue(cursor.alive)
        cursor.close()
        self.assertFalse(cursor.alive)


class ChangeStreamOffsetHelpersTests(unittest.TestCase):
    def test_parse_resume_token_and_resolve_offset_cover_remaining_paths(self):
        hub = ChangeStreamHub()
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1},
        )

        self.assertEqual(_parse_resume_token({"_data": "1"}), 1)
        self.assertEqual(_parse_resume_token({"_data": encode_change_stream_token(1)}), 1)
        self.assertEqual(
            _resolve_change_stream_offset(
                hub,
                resume_after=None,
                start_after={"_data": encode_change_stream_token(1)},
                start_at_operation_time=None,
            ),
            1,
        )

        with self.assertRaises(OperationFailure):
            _parse_resume_token({"_data": "abc"})
