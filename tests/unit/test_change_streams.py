import asyncio
import json
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamBackendInfo,
    ChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
    _parse_resume_token,
    _resolve_change_stream_offset,
    compile_change_stream_pipeline,
)
from mongoeco._change_streams.pipeline import apply_full_document_mode, normalize_full_document_mode
from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventSnapshot, encode_change_stream_token
from mongoeco._change_streams import journal as journal_module


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

    def test_change_stream_pipeline_helpers_cover_full_document_modes(self):
        with self.assertRaises(TypeError):
            normalize_full_document_mode(1)

        self.assertEqual(normalize_full_document_mode("required"), "required")
        with self.assertRaises(OperationFailure):
            normalize_full_document_mode("later")

        insert = {"operationType": "insert", "fullDocument": {"_id": 1}}
        update = {"operationType": "update", "fullDocument": {"_id": 1}}
        self.assertIs(apply_full_document_mode(insert, "default"), insert)
        self.assertNotIn("fullDocument", apply_full_document_mode(update, "default"))
        self.assertEqual(apply_full_document_mode(update, "whenAvailable"), update)
        with self.assertRaisesRegex(OperationFailure, "required"):
            apply_full_document_mode({"operationType": "update"}, "required")


class ChangeStreamHubTests(unittest.TestCase):
    def test_hub_constructor_and_properties_validate_inputs(self):
        with self.assertRaises(TypeError):
            ChangeStreamHub(max_retained_events=0)
        with self.assertRaises(TypeError):
            ChangeStreamHub(journal_path="")
        with self.assertRaises(TypeError):
            ChangeStreamHub(journal_fsync=1)
        with self.assertRaises(TypeError):
            ChangeStreamHub(journal_max_log_bytes=0)

        hub = ChangeStreamHub(max_retained_events=None)
        self.assertIsNone(hub.journal_path)
        self.assertFalse(hub.journal_fsync)
        self.assertEqual(hub.journal_max_log_bytes, 1_048_576)

    def test_hub_backend_info_reports_local_persistent_capabilities(self):
        hub = ChangeStreamHub(
            max_retained_events=12,
            journal_path="/tmp/mongoeco-change-streams.json",
            journal_fsync=True,
            journal_max_log_bytes=4096,
        )

        info = hub.backend_info

        self.assertIsInstance(info, ChangeStreamBackendInfo)
        self.assertEqual(
            info.to_document(),
            {
                "implementation": "local",
                "distributed": False,
                "persistent": True,
                "resumable": True,
                "resumableAcrossClientRestarts": True,
                "resumableAcrossProcesses": True,
                "resumableAcrossNodes": False,
                "boundedHistory": True,
                "maxRetainedEvents": 12,
                "journalEnabled": True,
                "journalFsync": True,
                "journalMaxLogBytes": 4096,
            },
        )

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

    def test_hub_journal_helpers_cover_empty_wait_and_error_paths(self):
        hub = ChangeStreamHub()
        hub._events = []  # noqa: SLF001
        self.assertEqual(hub.wait_for_event(0, timeout_seconds=0), (0, None))
        hub._journal_path = None  # noqa: SLF001
        hub._persist_locked()  # noqa: SLF001
        with patch.object(hub, "_end_offset_locked", side_effect=[1, 0]):
            self.assertEqual(hub.wait_for_event(0, timeout_seconds=0), (0, None))

        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            hub = ChangeStreamHub(journal_path=journal_path)
            hub._journal_event_log_path = log_path  # noqa: SLF001
            hub._compact_locked()  # noqa: SLF001
            hub._compact_locked()  # noqa: SLF001

            with open(journal_path, "w", encoding="utf-8") as handle:
                json.dump({"version": 0}, handle)
            with self.assertRaisesRegex(OperationFailure, "could not be loaded"):
                ChangeStreamHub(journal_path=journal_path)

            os.remove(journal_path)
            with open(journal_path, "w", encoding="utf-8") as handle:
                json.dump({"version": 1, "base_offset": "bad", "next_token": 1, "events": []}, handle)
            with self.assertRaisesRegex(OperationFailure, "could not be loaded"):
                ChangeStreamHub(journal_path=journal_path)

            os.remove(journal_path)
            with open(log_path, "w", encoding="utf-8") as handle:
                handle.write("{not-json}\n")
            with self.assertRaisesRegex(OperationFailure, "could not be loaded"):
                ChangeStreamHub(journal_path=journal_path)
            os.remove(log_path)
            with open(log_path, "w", encoding="utf-8") as handle:
                handle.write("{}\n")
            with patch("mongoeco._change_streams.hub.open", side_effect=OSError("boom")):
                with self.assertRaisesRegex(OperationFailure, "could not be loaded"):
                    ChangeStreamHub(journal_path=journal_path)

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

    def test_hub_state_reports_retention_journal_and_compaction_metadata(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            hub = ChangeStreamHub(
                max_retained_events=2,
                journal_path=journal_path,
                journal_max_log_bytes=4096,
            )
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
            state_before = hub.state.to_document()
            hub.compact_journal()
            state_after = hub.state.to_document()

        self.assertEqual(state_before["retainedEvents"], 2)
        self.assertEqual(state_before["currentOffset"], 2)
        self.assertEqual(state_before["retainedStartToken"], 1)
        self.assertEqual(state_before["retainedEndToken"], 2)
        self.assertTrue(state_before["journalEnabled"])
        self.assertTrue(state_before["eventLogExists"])
        self.assertEqual(state_before["journalMaxLogBytes"], 4096)
        self.assertFalse(state_after["eventLogExists"])
        self.assertGreaterEqual(state_after["journalCompactionCount"], 1)

    def test_hub_covers_none_retention_threshold_and_empty_compaction_paths(self):
        hub = ChangeStreamHub(max_retained_events=None)
        hub.publish(
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
        )
        self.assertEqual(hub.current_offset(), 1)
        hub.compact_journal()
        self.assertEqual(hub.current_offset(), 1)

    def test_hub_wait_for_event_detects_history_expiring_while_waiting(self):
        hub = ChangeStreamHub(max_retained_events=1)
        first = ChangeEventSnapshot(
            token=2,
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 2},
        )

        def _expire_history(_timeout=None):
            hub._events = [first]
            hub._base_offset = 1
            hub._next_token = 3

        with patch.object(hub._condition, "wait", side_effect=_expire_history):
            with self.assertRaisesRegex(OperationFailure, "history is no longer available"):
                hub.wait_for_event(0, timeout_seconds=None)

    def test_hub_load_journal_rejects_invalid_payloads_and_tolerates_blank_or_duplicate_log_entries(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            with open(journal_path, "w", encoding="utf-8") as handle:
                json.dump({"version": 2, "base_offset": 0, "next_token": 1, "events": []}, handle)

            with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
                ChangeStreamHub(journal_path=journal_path)

            with open(journal_path, "w", encoding="utf-8") as handle:
                json.dump({"version": 1, "base_offset": 0, "next_token": 2, "events": []}, handle)
            event = ChangeEventSnapshot(
                token=1,
                operation_type="insert",
                db_name="alpha",
                coll_name="users",
                document_key={"_id": 1},
            )
            with open(log_path, "w", encoding="utf-8") as handle:
                handle.write("\n")
                handle.write(journal_module.journal_event_entry(event))
                handle.write("\n")

            hub = ChangeStreamHub(journal_path=journal_path)
            self.assertEqual(hub.current_offset(), 0)

    def test_hub_load_journal_handles_event_log_open_failures(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            journal_path = os.path.join(temp_dir, "changes.json")
            log_path = f"{journal_path}.events"
            with open(journal_path, "w", encoding="utf-8") as handle:
                json.dump({"version": 1, "base_offset": 0, "next_token": 1, "events": []}, handle)
            with open(log_path, "w", encoding="utf-8") as handle:
                handle.write("")

            with patch("mongoeco._change_streams.hub.open", side_effect=OSError("boom")):
                with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
                    ChangeStreamHub(journal_path=journal_path)


class ChangeStreamJournalTests(unittest.TestCase):
    def test_journal_helpers_validate_shapes_and_fsync_errors(self):
        event = ChangeEventSnapshot(
            token=1,
            operation_type="insert",
            db_name="alpha",
            coll_name="users",
            document_key={"_id": 1},
            full_document={"_id": 1},
            update_description={"updatedFields": {"name": "Ada"}},
        )

        document = journal_module.snapshot_to_document(event)
        self.assertEqual(journal_module.snapshot_from_document(document), event)
        self.assertEqual(journal_module.snapshot_from_log_entry(json.loads(journal_module.journal_event_entry(event))), event)

        with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
            journal_module.snapshot_from_document("bad")
        with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
            journal_module.snapshot_from_document({"token": 0, "operation_type": "insert", "db_name": "a", "coll_name": "b", "document_key": {}})
        with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
            journal_module.snapshot_from_document({"token": 1, "operation_type": "insert", "db_name": "a", "coll_name": "b", "document_key": {}, "full_document": []})
        with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
            journal_module.snapshot_from_log_entry("bad")
        with self.assertRaisesRegex(OperationFailure, "journal could not be loaded"):
            journal_module.snapshot_from_log_entry({"version": 2, "event": {}, "checksum": "x"})

        with patch("mongoeco._change_streams.journal.os.open", side_effect=OSError("no-open")):
            journal_module.fsync_parent_directory("/tmp/example")
        with (
            patch("mongoeco._change_streams.journal.os.open", return_value=10),
            patch("mongoeco._change_streams.journal.os.fsync", side_effect=OSError("no-fsync")),
            patch("mongoeco._change_streams.journal.os.close") as close_mock,
        ):
            journal_module.fsync_parent_directory("/tmp/example")
        close_mock.assert_called_once_with(10)


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
