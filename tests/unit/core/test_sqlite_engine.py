import asyncio
import datetime
import sqlite3
import threading
import unittest
import uuid
from unittest.mock import Mock, patch

from mongoeco.compat import MongoDialect70
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import MatchAll, compile_filter
from mongoeco.core.sorting import sort_documents
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.types import ObjectId, UNDEFINED


class SQLiteEngineTests(unittest.IsolatedAsyncioTestCase):
    def test_delete_matching_document_sync_falls_back_for_custom_dialect(self):
        class CustomDialect(MongoDialect70):
            pass

        engine = SQLiteEngine()
        fake_connection = Mock()
        fake_connection.execute.return_value.rowcount = 1
        engine._require_connection = Mock(return_value=fake_connection)
        engine._load_documents = Mock(return_value=[("1", {"kind": "match"})])

        result = engine._delete_matching_document_sync(
            "db",
            "coll",
            {"kind": "match"},
            compile_filter({"kind": "match"}),
            None,
            CustomDialect(),
        )

        self.assertEqual(result.deleted_count, 1)
        fake_connection.execute.assert_called_once()

    def test_count_matching_documents_sync_falls_back_for_custom_dialect(self):
        class CustomDialect(MongoDialect70):
            pass

        engine = SQLiteEngine()
        engine._load_documents = Mock(
            return_value=[
                ("1", {"kind": "match"}),
                ("2", {"kind": "skip"}),
            ]
        )

        count = engine._count_matching_documents_sync(
            "db",
            "coll",
            {"kind": "match"},
            compile_filter({"kind": "match"}),
            None,
            CustomDialect(),
        )

        self.assertEqual(count, 1)

    def test_require_connection_raises_when_disconnected(self):
        engine = SQLiteEngine()

        with self.assertRaises(RuntimeError):
            engine._require_connection()

    def test_plan_fields_handles_match_all_leaf_and_logical_nodes(self):
        plan = compile_filter({"$or": [{"items.name": "a"}, {"kind": "view"}]})

        self.assertEqual(SQLiteEngine._plan_fields(MatchAll()), set())
        self.assertEqual(SQLiteEngine._plan_fields(plan), {"items.name", "kind"})

        class UnknownPlan:
            pass

        self.assertEqual(SQLiteEngine._plan_fields(UnknownPlan()), set())

    async def test_disconnect_handles_zero_and_refcounted_connections(self):
        engine = SQLiteEngine()

        await engine.disconnect()
        self.assertEqual(engine._connection_count, 0)

        await engine.connect()
        await engine.connect()
        await engine.disconnect()

        self.assertEqual(engine._connection_count, 1)
        self.assertIsNotNone(engine._connection)

        await engine.disconnect()
        self.assertEqual(engine._connection_count, 0)
        self.assertIsNone(engine._connection)

    async def test_disconnect_waits_for_active_scan_to_finish_before_closing_connection(self):
        engine = SQLiteEngine()
        await engine.connect()
        started = threading.Event()
        release = threading.Event()

        def blocked_iter(*args, **kwargs):
            started.set()
            release.wait()
            if False:
                yield None

        try:
            with patch.object(engine, "_iter_scan_documents_sync", side_effect=blocked_iter):
                iterator = engine.scan_collection("db", "coll").__aiter__()
                next_task = asyncio.create_task(iterator.__anext__())
                await asyncio.to_thread(started.wait, 1)

                disconnect_task = asyncio.create_task(engine.disconnect())
                await asyncio.sleep(0.05)

                self.assertFalse(disconnect_task.done())
                self.assertIsNotNone(engine._connection)

                release.set()
                with self.assertRaises(StopAsyncIteration):
                    await next_task
                await disconnect_task
        finally:
            release.set()
            if engine._connection is not None or engine._connection_count:
                await engine.disconnect()

        self.assertIsNone(engine._connection)
        self.assertEqual(engine._connection_count, 0)

    async def test_scan_collection_rejects_negative_skip_and_limit(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with self.assertRaises(ValueError):
                async for _ in engine.scan_collection("db", "coll", skip=-1):
                    pass

            with self.assertRaises(ValueError):
                async for _ in engine.scan_collection("db", "coll", limit=-1):
                    pass
        finally:
            await engine.disconnect()

    async def test_update_matching_document_supports_upsert(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            result = await engine.update_matching_document(
                "db",
                "coll",
                {"kind": "view"},
                {"$set": {"done": True}},
                upsert=True,
                upsert_seed={"kind": "view"},
            )
            found = await engine.get_document("db", "coll", result.upserted_id)
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertEqual(found, {"_id": result.upserted_id, "kind": "view", "done": True})

    async def test_update_matching_document_supports_upsert_with_explicit_seed_id(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            result = await engine.update_matching_document(
                "db",
                "coll",
                {"kind": "view"},
                {"$set": {"done": True}},
                upsert=True,
                upsert_seed={"_id": "seeded", "kind": "view"},
            )
            found = await engine.get_document("db", "coll", "seeded")
        finally:
            await engine.disconnect()

        self.assertEqual(result.upserted_id, "seeded")
        self.assertEqual(found, {"_id": "seeded", "kind": "view", "done": True})

    async def test_storage_key_distinguishes_int_and_float_ids(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": 1, "kind": "int"})
            await engine.put_document("db", "coll", {"_id": 1.0, "kind": "float"})
            int_doc = await engine.get_document("db", "coll", 1)
            float_doc = await engine.get_document("db", "coll", 1.0)
        finally:
            await engine.disconnect()

        self.assertEqual(int_doc, {"_id": 1, "kind": "int"})
        self.assertEqual(float_doc, {"_id": 1.0, "kind": "float"})

    async def test_sqlite_preserves_document_field_order_round_trip(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            document = {"_id": "1", "z": 1, "a": 2}
            await engine.put_document("db", "coll", document)
            found = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertEqual(list(found.keys()), ["_id", "z", "a"])
        self.assertEqual(found, document)

    async def test_put_and_get_document_support_bytes_ids_and_payloads(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            document = {"_id": b"123456789012", "payload": {"blob": b"\x00\x01\xff"}}

            await engine.put_document("db", "coll", document)
            found = await engine.get_document("db", "coll", b"123456789012")
        finally:
            await engine.disconnect()

        self.assertEqual(found, document)

    async def test_select_first_document_for_plan_rejects_array_traversing_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})
            with self.assertRaises(NotImplementedError):
                engine._select_first_document_for_plan("db", "coll", compile_filter({"items.name": "a"}))
        finally:
            await engine.disconnect()

    async def test_update_matching_document_upsert_raises_duplicate_on_secondary_unique_index_collision(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            with self.assertRaises(DuplicateKeyError):
                await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "missing"},
                    {"$set": {"email": "a@example.com"}},
                    upsert=True,
                    upsert_seed={"kind": "missing"},
                )
        finally:
            await engine.disconnect()

    async def test_update_matching_document_returns_zero_when_no_match_and_no_upsert(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "existing"})
            result = await engine.update_matching_document(
                "db",
                "coll",
                {"kind": "missing"},
                {"$set": {"done": True}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)
        self.assertIsNone(result.upserted_id)

    async def test_update_matching_document_does_not_fall_back_to_python_scan_when_sql_finds_no_match(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "existing"})
            with patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")):
                result = await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "missing"},
                    {"$set": {"done": True}},
                )
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 0)
        self.assertEqual(result.modified_count, 0)

    async def test_load_indexes_rejects_invalid_json_metadata(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            conn = engine._require_connection()
            conn.execute(
                """
                INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, unique_flag)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("db", "coll", "broken", "idx_broken", "{not-json", 0),
            )
            conn.commit()

            with self.assertRaises(OperationFailure):
                engine._load_indexes("db", "coll")
        finally:
            await engine.disconnect()

    async def test_load_indexes_rejects_non_list_metadata(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            conn = engine._require_connection()
            conn.execute(
                """
                INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, unique_flag)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                ("db", "coll", "broken", "idx_broken", '"field"', 0),
            )
            conn.commit()

            with self.assertRaises(OperationFailure):
                engine._load_indexes("db", "coll")
        finally:
            await engine.disconnect()

    async def test_update_matching_document_returns_zero_modified_for_noop(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "existing"})
            result = await engine.update_matching_document(
                "db",
                "coll",
                {"_id": "1"},
                {"$set": {"kind": "existing"}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 0)

    async def test_update_matching_document_raises_when_dotted_set_crosses_scalar_parent(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "profile": 1})

            with self.assertRaises(OperationFailure):
                await engine.update_matching_document(
                    "db",
                    "coll",
                    {"_id": "1"},
                    {"$set": {"profile.name": "Ada"}},
                )
            found = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertEqual(found, {"_id": "1", "profile": 1})

    async def test_update_matching_document_skips_non_matching_row_before_match_and_detects_duplicate_upsert_id(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "skip"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "match"})

            result = await engine.update_matching_document(
                "db",
                "coll",
                {"kind": "match"},
                {"$set": {"done": True}},
            )
            found = await engine.get_document("db", "coll", "2")

            with self.assertRaises(DuplicateKeyError):
                await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "missing"},
                    {"$set": {"done": True}},
                    upsert=True,
                    upsert_seed={"_id": "1", "kind": "missing"},
                )
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(found, {"_id": "2", "kind": "match", "done": True})

    async def test_delete_matching_document_skips_non_matching_row_before_match(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "skip"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "match"})

            result = await engine.delete_matching_document("db", "coll", {"kind": "match"})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 1)

    async def test_delete_matching_document_uses_sql_delete_for_translatable_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                result = await engine.delete_matching_document("db", "coll", {"kind": "view"})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 1)

    async def test_delete_matching_document_falls_back_for_array_traversing_embedded_document_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})
            await engine.put_document("db", "coll", {"_id": "2", "items": [{"name": "b"}]})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                result = await engine.delete_matching_document("db", "coll", {"items.name": "a"})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 1)
        self.assertGreater(match_plan.call_count, 0)

    async def test_delete_matching_document_falls_back_for_untranslatable_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "2", "data": {"b": 2}})
            await engine.put_document("db", "coll", {"_id": "1", "data": {"b": 1}})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                result = await engine.delete_matching_document("db", "coll", {"data": {"b": 1}})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 1)
        self.assertGreater(match_plan.call_count, 0)

    async def test_delete_matching_document_returns_zero_when_no_match(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "click"})
            result = await engine.delete_matching_document("db", "coll", {"kind": "view"})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 0)

    async def test_create_index_handles_existing_name_and_duplicate_unique_payload(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.put_document("db", "coll", {"_id": "2", "email": "b@example.com"})

            name = await engine.create_index("db", "coll", ["email"], unique=False, name="idx")
            same_name = await engine.create_index("db", "coll", ["email"], unique=False, name="idx")
            self.assertEqual(name, same_name)

            with self.assertRaises(DuplicateKeyError):
                await engine.create_index("db", "coll", ["other"], unique=False, name="idx")

            await engine.put_document("db", "dups", {"_id": "1", "email": "dup@example.com"})
            await engine.put_document("db", "dups", {"_id": "2", "email": "dup@example.com"})
            with self.assertRaises(DuplicateKeyError):
                await engine.create_index("db", "dups", ["email"], unique=True)

            physical_name = engine._physical_index_name("db", "coll", "idx")
            created = engine._require_connection().execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
                (physical_name,),
            ).fetchone()
            self.assertEqual(created[0], physical_name)
        finally:
            await engine.disconnect()

    async def test_create_unique_index_rejects_existing_array_traversal_payloads(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})

            with self.assertRaises(OperationFailure):
                await engine.create_index("db", "coll", ["items.name"], unique=True)
        finally:
            await engine.disconnect()

    async def test_validate_document_against_unique_indexes_ignores_non_unique_definitions(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with patch.object(engine, "_load_indexes", return_value=[{"unique": False, "fields": ["items.name"]}]):
                engine._validate_document_against_unique_indexes("db", "coll", {"items": [{"name": "a"}]})
        finally:
            await engine.disconnect()

    async def test_unique_index_rejects_future_documents_that_traverse_arrays(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["items.name"], unique=True)

            with self.assertRaises(OperationFailure):
                await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})
        finally:
            await engine.disconnect()

    async def test_explain_query_plan_uses_created_physical_index(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=False, name="idx")

            details = engine._explain_query_plan_sync("db", "coll", {"email": "a@example.com"})
        finally:
            await engine.disconnect()

        self.assertTrue(any("idx_" in detail for detail in details))

    async def test_explain_query_plan_uses_created_physical_index_for_codec_aware_filter(self):
        engine = SQLiteEngine()
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "session_id": session_id})
            await engine.create_index("db", "coll", ["session_id"], unique=False, name="idx_session")

            details = engine._explain_query_plan_sync("db", "coll", {"session_id": session_id})
        finally:
            await engine.disconnect()

        self.assertTrue(any("idx_" in detail for detail in details))

    async def test_explain_query_plan_rejects_array_traversing_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})
            with self.assertRaises(NotImplementedError):
                engine._explain_query_plan_sync("db", "coll", {"items.name": "a"})
        finally:
            await engine.disconnect()

    async def test_explain_query_plan_rejects_tagged_bytes_range_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "v": b"\x00"})
            with self.assertRaises(NotImplementedError):
                engine._explain_query_plan_sync(
                    "db",
                    "coll",
                    {"v": {"$gt": uuid.UUID("12345678-1234-5678-1234-567812345678")}},
                )
        finally:
            await engine.disconnect()

    async def test_explain_query_plan_rejects_top_level_array_comparisons(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "v": [1]})
            with self.assertRaises(NotImplementedError):
                engine._explain_query_plan_sync("db", "coll", {"v": {"$gt": 0}})
        finally:
            await engine.disconnect()

    async def test_select_first_document_for_plan_rejects_tagged_bytes_range_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "v": b"\x00"})
            with self.assertRaises(NotImplementedError):
                engine._select_first_document_for_plan(
                    "db",
                    "coll",
                    compile_filter({"v": {"$gt": uuid.UUID("12345678-1234-5678-1234-567812345678")}}),
                )
        finally:
            await engine.disconnect()

    async def test_unique_index_is_enforced_atomically_on_update(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.put_document("db", "coll", {"_id": "2", "email": "b@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            with self.assertRaises(DuplicateKeyError):
                await engine.update_matching_document(
                    "db",
                    "coll",
                    {"_id": "2"},
                    {"$set": {"email": "a@example.com"}},
                )
        finally:
            await engine.disconnect()

    async def test_put_document_with_overwrite_uses_atomic_upsert_and_respects_unique_indexes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"}, overwrite=False)
            await engine.put_document("db", "coll", {"_id": "2", "email": "b@example.com"}, overwrite=False)
            await engine.create_index("db", "coll", ["email"], unique=True)

            replaced = await engine.put_document(
                "db",
                "coll",
                {"_id": "2", "email": "c@example.com"},
                overwrite=True,
            )
            found = await engine.get_document("db", "coll", "2")

            with self.assertRaises(DuplicateKeyError):
                await engine.put_document(
                    "db",
                    "coll",
                    {"_id": "2", "email": "a@example.com"},
                    overwrite=True,
                )
        finally:
            await engine.disconnect()

        self.assertTrue(replaced)
        self.assertEqual(found, {"_id": "2", "email": "c@example.com"})

    async def test_put_document_without_overwrite_uses_sql_do_nothing(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            inserted = await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"}, overwrite=False)
            skipped = await engine.put_document("db", "coll", {"_id": "1", "email": "b@example.com"}, overwrite=False)
            found = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertTrue(inserted)
        self.assertFalse(skipped)
        self.assertEqual(found, {"_id": "1", "email": "a@example.com"})

    async def test_connect_migrates_legacy_indexes_table_without_physical_name(self):
        legacy_connection = sqlite3.connect(":memory:", check_same_thread=False)
        try:
            legacy_connection.execute(
                """
                CREATE TABLE indexes (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    fields TEXT NOT NULL,
                    unique_flag INTEGER NOT NULL,
                    PRIMARY KEY (db_name, coll_name, name)
                )
                """
            )
            legacy_connection.commit()
        finally:
            pass

        engine = SQLiteEngine()
        with patch("mongoeco.engines.sqlite.sqlite3.connect", return_value=legacy_connection):
            try:
                await engine.connect()
                columns = {
                    row[1]
                    for row in engine._require_connection().execute("PRAGMA table_info(indexes)").fetchall()
                }
            finally:
                await engine.disconnect()

        self.assertIn("physical_name", columns)

    async def test_drop_collection_removes_physical_index(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=True, name="idx")
            physical_name = engine._physical_index_name("db", "coll", "idx")

            await engine.drop_collection("db", "coll")

            row = engine._require_connection().execute(
                "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
                (physical_name,),
            ).fetchone()
        finally:
            await engine.disconnect()

        self.assertIsNone(row)

    def test_drop_collection_rolls_back_if_metadata_delete_fails(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        fake_connection.execute.side_effect = [
            None,
            None,
            sqlite3.OperationalError("boom"),
        ]
        engine._require_connection = Mock(return_value=fake_connection)
        engine._load_indexes = Mock(return_value=[{"physical_name": "idx_test"}])

        with self.assertRaises(sqlite3.OperationalError):
            engine._drop_collection_sync("db", "coll")

        fake_connection.rollback.assert_called_once()

    def test_delete_matching_document_sync_skips_non_matching_fallback_rows(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        fake_connection.execute.return_value.rowcount = 1
        engine._require_connection = Mock(return_value=fake_connection)
        engine._select_first_document_for_plan = Mock(return_value=None)
        engine._load_documents = Mock(
            return_value=[
                ("1", {"kind": "skip"}),
                ("2", {"kind": "match"}),
            ]
        )

        result = engine._delete_matching_document_sync("db", "coll", {"kind": "match"}, None, None)

        self.assertEqual(result.deleted_count, 1)
        fake_connection.execute.assert_called_once()
        fake_connection.commit.assert_called_once()

    def test_delete_matching_document_sync_returns_zero_when_fallback_finds_nothing(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        engine._require_connection = Mock(return_value=fake_connection)
        engine._build_select_sql = Mock(side_effect=NotImplementedError("fallback"))
        engine._load_documents = Mock(return_value=[])

        result = engine._delete_matching_document_sync("db", "coll", {"kind": "match"}, None, None)

        self.assertEqual(result.deleted_count, 0)

    def test_delete_matching_document_sync_falls_back_for_tagged_bytes_plan(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        engine._require_connection = Mock(return_value=fake_connection)
        engine._plan_has_array_traversing_paths = Mock(return_value=False)
        engine._plan_requires_python_for_array_comparisons = Mock(return_value=False)
        engine._plan_requires_python_for_bytes = Mock(return_value=True)
        engine._load_documents = Mock(return_value=[("1", {"v": b"\x00"})])

        result = engine._delete_matching_document_sync(
            "db",
            "coll",
            {"v": {"$gt": uuid.UUID("12345678-1234-5678-1234-567812345678")}},
            compile_filter({"v": {"$gt": uuid.UUID("12345678-1234-5678-1234-567812345678")}}),
            None,
        )

        self.assertEqual(result.deleted_count, 1)

    def test_update_matching_document_sync_commits_successful_upsert(self):
        engine = SQLiteEngine()
        engine._connect_sync()
        try:
            result = engine._update_matching_document_sync(
                "db",
                "coll",
                {"kind": "view"},
                {"$set": {"done": True}},
                True,
                {"_id": "seeded", "kind": "view"},
                None,
                None,
            )
            found = engine._get_document_sync("db", "coll", "seeded", None, None)
        finally:
            engine._disconnect_sync()

        self.assertEqual(result.upserted_id, "seeded")
        self.assertEqual(found, {"_id": "seeded", "kind": "view", "done": True})

    async def test_scan_collection_uses_sql_translation_for_simple_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                documents = [
                    document
                    async for document in engine.scan_collection("db", "coll", {"kind": "view"})
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "kind": "view"}])

    async def test_update_and_delete_prefer_explicit_plan_over_conflicting_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})
            plan = compile_filter({"kind": "view"})

            update_result = await engine.update_matching_document(
                "db",
                "coll",
                {"kind": "click"},
                {"$set": {"done": True}},
                plan=plan,
            )
            delete_result = await engine.delete_matching_document(
                "db",
                "coll",
                {"kind": "click"},
                plan=plan,
            )
            remaining = [doc async for doc in engine.scan_collection("db", "coll", sort=[("_id", 1)])]
        finally:
            await engine.disconnect()

        self.assertEqual(update_result.matched_count, 1)
        self.assertEqual(delete_result.deleted_count, 1)
        self.assertEqual(remaining, [{"_id": "2", "kind": "click"}])

    async def test_scan_collection_uses_sql_translation_for_sort_skip_and_limit(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view", "rank": 3})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view", "rank": 1})
            await engine.put_document("db", "coll", {"_id": "3", "kind": "view", "rank": 2})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                documents = [
                    document
                    async for document in engine.scan_collection(
                        "db",
                        "coll",
                        {"kind": "view"},
                        sort=[("rank", 1)],
                        skip=1,
                        limit=1,
                    )
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "3", "kind": "view", "rank": 2}])

    async def test_scan_collection_uses_sql_translation_for_skip_without_limit(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view"})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                documents = [
                    document
                    async for document in engine.scan_collection("db", "coll", {"kind": "view"}, skip=1)
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "2", "kind": "view"}])

    async def test_scan_collection_falls_back_to_query_engine_for_untranslatable_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "data": {"b": 1}})
            await engine.put_document("db", "coll", {"_id": "2", "data": {"b": 2}})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                documents = [
                    document
                    async for document in engine.scan_collection("db", "coll", {"data": {"b": 1}})
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "data": {"b": 1}}])
        self.assertGreater(match_plan.call_count, 0)

    async def test_scan_collection_falls_back_for_array_sort_with_skip_and_limit(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": [2, 3]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": [1, 4]})
            with patch.object(engine, "_load_documents", wraps=engine._load_documents) as load_documents:
                documents = [
                    document
                    async for document in engine.scan_collection("db", "coll", sort=[("rank", 1)], skip=1, limit=1)
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "rank": [2, 3]}])
        self.assertGreater(load_documents.call_count, 0)

    async def test_scan_collection_filters_before_materializing_python_sort_fallback(self):
        engine = SQLiteEngine()
        captured_lengths: list[int] = []
        await engine.connect()

        def _documents(_db_name, _coll_name):
            for document_id, payload in (
                ("1", {"name": "Ada", "rank": [2, 3]}),
                ("2", {"name": "Bob", "rank": [1, 4]}),
                ("3", {"name": "Ana", "rank": [0, 5]}),
            ):
                yield document_id, {"_id": document_id, **payload}

        def _sort(documents, sort):
            captured_lengths.append(len(documents))
            return sort_documents(documents, sort)

        try:
            with (
                patch.object(engine, "_load_documents", side_effect=_documents),
                patch("mongoeco.engines.sqlite.sort_documents", side_effect=_sort),
            ):
                documents = [
                    document
                    async for document in engine.scan_collection(
                        "db",
                        "coll",
                        {"name": {"$regex": "^A"}},
                        sort=[("rank", 1)],
                    )
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "3", "name": "Ana", "rank": [0, 5]}, {"_id": "1", "name": "Ada", "rank": [2, 3]}])
        self.assertEqual(captured_lengths, [2])

    async def test_scan_collection_automatically_falls_back_for_array_sort(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": [2, 3]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": [1, 4]})

            with patch.object(engine, "_load_documents", wraps=engine._load_documents) as load_documents:
                documents = [
                    document
                    async for document in engine.scan_collection("db", "coll", sort=[("rank", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "2", "rank": [1, 4]}, {"_id": "1", "rank": [2, 3]}])
        self.assertGreater(load_documents.call_count, 0)

    async def test_sort_requires_python_for_array_traversing_plan_and_sort_key(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})
            plan = compile_filter({"items.name": "a"})

            self.assertTrue(engine._sort_requires_python("db", "coll", plan, [("kind", 1)]))
            self.assertTrue(engine._sort_requires_python("db", "coll", MatchAll(), [("items.name", 1)]))
        finally:
            await engine.disconnect()

    async def test_sort_requires_python_when_tagged_bytes_are_present(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "payload": b"abc"})

            self.assertTrue(engine._sort_requires_python("db", "coll", MatchAll(), [("payload", 1)]))
        finally:
            await engine.disconnect()

    async def test_sort_requires_python_when_tagged_undefined_is_present(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "payload": UNDEFINED})

            self.assertTrue(engine._sort_requires_python("db", "coll", MatchAll(), [("payload", 1)]))
        finally:
            await engine.disconnect()

    async def test_scan_collection_falls_back_for_undefined_range_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "legacy", "v": UNDEFINED})
            documents = [
                document
                async for document in engine.scan_collection(
                    "db",
                    "coll",
                    {"v": {"$gt": 0}},
                )
            ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [])

    async def test_delete_matching_document_falls_back_for_undefined_range_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "legacy", "v": UNDEFINED})
            result = await engine.delete_matching_document(
                "db",
                "coll",
                {"v": {"$gt": 0}},
            )
            remaining = await engine.get_document("db", "coll", "legacy")
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 0)
        self.assertEqual(remaining, {"_id": "legacy", "v": UNDEFINED})

    async def test_count_matching_documents_falls_back_for_undefined_range_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "legacy", "v": UNDEFINED})
            count = await engine.count_matching_documents(
                "db",
                "coll",
                {"v": {"$gt": 0}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(count, 0)

    async def test_select_and_explain_fall_back_for_tagged_undefined_comparison_fields(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "payload": UNDEFINED})
            plan = compile_filter({"payload": {"$gt": 0}})

            with self.assertRaises(NotImplementedError):
                engine._select_first_document_for_plan("db", "coll", plan)
            with self.assertRaises(NotImplementedError):
                engine._explain_query_plan_sync("db", "coll", {"payload": {"$gt": 0}})
        finally:
            await engine.disconnect()

    async def test_scan_collection_automatically_falls_back_for_plain_object_sort(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "profile": {"rank": 2}})
            await engine.put_document("db", "coll", {"_id": "2", "profile": {"rank": 1}})

            with patch.object(engine, "_load_documents", wraps=engine._load_documents) as load_documents:
                documents = [
                    document
                    async for document in engine.scan_collection("db", "coll", sort=[("profile", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(
            documents,
            [{"_id": "2", "profile": {"rank": 1}}, {"_id": "1", "profile": {"rank": 2}}],
        )
        self.assertGreater(load_documents.call_count, 0)

    async def test_count_matching_documents_uses_sql_translation_for_simple_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                count = await engine.count_matching_documents("db", "coll", {"kind": "view"})
        finally:
            await engine.disconnect()

        self.assertEqual(count, 1)

    async def test_count_matching_documents_falls_back_to_query_engine_for_untranslatable_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "data": {"b": 1}})
            await engine.put_document("db", "coll", {"_id": "2", "data": {"b": 2}})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                count = await engine.count_matching_documents("db", "coll", {"data": {"b": 1}})
        finally:
            await engine.disconnect()

        self.assertEqual(count, 1)
        self.assertGreater(match_plan.call_count, 0)

    async def test_count_matching_documents_falls_back_for_array_traversing_embedded_document_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})
            await engine.put_document("db", "coll", {"_id": "2", "items": [{"name": "b"}]})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                count = await engine.count_matching_documents("db", "coll", {"items.name": "a"})
        finally:
            await engine.disconnect()

        self.assertEqual(count, 1)
        self.assertGreater(match_plan.call_count, 0)

    async def test_count_matching_documents_falls_back_for_tagged_bytes_range_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "v": b"\x00"})
            count = await engine.count_matching_documents(
                "db",
                "coll",
                {"v": {"$gt": uuid.UUID("12345678-1234-5678-1234-567812345678")}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(count, 1)

    def test_iter_scan_documents_sync_python_fallback_honors_skip(self):
        engine = SQLiteEngine()
        engine._connect_sync()
        try:
            with (
                patch.object(engine, "_build_select_sql", side_effect=NotImplementedError("fallback")),
                patch.object(engine, "_load_documents", return_value=[("1", {"_id": "1"}), ("2", {"_id": "2"})]),
            ):
                documents = list(engine._iter_scan_documents_sync("db", "coll", None, MatchAll(), None, None, 1, None, None))
        finally:
            engine._disconnect_sync()

        self.assertEqual(documents, [{"_id": "2"}])

    def test_iter_scan_documents_sync_python_sort_fallback_respects_stop_event(self):
        engine = SQLiteEngine()
        engine._connect_sync()
        stop_event = threading.Event()
        stop_event.set()
        try:
            with (
                patch.object(engine, "_build_select_sql", side_effect=NotImplementedError("fallback")),
                patch.object(engine, "_load_documents", return_value=[("1", {"_id": "1"})]),
            ):
                documents = list(
                    engine._iter_scan_documents_sync(
                        "db",
                        "coll",
                        None,
                        MatchAll(),
                        None,
                        [("_id", 1)],
                        0,
                        None,
                        None,
                        stop_event=stop_event,
                    )
                )
        finally:
            engine._disconnect_sync()

        self.assertEqual(documents, [])

    async def test_scan_collection_uses_sql_translation_for_codec_aware_equality_filters(self):
        object_id = ObjectId("0123456789abcdef01234567")
        created_at = datetime.datetime(2025, 1, 2, 3, 4, 5)
        session_id = uuid.UUID("12345678-1234-5678-1234-567812345678")
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document(
                "db",
                "coll",
                {
                    "_id": object_id,
                    "created_at": created_at,
                    "session_id": session_id,
                },
            )
            await engine.put_document(
                "db",
                "coll",
                {
                    "_id": ObjectId("abcdef0123456789abcdef01"),
                    "created_at": datetime.datetime(2024, 1, 2, 3, 4, 5),
                    "session_id": uuid.UUID("87654321-4321-8765-4321-876543218765"),
                },
            )

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                by_id = [document async for document in engine.scan_collection("db", "coll", {"_id": object_id})]
                by_datetime = [
                    document
                    async for document in engine.scan_collection("db", "coll", {"created_at": created_at})
                ]
                by_uuid = [
                    document
                    async for document in engine.scan_collection("db", "coll", {"session_id": session_id})
                ]
        finally:
            await engine.disconnect()

        expected = [{"_id": object_id, "created_at": created_at, "session_id": session_id}]
        self.assertEqual(by_id, expected)
        self.assertEqual(by_datetime, expected)
        self.assertEqual(by_uuid, expected)

    async def test_count_matching_documents_uses_sql_translation_for_codec_aware_membership(self):
        ids = (
            ObjectId("0123456789abcdef01234567"),
            ObjectId("abcdef0123456789abcdef01"),
        )
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": ids[0], "kind": "first"})
            await engine.put_document("db", "coll", {"_id": ids[1], "kind": "second"})
            await engine.put_document("db", "coll", {"_id": ObjectId("111111111111111111111111"), "kind": "third"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                count = await engine.count_matching_documents("db", "coll", {"_id": {"$in": list(ids)}})
        finally:
            await engine.disconnect()

        self.assertEqual(count, 2)

    async def test_scan_collection_uses_sql_translation_for_codec_aware_sort(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "created_at": datetime.datetime(2025, 1, 2, 3, 4, 5)})
            await engine.put_document("db", "coll", {"_id": "2", "created_at": datetime.datetime(2025, 1, 1, 3, 4, 5)})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                documents = [
                    document["_id"]
                    async for document in engine.scan_collection("db", "coll", sort=[("created_at", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "1"])

    async def test_scan_collection_does_not_deadlock_on_early_termination(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view"})

            async def _consume_first():
                async for document in engine.scan_collection("db", "coll", {"kind": "view"}):
                    return document
                return None

            first = await asyncio.wait_for(_consume_first(), timeout=1)
        finally:
            await engine.disconnect()

        self.assertEqual(first, {"_id": "1", "kind": "view"})

    async def test_scan_collection_closes_fallback_source_on_early_termination(self):
        engine = SQLiteEngine()
        await engine.connect()
        closed = threading.Event()

        def documents():
            try:
                yield ("1", {"_id": "1", "data": {"b": 1}})
                yield ("2", {"_id": "2", "data": {"b": 1}})
            finally:
                closed.set()

        try:
            with patch.object(engine, "_load_documents", side_effect=lambda *_args, **_kwargs: documents()):
                async for _document in engine.scan_collection("db", "coll", {"data": {"b": 1}}):
                    break
                await asyncio.sleep(0.05)
        finally:
            await engine.disconnect()

        self.assertTrue(closed.is_set())

    async def test_iter_scan_documents_stops_when_stop_event_is_set(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view"})
            stop_event = threading.Event()
            seen: list[dict[str, object]] = []

            for document in engine._iter_scan_documents_sync(
                "db",
                "coll",
                {"kind": "view"},
                None,
                None,
                [("_id", 1)],
                0,
                None,
                None,
                stop_event,
            ):
                seen.append(document)
                stop_event.set()
        finally:
            await engine.disconnect()

        self.assertEqual(seen, [{"_id": "1", "kind": "view"}])

    async def test_iter_scan_documents_skips_python_fallback_rows_when_stop_event_is_pre_set(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "data": {"b": 1}})
            stop_event = threading.Event()
            stop_event.set()

            seen = list(
                engine._iter_scan_documents_sync(
                    "db",
                    "coll",
                    {"data": {"b": 1}},
                    None,
                    None,
                    None,
                    0,
                    None,
                    None,
                    stop_event,
                )
            )
        finally:
            await engine.disconnect()

        self.assertEqual(seen, [])

    async def test_iter_scan_documents_streams_python_fallback_without_sort_and_respects_limit(self):
        engine = SQLiteEngine()
        yielded: list[str] = []

        def _documents(_db_name, _coll_name):
            for document_id, name in (("1", "Ada"), ("2", "Ana"), ("3", "Bob")):
                yielded.append(document_id)
                yield document_id, {"_id": document_id, "name": name}

        with patch.object(engine, "_load_documents", side_effect=_documents):
            seen = list(
                engine._iter_scan_documents_sync(
                    "db",
                    "coll",
                    {"name": {"$regex": "^A"}},
                    None,
                    None,
                    None,
                    0,
                    1,
                    None,
                    None,
                )
            )

        self.assertEqual(seen, [{"_id": "1", "name": "Ada"}])
        self.assertEqual(yielded, ["1"])

    async def test_scan_collection_does_not_enqueue_document_after_stop_event_is_set(self):
        engine = SQLiteEngine()
        await engine.connect()

        def _wrapped(*args, **kwargs):
            stop_event = kwargs.get("stop_event")
            if stop_event is None and args and isinstance(args[-1], threading.Event):
                stop_event = args[-1]
            if stop_event is not None:
                stop_event.set()
            yield {"_id": "1", "kind": "view"}

        try:
            with patch.object(engine, "_iter_scan_documents_sync", side_effect=_wrapped):
                seen = [document async for document in engine.scan_collection("db", "coll", {"kind": "view"})]
        finally:
            await engine.disconnect()

        self.assertEqual(seen, [])

    async def test_scan_collection_uses_sql_translation_for_scalar_not_equals_with_mixed_types(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "name": "Ada"})
            await engine.put_document("db", "coll", {"_id": "2", "name": {"nested": 1}})
            await engine.put_document("db", "coll", {"_id": "3"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                documents = [
                    document["_id"]
                    async for document in engine.scan_collection("db", "coll", {"name": {"$ne": "Ada"}}, sort=[("_id", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "3"])

    async def test_scan_collection_uses_sql_translation_for_codec_aware_not_equals_on_arrays(self):
        engine = SQLiteEngine()
        oid1 = ObjectId("0123456789abcdef01234567")
        oid2 = ObjectId("abcdef0123456789abcdef01")
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "refs": [oid1, oid2]})
            await engine.put_document("db", "coll", {"_id": "2", "refs": [oid2]})
            await engine.put_document("db", "coll", {"_id": "3"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                documents = [
                    document["_id"]
                    async for document in engine.scan_collection("db", "coll", {"refs": {"$ne": oid1}}, sort=[("_id", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "3"])

    async def test_scan_collection_keeps_sql_sort_for_scalar_dot_notation(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "profile": {"rank": 2}})
            await engine.put_document("db", "coll", {"_id": "2", "profile": {"rank": 1}})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                documents = [
                    document["_id"]
                    async for document in engine.scan_collection("db", "coll", sort=[("profile.rank", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "1"])

    async def test_scan_collection_falls_back_for_array_traversing_embedded_document_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}, {"name": "b"}]})
            await engine.put_document("db", "coll", {"_id": "2", "items": [{"name": "c"}]})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                documents = [
                    document["_id"]
                    async for document in engine.scan_collection("db", "coll", {"items.name": "a"}, sort=[("_id", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertGreater(match_plan.call_count, 0)

    async def test_scan_collection_falls_back_for_array_traversing_sort_key(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "b"}]})
            await engine.put_document("db", "coll", {"_id": "2", "items": [{"name": "a"}]})

            with patch.object(engine, "_load_documents", wraps=engine._load_documents) as load_documents:
                documents = [
                    document["_id"]
                    async for document in engine.scan_collection("db", "coll", sort=[("items.name", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "1"])
        self.assertGreater(load_documents.call_count, 0)

    async def test_update_matching_document_uses_sql_selected_candidate_for_simple_updates(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "skip", "name": "old"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "match", "name": "old"})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                result = await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "match"},
                    {"$set": {"name": "new"}},
                )
                found = await engine.get_document("db", "coll", "2")
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(found, {"_id": "2", "kind": "match", "name": "new"})

    async def test_update_matching_document_falls_back_for_untranslatable_filter_and_skips_non_matches(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "data": {"b": 2}, "name": "old"})
            await engine.put_document("db", "coll", {"_id": "2", "data": {"b": 1}, "name": "old"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                result = await engine.update_matching_document(
                    "db",
                    "coll",
                    {"data": {"b": 1}},
                    {"$set": {"name": "new"}},
                )
                found = await engine.get_document("db", "coll", "2")
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(found, {"_id": "2", "data": {"b": 1}, "name": "new"})
        self.assertGreater(match_plan.call_count, 1)

    async def test_update_matching_document_uses_sql_update_for_nested_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "match"})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                result = await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "match"},
                    {"$set": {"profile.name": "Ada"}},
                )
                found = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(found, {"_id": "1", "kind": "match", "profile": {"name": "Ada"}})

    async def test_update_matching_document_fallback_replace_maps_integrity_error(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            await engine.put_document("db", "coll", {"_id": "2", "email": "b@example.com"})
            await engine.create_index("db", "coll", ["email"], unique=True)

            with patch("mongoeco.engines.sqlite.translate_update_spec", side_effect=NotImplementedError("nested")):
                with self.assertRaises(DuplicateKeyError):
                    await engine.update_matching_document(
                        "db",
                        "coll",
                        {"_id": "2"},
                        {"$set": {"profile.name": "Ada", "email": "a@example.com"}},
                    )
        finally:
            await engine.disconnect()

    async def test_update_matching_document_fallback_replace_succeeds_without_conflict(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "match"})

            with patch("mongoeco.engines.sqlite.translate_update_spec", side_effect=NotImplementedError("fallback")):
                result = await engine.update_matching_document(
                    "db",
                    "coll",
                    {"kind": "match"},
                    {"$set": {"profile.name": "Ada"}},
                )
                found = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertEqual(result.matched_count, 1)
        self.assertEqual(result.modified_count, 1)
        self.assertEqual(found, {"_id": "1", "kind": "match", "profile": {"name": "Ada"}})
