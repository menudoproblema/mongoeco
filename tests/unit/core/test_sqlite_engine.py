import asyncio
import datetime
import mongoeco.core.search as search_module
import os
import sqlite3
import threading
import tempfile
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect70
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import MatchAll, compile_filter
from mongoeco.core.search import compile_classic_text_query
from mongoeco.core.sorting import sort_documents
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.engines.sqlite import _SQLITE_SHARED_EXECUTORS, _shutdown_sqlite_shared_executors
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, ExecutionTimeout, InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import Decimal128, EngineIndexRecord, ObjectId, SearchIndexDefinition, UNDEFINED


class SQLiteEngineTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _scan(
        engine: SQLiteEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object] | None = None,
        *,
        plan=None,
        projection=None,
        collation=None,
        sort=None,
        skip: int = 0,
        limit: int | None = None,
        hint=None,
        comment=None,
        max_time_ms: int | None = None,
        dialect=None,
        context=None,
    ):
        return engine.scan_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics(
                filter_spec,
                plan=plan,
                projection=projection,
                collation=collation,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                dialect=dialect,
            ),
            context=context,
        )

    @staticmethod
    async def _count(
        engine: SQLiteEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object] | None = None,
        *,
        plan=None,
        collation=None,
        dialect=None,
        context=None,
    ) -> int:
        return await engine.count_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics(
                filter_spec,
                plan=plan,
                collation=collation,
                dialect=dialect,
            ),
            context=context,
        )

    @staticmethod
    async def _update(
        engine: SQLiteEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object],
        update_spec: dict[str, object],
        *,
        upsert: bool = False,
        upsert_seed=None,
        selector_filter=None,
        array_filters=None,
        plan=None,
        collation=None,
        sort=None,
        hint=None,
        comment=None,
        max_time_ms=None,
        let=None,
        dialect=None,
        context=None,
        bypass_document_validation: bool = False,
    ):
        effective_dialect = dialect or MONGODB_DIALECT_70
        return await engine.update_with_operation(
            db_name,
            coll_name,
            compile_update_operation(
                filter_spec,
                collation=collation,
                sort=sort,
                array_filters=array_filters,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                dialect=effective_dialect,
                plan=plan,
                update_spec=update_spec,
            ),
            upsert=upsert,
            upsert_seed=upsert_seed,
            selector_filter=selector_filter,
            dialect=effective_dialect,
            context=context,
            bypass_document_validation=bypass_document_validation,
        )

    @staticmethod
    async def _delete(
        engine: SQLiteEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object],
        *,
        plan=None,
        collation=None,
        sort=None,
        hint=None,
        comment=None,
        max_time_ms=None,
        let=None,
        dialect=None,
        context=None,
    ):
        effective_dialect = dialect or MONGODB_DIALECT_70
        return await engine.delete_with_operation(
            db_name,
            coll_name,
            compile_update_operation(
                filter_spec,
                collation=collation,
                sort=sort,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                let=let,
                dialect=effective_dialect,
                plan=plan,
            ),
            dialect=effective_dialect,
            context=context,
        )

    @staticmethod
    async def _explain(
        engine: SQLiteEngine,
        db_name: str,
        coll_name: str,
        filter_spec: dict[str, object] | None = None,
        *,
        plan=None,
        collation=None,
        sort=None,
        skip: int = 0,
        limit: int | None = None,
        hint=None,
        comment=None,
        max_time_ms=None,
        dialect=None,
        context=None,
    ):
        return await engine.explain_find_semantics(
            db_name,
            coll_name,
            compile_find_semantics(
                filter_spec,
                plan=plan,
                collation=collation,
                sort=sort,
                skip=skip,
                limit=limit,
                hint=hint,
                comment=comment,
                max_time_ms=max_time_ms,
                dialect=dialect,
            ),
            context=context,
        )

    async def test_scan_collection_records_comment_and_max_time_in_session_state(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            session = ClientSession()
            engine.create_session_state(session)
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})

            documents = [
                doc
                async for doc in self._scan(engine, 
                    "db",
                    "coll",
                    {"kind": "view"},
                    comment="trace-sqlite",
                    max_time_ms=25,
                    context=session,
                )
            ]
            state = session.get_engine_state(f"sqlite:{id(engine)}")
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "kind": "view"}])
        self.assertIsInstance(state, dict)
        self.assertEqual(state["last_operation"]["comment"], "trace-sqlite")
        self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    async def test_scan_collection_enforces_max_time_ms_deadline(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            with patch("mongoeco.engines.sqlite.enforce_deadline", side_effect=ExecutionTimeout("operation exceeded time limit")):
                with self.assertRaises(ExecutionTimeout):
                    [
                        doc
                        async for doc in self._scan(engine, 
                            "db",
                            "coll",
                            {"kind": "view"},
                            max_time_ms=1,
                        )
                    ]
        finally:
            await engine.disconnect()

    async def test_load_documents_generator_can_finalize_after_disconnect(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            loader = engine._load_documents("db", "coll")
            self.assertEqual(next(loader)[1], {"_id": "1", "kind": "view"})
        finally:
            await engine.disconnect()

        loader.close()

    async def test_create_index_enforces_max_time_ms_deadline(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})
            with patch(
                "mongoeco.engines.sqlite.enforce_deadline",
                side_effect=ExecutionTimeout("operation exceeded time limit"),
            ):
                with self.assertRaises(ExecutionTimeout):
                    await engine.create_index(
                        "db",
                        "coll",
                        ["email"],
                        unique=True,
                        max_time_ms=1,
                    )
        finally:
            await engine.disconnect()

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
        self.assertGreaterEqual(fake_connection.execute.call_count, 2)
        fake_connection.commit.assert_called_once()

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

    def test_multikey_helpers_cover_supported_unsupported_and_logical_translation_paths(self):
        engine = SQLiteEngine()
        engine._connection = Mock()

        self.assertEqual(SQLiteEngine._normalize_multikey_number(1), SQLiteEngine._normalize_multikey_number(1.0))
        self.assertLess(
            SQLiteEngine._normalize_multikey_number(-10),
            SQLiteEngine._normalize_multikey_number(-1),
        )
        self.assertLess(
            SQLiteEngine._normalize_multikey_number(-1),
            SQLiteEngine._normalize_multikey_number(0),
        )
        self.assertLess(
            SQLiteEngine._normalize_multikey_number(1.5),
            SQLiteEngine._normalize_multikey_number(2),
        )
        self.assertEqual(SQLiteEngine._multikey_value_signature(None), ("null", ""))
        self.assertEqual(SQLiteEngine._multikey_value_signature(True), ("bool", "1"))
        self.assertEqual(SQLiteEngine._multikey_value_signature(7), ("number", SQLiteEngine._normalize_multikey_number(7)))
        self.assertEqual(SQLiteEngine._multikey_value_signature("Ada"), ("string", "Ada"))
        self.assertEqual(SQLiteEngine._multikey_value_signature(uuid.UUID("12345678-1234-5678-1234-567812345678")), ("uuid", "12345678-1234-5678-1234-567812345678"))
        self.assertEqual(SQLiteEngine._multikey_value_signature(UNDEFINED), ("undefined", "1"))
        self.assertEqual(SQLiteEngine._multikey_signatures_for_query_value(None, null_matches_undefined=True), (("null", ""), ("undefined", "1")))
        self.assertIsNone(SQLiteEngine._multikey_value_signature({"x": 1}))
        self.assertIsNone(SQLiteEngine._multikey_value_signature(DocumentCodec._tagged_value("dict", {})))

        with self.assertRaises(NotImplementedError):
            SQLiteEngine._normalize_multikey_number(float("nan"))
        with self.assertRaises(NotImplementedError):
            SQLiteEngine._multikey_signatures_for_query_value({"x": 1})

        engine._lookup_collection_id = Mock(return_value=1)
        engine._find_multikey_index = Mock(return_value={"name": "idx_tags", "multikey_physical_name": "mkidx_test"})
        eq_sql, _ = engine._translate_query_plan_with_multikey(
            "db",
            "coll",
            compile_filter({"tags": "python"}),
        )
        self.assertIn("multikey_entries", eq_sql)

        in_sql, _ = engine._translate_query_plan_with_multikey(
            "db",
            "coll",
            compile_filter({"tags": {"$in": ["python", "sqlite"]}}),
        )
        self.assertIn("multikey_entries", in_sql)
        self.assertIn(" OR ", in_sql)

        gt_sql, gt_params = engine._translate_query_plan_with_multikey(
            "db",
            "coll",
            compile_filter({"tags": {"$gt": 2}}),
        )
        self.assertIn("multikey_entries", gt_sql)
        self.assertIn("collection_id", gt_sql)
        self.assertIn("type_score", gt_sql)
        self.assertIn("element_key > ?", gt_sql)
        self.assertIn(SQLiteEngine._normalize_multikey_number(2), gt_params)

        engine._find_multikey_index = Mock(return_value=None)
        fallback_sql, _ = engine._translate_query_plan_with_multikey(
            "db",
            "coll",
            compile_filter({"tags": {"$in": ["python"]}}),
        )
        self.assertIn("json_each", fallback_sql)

        logical_sql, _ = engine._translate_query_plan_with_multikey(
            "db",
            "coll",
            compile_filter({"$and": [{"tags": "python"}, {"kind": "view"}]}),
        )
        self.assertIn(" AND ", logical_sql)

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
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        engine = SQLiteEngine(path)
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
                iterator = self._scan(engine, "db", "coll").__aiter__()
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
            if os.path.exists(path):
                os.remove(path)

        self.assertIsNone(engine._connection)
        self.assertEqual(engine._connection_count, 0)

    async def test_scan_collection_rejects_negative_skip_and_limit(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with self.assertRaises(ValueError):
                async for _ in self._scan(engine, "db", "coll", skip=-1):
                    pass

            with self.assertRaises(ValueError):
                async for _ in self._scan(engine, "db", "coll", limit=-1):
                    pass
        finally:
            await engine.disconnect()

    async def test_scan_collection_async_producer_flushes_batch_before_reraising_errors(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        engine = SQLiteEngine(path)
        await engine.connect()
        yielded = []

        def broken_iter(*args, **kwargs):
            del args, kwargs
            yield {"_id": "1", "kind": "view"}
            raise RuntimeError("boom")

        try:
            with patch.object(engine, "_iter_scan_documents_sync", side_effect=broken_iter):
                with self.assertRaisesRegex(RuntimeError, "boom"):
                    async for document in self._scan(engine, "db", "coll"):
                        yielded.append(document)
        finally:
            await engine.disconnect()
            if os.path.exists(path):
                os.remove(path)

        self.assertEqual(yielded, [{"_id": "1", "kind": "view"}])

    async def test_sqlite_snapshot_options_and_profile_namespace_none_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            self.assertEqual(engine._snapshot_bulk_insert_validation_options_sync("db", "missing", None), {})
            self.assertIsNone(engine._get_document_sync("db", "system.profile", "missing", None))
            self.assertFalse(engine._delete_document_sync("db", "system.profile", "missing", None))
        finally:
            await engine.disconnect()

    async def test_update_matching_document_supports_upsert(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            result = await self._update(engine, 
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
            result = await self._update(engine, 
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

    def test_delete_document_sync_rolls_back_on_failure(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        fake_connection.execute.side_effect = [None, sqlite3.OperationalError("boom")]
        engine._require_connection = Mock(return_value=fake_connection)

        with self.assertRaises(sqlite3.OperationalError):
            engine._delete_document_sync("db", "coll", "1", None)

        fake_connection.rollback.assert_called_once()

    def test_replace_multikey_entries_for_non_multikey_index_only_clears_existing_rows(self):
        engine = SQLiteEngine()
        connection = Mock()

        engine._replace_multikey_entries_for_index_for_document(
            connection,
            "db",
            "coll",
            "storage",
            {"tags": ["python"]},
            {"name": "idx_plain", "fields": ["tags"], "multikey": False},
        )

        connection.execute.assert_called_once()
        connection.executemany.assert_not_called()

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
                await self._update(engine, 
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
            result = await self._update(engine, 
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
                result = await self._update(engine, 
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

    def test_load_indexes_uses_versioned_cache(self):
        engine = SQLiteEngine()
        cursor = Mock()
        cursor.fetchall.return_value = [
            (
                "idx_email",
                "idx_physical",
                '["email"]',
                '[["email", 1]]',
                0,
                0,
                None,
                0,
                None,
            )
        ]
        connection = Mock()
        connection.execute.return_value = cursor
        engine._connection = connection

        first = engine._load_indexes("db", "coll")
        second = engine._load_indexes("db", "coll")

        self.assertEqual(connection.execute.call_count, 1)
        self.assertEqual(first, second)

        engine._mark_index_metadata_changed("other", "collection")
        third = engine._load_indexes("db", "coll")

        self.assertEqual(connection.execute.call_count, 1)
        self.assertEqual(first, third)

        engine._mark_index_metadata_changed("db", "coll")
        fourth = engine._load_indexes("db", "coll")

        self.assertEqual(connection.execute.call_count, 2)
        self.assertEqual(first, fourth)

    def test_ensure_executor_uses_configured_worker_count(self):
        engine = SQLiteEngine(executor_workers=7)
        try:
            executor = engine._ensure_executor()
            self.assertEqual(executor._max_workers, 7)
        finally:
            engine._shutdown_executor()

    def test_in_memory_sqlite_defaults_to_single_executor_worker(self):
        engine = SQLiteEngine(path=":memory:")
        try:
            executor = engine._ensure_executor()
            self.assertEqual(executor._max_workers, 1)
        finally:
            engine._shutdown_executor()

    def test_constructor_rejects_non_positive_executor_workers(self):
        with self.assertRaisesRegex(ValueError, "executor_workers must be positive"):
            SQLiteEngine(executor_workers=0)

    def test_shutdown_sqlite_shared_executors_closes_registered_pools(self):
        executor = Mock()
        _SQLITE_SHARED_EXECUTORS[99] = executor
        try:
            _shutdown_sqlite_shared_executors()
        finally:
            _SQLITE_SHARED_EXECUTORS.clear()
        executor.shutdown.assert_called_once_with(wait=True, cancel_futures=True)

    async def test_session_transaction_helpers_validate_connection_and_owner(self):
        engine = SQLiteEngine()
        session = ClientSession()
        other = ClientSession()
        engine.create_session_state(session)
        engine.create_session_state(other)

        with self.assertRaisesRegex(InvalidOperation, "must be connected before starting a transaction"):
            engine._start_session_transaction(session)

        await engine.connect()
        try:
            engine._start_session_transaction(session)
            with self.assertRaisesRegex(InvalidOperation, "active transaction bound to another session"):
                engine._start_session_transaction(other)
            with self.assertRaisesRegex(InvalidOperation, "does not own the active SQLite transaction"):
                engine._commit_session_transaction(other)
            engine._abort_session_transaction(other)
            self.assertEqual(engine._transaction_owner_session_id, session.session_id)
            engine._abort_session_transaction(session)
            self.assertIsNone(engine._transaction_owner_session_id)
        finally:
            await engine.disconnect()

        engine._abort_session_transaction(session)

    def test_ensure_physical_indexes_commit_when_creating_outside_transaction(self):
        conn = sqlite3.connect(":memory:")
        conn.execute(
            "CREATE TABLE multikey_entries (collection_id INTEGER, index_name TEXT, storage_key TEXT, element_type TEXT, type_score INTEGER, element_key TEXT)"
        )
        conn.execute(
            "CREATE TABLE scalar_index_entries (collection_id INTEGER, index_name TEXT, storage_key TEXT, element_type TEXT, type_score INTEGER, element_key TEXT)"
        )
        engine = SQLiteEngine()
        multikey = EngineIndexRecord(
            name="tags_1",
            physical_name="idx_tags",
            fields=["tags"],
            key=[("tags", 1)],
            unique=False,
            multikey=True,
            multikey_physical_name="mkidx_tags",
        )
        scalar = EngineIndexRecord(
            name="kind_1",
            physical_name="idx_kind",
            fields=["kind"],
            key=[("kind", 1)],
            unique=False,
            scalar_physical_name="scidx_kind",
        )

        engine._ensure_multikey_physical_indexes_sync(conn, [multikey])
        engine._ensure_scalar_physical_indexes_sync(conn, [scalar])

        index_names = {
            row[1]
            for row in conn.execute("PRAGMA index_list(multikey_entries)").fetchall()
        }
        index_names.update(
            row[1]
            for row in conn.execute("PRAGMA index_list(scalar_index_entries)").fetchall()
        )
        self.assertIn("mkidx_tags", index_names)
        self.assertIn("scidx_kind", index_names)
        self.assertFalse(conn.in_transaction)
        conn.close()

    def test_supports_fts5_tolerates_database_errors(self):
        class _BrokenConnection:
            def execute(self, _sql):
                raise sqlite3.DatabaseError("broken")

        engine = SQLiteEngine()
        self.assertFalse(engine._supports_fts5(_BrokenConnection()))
        self.assertFalse(engine._fts5_available)

    async def test_put_document_invalidates_collection_feature_cache(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            feature_key = ("db", "coll", "traverses_array:items.name")
            self.assertFalse(
                engine._field_traverses_array_in_collection("db", "coll", "items.name")
            )
            self.assertIn(feature_key, engine._collection_features_cache)

            await engine.put_document("db", "coll", {"_id": "1", "items": [{"name": "a"}]})

            self.assertNotIn(feature_key, engine._collection_features_cache)
            self.assertTrue(
                engine._field_traverses_array_in_collection("db", "coll", "items.name")
            )
        finally:
            await engine.disconnect()

    def test_default_sqlite_engines_share_process_executor(self):
        first = SQLiteEngine()
        second = SQLiteEngine()
        executor_a = first._ensure_executor()
        executor_b = second._ensure_executor()
        try:
            self.assertIs(executor_a, executor_b)
        finally:
            first._shutdown_executor()
            second._shutdown_executor()

    def test_lookup_collection_id_uses_cache(self):
        engine = SQLiteEngine()
        cursor = Mock()
        cursor.fetchone.return_value = (7, 7)
        connection = Mock()
        connection.execute.return_value = cursor

        first = engine._lookup_collection_id(connection, "db", "coll")
        second = engine._lookup_collection_id(connection, "db", "coll")

        self.assertEqual(first, 7)
        self.assertEqual(second, 7)
        self.assertEqual(connection.execute.call_count, 1)

    async def test_create_index_builds_multikey_entries_while_holding_engine_lock(self):
        engine = SQLiteEngine()
        await engine.connect()
        lock_ownership: list[bool] = []
        try:
            await engine.put_document("db", "coll", {"_id": "1", "tags": ["python"]})
            await engine.put_document("db", "coll", {"_id": "2", "tags": ["sqlite"]})

            original = engine._replace_multikey_entries_for_index_for_document

            def wrapped(*args, **kwargs):
                is_owned = getattr(engine._lock, "_is_owned", lambda: True)
                lock_ownership.append(bool(is_owned()))
                return original(*args, **kwargs)

            with patch.object(engine, "_replace_multikey_entries_for_index_for_document", side_effect=wrapped):
                await engine.create_index("db", "coll", ["tags"], unique=False, name="idx_tags")
        finally:
            await engine.disconnect()

        self.assertTrue(lock_ownership)
        self.assertTrue(all(lock_ownership))

    async def test_drop_and_rename_collection_invalidate_collection_id_cache(self):
        engine = SQLiteEngine()
        await engine.connect()
        repopulated_cache_id: int | None = None
        try:
            await engine.put_document("db", "users", {"_id": "1"})
            original_collection_id = engine._lookup_collection_id(engine._require_connection(), "db", "users")
            self.assertEqual(engine._collection_id_cache[("db", "users")], original_collection_id)

            await engine.rename_collection("db", "users", "users_archive")
            self.assertNotIn(("db", "users"), engine._collection_id_cache)
            renamed_collection_id = engine._lookup_collection_id(engine._require_connection(), "db", "users_archive")
            self.assertEqual(engine._collection_id_cache[("db", "users_archive")], renamed_collection_id)

            await engine.drop_collection("db", "users_archive")
            self.assertNotIn(("db", "users_archive"), engine._collection_id_cache)

            await engine.put_document("db", "users_archive", {"_id": "2"})
            recreated_collection_id = engine._lookup_collection_id(engine._require_connection(), "db", "users_archive")
            repopulated_cache_id = engine._collection_id_cache[("db", "users_archive")]
        finally:
            await engine.disconnect()

        self.assertEqual(recreated_collection_id, repopulated_cache_id)

    async def test_put_documents_bulk_sync_uses_prepared_documents_without_serializing_inside_lock(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            _, snapshot_indexes = await engine._run_blocking(
                engine._snapshot_bulk_insert_preparation_sync,
                "db",
                "coll",
                None,
            )
            documents = [{"_id": "1", "email": "a@example.com"}]
            prepared_documents = [
                (
                    engine._storage_key("1"),
                    engine._serialize_document(documents[0]),
                    [],
                )
            ]
            with patch.object(engine, "_serialize_document", side_effect=AssertionError("serialize-in-lock")):
                results = await engine._run_blocking(
                    engine._put_documents_bulk_sync,
                    "db",
                    "coll",
                    documents,
                    prepared_documents,
                    snapshot_indexes,
                    None,
                    bypass_document_validation=True,
                    snapshot_options=None,
                )
                found = await engine.get_document("db", "coll", "1")
        finally:
            await engine.disconnect()

        self.assertEqual(results, [True])
        self.assertEqual(found, documents[0])

    async def test_put_documents_bulk_prefers_precomputed_multikey_rows_when_index_snapshot_is_stable(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["tags"], unique=False, name="idx_tags")
            snapshot_options, snapshot_indexes = await engine._run_blocking(
                engine._snapshot_bulk_insert_preparation_sync,
                "db",
                "coll",
                None,
            )
            del snapshot_options
            documents = [{"_id": "1", "tags": ["python", "sqlite"]}]
            prepared_documents = [
                engine._prepare_bulk_document_with_indexes_sync(documents[0], snapshot_indexes)
            ]
            with patch.object(engine, "_build_multikey_rows_for_document", side_effect=AssertionError("recomputed")):
                results = await engine._run_blocking(
                    engine._put_documents_bulk_sync,
                    "db",
                    "coll",
                    documents,
                    prepared_documents,
                    snapshot_indexes,
                    None,
                    bypass_document_validation=True,
                    snapshot_options=None,
                )
                rows = engine._require_connection().execute(
                    """
                    SELECT index_name, element_key
                    FROM multikey_entries
                    WHERE collection_id = ? AND storage_key = ?
                    ORDER BY index_name, element_key
                    """,
                    (1, engine._storage_key("1")),
                ).fetchall()
        finally:
            await engine.disconnect()

        self.assertEqual(results, [True])
        self.assertEqual(rows, [("idx_tags", "python"), ("idx_tags", "sqlite")])

    async def test_put_documents_bulk_sync_respects_sparse_unique_indexes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=True, sparse=True)
            snapshot_options, snapshot_indexes = await engine._run_blocking(
                engine._snapshot_bulk_insert_preparation_sync,
                "db",
                "coll",
                None,
            )
            documents = [
                {"_id": "1"},
                {"_id": "2", "email": "a@example.com"},
                {"_id": "3", "email": "a@example.com"},
            ]
            prepared_documents = [
                engine._prepare_bulk_document_with_indexes_sync(document, snapshot_indexes)
                for document in documents
            ]
            results = await engine._run_blocking(
                engine._put_documents_bulk_sync,
                "db",
                "coll",
                documents,
                prepared_documents,
                snapshot_indexes,
                None,
                bypass_document_validation=True,
                snapshot_options=snapshot_options,
            )
            found = [await engine.get_document("db", "coll", doc_id) for doc_id in ("1", "2", "3")]
        finally:
            await engine.disconnect()

        self.assertEqual(results, [True, True, False])
        self.assertEqual(found, [{"_id": "1"}, {"_id": "2", "email": "a@example.com"}, None])

    async def test_put_documents_bulk_sync_respects_partial_unique_indexes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                unique=True,
                partial_filter_expression={"active": True},
            )
            snapshot_options, snapshot_indexes = await engine._run_blocking(
                engine._snapshot_bulk_insert_preparation_sync,
                "db",
                "coll",
                None,
            )
            documents = [
                {"_id": "1", "email": "a@example.com", "active": False},
                {"_id": "2", "email": "a@example.com", "active": True},
                {"_id": "3", "email": "a@example.com", "active": True},
            ]
            prepared_documents = [
                engine._prepare_bulk_document_with_indexes_sync(document, snapshot_indexes)
                for document in documents
            ]
            results = await engine._run_blocking(
                engine._put_documents_bulk_sync,
                "db",
                "coll",
                documents,
                prepared_documents,
                snapshot_indexes,
                None,
                bypass_document_validation=True,
                snapshot_options=snapshot_options,
            )
            found = [await engine.get_document("db", "coll", doc_id) for doc_id in ("1", "2", "3")]
        finally:
            await engine.disconnect()

        self.assertEqual(results, [True, True, False])
        self.assertEqual(
            found,
            [
                {"_id": "1", "email": "a@example.com", "active": False},
                {"_id": "2", "email": "a@example.com", "active": True},
                None,
            ],
        )

    async def test_connect_defers_multikey_physical_index_recreation_until_index_metadata_load(self):
        handle = tempfile.NamedTemporaryFile(suffix=".sqlite3", delete=False)
        handle.close()
        path = handle.name
        engine = SQLiteEngine(path)
        try:
            await engine.connect()
            try:
                await engine.put_document("db", "coll", {"_id": "1", "tags": ["python"]})
                await engine.create_index("db", "coll", ["tags"], unique=False, name="idx_tags")
                physical_name = engine._physical_multikey_index_name("db", "coll", "idx_tags")
                conn = engine._require_connection()
                conn.execute(f"DROP INDEX IF EXISTS {engine._quote_identifier(physical_name)}")
                conn.commit()
                engine._ensured_multikey_physical_indexes.discard(physical_name)
            finally:
                await engine.disconnect()

            reopened = SQLiteEngine(path)
            await reopened.connect()
            try:
                conn = reopened._require_connection()
                before = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
                    (physical_name,),
                ).fetchone()
                self.assertIsNone(before)

                reopened._load_indexes("db", "coll")

                after = conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
                    (physical_name,),
                ).fetchone()
            finally:
                await reopened.disconnect()
        finally:
            os.unlink(path)

        self.assertEqual(after[0], physical_name)

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
            result = await self._update(engine, 
                "db",
                "coll",
                {"_id": "1"},
                {"$set": {"kind": "existing"}},
            )
        finally:
            await engine.disconnect()

    async def test_multikey_entries_include_decimal128_array_values(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["scores"], unique=False, name="idx_scores")
            await engine.put_document(
                "db",
                "coll",
                {"_id": "1", "scores": [Decimal128("1.5"), Decimal128("2.5")]},
            )
            rows = engine._require_connection().execute(
                """
                SELECT index_name, element_key
                FROM multikey_entries
                WHERE collection_id = ? AND storage_key = ?
                ORDER BY element_key
                """,
                (1, engine._storage_key("1")),
            ).fetchall()
        finally:
            await engine.disconnect()

        self.assertEqual(len(rows), 2)
        self.assertEqual({row[0] for row in rows}, {"idx_scores"})

    async def test_update_matching_document_raises_when_dotted_set_crosses_scalar_parent(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "profile": 1})

            with self.assertRaises(OperationFailure):
                await self._update(engine, 
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

            result = await self._update(engine, 
                "db",
                "coll",
                {"kind": "match"},
                {"$set": {"done": True}},
            )
            found = await engine.get_document("db", "coll", "2")

            with self.assertRaises(DuplicateKeyError):
                await self._update(engine, 
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

            result = await self._delete(engine, "db", "coll", {"kind": "match"})
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
                result = await self._delete(engine, "db", "coll", {"kind": "view"})
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
                result = await self._delete(engine, "db", "coll", {"items.name": "a"})
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
                result = await self._delete(engine, "db", "coll", {"data": {"b": 1}})
        finally:
            await engine.disconnect()

        self.assertEqual(result.deleted_count, 1)
        self.assertGreater(match_plan.call_count, 0)

    async def test_delete_matching_document_returns_zero_when_no_match(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "click"})
            result = await self._delete(engine, "db", "coll", {"kind": "view"})
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

            with self.assertRaises(OperationFailure):
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

    async def test_validate_document_against_unique_indexes_uses_scalar_entries_for_simple_unique_indexes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=True, name="email_unique")
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})

            with patch.object(engine, "_load_documents", side_effect=AssertionError("full-scan")):
                with self.assertRaises(DuplicateKeyError):
                    engine._validate_document_against_unique_indexes(
                        "db",
                        "coll",
                        {"_id": "2", "email": "a@example.com"},
                    )
        finally:
            await engine.disconnect()

    async def test_validate_document_against_unique_indexes_skips_full_scan_when_scalar_fast_path_finds_no_conflict(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=True, name="email_unique")
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com"})

            with patch.object(engine, "_load_documents", side_effect=AssertionError("full-scan")):
                engine._validate_document_against_unique_indexes(
                    "db",
                    "coll",
                    {"_id": "2", "email": "b@example.com"},
                )
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

    async def test_list_indexes_and_index_information_include_hidden_metadata(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], hidden=True, name="email_hidden")
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes[1],
            {
                "name": "email_hidden",
                "key": {"email": 1},
                "unique": False,
                "hidden": True,
            },
        )
        self.assertTrue(info["email_hidden"]["hidden"])

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

    async def test_explain_query_plan_uses_multikey_index_for_array_membership(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "tags": ["python", "mongodb"]})
            await engine.put_document("db", "coll", {"_id": "2", "tags": ["sqlite"]})
            await engine.create_index("db", "coll", ["tags"], unique=False, name="idx_tags")

            details = engine._explain_query_plan_sync("db", "coll", {"tags": "python"})
        finally:
            await engine.disconnect()

        self.assertTrue(any("mkidx_" in detail for detail in details))

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

    async def test_explain_query_plan_supports_homogeneous_top_level_array_comparisons(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "v": [1]})
            details = engine._explain_query_plan_sync("db", "coll", {"v": {"$gt": 0}})
        finally:
            await engine.disconnect()

        self.assertTrue(details)
        self.assertTrue(any("SCAN" in step or "SEARCH" in step for step in details))

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
                await self._update(engine, 
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

    async def test_multikey_entries_track_insert_update_delete_and_drop_collection(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "tags": ["python", "mongodb"]})
            await engine.create_index("db", "coll", ["tags"], unique=False, name="idx_tags")

            conn = engine._require_connection()
            rows = conn.execute(
                """
                SELECT collection_id, element_type, type_score, element_key
                FROM multikey_entries
                WHERE collection_id = ? AND index_name = ?
                ORDER BY element_key
                """,
                (1, "idx_tags"),
            ).fetchall()
            self.assertEqual(rows, [(1, "string", 3, "mongodb"), (1, "string", 3, "python")])

            await self._update(engine, 
                "db",
                "coll",
                {"_id": "1"},
                {"$set": {"tags": ["sqlite"]}},
            )
            rows = conn.execute(
                """
                SELECT collection_id, element_type, type_score, element_key
                FROM multikey_entries
                WHERE collection_id = ? AND index_name = ?
                ORDER BY element_key
                """,
                (1, "idx_tags"),
            ).fetchall()
            self.assertEqual(rows, [(1, "string", 3, "sqlite")])

            deleted = await self._delete(engine, "db", "coll", {"tags": "sqlite"})
            self.assertEqual(deleted.deleted_count, 1)
            remaining = conn.execute(
                """
                SELECT COUNT(*)
                FROM multikey_entries
                WHERE collection_id = ? AND index_name = ?
                """,
                (1, "idx_tags"),
            ).fetchone()
            self.assertEqual(remaining[0], 0)

            await engine.put_document("db", "coll", {"_id": "2", "tags": ["python"]})
            await engine.drop_collection("db", "coll")
            leftover = conn.execute(
                """
                SELECT COUNT(*)
                FROM multikey_entries
                WHERE collection_id = ?
                """,
                (1,),
            ).fetchone()
            self.assertEqual(leftover[0], 0)
        finally:
            await engine.disconnect()

    async def test_creating_second_multikey_index_preserves_existing_multikey_entries(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document(
                "db",
                "coll",
                {"_id": "1", "tags": ["python"], "cats": ["backend"]},
            )
            await engine.create_index("db", "coll", ["tags"], unique=False, name="idx_tags")
            await engine.create_index("db", "coll", ["cats"], unique=False, name="idx_cats")

            count_tags = await self._count(engine, "db", "coll", {"tags": "python"})
            count_cats = await self._count(engine, "db", "coll", {"cats": "backend"})
            rows = engine._require_connection().execute(
                """
                SELECT index_name, element_key
                FROM multikey_entries
                WHERE collection_id = ? AND storage_key = ?
                ORDER BY index_name, element_key
                """,
                (1, engine._storage_key("1")),
            ).fetchall()
        finally:
            await engine.disconnect()

        self.assertEqual(count_tags, 1)
        self.assertEqual(count_cats, 1)
        self.assertEqual(rows, [("idx_cats", "backend"), ("idx_tags", "python")])

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
        self.assertIn("keys", columns)

    async def test_connect_migrates_legacy_search_and_scalar_storage_shapes(self):
        fd, path = tempfile.mkstemp(suffix=".sqlite")
        os.close(fd)
        legacy = sqlite3.connect(path, check_same_thread=False)
        try:
            legacy.execute(
                """
                CREATE TABLE collections (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    PRIMARY KEY (db_name, coll_name)
                )
                """
            )
            legacy.execute(
                """
                CREATE TABLE documents (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    storage_key TEXT NOT NULL,
                    document TEXT NOT NULL,
                    PRIMARY KEY (db_name, coll_name, storage_key)
                )
                """
            )
            legacy.execute(
                """
                CREATE TABLE indexes (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    physical_name TEXT,
                    fields TEXT NOT NULL,
                    keys TEXT,
                    unique_flag INTEGER NOT NULL,
                    sparse_flag INTEGER NOT NULL DEFAULT 0,
                    hidden_flag INTEGER NOT NULL DEFAULT 0,
                    partial_filter_json TEXT,
                    expire_after_seconds INTEGER,
                    multikey_flag INTEGER NOT NULL DEFAULT 0,
                    multikey_physical_name TEXT,
                    PRIMARY KEY (db_name, coll_name, name)
                )
                """
            )
            legacy.execute(
                """
                CREATE TABLE search_indexes (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    name TEXT NOT NULL,
                    index_type TEXT NOT NULL,
                    definition_json TEXT NOT NULL,
                    PRIMARY KEY (db_name, coll_name, name)
                )
                """
            )
            legacy.execute(
                """
                CREATE TABLE multikey_entries (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    storage_key TEXT NOT NULL,
                    element_type TEXT NOT NULL,
                    element_key TEXT NOT NULL
                )
                """
            )
            legacy.execute(
                """
                CREATE TABLE scalar_index_entries (
                    db_name TEXT NOT NULL,
                    coll_name TEXT NOT NULL,
                    index_name TEXT NOT NULL,
                    storage_key TEXT NOT NULL,
                    element_type TEXT NOT NULL,
                    element_key TEXT NOT NULL
                )
                """
            )
            payload = SQLiteEngine(path)._serialize_document({"_id": "1", "email": "ada@example.com", "tags": ["python"]})
            legacy.execute("INSERT INTO collections (db_name, coll_name) VALUES (?, ?)", ("db", "coll"))
            legacy.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", repr("1"), payload),
            )
            legacy.execute(
                """
                INSERT INTO indexes (
                    db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag,
                    hidden_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "db",
                    "coll",
                    "email_1",
                    "idx_email",
                    '["email"]',
                    '[["email", 1]]',
                    0,
                    0,
                    0,
                    None,
                    None,
                    1,
                    "mkidx_email",
                ),
            )
            legacy.execute(
                """
                INSERT INTO search_indexes (db_name, coll_name, name, index_type, definition_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    "db",
                    "coll",
                    "text",
                    "search",
                    '{"mappings": {"dynamic": true}}',
                ),
            )
            legacy.commit()
            legacy.close()

            engine = SQLiteEngine(path)
            await engine.connect()
            try:
                conn = engine._require_connection()
                collection_columns = {row[1] for row in conn.execute("PRAGMA table_info(collections)").fetchall()}
                index_columns = {row[1] for row in conn.execute("PRAGMA table_info(indexes)").fetchall()}
                search_columns = {row[1] for row in conn.execute("PRAGMA table_info(search_indexes)").fetchall()}
                multikey_columns = {row[1] for row in conn.execute("PRAGMA table_info(multikey_entries)").fetchall()}
                scalar_columns = {row[1] for row in conn.execute("PRAGMA table_info(scalar_index_entries)").fetchall()}

                self.assertIn("options_json", collection_columns)
                self.assertIn("collection_id", collection_columns)
                self.assertIn("scalar_physical_name", index_columns)
                self.assertIn("physical_name", search_columns)
                self.assertIn("ready_at_epoch", search_columns)
                self.assertIn("collection_id", multikey_columns)
                self.assertNotIn("db_name", multikey_columns)
                self.assertIn("type_score", scalar_columns)
                self.assertNotIn("db_name", scalar_columns)

                scalar_name = conn.execute(
                    "SELECT scalar_physical_name FROM indexes WHERE db_name = ? AND coll_name = ? AND name = ?",
                    ("db", "coll", "email_1"),
                ).fetchone()[0]
                physical_name = conn.execute(
                    "SELECT physical_name FROM search_indexes WHERE db_name = ? AND coll_name = ? AND name = ?",
                    ("db", "coll", "text"),
                ).fetchone()[0]
                scalar_rows = conn.execute("SELECT COUNT(*) FROM scalar_index_entries").fetchone()[0]

                self.assertTrue(scalar_name)
                self.assertTrue(physical_name)
                self.assertGreater(scalar_rows, 0)
            finally:
                await engine.disconnect()
        finally:
            if os.path.exists(path):
                os.remove(path)

    async def test_list_indexes_includes_builtin_id_and_index_information_round_trips_key_spec(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", [("email", 1), ("created_at", -1)], unique=True)
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes,
            [
                {"name": "_id_", "key": {"_id": 1}, "unique": True},
                {
                    "name": "email_1_created_at_-1",
                    "key": {"email": 1, "created_at": -1},
                    "unique": True,
                },
            ],
        )
        self.assertEqual(
            info,
            {
                "_id_": {"key": [("_id", 1)], "unique": True},
                "email_1_created_at_-1": {"key": [("email", 1), ("created_at", -1)], "unique": True},
            },
        )

    async def test_sparse_and_partial_index_metadata_round_trips(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                sparse=True,
                partial_filter_expression={"active": True},
            )
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes[1],
            {
                "name": "email_1",
                "key": {"email": 1},
                "unique": False,
                "sparse": True,
                "partialFilterExpression": {"active": True},
            },
        )

    async def test_ttl_index_metadata_and_opportunistic_expiration_round_trip(self):
        engine = SQLiteEngine()
        past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
        future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=120)
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "expired", "expires_at": past, "name": "old"})
            await engine.put_document("db", "coll", {"_id": "fresh", "expires_at": future, "name": "new"})
            await engine.create_index("db", "coll", ["expires_at"], expire_after_seconds=30)
            indexes = await engine.list_indexes("db", "coll")
            info = await engine.index_information("db", "coll")
            found = await engine.get_document("db", "coll", "expired")
            remaining = [document async for document in self._scan(engine, "db", "coll", {})]
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes[1],
            {
                "name": "expires_at_1",
                "key": {"expires_at": 1},
                "unique": False,
                "expireAfterSeconds": 30,
            },
        )
        self.assertEqual(
            info["expires_at_1"],
            {"key": [("expires_at", 1)], "expireAfterSeconds": 30},
        )
        self.assertIsNone(found)
        self.assertEqual(remaining, [{"_id": "fresh", "expires_at": future, "name": "new"}])

    async def test_virtual_unique_indexes_enforce_sparse_and_partial_semantics(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "sparse", ["email"], unique=True, sparse=True)
            await engine.put_document("db", "sparse", {"_id": "1"})
            await engine.put_document("db", "sparse", {"_id": "2"})
            await engine.put_document("db", "sparse", {"_id": "3", "email": "a@example.com"})
            with self.assertRaises(DuplicateKeyError):
                await engine.put_document("db", "sparse", {"_id": "4", "email": "a@example.com"})

            await engine.create_index(
                "db",
                "partial",
                ["email"],
                unique=True,
                partial_filter_expression={"active": True},
            )
            await engine.put_document("db", "partial", {"_id": "1", "email": "a@example.com", "active": False})
            await engine.put_document("db", "partial", {"_id": "2", "email": "a@example.com", "active": False})
            await engine.put_document("db", "partial", {"_id": "3", "email": "a@example.com", "active": True})
            with self.assertRaises(DuplicateKeyError):
                await engine.put_document("db", "partial", {"_id": "4", "email": "a@example.com", "active": True})
        finally:
            await engine.disconnect()

    async def test_hint_rejects_partial_index_when_query_does_not_imply_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com", "active": False})
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                partial_filter_expression={"active": True},
                name="idx_email_active",
            )
            with self.assertRaises(OperationFailure):
                await self._explain(engine, 
                    "db",
                    "coll",
                    {"email": "a@example.com"},
                    hint="idx_email_active",
                )
        finally:
            await engine.disconnect()

    async def test_simple_equals_fast_path_does_not_use_partial_indexes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com", "active": False})
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                partial_filter_expression={"active": True},
                name="idx_email_active",
            )
            semantics = compile_find_semantics({"email": "a@example.com"})

            with patch.object(
                engine,
                "_compile_read_execution_plan",
                wraps=engine._compile_read_execution_plan,
            ) as compile_plan:
                await engine.plan_find_semantics("db", "coll", semantics)
        finally:
            await engine.disconnect()

        self.assertEqual(compile_plan.call_count, 1)

    async def test_hint_rejects_sparse_index_when_query_does_not_imply_field_presence(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1"})
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                sparse=True,
                name="idx_email_sparse",
            )
            with self.assertRaises(OperationFailure):
                await self._explain(engine, 
                    "db",
                    "coll",
                    {},
                    hint="idx_email_sparse",
                )
        finally:
            await engine.disconnect()

    async def test_hint_by_key_pattern_supports_id_index_and_rejects_unusable_partial_index(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "email": "a@example.com", "active": False})
            await engine.create_index(
                "db",
                "coll",
                ["email"],
                partial_filter_expression={"active": True},
                name="idx_email_active",
            )

            id_plan = await self._explain(
                engine,
                "db",
                "coll",
                {"_id": "1"},
                hint=[("_id", 1)],
            )
            with self.assertRaises(OperationFailure):
                await self._explain(
                    engine,
                    "db",
                    "coll",
                    {"email": "a@example.com"},
                    hint=[("email", 1)],
                )
        finally:
            await engine.disconnect()

        self.assertEqual(id_plan.hinted_index, "_id_")

    def test_serialize_value_for_sql_preserves_scalars_and_encodes_complex_values(self):
        engine = SQLiteEngine()

        self.assertIsNone(engine._serialize_value_for_sql(None))
        self.assertEqual(engine._serialize_value_for_sql(3), 3)
        self.assertEqual(engine._serialize_value_for_sql(3.5), 3.5)
        self.assertEqual(engine._serialize_value_for_sql("Ada"), "Ada")
        self.assertEqual(engine._serialize_value_for_sql(True), True)
        encoded = engine._serialize_value_for_sql({"a": 1})

        self.assertEqual(encoded, {"a": 1})

    async def test_get_document_sync_uses_profiler_namespace_entries(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            engine._profiler.set_level("db", 2)
            engine._profiler.record(
                "db",
                op="query",
                namespace="db.system.profile",
                command={"comment": "trace"},
                duration_micros=1000,
            )
            document = engine._get_document_sync(
                "db",
                engine._PROFILE_COLLECTION_NAME,
                1,
                {"command.comment": 1},
            )
            missing = engine._get_document_sync(
                "db",
                engine._PROFILE_COLLECTION_NAME,
                99,
                None,
            )
        finally:
            await engine.disconnect()

        self.assertEqual(document, {"command": {"comment": "trace"}, "_id": 1})
        self.assertIsNone(missing)

    def test_purge_expired_documents_sync_returns_zero_when_index_loading_fails(self):
        engine = SQLiteEngine()
        connection = sqlite3.connect(":memory:")

        with patch.object(engine, "_load_indexes", side_effect=RuntimeError("boom")):
            purged = engine._purge_expired_documents_sync(
                connection,
                "db",
                "coll",
                context=None,
            )

        self.assertEqual(purged, 0)
        connection.close()

    async def test_drop_index_and_drop_indexes_preserve_builtin_id_index(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=False, name="idx_email")
            await engine.create_index("db", "coll", ["kind"], unique=False, name="idx_kind")
            await engine.drop_index("db", "coll", "idx_email")
            after_single_drop = await engine.list_indexes("db", "coll")
            await engine.drop_indexes("db", "coll")
            after_drop_all = await engine.list_indexes("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            after_single_drop,
            [
                {"name": "_id_", "key": {"_id": 1}, "unique": True},
                {"name": "idx_kind", "key": {"kind": 1}, "unique": False},
            ],
        )
        self.assertEqual(
            after_drop_all,
            [{"name": "_id_", "key": {"_id": 1}, "unique": True}],
        )

    async def test_drop_index_by_key_pattern_rejects_ambiguous_aliases(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_index("db", "coll", ["email"], unique=False, name="email_primary")
            await engine.create_index("db", "coll", ["email"], unique=False, name="email_alias")

            with self.assertRaisesRegex(OperationFailure, "multiple indexes found with key pattern"):
                await engine.drop_index("db", "coll", [("email", 1)])

            await engine.drop_index("db", "coll", "email_primary")
            indexes = await engine.list_indexes("db", "coll")
        finally:
            await engine.disconnect()

        self.assertEqual(
            indexes,
            [
                {"name": "_id_", "key": {"_id": 1}, "unique": True},
                {"name": "email_alias", "key": {"email": 1}, "unique": False},
            ],
        )

    async def test_builtin_id_index_cannot_be_dropped(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with self.assertRaises(OperationFailure):
                await engine.drop_index("db", "coll", "_id_")
            with self.assertRaises(OperationFailure):
                await engine.drop_index("db", "coll", [("_id", 1)])
        finally:
            await engine.disconnect()

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

    async def test_create_collection_registers_empty_namespace_and_rejects_duplicates(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_collection("db", "empty", options={"capped": True})
            self.assertEqual(await engine.list_databases(), ["db"])
            self.assertEqual(await engine.list_collections("db"), ["empty"])
            self.assertEqual(
                await engine.collection_options("db", "empty"),
                {"capped": True},
            )
            with self.assertRaises(CollectionInvalid):
                await engine.create_collection("db", "empty")
        finally:
            await engine.disconnect()

    async def test_profile_settings_make_system_profile_namespace_visible(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            engine._profiler.set_level("db", 1)

            self.assertEqual(await engine.list_databases(), ["db"])
            self.assertEqual(await engine.list_collections("db"), ["system.profile"])
            self.assertEqual(await engine.collection_options("db", "system.profile"), {})
        finally:
            await engine.disconnect()

    async def test_delete_last_document_does_not_remove_collection_metadata(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1"}, overwrite=True)
            await engine.delete_document("db", "coll", "1")
            self.assertEqual(await engine.list_collections("db"), ["coll"])
        finally:
            await engine.disconnect()

    async def test_rename_collection_moves_namespace_metadata(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_collection("db", "events", options={"capped": True})
            await engine.put_document("db", "events", {"_id": "1"}, overwrite=True)
            await engine.create_index("db", "events", ["kind"], name="kind_idx")

            await engine.rename_collection("db", "events", "archived")

            self.assertEqual(await engine.list_collections("db"), ["archived"])
            self.assertEqual(await engine.get_document("db", "archived", "1"), {"_id": "1"})
            self.assertIn("kind_idx", await engine.index_information("db", "archived"))
            self.assertEqual(await engine.collection_options("db", "archived"), {"capped": True})
        finally:
            await engine.disconnect()

    async def test_explain_search_documents_reports_pending_status(self):
        engine = SQLiteEngine(simulate_search_index_latency=60.0)
        await engine.connect()
        try:
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {"title": {"type": "string"}},
                        }
                    },
                    name="by_text",
                ),
            )

            explanation = await engine.explain_search_documents(
                "db",
                "coll",
                "$search",
                {"index": "by_text", "text": {"query": "ada", "path": "title"}},
            )
        finally:
            await engine.disconnect()

        self.assertEqual(explanation.hinted_index, "by_text")
        self.assertEqual(explanation.details["status"], "PENDING")
        self.assertIn(explanation.details["backend"], {"python", "fts5"})
        self.assertIn("backendAvailable", explanation.details)
        self.assertIn("backendMaterialized", explanation.details)
        self.assertIn("physicalName", explanation.details)
        self.assertIn("readyAtEpoch", explanation.details)
        self.assertIn("fts5Available", explanation.details)
        self.assertIsNotNone(explanation.details["readyAtEpoch"])

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

    def test_drop_collection_resolves_collection_id_once(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        fake_connection.execute.return_value = None
        engine._require_connection = Mock(return_value=fake_connection)
        engine._load_indexes = Mock(return_value=[])
        engine._load_search_index_rows = Mock(return_value=[])
        engine._lookup_collection_id = Mock(return_value=7)

        engine._drop_collection_sync("db", "coll")

        engine._lookup_collection_id.assert_called_once_with(fake_connection, "db", "coll")

    def test_delete_matching_document_sync_skips_non_matching_fallback_rows(self):
        engine = SQLiteEngine()
        fake_connection = Mock()
        fake_connection.execute.return_value.rowcount = 1
        engine._require_connection = Mock(return_value=fake_connection)
        engine._select_first_document_for_plan = Mock(side_effect=NotImplementedError("fallback"))
        engine._load_documents = Mock(
            return_value=[
                ("1", {"kind": "skip"}),
                ("2", {"kind": "match"}),
            ]
        )

        result = engine._delete_matching_document_sync("db", "coll", {"kind": "match"}, None, None)

        self.assertEqual(result.deleted_count, 1)
        self.assertGreaterEqual(fake_connection.execute.call_count, 2)
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
                    async for document in self._scan(engine, "db", "coll", {"kind": "view"})
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "kind": "view"}])

    async def test_build_select_sql_omits_json_each_for_scalar_only_equals_fields(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "active": True})
            await engine.put_document("db", "coll", {"_id": "2", "active": False})

            sql, params = engine._build_select_sql(
                "db",
                "coll",
                compile_filter({"active": True}),
                select_clause="document",
            )
        finally:
            await engine.disconnect()

        self.assertNotIn("json_each", sql)
        self.assertIn("json_extract(document, '$.active') = ?", sql)
        self.assertEqual(params[-1], 1)

    async def test_build_select_sql_keeps_json_each_for_top_level_array_equals_fields(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "active": [True]})
            await engine.put_document("db", "coll", {"_id": "2", "active": False})

            sql, _ = engine._build_select_sql(
                "db",
                "coll",
                compile_filter({"active": True}),
                select_clause="document",
            )
        finally:
            await engine.disconnect()

        self.assertIn("json_each", sql)

    async def test_build_select_sql_omits_type_order_case_for_homogeneous_numeric_comparisons(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "age": 18})
            await engine.put_document("db", "coll", {"_id": "2", "age": 40})

            sql, params = engine._build_select_sql(
                "db",
                "coll",
                compile_filter({"age": {"$gte": 20}}),
                select_clause="document",
            )
        finally:
            await engine.disconnect()

        self.assertNotIn("CASE WHEN", sql)
        self.assertIn("json_extract(document, '$.age') >= ?", sql)
        self.assertEqual(params[-1], 20)

    async def test_build_select_sql_keeps_type_order_case_for_mixed_type_comparisons(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "age": 18})
            await engine.put_document("db", "coll", {"_id": "2", "age": None})

            sql, _ = engine._build_select_sql(
                "db",
                "coll",
                compile_filter({"age": {"$lt": 20}}),
                select_clause="document",
            )
        finally:
            await engine.disconnect()

        self.assertIn("CASE WHEN", sql)

    async def test_build_select_sql_uses_scalar_index_entries_for_homogeneous_single_field_sort(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "score": 10})
            await engine.put_document("db", "coll", {"_id": "2", "score": 30})
            await engine.create_index("db", "coll", [("score", -1)])

            sql, _ = engine._build_select_sql(
                "db",
                "coll",
                MatchAll(),
                select_clause="document",
                sort=[("score", -1)],
                limit=5,
            )
        finally:
            await engine.disconnect()

        self.assertIn("FROM scalar_index_entries", sql)
        self.assertIn("ORDER BY scalar_index_entries.element_key DESC", sql)
        self.assertNotIn("CASE WHEN", sql)

    async def test_build_select_sql_uses_plain_value_order_for_uniform_unindexed_scalar_sort(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "score": 10})
            await engine.put_document("db", "coll", {"_id": "2", "score": 30})

            sql, _ = engine._build_select_sql(
                "db",
                "coll",
                MatchAll(),
                select_clause="document",
                sort=[("score", -1)],
            )
        finally:
            await engine.disconnect()

        self.assertNotIn("FROM scalar_index_entries", sql)
        self.assertIn("ORDER BY COALESCE(json_extract(document, '$.score.\"$mongoeco\".value'), json_extract(document, '$.score')) DESC", sql)
        self.assertNotIn("CASE WHEN", sql)

    async def test_build_select_sql_keeps_type_order_case_for_sorts_with_nulls(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "score": 10})
            await engine.put_document("db", "coll", {"_id": "2", "score": None})

            sql, _ = engine._build_select_sql(
                "db",
                "coll",
                MatchAll(),
                select_clause="document",
                sort=[("score", -1)],
            )
        finally:
            await engine.disconnect()

        self.assertIn("CASE WHEN", sql)

    async def test_plan_find_semantics_uses_scalar_entries_for_top_level_string_equality(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "username": "ada"})
            await engine.create_index("db", "coll", [("username", 1)])

            plan = await engine.plan_find_semantics(
                "db",
                "coll",
                compile_find_semantics({"username": "ada"}, limit=1),
            )
        finally:
            await engine.disconnect()

        self.assertTrue(plan.use_sql)
        self.assertIsNotNone(plan.sql)
        self.assertIn("INDEXED BY", plan.sql)
        self.assertIn("FROM scalar_index_entries", plan.sql)
        self.assertIn("JOIN documents", plan.sql)
        self.assertIn("scalar_index_entries.collection_id = ?", plan.sql)
        self.assertIn("scalar_index_entries.index_name = ?", plan.sql)
        self.assertIn("LIMIT 1", plan.sql)
        self.assertEqual(list(plan.params[-2:]), [3, "ada"])

    async def test_plan_find_semantics_uses_scalar_entries_for_top_level_numeric_range(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "age": 18})
            await engine.put_document("db", "coll", {"_id": "2", "age": 40})
            await engine.create_index("db", "coll", [("age", 1)])

            plan = await engine.plan_find_semantics(
                "db",
                "coll",
                compile_find_semantics({"age": {"$gte": 20}}, limit=1),
            )
        finally:
            await engine.disconnect()

        self.assertTrue(plan.use_sql)
        self.assertIsNotNone(plan.sql)
        self.assertIn("FROM scalar_index_entries", plan.sql)
        self.assertIn("scalar_index_entries.type_score = ?", plan.sql)
        self.assertIn("scalar_index_entries.element_key >= ?", plan.sql)
        self.assertIn("LIMIT 1", plan.sql)

    async def test_plan_find_semantics_keeps_generic_sql_for_mixed_type_top_level_numeric_range(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "age": 18})
            await engine.put_document("db", "coll", {"_id": "2", "age": "40"})
            await engine.create_index("db", "coll", [("age", 1)])

            plan = await engine.plan_find_semantics(
                "db",
                "coll",
                compile_find_semantics({"age": {"$gte": 20}}, limit=1),
            )
        finally:
            await engine.disconnect()

        self.assertTrue(plan.use_sql)
        self.assertIsNotNone(plan.sql)
        self.assertNotIn("FROM scalar_index_entries", plan.sql)

    async def test_explain_query_plan_uses_scalar_entries_for_top_level_numeric_range(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "age": 18})
            await engine.put_document("db", "coll", {"_id": "2", "age": 40})
            await engine.create_index("db", "coll", [("age", 1)])

            explained = engine._explain_query_plan_sync("db", "coll", {"age": {"$gte": 20}}, limit=1)
        finally:
            await engine.disconnect()

        self.assertTrue(any("scalar_index_entries" in row for row in explained))

    async def test_select_first_document_for_plan_uses_scalar_entries_for_top_level_numeric_range(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "age": 18})
            await engine.put_document("db", "coll", {"_id": "2", "age": 40})
            await engine.create_index("db", "coll", [("age", 1)])

            selected = engine._select_first_document_for_plan(
                "db",
                "coll",
                compile_filter({"age": {"$gte": 20}}),
            )
        finally:
            await engine.disconnect()

        self.assertIsNotNone(selected)
        assert selected is not None
        self.assertEqual(selected[1]["_id"], "2")

    async def test_scalar_index_entries_track_updates_for_top_level_scalar_indexes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "username": "ada"})
            await engine.create_index("db", "coll", [("username", 1)])

            initial_match = [doc async for doc in self._scan(engine, "db", "coll", {"username": "ada"})]

            await self._update(
                engine,
                "db",
                "coll",
                {"_id": "1"},
                {"$set": {"username": "bea"}},
            )

            old_match = [doc async for doc in self._scan(engine, "db", "coll", {"username": "ada"})]
            new_match = [doc async for doc in self._scan(engine, "db", "coll", {"username": "bea"})]
        finally:
            await engine.disconnect()

        self.assertEqual(initial_match, [{"_id": "1", "username": "ada"}])
        self.assertEqual(old_match, [])
        self.assertEqual(new_match, [{"_id": "1", "username": "bea"}])

    async def test_update_and_delete_prefer_explicit_plan_over_conflicting_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})
            plan = compile_filter({"kind": "view"})

            update_result = await self._update(engine, 
                "db",
                "coll",
                {"kind": "click"},
                {"$set": {"done": True}},
                plan=plan,
            )
            delete_result = await self._delete(engine, 
                "db",
                "coll",
                {"kind": "click"},
                plan=plan,
            )
            remaining = [doc async for doc in self._scan(engine, "db", "coll", sort=[("_id", 1)])]
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
                    async for document in self._scan(engine, 
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
                    async for document in self._scan(engine, "db", "coll", {"kind": "view"}, skip=1)
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "2", "kind": "view"}])

    def test_deserialize_document_skips_codec_decode_for_plain_json_payload(self):
        engine = SQLiteEngine()
        payload = '{"_id":"1","kind":"view","tags":["python","sqlite"]}'

        with patch.object(engine._codec, "decode", side_effect=AssertionError("decode")):
            document = engine._deserialize_document(payload)

        self.assertEqual(
            document,
            {"_id": "1", "kind": "view", "tags": ["python", "sqlite"]},
        )

    def test_deserialize_document_propagates_codec_type_errors(self):
        engine = SQLiteEngine()
        payload = '{"wrapped":{"$mongoeco":{"type":"int32","value":1}}}'

        with patch.object(engine._codec, "decode", side_effect=TypeError("broken")):
            with self.assertRaisesRegex(TypeError, "broken"):
                engine._deserialize_document(payload)

    async def test_scan_collection_falls_back_to_query_engine_for_untranslatable_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "data": {"b": 1}})
            await engine.put_document("db", "coll", {"_id": "2", "data": {"b": 2}})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                documents = [
                    document
                    async for document in self._scan(engine, "db", "coll", {"data": {"b": 1}})
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "data": {"b": 1}}])
        self.assertGreater(match_plan.call_count, 0)

    async def test_scan_collection_falls_back_to_query_engine_for_top_level_json_schema(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "name": "Ada", "age": 10})
            await engine.put_document("db", "coll", {"_id": "2", "name": "Bob", "age": "old"})
            await engine.put_document("db", "coll", {"_id": "3", "age": 11})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", wraps=QueryEngine.match_plan) as match_plan:
                documents = [
                    document
                    async for document in self._scan(
                        engine,
                        "db",
                        "coll",
                        {
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        },
                        sort=[("_id", 1)],
                    )
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "name": "Ada", "age": 10}])
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
                    async for document in self._scan(engine, "db", "coll", sort=[("rank", 1)], skip=1, limit=1)
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "1", "rank": [2, 3]}])
        self.assertEqual(load_documents.call_count, 0)

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

        def _sort(documents, sort, **kwargs):
            captured_lengths.append(len(documents))
            return sort_documents(documents, sort, **kwargs)

        try:
            with (
                patch.object(engine, "_load_documents", side_effect=_documents),
                patch("mongoeco.engines.semantic_core.sort_documents", side_effect=_sort),
            ):
                documents = [
                    document
                    async for document in self._scan(engine, 
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

    async def test_scan_collection_python_sort_fallback_keeps_only_skip_plus_limit_window(self):
        engine = SQLiteEngine()
        await engine.connect()

        def _documents(_db_name, _coll_name):
            for document_id, payload in (
                ("1", {"name": "Ada", "rank": [2, 3]}),
                ("2", {"name": "Bob", "rank": [1, 4]}),
                ("3", {"name": "Ana", "rank": [0, 5]}),
                ("4", {"name": "Ari", "rank": [-1, 5]}),
            ):
                yield document_id, {"_id": document_id, **payload}

        captured_lengths: list[int] = []

        try:
            with (
                patch.object(engine, "_load_documents", side_effect=_documents),
                patch("mongoeco.engines.semantic_core.sort_documents_limited") as limited_sort,
            ):
                from mongoeco.core.sorting import sort_documents_limited as real_sort_documents_limited

                def _limited(documents, sort, **kwargs):
                    materialized = list(documents)
                    captured_lengths.append(len(materialized))
                    return real_sort_documents_limited(materialized, sort, **kwargs)

                limited_sort.side_effect = _limited
                documents = [
                    document
                    async for document in self._scan(engine, 
                        "db",
                        "coll",
                        {"name": {"$regex": "^A"}},
                        sort=[("rank", 1)],
                        skip=1,
                        limit=1,
                    )
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "3", "name": "Ana", "rank": [0, 5]}])
        self.assertEqual(captured_lengths, [3])

    async def test_scan_collection_automatically_falls_back_for_array_sort(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "rank": [2, 3]})
            await engine.put_document("db", "coll", {"_id": "2", "rank": [1, 4]})

            with patch.object(engine, "_load_documents", wraps=engine._load_documents) as load_documents:
                documents = [
                    document
                    async for document in self._scan(engine, "db", "coll", sort=[("rank", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, [{"_id": "2", "rank": [1, 4]}, {"_id": "1", "rank": [2, 3]}])
        self.assertEqual(load_documents.call_count, 0)

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
                async for document in self._scan(engine, 
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
            result = await self._delete(engine, 
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
            count = await self._count(engine, 
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
                    async for document in self._scan(engine, "db", "coll", sort=[("profile", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(
            documents,
            [{"_id": "2", "profile": {"rank": 1}}, {"_id": "1", "profile": {"rank": 2}}],
        )
        self.assertEqual(load_documents.call_count, 0)

    async def test_count_matching_documents_uses_sql_translation_for_simple_filters(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "click"})

            with patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")):
                count = await self._count(engine, "db", "coll", {"kind": "view"})
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
                count = await self._count(engine, "db", "coll", {"data": {"b": 1}})
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
                count = await self._count(engine, "db", "coll", {"items.name": "a"})
        finally:
            await engine.disconnect()

        self.assertEqual(count, 1)
        self.assertGreater(match_plan.call_count, 0)

    async def test_count_matching_documents_falls_back_for_tagged_bytes_range_filter(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "v": b"\x00"})
            count = await self._count(engine, 
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
                by_id = [document async for document in self._scan(engine, "db", "coll", {"_id": object_id})]
                by_datetime = [
                    document
                    async for document in self._scan(engine, "db", "coll", {"created_at": created_at})
                ]
                by_uuid = [
                    document
                    async for document in self._scan(engine, "db", "coll", {"session_id": session_id})
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
                count = await self._count(engine, "db", "coll", {"_id": {"$in": list(ids)}})
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
                    async for document in self._scan(engine, "db", "coll", sort=[("created_at", 1)])
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
                async for document in self._scan(engine, "db", "coll", {"kind": "view"}):
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
                async for _document in self._scan(engine, "db", "coll", {"data": {"b": 1}}):
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
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as handle:
            sqlite_path = handle.name
        engine = SQLiteEngine(path=sqlite_path)
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
                seen = [document async for document in self._scan(engine, "db", "coll", {"kind": "view"})]
        finally:
            await engine.disconnect()
            Path(sqlite_path).unlink(missing_ok=True)

        self.assertEqual(seen, [])

    async def test_scan_collection_file_backend_batches_queue_reads(self):
        with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as handle:
            sqlite_path = handle.name
        engine = SQLiteEngine(path=sqlite_path)
        await engine.connect()
        documents = [{"_id": str(index)} for index in range(5)]
        get_call_count = 0
        original_run_blocking = engine._run_blocking

        async def _counting_run_blocking(func, *args, **kwargs):
            nonlocal get_call_count
            if getattr(func, "__name__", "") == "get":
                get_call_count += 1
            return await original_run_blocking(func, *args, **kwargs)

        def _wrapped(*args, **kwargs):
            yield from documents

        try:
            with (
                patch("mongoeco.engines.sqlite._ASYNC_SCAN_QUEUE_BATCH_SIZE", 2),
                patch.object(engine, "_run_blocking", side_effect=_counting_run_blocking),
                patch.object(engine, "_iter_scan_documents_sync", side_effect=_wrapped),
            ):
                seen = [document async for document in self._scan(engine, "db", "coll", {"kind": "view"})]
        finally:
            await engine.disconnect()
            Path(sqlite_path).unlink(missing_ok=True)

        self.assertEqual(seen, documents)
        self.assertEqual(get_call_count, 4)

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
                    async for document in self._scan(engine, "db", "coll", {"name": {"$ne": "Ada"}}, sort=[("_id", 1)])
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
                    async for document in self._scan(engine, "db", "coll", {"refs": {"$ne": oid1}}, sort=[("_id", 1)])
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
                    async for document in self._scan(engine, "db", "coll", sort=[("profile.rank", 1)])
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
                    async for document in self._scan(engine, "db", "coll", {"items.name": "a"}, sort=[("_id", 1)])
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
                    async for document in self._scan(engine, "db", "coll", sort=[("items.name", 1)])
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "1"])
        self.assertEqual(load_documents.call_count, 0)

    async def test_scan_collection_uses_hybrid_sql_filter_with_python_sort_for_tagged_bytes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "kind": "view", "payload": b"\x02"})
            await engine.put_document("db", "coll", {"_id": "2", "kind": "view", "payload": b"\x01"})
            await engine.put_document("db", "coll", {"_id": "3", "kind": "skip", "payload": b"\x00"})

            with (
                patch("mongoeco.engines.sqlite.QueryEngine.match_plan", side_effect=AssertionError("fallback")),
                patch.object(engine, "_load_documents", side_effect=AssertionError("loaded")),
            ):
                documents = [
                    document["_id"]
                    async for document in self._scan(engine, 
                        "db",
                        "coll",
                        {"kind": "view"},
                        sort=[("payload", 1)],
                    )
                ]
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["2", "1"])

    async def test_scan_and_explain_size_filter_use_sql_pushdown(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "tags": ["python", "sqlite"]})
            await engine.put_document("db", "coll", {"_id": "2", "tags": ["python"]})
            await engine.put_document("db", "coll", {"_id": "3", "tags": "python"})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"tags": {"$size": 2}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"tags": {"$size": 2}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_mod_filter_use_sql_pushdown_for_scalar_integers(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "value": 5})
            await engine.put_document("db", "coll", {"_id": "2", "value": 4})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"value": {"$mod": [2, 1]}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"value": {"$mod": [2, 1]}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_all_filter_use_sql_pushdown_for_simple_scalar_arrays(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "tags": ["python", "sqlite"]})
            await engine.put_document("db", "coll", {"_id": "2", "tags": ["python"]})
            await engine.put_document("db", "coll", {"_id": "3", "tags": "python"})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"tags": {"$all": ["python", "sqlite"]}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"tags": {"$all": ["python", "sqlite"]}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_elem_match_filter_use_sql_pushdown_for_scalar_array_predicate(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "scores": [7, 2]})
            await engine.put_document("db", "coll", {"_id": "2", "scores": [4]})
            await engine.put_document("db", "coll", {"_id": "3", "scores": 7})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"scores": {"$elemMatch": {"$gt": 5}}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"scores": {"$elemMatch": {"$gt": 5}}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_range_filter_use_sql_pushdown_for_mixed_scalar_and_array_numbers(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "value": 7})
            await engine.put_document("db", "coll", {"_id": "2", "value": [8, 1]})
            await engine.put_document("db", "coll", {"_id": "3", "value": [2]})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"value": {"$gt": 5}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"value": {"$gt": 5}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1", "2"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_explain_mod_filter_falls_back_when_collection_contains_arrays(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "value": 5})
            await engine.put_document("db", "coll", {"_id": "2", "value": [5]})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"value": {"$mod": [2, 1]}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"value": {"$mod": [2, 1]}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1", "2"])
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["pushdown"]["mode"], "python")
        self.assertFalse(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.details["fallback_reason"], "Query operator not yet translated to SQL")
        self.assertEqual(explanation.planning_issues[0].scope, "engine")
        self.assertEqual(explanation.planning_issues[0].message, "Query operator not yet translated to SQL")

    async def test_explain_mod_filter_falls_back_when_collection_contains_real_values(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "value": 5})
            await engine.put_document("db", "coll", {"_id": "2", "value": 4.5})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"value": {"$mod": [2, 1]}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"value": {"$mod": [2, 1]}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["pushdown"]["mode"], "python")
        self.assertFalse(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.details["fallback_reason"], "Query operator not yet translated to SQL")
        self.assertEqual(explanation.details["pushdown_hints"][0]["operator"], "$mod")
        self.assertEqual(explanation.details["pushdown_hints"][0]["priority"], "high")
        self.assertEqual(explanation.planning_issues[0].scope, "engine")
        self.assertEqual(explanation.planning_issues[0].message, "Query operator not yet translated to SQL")

    async def test_explain_range_filter_falls_back_when_array_elements_have_incompatible_types(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "value": 7})
            await engine.put_document("db", "coll", {"_id": "2", "value": ["x", 8]})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"value": {"$gt": 5}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"value": {"$gt": 5}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1", "2"])
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["pushdown"]["mode"], "python")
        self.assertFalse(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.details["fallback_reason"], "Top-level array comparisons require Python fallback")
        self.assertTrue(any(hint["operator"] == "range-comparison" for hint in explanation.details["pushdown_hints"]))
        self.assertTrue(any(hint["operator"] == "array-comparison" for hint in explanation.details["pushdown_hints"]))

    async def test_scan_and_explain_regex_prefix_filter_use_sql_pushdown_for_scalar_strings(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada Lovelace"})
            await engine.put_document("db", "coll", {"_id": "2", "title": "Grace Hopper"})
            await engine.put_document("db", "coll", {"_id": "3", "title": 123})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"title": {"$regex": "^Ada"}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "^Ada"}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_regex_exact_filter_use_sql_pushdown_for_scalar_strings(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada"})
            await engine.put_document("db", "coll", {"_id": "2", "title": "Ada Lovelace"})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"title": {"$regex": "^Ada$"}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "^Ada$"}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_regex_contains_and_suffix_filters_use_sql_pushdown(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada Lovelace"})
            await engine.put_document("db", "coll", {"_id": "2", "title": "Lovelace"})
            await engine.put_document("db", "coll", {"_id": "3", "title": "Grace Hopper"})

            contains_documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"title": {"$regex": "Love"}}, sort=[("_id", 1)])
            ]
            contains_explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "Love"}})

            suffix_documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"title": {"$regex": "Lovelace$"}}, sort=[("_id", 1)])
            ]
            suffix_explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "Lovelace$"}})
        finally:
            await engine.disconnect()

        self.assertEqual(contains_documents, ["1", "2"])
        self.assertEqual(contains_explanation.strategy, "sql")
        self.assertEqual(contains_explanation.details["pushdown"]["mode"], "sql")
        self.assertEqual(contains_explanation.planning_issues, ())

        self.assertEqual(suffix_documents, ["1", "2"])
        self.assertEqual(suffix_explanation.strategy, "sql")
        self.assertEqual(suffix_explanation.details["pushdown"]["mode"], "sql")
        self.assertEqual(suffix_explanation.planning_issues, ())

    async def test_explain_regex_filter_falls_back_when_collection_contains_arrays(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada Lovelace"})
            await engine.put_document("db", "coll", {"_id": "2", "title": ["Ada Lovelace"]})

            documents = [
                document["_id"]
                async for document in self._scan(engine, "db", "coll", {"title": {"$regex": "^Ada"}}, sort=[("_id", 1)])
            ]
            explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "^Ada"}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1", "2"])
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["pushdown"]["mode"], "python")
        self.assertFalse(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.details["fallback_reason"], "Query operator not yet translated to SQL")
        self.assertEqual(explanation.details["pushdown_hints"][0]["operator"], "$regex")
        self.assertEqual(explanation.details["pushdown_hints"][0]["priority"], "high")
        self.assertEqual(explanation.planning_issues[0].scope, "engine")
        self.assertEqual(explanation.planning_issues[0].message, "Query operator not yet translated to SQL")

    async def test_explain_regex_filter_with_ascii_i_options_use_sql_pushdown(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "ada lovelace"})

            documents = [
                document["_id"]
                async for document in self._scan(
                    engine,
                    "db",
                    "coll",
                    {"title": {"$regex": "^Ada", "$options": "i"}},
                    sort=[("_id", 1)],
                )
            ]
            explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "^Ada", "$options": "i"}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_scan_and_explain_regex_ascii_ignore_case_use_sql_pushdown(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "ada lovelace"})
            await engine.put_document("db", "coll", {"_id": "2", "title": "grace hopper"})

            documents = [
                document["_id"]
                async for document in self._scan(
                    engine,
                    "db",
                    "coll",
                    {"title": {"$regex": "^Ada", "$options": "i"}},
                    sort=[("_id", 1)],
                )
            ]
            explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "^Ada", "$options": "i"}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "sql")
        self.assertEqual(explanation.details["pushdown"]["mode"], "sql")
        self.assertTrue(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.planning_issues, ())

    async def test_explain_regex_ignore_case_falls_back_with_non_ascii_text(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "álvaro"})

            documents = [
                document["_id"]
                async for document in self._scan(
                    engine,
                    "db",
                    "coll",
                    {"title": {"$regex": "^Ál", "$options": "i"}},
                    sort=[("_id", 1)],
                )
            ]
            explanation = await self._explain(engine, "db", "coll", {"title": {"$regex": "^Ál", "$options": "i"}})
        finally:
            await engine.disconnect()

        self.assertEqual(documents, ["1"])
        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["pushdown"]["mode"], "python")
        self.assertFalse(explanation.details["pushdown"]["usesSqlRuntime"])
        self.assertEqual(explanation.details["fallback_reason"], "Query operator not yet translated to SQL")
        self.assertEqual(explanation.details["pushdown_hints"][0]["operator"], "$regex")
        self.assertEqual(explanation.details["pushdown_hints"][0]["nextStep"], "broaden regex pushdown beyond literal-safe patterns or support non-ASCII ignore-case semantics")
        self.assertEqual(explanation.planning_issues[0].scope, "engine")
        self.assertEqual(explanation.planning_issues[0].message, "Query operator not yet translated to SQL")

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
                result = await self._update(engine, 
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
                result = await self._update(engine, 
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
                result = await self._update(engine, 
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

            with patch("mongoeco.engines.sqlite.translate_compiled_update_plan", side_effect=NotImplementedError("nested")):
                with self.assertRaises(DuplicateKeyError):
                    await self._update(engine, 
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

            with patch("mongoeco.engines.sqlite.translate_compiled_update_plan", side_effect=NotImplementedError("fallback")):
                result = await self._update(engine, 
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

    def test_sqlite_helper_edges_cover_ttl_hint_and_scalar_fast_path_branches(self):
        engine = SQLiteEngine()
        aware = datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone(datetime.timedelta(hours=2)))
        naive = datetime.datetime(2026, 1, 1)
        ttl_index = EngineIndexRecord(name="expires_1", fields=["expires_at"], key=[("expires_at", 1)], unique=False, expire_after_seconds=30)

        self.assertIsNone(engine._coerce_ttl_datetime("nope"))
        self.assertEqual(
            engine._coerce_ttl_datetime(naive),
            naive.replace(tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(
            engine._coerce_ttl_datetime(aware),
            aware.astimezone(datetime.timezone.utc),
        )
        self.assertFalse(engine._document_expired_by_ttl({"expires_at": "nope"}, ttl_index, now=datetime.datetime.now(datetime.timezone.utc)))
        self.assertFalse(
            engine._document_expired_by_ttl(
                {"expires_at": datetime.datetime.now(datetime.timezone.utc)},
                EngineIndexRecord(name="compound", fields=["a", "b"], key=[("a", 1), ("b", 1)], unique=False, expire_after_seconds=30),
                now=datetime.datetime.now(datetime.timezone.utc),
            )
        )

        self.assertEqual(engine._resolve_hint_index("db", "coll", "_id_")["name"], "_id_")
        self.assertEqual(engine._resolve_hint_index("db", "coll", [("_id", 1)])["name"], "_id_")

        with patch.object(engine, "_load_indexes", return_value=[
            {"fields": ["kind"], "sparse": True, "partial_filter_expression": None, "scalar_physical_name": "a", "key": [("kind", 1)]},
            {"fields": ["kind"], "sparse": False, "partial_filter_expression": {"x": 1}, "scalar_physical_name": "b", "key": [("kind", 1)]},
            {"fields": ["kind"], "sparse": False, "partial_filter_expression": None, "scalar_physical_name": None, "key": [("kind", 1)]},
        ]):
            self.assertIsNone(engine._find_scalar_sort_index("db", "coll", "kind"))
            self.assertIsNone(engine._find_scalar_fast_path_index("db", "coll", "kind"))
        with patch.object(engine, "_load_indexes", return_value=[
            {"fields": ["kind"], "sparse": False, "partial_filter_expression": None, "scalar_physical_name": "a", "key": [("kind", "text")]},
        ]):
            self.assertIsNone(engine._find_scalar_index("db", "coll", "kind"))

        self.assertIsNone(engine._scalar_range_signature({"x": 1}))
        self.assertIsNone(engine._scalar_range_signature(b"bytes"))

    def test_sqlite_collection_feature_helpers_cover_empty_bool_and_mismatch_branches(self):
        engine = SQLiteEngine()
        conn = Mock()
        conn.execute.return_value.fetchone.side_effect = [
            None,
            (0, 0, 0, 0),
            (2, 0, 0, 2),
            None,
            object(),
        ]
        engine._connection = conn

        self.assertIsNone(engine._field_has_uniform_scalar_sort_type_in_collection("db", "empty", "kind"))
        self.assertIsNone(engine._field_has_uniform_scalar_sort_type_in_collection("db", "zero", "kind"))
        self.assertEqual(engine._field_has_uniform_scalar_sort_type_in_collection("db", "bools", "kind"), "bool")
        self.assertFalse(engine._field_has_comparison_type_mismatch_in_collection("db", "coll", "kind", "string"))
        self.assertTrue(engine._field_has_comparison_type_mismatch_in_collection("db", "coll", "kind", "bool"))
        self.assertTrue(engine._field_has_comparison_type_mismatch_in_collection("db", "coll", "kind", "uuid"))

    async def test_sqlite_create_index_and_namespace_error_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with self.assertRaisesRegex(TypeError, "expire_after_seconds"):
                await engine.create_index("db", "coll", ["expires_at"], expire_after_seconds=-1)
            with self.assertRaisesRegex(OperationFailure, "TTL indexes require a single-field key pattern"):
                await engine.create_index("db", "coll", [("a", 1), ("b", 1)], expire_after_seconds=10)
            with self.assertRaisesRegex(OperationFailure, "TTL indexes cannot be created on _id"):
                await engine.create_index("db", "coll", ["_id"], expire_after_seconds=10)
            with self.assertRaisesRegex(OperationFailure, "special index types currently require a single-field key pattern"):
                await engine.create_index("db", "coll", [("a", "text"), ("b", 1)])
            with self.assertRaisesRegex(OperationFailure, "do not support unique"):
                await engine.create_index("db", "coll", [("a", "text")], unique=True)
            with self.assertRaisesRegex(OperationFailure, "Conflicting index definition for '_id_'"):
                await engine.create_index("db", "coll", ["_id"], unique=False)
            with self.assertRaisesRegex(OperationFailure, "Conflicting index definition for '_id_'"):
                await engine.create_index("db", "coll", ["name"], name="_id_")

            with self.assertRaisesRegex(CollectionInvalid, "does not exist"):
                await engine.collection_options("db", "missing")
            with self.assertRaisesRegex(CollectionInvalid, "must differ"):
                await engine.rename_collection("db", "same", "same")

            await engine.create_collection("db", "coll")
            await engine.create_collection("db", "other")
            with self.assertRaisesRegex(CollectionInvalid, "already exists"):
                await engine.rename_collection("db", "coll", "other")
        finally:
            await engine.disconnect()

    async def test_sqlite_search_admin_and_namespace_error_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.drop_search_index("db", "coll", "missing")
            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.update_search_index(
                    "db",
                    "coll",
                    "missing",
                    SearchIndexDefinition({"mappings": {"dynamic": True}}, name="missing"),
                )

            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                    name="vec",
                    index_type="vectorSearch",
                ),
            )
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search"),
            )

            with self.assertRaisesRegex(OperationFailure, "does not support \\$search"):
                await engine.search_documents(
                    "db",
                    "coll",
                    "$search",
                    {"index": "vec", "text": {"query": "ada", "path": "title"}},
                )
            with self.assertRaisesRegex(OperationFailure, "does not support \\$vectorSearch"):
                await engine.search_documents(
                    "db",
                    "coll",
                    "$vectorSearch",
                    {"index": "text", "path": "embedding", "queryVector": [0.1, 0.2], "numCandidates": 5, "limit": 5},
                )
        finally:
            await engine.disconnect()

    async def test_sqlite_load_indexes_covers_legacy_row_shapes_and_invalid_metadata_variants(self):
        engine = SQLiteEngine()
        cursor = Mock()
        connection = Mock()
        connection.execute.return_value = cursor
        engine._connection = connection

        cursor.fetchall.return_value = [
            ("legacy10", "idx_legacy10", '["email"]', '[["email", 1]]', 0, 0, None, 0, None, None),
        ]
        indexes = engine._load_indexes("db", "coll")
        self.assertTrue(any(index["name"] == "legacy10" for index in indexes))
        self.assertEqual(indexes[0]["expire_after_seconds"], 0)

        cursor.fetchall.return_value = [
            ("bad_fields", "idx_bad_fields", '["email", 1]', '[["email", 1]]', 0, 0, None, None, 0, None, None),
        ]
        engine._mark_index_metadata_changed("db", "coll")
        with self.assertRaises(OperationFailure):
            engine._load_indexes("db", "coll")

        cursor.fetchall.return_value = [
            ("bad_keys", "idx_bad_keys", '["email"]', '[["email", 2]]', 0, 0, None, None, 0, None, None),
        ]
        engine._mark_index_metadata_changed("db", "coll")
        with self.assertRaises(OperationFailure):
            engine._load_indexes("db", "coll")

    async def test_sqlite_search_sync_and_explain_details_cover_empty_and_fallback_shapes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search"),
            )
            self.assertEqual(
                engine._search_documents_sync(
                    "db",
                    "coll",
                    "$search",
                    {"index": "text", "text": {"query": "ada", "path": "title"}},
                    None,
                    None,
                ),
                [],
            )

            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada Lovelace"})
            docs = engine._search_documents_sync(
                "db",
                "coll",
                "$search",
                {"index": "text", "phrase": {"query": "Ada Lovelace", "path": "title"}},
                None,
                None,
            )
            self.assertEqual([doc["_id"] for doc in docs], ["1"])

            with patch("mongoeco.engines.sqlite.describe_virtual_index_usage", return_value={"virtual": True}):
                explanation = await engine.explain_find_semantics(
                    "db",
                    "coll",
                    compile_find_semantics({"title": "Ada"}),
                )
        finally:
            await engine.disconnect()

        self.assertTrue(explanation.details["virtual"])
        self.assertIn("engine_details", explanation.details)

    async def test_sqlite_session_commit_require_connection_and_bound_connection_paths(self):
        engine = SQLiteEngine()
        session = ClientSession()
        other = ClientSession()
        engine.create_session_state(session)
        engine.create_session_state(other)

        await engine.connect()
        try:
            engine._start_session_transaction(session)
            with self.assertRaisesRegex(InvalidOperation, "active transaction bound to another session"):
                engine._require_connection(other)

            previous_version = engine._mvcc_version
            engine._commit_session_transaction(session)
            self.assertIsNone(engine._transaction_owner_session_id)
            self.assertFalse(session.in_transaction)
            self.assertEqual(engine._mvcc_version, previous_version + 1)

            bound = object()
            with engine._bind_connection(bound):
                self.assertIs(engine._require_connection(), bound)
        finally:
            await engine.disconnect()

        with self.assertRaisesRegex(InvalidOperation, "SQLiteEngine is not connected"):
            engine._commit_session_transaction(session)

    def test_sqlite_misc_helpers_cover_record_metadata_and_decode_variants(self):
        class LegacyCodec:
            @staticmethod
            def decode(payload):
                return {"decoded": payload}

        engine = SQLiteEngine(codec=LegacyCodec)
        session = ClientSession()
        engine.create_session_state(session)

        engine._record_operation_metadata(
            None,
            operation="find",
            comment="trace",
            max_time_ms=5,
            hint="_id_",
        )
        engine._record_operation_metadata(
            session,
            operation="find",
            comment="trace",
            max_time_ms=5,
            hint="_id_",
        )

        state = session.get_engine_state(engine._engine_key())
        self.assertEqual(state["last_operation"]["operation"], "find")
        self.assertEqual(engine._multikey_type_score("undefined"), 0)

        with patch("mongoeco.engines.sqlite.inspect.signature", side_effect=ValueError("no-signature")):
            self.assertEqual(
                engine._decode_codec_payload({"_id": "1"}, preserve_bson_wrappers=False),
                {"decoded": {"_id": "1"}},
            )

    async def test_sqlite_search_backend_helpers_cover_pending_drop_load_and_create_paths(self):
        engine = SQLiteEngine(simulate_search_index_latency=0.01)
        definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search")
        vector_definition = SearchIndexDefinition(
            {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
            name="vec",
            index_type="vectorSearch",
        )

        self.assertIsNotNone(engine._pending_search_index_ready_at())
        self.assertFalse(engine._search_index_is_ready_sync(time.time() + 60))
        self.assertTrue(engine._search_index_is_ready_sync(time.time() - 1))

        await engine.connect()
        try:
            conn = engine._require_connection()
            self.assertEqual(
                engine._ensure_search_backend_sync(conn, "db", "coll", vector_definition, None),
                engine._physical_search_index_name("db", "coll", "vec"),
            )

            with patch.object(engine, "_supports_fts5", return_value=False):
                resolved = engine._ensure_search_backend_sync(conn, "db", "coll", definition, None)
            self.assertEqual(resolved, engine._physical_search_index_name("db", "coll", "text"))

            with patch.object(engine, "_load_search_index_rows", return_value=[(definition, resolved, None)]):
                self.assertEqual(engine._load_search_indexes("db", "coll"), [definition])

            engine._drop_search_backend_sync(conn, None)

            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada Lovelace"})
            physical_name = engine._physical_search_index_name("db", "coll", "text")
            created = engine._ensure_search_backend_sync(conn, "db", "coll", definition, physical_name)
            self.assertEqual(created, physical_name)
            self.assertIn(physical_name, engine._ensured_search_backends)
            self.assertFalse(conn.in_transaction)
            self.assertGreater(
                conn.execute(f"SELECT COUNT(*) FROM {engine._quote_identifier(physical_name)}").fetchone()[0],
                0,
            )

            second = engine._ensure_search_backend_sync(conn, "db", "coll", definition, physical_name)
            self.assertEqual(second, physical_name)

            engine._drop_search_backend_sync(conn, physical_name)
            self.assertNotIn(physical_name, engine._ensured_search_backends)
            self.assertFalse(engine._sqlite_table_exists(conn, physical_name))
        finally:
            await engine.disconnect()

    async def test_sqlite_explain_find_semantics_covers_hybrid_and_python_fallback_detail_shapes(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "profile": {"rank": 2}, "title": "Ada"})
            await engine.put_document("db", "coll", {"_id": "2", "profile": {"rank": 1}, "title": "Bob"})
            await engine.create_index("db", "coll", [("title", 1)])

            sql = await self._explain(engine, "db", "coll", {"title": "Ada"})
            self.assertEqual(sql.details["pushdown"]["mode"], "sql")
            self.assertTrue(sql.details["pushdown"]["usesSqlRuntime"])
            self.assertFalse(sql.details["pushdown"]["pythonSort"])
            self.assertIn("engine_details", sql.details)
            self.assertEqual(sql.planning_issues, ())

            hybrid = await self._explain(engine, "db", "coll", sort=[("profile", 1)])
            self.assertEqual(hybrid.details["fallback_reason"], "Sort requires Python fallback")
            self.assertEqual(hybrid.details["pushdown"]["mode"], "hybrid")
            self.assertTrue(hybrid.details["pushdown"]["usesSqlRuntime"])
            self.assertTrue(hybrid.details["pushdown"]["pythonSort"])
            self.assertIn("engine_details", hybrid.details)
            self.assertTrue(any(hint["operator"] == "sort" for hint in hybrid.details["pushdown_hints"]))
            self.assertEqual(hybrid.planning_issues[0].scope, "engine")
            self.assertEqual(hybrid.planning_issues[0].message, "Sort requires Python fallback")

            with patch("mongoeco.engines.sqlite.describe_virtual_index_usage", return_value={"virtual": True}):
                python_fallback = await self._explain(
                    engine,
                    "db",
                    "coll",
                    {"title": "Ada"},
                    collation={"locale": "en", "strength": 2},
                )
        finally:
            await engine.disconnect()

        self.assertEqual(python_fallback.details["fallback_reason"], "Collation requires Python fallback")
        self.assertEqual(python_fallback.details["pushdown"]["mode"], "python")
        self.assertFalse(python_fallback.details["pushdown"]["usesSqlRuntime"])
        self.assertTrue(any(hint["operator"] == "collation" for hint in python_fallback.details["pushdown_hints"]))
        self.assertEqual(python_fallback.planning_issues[0].scope, "engine")
        self.assertEqual(python_fallback.planning_issues[0].message, "Collation requires Python fallback")
        self.assertTrue(python_fallback.details["virtual"])

    async def test_sqlite_explain_find_semantics_covers_non_dict_detail_merges(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada"})
            await engine.create_index("db", "coll", [("title", "text")], name="title_text")
            semantics = compile_find_semantics({"title": "Ada"})
            with patch("mongoeco.engines.sqlite.sqlite_pushdown_details", return_value="legacy"), patch(
                "mongoeco.engines.sqlite.describe_virtual_index_usage",
                return_value={"virtual": True},
            ):
                explanation = await engine.explain_find_semantics("db", "coll", semantics)
            with patch("mongoeco.engines.sqlite.sqlite_pushdown_details", return_value="legacy"):
                text_explanation = await engine.explain_find_semantics(
                    "db",
                    "coll",
                    compile_find_semantics(text_query=compile_classic_text_query({"$search": "Ada"})),
                )
        finally:
            await engine.disconnect()

        self.assertEqual(explanation.details["pushdown"], "legacy")
        self.assertIsInstance(explanation.details["engine_details"], list)
        self.assertTrue(explanation.details["virtual"])
        self.assertIn("textQuery", text_explanation.details)

    async def test_explain_elem_match_filter_reports_operator_pushdown_hint(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "items": [{"score": 7}]})
            explanation = await self._explain(engine, "db", "coll", {"items": {"$elemMatch": {"score": {"$gt": 5}}}})
        finally:
            await engine.disconnect()

        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["fallback_reason"], "Only scalar $elemMatch shapes are translated to SQL")
        self.assertTrue(any(hint["operator"] == "$elemMatch" for hint in explanation.details["pushdown_hints"]))

    async def test_explain_array_comparison_reports_range_and_array_pushdown_hints(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "value": ["x", 7]})
            explanation = await self._explain(engine, "db", "coll", {"value": {"$gt": 5}})
        finally:
            await engine.disconnect()

        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["fallback_reason"], "Top-level array comparisons require Python fallback")
        self.assertTrue(any(hint["operator"] == "range-comparison" for hint in explanation.details["pushdown_hints"]))
        self.assertTrue(any(hint["operator"] == "array-comparison" for hint in explanation.details["pushdown_hints"]))

    async def test_explain_geo_filter_reports_geo_pushdown_hints(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document(
                "db",
                "coll",
                {"_id": "1", "location": {"type": "Point", "coordinates": [0, 0]}},
            )
            explanation = await self._explain(
                engine,
                "db",
                "coll",
                {
                    "location": {
                        "$geoWithin": {
                            "$geometry": {
                                "type": "Polygon",
                                "coordinates": [[[-1, -1], [1, -1], [1, 1], [-1, 1], [-1, -1]]],
                            }
                        }
                    }
                },
            )
        finally:
            await engine.disconnect()

        self.assertEqual(explanation.strategy, "python")
        self.assertEqual(explanation.details["fallback_reason"], "Geospatial operators require Python query fallback")
        self.assertTrue(any(hint["operator"] == "$geoWithin" for hint in explanation.details["pushdown_hints"]))
        self.assertTrue(any(hint["operator"] == "geo-runtime" for hint in explanation.details["pushdown_hints"]))

    async def test_sqlite_runtime_diagnostics_surface_planner_search_and_cache_state(self):
        engine = SQLiteEngine(simulate_search_index_latency=60.0)
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "title": "Ada"})
            await engine.create_index("db", "coll", [("title", 1)], name="title_idx")
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search"),
            )
            runtime = engine._runtime_diagnostics_info()
        finally:
            await engine.disconnect()

        self.assertEqual(runtime["planner"]["engine"], "sqlite")
        self.assertEqual(runtime["planner"]["pushdownModes"], ["sql", "hybrid", "python"])
        self.assertTrue(runtime["planner"]["hybridSortFallback"])
        self.assertEqual(runtime["search"]["backend"], "fts5-or-usearch-or-python-fallback")
        self.assertEqual(runtime["search"]["declaredIndexCount"], 1)
        self.assertEqual(runtime["search"]["pendingIndexCount"], 1)
        self.assertGreaterEqual(runtime["search"]["ensuredBackendCount"], 0)
        self.assertEqual(runtime["search"]["vectorBackend"], "usearch")
        self.assertEqual(runtime["search"]["declaredVectorIndexCount"], 0)
        self.assertEqual(runtime["search"]["pendingVectorIndexCount"], 0)
        self.assertEqual(runtime["search"]["materializedVectorBackendCount"], 0)
        self.assertGreaterEqual(runtime["caches"]["indexMetadataVersionEntries"], 1)
        self.assertGreaterEqual(runtime["caches"]["collectionFeatureCacheEntries"], 0)
        self.assertGreaterEqual(runtime["caches"]["ensuredMultikeyPhysicalIndexCount"], 0)
        self.assertGreaterEqual(runtime["caches"]["vectorSearchBackendCount"], 0)

    async def test_sqlite_vector_search_uses_usearch_backend_and_reports_runtime_diagnostics(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "embedding": [1.0, 0.0, 0.0], "kind": "keep"})
            await engine.put_document("db", "coll", {"_id": "2", "embedding": [0.9, 0.1, 0.0], "kind": "drop"})
            await engine.put_document("db", "coll", {"_id": "3", "embedding": [0.0, 1.0, 0.0]})
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 3,
                                "similarity": "cosine",
                                "connectivity": 8,
                                "expansionAdd": 16,
                                "expansionSearch": 24,
                            }
                        ]
                    },
                    name="vec",
                    index_type="vectorSearch",
                ),
            )

            results = await engine.search_documents(
                "db",
                "coll",
                "$vectorSearch",
                {
                    "index": "vec",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "limit": 1,
                    "numCandidates": 2,
                    "filter": {"kind": "keep"},
                },
            )
            explanation = await engine.explain_search_documents(
                "db",
                "coll",
                "$vectorSearch",
                {
                    "index": "vec",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0, 0.0],
                    "limit": 1,
                    "numCandidates": 2,
                    "filter": {"kind": "keep"},
                },
            )
            runtime = engine._runtime_diagnostics_info()
        finally:
            await engine.disconnect()

        self.assertEqual([document["_id"] for document in results], ["1"])
        self.assertEqual(explanation.details["backend"], "usearch")
        self.assertEqual(explanation.details["mode"], "ann")
        self.assertTrue(explanation.details["backendMaterialized"])
        self.assertEqual(explanation.details["filterMode"], "post-candidate")
        self.assertEqual(explanation.details["exactFallbackReason"], None)
        self.assertEqual(explanation.details["vectorBackend"]["backend"], "usearch")
        self.assertEqual(explanation.details["vectorBackend"]["connectivity"], 8)
        self.assertEqual(explanation.details["vectorBackend"]["expansionAdd"], 16)
        self.assertEqual(explanation.details["vectorBackend"]["expansionSearch"], 24)
        self.assertEqual(runtime["search"]["declaredVectorIndexCount"], 1)
        self.assertEqual(runtime["search"]["materializedVectorBackendCount"], 1)
        self.assertEqual(runtime["caches"]["vectorSearchBackendCount"], 1)

    async def test_sqlite_vector_search_exact_fallback_and_explain_edge_paths(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("db", "coll", {"_id": "1", "embedding": [1.0, 0.0], "kind": "keep"})
            await engine.put_document("db", "coll", {"_id": "2", "embedding": [0.9, 0.1], "kind": "drop"})
            await engine.put_document("db", "coll", {"_id": "3", "embedding": ["bad", 1.0], "kind": "keep"})
            await engine.create_search_index(
                "db",
                "coll",
                SearchIndexDefinition(
                    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
                    name="vec",
                    index_type="vectorSearch",
                ),
            )

            rows = engine._load_search_index_rows("db", "coll", name="vec")
            definition, _physical_name, _ready_at_epoch = rows[0]
            exact_hits = engine._exact_vector_hits_sync(
                "db",
                "coll",
                definition,
                search_module.compile_vector_search_query(  # type: ignore[name-defined]
                    {
                        "index": "vec",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 2,
                        "numCandidates": 2,
                        "filter": {"kind": "keep"},
                    }
                ),
            )
            self.assertEqual([document["_id"] for _score, document in exact_hits], ["1"])

            with patch(
                "mongoeco.engines._sqlite_search_runtime.search_sqlite_vector_backend",
                return_value=[(engine._storage_key("missing"), 0.1), (engine._storage_key("2"), 0.2)],
            ):
                fallback_docs = await engine.search_documents(
                    "db",
                    "coll",
                    "$vectorSearch",
                    {
                        "index": "vec",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 1,
                        "numCandidates": 2,
                        "filter": {"kind": "keep"},
                    },
                )
            self.assertEqual([document["_id"] for document in fallback_docs], ["1"])

            with patch(
                "mongoeco.engines._sqlite_search_runtime.search_sqlite_vector_backend",
                return_value=[(engine._storage_key("2"), 0.2)],
            ):
                explanation = await engine.explain_search_documents(
                    "db",
                    "coll",
                    "$vectorSearch",
                    {
                        "index": "vec",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 1,
                        "numCandidates": 1,
                        "filter": {"kind": "keep"},
                    },
                )
            self.assertEqual(explanation.details["exactFallbackReason"], "post-filter-underflow")
            self.assertEqual(explanation.details["documentsFiltered"], 1)

            with self.assertRaisesRegex(OperationFailure, "search index not found"):
                await engine.explain_search_documents(
                    "db",
                    "coll",
                    "$vectorSearch",
                    {
                        "index": "missing",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "limit": 1,
                        "numCandidates": 1,
                    },
                )
        finally:
            await engine.disconnect()

    async def test_sqlite_drop_database_clears_runtime_state_and_profiler(self):
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.put_document("alpha", "users", {"_id": "1", "email": "a@example.com"})
            await engine.create_index("alpha", "users", ["email"], name="idx_email")
            await engine.create_search_index(
                "alpha",
                "users",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="text", index_type="search"),
            )
            engine._profiler.set_level("alpha", 2)
            engine._record_profile_event(
                "alpha",
                op="query",
                command={"find": "users"},
                duration_micros=1000,
            )
            engine._lookup_collection_id(engine._require_connection(), "alpha", "users")
            engine._field_traverses_array_in_collection("alpha", "users", "items.name")

            with patch.object(engine, "_drop_search_backend_sync", wraps=engine._drop_search_backend_sync) as drop_search_backend:
                await engine.drop_database("alpha")

            self.assertEqual(engine._profiler.count_entries("alpha"), 0)
            self.assertFalse(any(key[0] == "alpha" for key in engine._collection_id_cache))
            self.assertFalse(any(key[0] == "alpha" for key in engine._index_metadata_versions))
            self.assertFalse(any(key[0] == "alpha" for key in engine._index_cache))
            self.assertFalse(any(key[0] == "alpha" for key in engine._collection_features_cache))
            self.assertGreaterEqual(drop_search_backend.call_count, 1)
            self.assertEqual(
                engine._require_connection().execute(
                    "SELECT COUNT(*) FROM collections WHERE db_name = ?",
                    ("alpha",),
                ).fetchone()[0],
                0,
            )
        finally:
            await engine.disconnect()

    async def test_update_with_operation_requires_compiled_update_plan(self):
        engine = SQLiteEngine()

        with self.assertRaisesRegex(OperationFailure, "compiled update plan"):
            await engine.update_with_operation(
                "db",
                "coll",
                Mock(compiled_update_plan=None, compiled_upsert_plan=None),
            )
