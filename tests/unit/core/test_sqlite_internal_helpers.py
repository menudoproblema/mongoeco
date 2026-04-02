import asyncio
import ast
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import Mock, patch

from mongoeco.engines import _sqlite_admin_runtime as admin_runtime_module
from mongoeco.engines import _sqlite_index_admin as index_admin
from mongoeco.engines import _sqlite_namespace_admin as namespace_admin
from mongoeco.engines import _sqlite_search_admin as search_admin
from mongoeco.engines import _sqlite_write_ops as write_ops
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.engines.profiling import EngineProfiler
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.types import EngineIndexRecord, SearchIndexDefinition
from mongoeco.compat import MONGODB_DIALECT_70


class SQLiteInternalHelperTests(unittest.TestCase):
    def test_sqlite_engine_module_keeps_admin_runtime_boundary(self):
        module_path = Path(__file__).resolve().parents[3] / "src" / "mongoeco" / "engines" / "sqlite.py"
        tree = ast.parse(module_path.read_text(encoding="utf-8"))
        imported_modules = {
            node.module
            for node in tree.body
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }

        self.assertIn("mongoeco.engines._sqlite_admin_runtime", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_explain_contract", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_search_backend", imported_modules)
        self.assertIn("mongoeco.engines._sqlite_session_runtime", imported_modules)

    def test_sqlite_profile_namespace_paths_delegate_to_admin_runtime(self):
        engine = SQLiteEngine()
        with patch.object(
            engine._admin_runtime,
            "profile_namespace_document",
            return_value={"_id": "p1", "op": "query"},
        ) as doc_mock:
            document = engine._get_document_sync(
                "db",
                "system.profile",
                "p1",
                projection={"op": 1, "_id": 0},
            )
        self.assertEqual(document, {"_id": "p1", "op": "query"})
        doc_mock.assert_called_once()

        with patch.object(
            engine._admin_runtime,
            "profile_namespace_documents",
            return_value=[{"_id": "p1", "seq": 1}],
        ) as list_mock:
            self.assertEqual(engine._profile_documents("db"), [{"_id": "p1", "seq": 1}])
        list_mock.assert_called_once_with("db")

    def test_sqlite_admin_runtime_covers_collection_existence_and_options_helpers(self):
        conn = self._connection()
        try:
            class _EngineStub:
                _PROFILE_COLLECTION_NAME = "system.profile"

                def __init__(self, connection):
                    self._connection = connection
                    self._profiler = EngineProfiler("sqlite")

            runtime = admin_runtime_module.SQLiteAdminRuntime(_EngineStub(conn))

            self.assertFalse(runtime.collection_exists(conn, "db", "coll"))

            conn.execute(
                "INSERT INTO collections (db_name, coll_name, options_json) VALUES (?, ?, ?)",
                ("db", "coll", '{"capped": true}'),
            )
            self.assertTrue(runtime.collection_exists(conn, "db", "coll"))
            self.assertEqual(runtime.collection_options_or_empty(conn, "db", "coll"), {"capped": True})

            conn.execute("DELETE FROM collections WHERE db_name = ? AND coll_name = ?", ("db", "coll"))
            self.assertEqual(runtime.collection_options_or_empty(conn, "db", "coll"), {})

            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "docs_only", "1", '{"_id":"1"}'),
            )
            conn.execute(
                "INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("db", "indexes_only", "ix", "ix_physical", '["name"]', '[["name", 1]]', 0, 0, None, None, 0, None, None),
            )
            conn.execute(
                "INSERT INTO search_indexes (db_name, coll_name, name, index_type, definition_json, physical_name, ready_at_epoch) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("db", "search_only", "search_idx", "search", '{"mappings":{"dynamic":true}}', "fts_idx", 10.0),
            )

            self.assertTrue(runtime.collection_exists(conn, "db", "docs_only"))
            self.assertTrue(runtime.collection_exists(conn, "db", "indexes_only"))
            self.assertTrue(runtime.collection_exists(conn, "db", "search_only"))
        finally:
            conn.close()

    def test_sqlite_admin_runtime_profile_helpers_and_cache_cleanup(self):
        class _EngineStub:
            _PROFILE_COLLECTION_NAME = "system.profile"

            def __init__(self):
                self._profiler = EngineProfiler("sqlite")
                self._index_metadata_versions = {
                    ("db", "coll"): 1,
                    ("db", "other"): 2,
                    ("other", "coll"): 3,
                }

        engine = _EngineStub()
        runtime = admin_runtime_module.SQLiteAdminRuntime(engine)

        self.assertTrue(runtime.is_profile_namespace("system.profile"))
        self.assertFalse(runtime.is_profile_namespace("users"))

        status = runtime.set_profiling_level("db", 2, slow_ms=25)
        self.assertEqual(status.to_document()["level"], 2)
        runtime.record_profile_event(
            "db",
            op="query",
            command={"find": "users"},
            duration_micros=2_500,
            fallback_reason="scan",
            ok=0.0,
            errmsg="boom",
        )
        documents = runtime.profile_namespace_documents("db")
        self.assertEqual(len(documents), 1)
        self.assertEqual(documents[0]["fallbackReason"], "scan")
        self.assertEqual(documents[0]["errmsg"], "boom")
        self.assertEqual(runtime.profile_namespace_document("db", 1), documents[0])
        self.assertEqual(
            runtime.profile_namespace_document(
                "db",
                1,
                projection={"op": 1, "_id": 0},
                dialect=MONGODB_DIALECT_70,
            ),
            {"op": "query"},
        )
        self.assertIsNone(runtime.profile_namespace_document("db", 99))

        runtime.clear_profile_namespace("db")
        self.assertEqual(runtime.profile_namespace_documents("db"), [])

        runtime.clear_index_metadata_versions_for_database("db")
        self.assertEqual(engine._index_metadata_versions, {("other", "coll"): 3})

    def test_sqlite_admin_runtime_delegates_namespace_entrypoints_with_engine_wiring(self):
        conn = self._connection()
        try:
            class _EngineStub:
                _PROFILE_COLLECTION_NAME = "system.profile"

                def __init__(self, connection):
                    self._connection = connection
                    self._profiler = EngineProfiler("sqlite")
                    self._index_metadata_versions = {}
                    self.calls = []

                def _require_connection(self, context):
                    self.calls.append(("require_connection", context))
                    return self._connection

                def _begin_write(self, current, context):
                    self.calls.append(("begin_write", current is self._connection, context))

                def _commit_write(self, current, context):
                    self.calls.append(("commit_write", current is self._connection, context))

                def _rollback_write(self, current, context):
                    self.calls.append(("rollback_write", current is self._connection, context))

                def _quote_identifier(self, value):
                    return f'"{value}"'

                def _drop_search_backend_sync(self, current, physical_name):
                    self.calls.append(("drop_search_backend", current is self._connection, physical_name))

                def _invalidate_index_cache(self):
                    self.calls.append(("invalidate_index_cache",))

                def _invalidate_collection_id_cache(self):
                    self.calls.append(("invalidate_collection_id_cache",))

                def _invalidate_collection_features_cache(self):
                    self.calls.append(("invalidate_collection_features_cache",))

            engine = _EngineStub(conn)
            runtime = admin_runtime_module.SQLiteAdminRuntime(engine)

            with patch.object(
                admin_runtime_module,
                "_sqlite_collection_options",
                return_value={"capped": True},
            ) as collection_options_mock:
                self.assertEqual(runtime.collection_options("db", "coll", context="ctx"), {"capped": True})
            collection_options_mock.assert_called_once()
            self.assertEqual(collection_options_mock.call_args.kwargs["conn"], conn)
            self.assertEqual(collection_options_mock.call_args.kwargs["profile_collection_name"], "system.profile")

            with patch.object(admin_runtime_module, "_sqlite_drop_database") as drop_database_mock:
                runtime.drop_database("db", context="ctx")
            drop_kwargs = drop_database_mock.call_args.kwargs
            self.assertEqual(drop_kwargs["conn"], conn)
            self.assertIs(drop_kwargs["clear_profiler"].__self__, engine._profiler)
            self.assertIs(drop_kwargs["clear_index_metadata_versions"].__self__, runtime)
            drop_kwargs["begin_write"](conn)
            drop_kwargs["commit_write"](conn)
            drop_kwargs["rollback_write"](conn)

            with patch.object(admin_runtime_module, "_sqlite_list_databases", return_value=["db"]) as list_databases_mock:
                self.assertEqual(runtime.list_databases(context="ctx"), ["db"])
            self.assertEqual(list_databases_mock.call_args.kwargs["conn"], conn)
            self.assertIs(list_databases_mock.call_args.kwargs["profiler"], engine._profiler)

            with patch.object(admin_runtime_module, "_sqlite_list_collections", return_value=["coll"]) as list_collections_mock:
                self.assertEqual(runtime.list_collections("db", context="ctx"), ["coll"])
            self.assertEqual(list_collections_mock.call_args.kwargs["conn"], conn)
            self.assertEqual(list_collections_mock.call_args.kwargs["db_name"], "db")
            self.assertEqual(
                list_collections_mock.call_args.kwargs["profile_collection_name"],
                "system.profile",
            )

            self.assertEqual(
                engine.calls,
                [
                    ("require_connection", "ctx"),
                    ("require_connection", "ctx"),
                    ("begin_write", True, "ctx"),
                    ("commit_write", True, "ctx"),
                    ("rollback_write", True, "ctx"),
                    ("require_connection", "ctx"),
                    ("require_connection", "ctx"),
                ],
            )
        finally:
            conn.close()

    def _connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(":memory:")
        conn.execute(
            """
            CREATE TABLE documents (
                db_name TEXT,
                coll_name TEXT,
                storage_key TEXT,
                document TEXT,
                PRIMARY KEY (db_name, coll_name, storage_key)
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE indexes (
                db_name TEXT,
                coll_name TEXT,
                name TEXT,
                physical_name TEXT,
                fields TEXT,
                keys TEXT,
                unique_flag INTEGER,
                sparse_flag INTEGER,
                partial_filter_json TEXT,
                expire_after_seconds INTEGER,
                multikey_flag INTEGER,
                multikey_physical_name TEXT,
                scalar_physical_name TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE collections (
                collection_id INTEGER PRIMARY KEY AUTOINCREMENT,
                db_name TEXT,
                coll_name TEXT,
                options_json TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE search_indexes (
                db_name TEXT,
                coll_name TEXT,
                name TEXT,
                index_type TEXT,
                definition_json TEXT,
                physical_name TEXT,
                ready_at_epoch REAL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE scalar_index_entries (
                collection_id INTEGER,
                index_name TEXT,
                storage_key TEXT,
                element_type TEXT,
                type_score INTEGER,
                element_key TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE multikey_entries (
                collection_id INTEGER,
                index_name TEXT,
                storage_key TEXT,
                element_type TEXT,
                type_score INTEGER,
                element_key TEXT
            )
            """
        )
        return conn

    def test_sqlite_index_admin_helpers_cover_builtin_id_and_rollback_paths(self):
        self.assertEqual(index_admin.list_index_documents([])[0]["name"], "_id_")
        self.assertIn("_id_", index_admin.build_index_information([]))

        conn = self._connection()
        try:
            name = index_admin.create_index(
                conn,
                db_name="db",
                coll_name="coll",
                keys=[("_id", 1)],
                unique=True,
                name=None,
                sparse=False,
                hidden=False,
                partial_filter_expression=None,
                expire_after_seconds=None,
                deadline=None,
                enforce_deadline_fn=lambda _deadline: None,
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                purge_expired_documents=lambda *_args: None,
                mark_index_metadata_changed=lambda *_args: None,
                invalidate_collection_features_cache=lambda *_args: None,
                load_indexes=lambda *_args: [],
                field_traverses_array_in_collection=lambda *_args: False,
                supports_multikey_index=lambda *_args: False,
                physical_index_name=lambda *_args: "physical_idx",
                physical_multikey_index_name=lambda *_args: "multikey_idx",
                physical_scalar_index_name=lambda *_args: "scalar_idx",
                is_builtin_id_index=lambda keys: keys == [("_id", 1)],
                replace_multikey_entries_for_document=lambda *_args: None,
                replace_scalar_entries_for_document=lambda *_args: None,
                load_documents=lambda *_args: [],
                quote_identifier=lambda name: f'"{name}"',
            )
            self.assertEqual(name, "_id_")

            with self.assertRaisesRegex(OperationFailure, "index not found with name"):
                index_admin.drop_index(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    index_or_name="missing",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    load_indexes=lambda *_args: [],
                    lookup_collection_id=lambda *_args: 1,
                    quote_identifier=lambda value: f'"{value}"',
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                    is_builtin_id_index=lambda _keys: False,
                )

            with self.assertRaisesRegex(OperationFailure, "index not found with key pattern"):
                index_admin.drop_index(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    index_or_name=[("email", 1)],
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    load_indexes=lambda *_args: [],
                    lookup_collection_id=lambda *_args: 1,
                    quote_identifier=lambda value: f'"{value}"',
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                    is_builtin_id_index=lambda _keys: False,
                )

            target = EngineIndexRecord(
                name="email_1",
                fields=["email"],
                key=[("email", 1)],
                unique=False,
                physical_name="idx_email_1",
            )
            with self.assertRaises(RuntimeError):
                index_admin.drop_all_indexes(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
                    rollback_write=Mock(),
                    load_indexes=lambda *_args: [target],
                    lookup_collection_id=lambda *_args: 1,
                    quote_identifier=lambda value: f'"{value}"',
                    mark_index_metadata_changed=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
        finally:
            conn.close()

    def test_sqlite_search_admin_helpers_cover_documents_and_conflicts(self):
        conn = self._connection()
        try:
            definition = SearchIndexDefinition({"mappings": {"dynamic": True}}, name="search_idx")
            backend_calls = []
            begin = Mock()
            commit = Mock()
            rollback = Mock()

            name = search_admin.create_search_index(
                conn,
                db_name="db",
                coll_name="coll",
                definition=definition,
                deadline=None,
                begin_write=lambda current: begin(current),
                ensure_collection_row=lambda current, db_name, coll_name: current.execute(
                    "INSERT INTO collections (db_name, coll_name, options_json) VALUES (?, ?, ?)",
                    (db_name, coll_name, "{}"),
                ),
                commit_write=lambda current: commit(current),
                rollback_write=lambda current: rollback(current),
                ensure_search_backend=lambda *_args: backend_calls.append("ensure"),
                physical_search_index_name=lambda *_args: "fts_idx",
                pending_ready_at=lambda: 42.0,
            )
            self.assertEqual(name, "search_idx")
            self.assertEqual(backend_calls, ["ensure"])

            rows = conn.execute(
                "SELECT index_type, definition_json, physical_name, ready_at_epoch FROM search_indexes"
            ).fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], "search")
            self.assertEqual(rows[0][2], "fts_idx")

            documents = search_admin.list_search_index_documents(
                [(definition, "fts_idx", 42.0)],
                is_ready=lambda ready_at: ready_at is not None and ready_at <= 42.0,
                name="search_idx",
            )
            self.assertEqual(len(documents), 1)
            self.assertEqual(documents[0]["name"], "search_idx")
            self.assertTrue(documents[0]["queryable"])

            same_name = search_admin.create_search_index(
                conn,
                db_name="db",
                coll_name="coll",
                definition=definition,
                deadline=None,
                begin_write=lambda _conn: None,
                ensure_collection_row=lambda *_args: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                ensure_search_backend=lambda *_args: (_ for _ in ()).throw(AssertionError("unexpected backend rebuild")),
                physical_search_index_name=lambda *_args: "fts_idx",
                pending_ready_at=lambda: 84.0,
            )
            self.assertEqual(same_name, "search_idx")

            with self.assertRaisesRegex(OperationFailure, "Conflicting search index definition"):
                search_admin.create_search_index(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    definition=SearchIndexDefinition({"mappings": {"dynamic": False}}, name="search_idx"),
                    deadline=None,
                    begin_write=lambda _conn: None,
                    ensure_collection_row=lambda *_args: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda current: rollback(current),
                    ensure_search_backend=lambda *_args: None,
                    physical_search_index_name=lambda *_args: "fts_idx",
                    pending_ready_at=lambda: 84.0,
                )
            self.assertTrue(rollback.called)
        finally:
            conn.close()

    def test_sqlite_namespace_admin_helpers_update_catalog_and_invalidate_runtime(self):
        conn = self._connection()
        try:
            def _collection_exists(current, db_name, coll_name):
                row = current.execute(
                    "SELECT 1 FROM collections WHERE db_name = ? AND coll_name = ?",
                    (db_name, coll_name),
                ).fetchone()
                return row is not None

            def _ensure_collection_row(current, db_name, coll_name, options=None):
                current.execute(
                    "INSERT INTO collections (db_name, coll_name, options_json) VALUES (?, ?, ?)",
                    (db_name, coll_name, "{}" if options is None else str(options)),
                )

            namespace_admin.create_collection(
                conn=conn,
                db_name="db",
                coll_name="coll",
                options={"capped": True},
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                collection_exists=_collection_exists,
                ensure_collection_row=_ensure_collection_row,
            )

            self.assertEqual(
                namespace_admin.list_databases(
                    conn=conn,
                    profiler=EngineProfiler("sqlite"),
                    list_database_names=lambda current: [
                        row[0] for row in current.execute("SELECT DISTINCT db_name FROM collections").fetchall()
                    ],
                ),
                ["db"],
            )

            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", "1", '{"_id":"1"}'),
            )
            conn.execute(
                "INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("db", "coll", "idx", "idx_physical", '["name"]', '[["name", 1]]', 0, 0, None, None, 0, None, None),
            )
            conn.execute(
                "INSERT INTO search_indexes (db_name, coll_name, name, index_type, definition_json, physical_name, ready_at_epoch) VALUES (?, ?, ?, ?, ?, ?, ?)",
                ("db", "coll", "search_idx", "search", '{"mappings":{"dynamic":true}}', "fts_idx", 10.0),
            )

            invalidate_collection_id_cache = Mock()
            invalidate_collection_features_cache = Mock()
            mark_index_metadata_changed = Mock()
            namespace_admin.rename_collection(
                conn=conn,
                db_name="db",
                coll_name="coll",
                new_name="renamed",
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                collection_exists=_collection_exists,
                mark_index_metadata_changed=mark_index_metadata_changed,
                invalidate_collection_id_cache=invalidate_collection_id_cache,
                invalidate_collection_features_cache=invalidate_collection_features_cache,
            )

            self.assertTrue(_collection_exists(conn, "db", "renamed"))
            self.assertFalse(_collection_exists(conn, "db", "coll"))
            self.assertEqual(mark_index_metadata_changed.call_count, 2)
            self.assertEqual(invalidate_collection_id_cache.call_count, 2)
            self.assertEqual(invalidate_collection_features_cache.call_count, 2)

            clear_versions = Mock()
            invalidate_index_cache = Mock()
            invalidate_collection_id_cache = Mock()
            invalidate_collection_features_cache = Mock()
            clear_profiler = Mock()
            dropped_backends = []

            namespace_admin.drop_database(
                conn=conn,
                db_name="db",
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda _conn: None,
                quote_identifier=lambda value: f'"{value}"',
                drop_search_backend=lambda _conn, physical_name: dropped_backends.append(physical_name),
                clear_index_metadata_versions=clear_versions,
                invalidate_index_cache=invalidate_index_cache,
                invalidate_collection_id_cache=invalidate_collection_id_cache,
                invalidate_collection_features_cache=invalidate_collection_features_cache,
                clear_profiler=clear_profiler,
            )

            self.assertEqual(dropped_backends, ["fts_idx"])
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM collections WHERE db_name = 'db'").fetchone()[0], 0)
            clear_versions.assert_called_once_with("db")
            invalidate_index_cache.assert_called_once_with()
            invalidate_collection_id_cache.assert_called_once_with()
            invalidate_collection_features_cache.assert_called_once_with()
            clear_profiler.assert_called_once_with("db")
        finally:
            conn.close()

    def test_sqlite_engine_delegates_namespace_admin_runtime_boundaries(self):
        engine = SQLiteEngine()
        engine._connection = sqlite3.connect(":memory:")
        try:
            class StubAdminRuntime:
                def __init__(self):
                    self.calls = []

                def collection_options(self, db_name, coll_name, *, context=None):
                    self.calls.append(("collection_options", db_name, coll_name, context))
                    return {"capped": True}

                def drop_database(self, db_name, *, context=None):
                    self.calls.append(("drop_database", db_name, context))

                def list_databases(self, *, context=None):
                    self.calls.append(("list_databases", context))
                    return ["db"]

                def list_collections(self, db_name, *, context=None):
                    self.calls.append(("list_collections", db_name, context))
                    return ["coll"]

                def record_profile_event(self, db_name, **kwargs):
                    self.calls.append(("record_profile_event", db_name, kwargs["op"]))

                def is_profile_namespace(self, coll_name):
                    self.calls.append(("is_profile_namespace", coll_name))
                    return coll_name == "system.profile"

                def profile_document(self, db_name, profile_id):
                    self.calls.append(("profile_document", db_name, profile_id))
                    return {"_id": profile_id, "ok": 1}

                def clear_profile_namespace(self, db_name):
                    self.calls.append(("clear_profile_namespace", db_name))

                def set_profiling_level(self, db_name, level, *, slow_ms=None):
                    self.calls.append(("set_profiling_level", db_name, level, slow_ms))
                    return {"was": 0, "slowms": slow_ms}

            stub = StubAdminRuntime()
            engine._admin_runtime = stub

            self.assertEqual(engine._collection_options_sync("db", "coll"), {"capped": True})
            engine._drop_database_sync("db")
            self.assertEqual(engine._list_databases_sync(), ["db"])
            self.assertEqual(engine._list_collections_sync("db"), ["coll"])
            engine._record_profile_event("db", op="find", command={"find": "coll"}, duration_micros=10)
            self.assertTrue(engine._is_profile_namespace("system.profile"))
            self.assertEqual(
                engine._get_document_sync("db", "system.profile", 7, projection=None),
                {"_id": 7, "ok": 1},
            )
            engine._drop_collection_sync("db", "system.profile")
            self.assertEqual(
                asyncio.run(engine.set_profiling_level("db", 1, slow_ms=25)),
                {"was": 0, "slowms": 25},
            )
            self.assertEqual(
                stub.calls,
                [
                    ("collection_options", "db", "coll", None),
                    ("drop_database", "db", None),
                    ("list_databases", None),
                    ("list_collections", "db", None),
                    ("record_profile_event", "db", "find"),
                    ("is_profile_namespace", "system.profile"),
                    ("is_profile_namespace", "system.profile"),
                    ("profile_document", "db", 7),
                    ("is_profile_namespace", "system.profile"),
                    ("clear_profile_namespace", "db"),
                    ("set_profiling_level", "db", 1, 25),
                ],
            )
        finally:
            engine._connection.close()

    def test_sqlite_write_helpers_cover_duplicate_snapshot_and_rollback_paths(self):
        conn = self._connection()
        try:
            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", "1", '{"_id": "1"}'),
            )
            with self.assertRaises(DuplicateKeyError):
                write_ops.put_document(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    document={"_id": "1"},
                    overwrite=False,
                    bypass_document_validation=False,
                    storage_key="1",
                    serialized_document='{"_id":"1"}',
                    purge_expired_documents=lambda *_args: None,
                    begin_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    commit_write=lambda _conn: None,
                    collection_options_or_empty=lambda *_args: {},
                    load_existing_document_for_storage_key=lambda *_args: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    load_indexes=lambda *_args: [],
                    rebuild_multikey_entries_for_document=lambda *_args: None,
                    supports_scalar_index=lambda _idx: False,
                    rebuild_scalar_entries_for_document=lambda *_args: None,
                    load_search_index_rows=lambda *_args: [],
                    replace_search_entries_for_document=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )

            with patch("mongoeco.engines._sqlite_write_ops.enforce_collection_document_validation") as validate_doc:
                results = write_ops.put_documents_bulk(
                    conn,
                    db_name="db",
                    coll_name="bulk",
                    documents=[{"_id": "1"}, {"_id": "1"}],
                    prepared_documents=[
                        ("1", '{"_id":"1"}', []),
                        ("1", '{"_id":"1"}', []),
                    ],
                    snapshot_indexes=[],
                    bypass_document_validation=False,
                    snapshot_options={"validator": {"x": 1}},
                    purge_expired_documents=lambda *_args: None,
                    collection_options_or_empty=lambda *_args: {},
                    load_indexes=lambda *_args: [],
                    load_search_index_rows=lambda *_args: [],
                    begin_write=lambda _conn: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    lookup_collection_id=lambda *_args: 1,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    delete_multikey_entries_for_storage_key=lambda *_args: None,
                    delete_scalar_entries_for_storage_key=lambda *_args: None,
                    build_multikey_rows_for_document=lambda *_args: [],
                    ensure_multikey_physical_indexes=lambda *_args: None,
                    build_scalar_rows_for_document=lambda *_args: [],
                    ensure_scalar_physical_indexes=lambda *_args: None,
                    replace_search_entries_for_document=lambda *_args: None,
                    commit_write=lambda _conn: None,
                    rollback_write=lambda _conn: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
                self.assertEqual(results, [True, False])
                self.assertEqual(validate_doc.call_count, 2)

            rollback = Mock()
            with self.assertRaises(RuntimeError):
                write_ops.put_documents_bulk(
                    conn,
                    db_name="db",
                    coll_name="bulk2",
                    documents=[{"_id": "2"}],
                    prepared_documents=[("2", '{"_id":"2"}', [("idx", "str", 1, "k")])],
                    snapshot_indexes=[],
                    bypass_document_validation=True,
                    snapshot_options=None,
                    purge_expired_documents=lambda *_args: None,
                    collection_options_or_empty=lambda *_args: {},
                    load_indexes=lambda *_args: [EngineIndexRecord(name="idx", fields=["name"], key=[("name", 1)], unique=False, multikey=True)],
                    load_search_index_rows=lambda *_args: [],
                    begin_write=lambda _conn: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    lookup_collection_id=lambda *_args: 1,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    delete_multikey_entries_for_storage_key=lambda *_args: None,
                    delete_scalar_entries_for_storage_key=lambda *_args: None,
                    build_multikey_rows_for_document=lambda *_args: [("idx", "str", 1, "built")],
                    ensure_multikey_physical_indexes=lambda *_args: None,
                    build_scalar_rows_for_document=lambda *_args: [],
                    ensure_scalar_physical_indexes=lambda *_args: None,
                    replace_search_entries_for_document=lambda *_args: (_ for _ in ()).throw(RuntimeError("boom")),
                    commit_write=lambda _conn: None,
                    rollback_write=rollback,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
            rollback.assert_called_once()

            rollback = Mock()
            with self.assertRaises(RuntimeError):
                write_ops.delete_document(
                    conn,
                    db_name="db",
                    coll_name="bulk2",
                    storage_key="2",
                    begin_write=lambda _conn: None,
                    commit_write=lambda _conn: (_ for _ in ()).throw(RuntimeError("boom")),
                    rollback_write=rollback,
                    delete_multikey_entries_for_storage_key=lambda *_args: None,
                    delete_scalar_entries_for_storage_key=lambda *_args: None,
                    delete_search_entries_for_storage_key=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: None,
                )
            rollback.assert_called_once()
        finally:
            conn.close()
