import asyncio
from types import SimpleNamespace
from unittest.mock import Mock
import unittest

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.query_plan import MatchAll
from mongoeco.engines import _sqlite_index_admin as index_admin
from mongoeco.engines import _sqlite_modify_ops as modify_ops
from mongoeco.engines import _sqlite_write_ops as write_ops
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import EngineIndexRecord


class SQLiteSessionRuntimeTests(unittest.TestCase):
    def test_session_runtime_tracks_transaction_ownership_and_bindings(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        try:
            runtime = engine._session_runtime
            session = ClientSession()
            other = ClientSession()

            runtime.create_session_state(session)
            runtime.create_session_state(other)

            state = session.get_engine_context(engine._engine_key())
            self.assertIsNotNone(state)
            self.assertTrue(state.connected)
            self.assertTrue(state.supports_transactions)
            self.assertFalse(state.transaction_active)
            self.assertIn("snapshot_version", state.metadata)

            session.start_transaction()
            self.assertEqual(engine._transaction_owner_session_id, session.session_id)
            self.assertTrue(session.get_engine_context(engine._engine_key()).transaction_active)

            with self.assertRaisesRegex(InvalidOperation, "another session"):
                runtime.require_connection(other)

            conn = runtime.require_connection(session)
            with runtime.bind_connection(conn):
                self.assertIs(runtime.require_connection(None), conn)

            session.commit_transaction()
            self.assertIsNone(engine._transaction_owner_session_id)
            self.assertFalse(session.get_engine_context(engine._engine_key()).transaction_active)
        finally:
            asyncio.run(engine.disconnect())

    def test_failed_session_commit_keeps_owner_until_abort(self):
        engine = SQLiteEngine()
        runtime = engine._session_runtime
        conn = Mock()
        conn.commit.side_effect = RuntimeError("commit boom")
        engine._connection = conn
        session = ClientSession()
        runtime.create_session_state(session)

        session.start_transaction()
        with self.assertRaisesRegex(RuntimeError, "commit boom"):
            session.commit_transaction()

        self.assertTrue(session.in_transaction)
        self.assertEqual(engine._transaction_owner_session_id, session.session_id)
        self.assertTrue(session.get_engine_context(engine._engine_key()).transaction_active)

        session.abort_transaction()

        conn.rollback.assert_called_once_with()
        self.assertFalse(session.in_transaction)
        self.assertIsNone(engine._transaction_owner_session_id)
        self.assertFalse(session.get_engine_context(engine._engine_key()).transaction_active)

    def test_failed_session_abort_keeps_owner_until_retry(self):
        engine = SQLiteEngine()
        runtime = engine._session_runtime
        conn = Mock()
        conn.rollback.side_effect = [RuntimeError("rollback boom"), None]
        engine._connection = conn
        session = ClientSession()
        runtime.create_session_state(session)

        session.start_transaction()
        with self.assertRaisesRegex(RuntimeError, "rollback boom"):
            session.abort_transaction()

        self.assertTrue(session.in_transaction)
        self.assertEqual(engine._transaction_owner_session_id, session.session_id)
        self.assertTrue(session.get_engine_context(engine._engine_key()).transaction_active)

        session.abort_transaction()

        self.assertEqual(conn.rollback.call_count, 2)
        self.assertFalse(session.in_transaction)
        self.assertIsNone(engine._transaction_owner_session_id)
        self.assertFalse(session.get_engine_context(engine._engine_key()).transaction_active)

    def test_failed_close_abort_keeps_session_retryable(self):
        engine = SQLiteEngine()
        runtime = engine._session_runtime
        conn = Mock()
        conn.rollback.side_effect = [RuntimeError("rollback boom"), None]
        engine._connection = conn
        session = ClientSession()
        runtime.create_session_state(session)

        session.start_transaction()
        with self.assertRaisesRegex(RuntimeError, "rollback boom"):
            session.close()

        self.assertTrue(session.active)
        self.assertTrue(session.in_transaction)
        self.assertEqual(engine._transaction_owner_session_id, session.session_id)

        session.abort_transaction()

        self.assertEqual(conn.rollback.call_count, 2)
        self.assertFalse(session.in_transaction)
        self.assertIsNone(engine._transaction_owner_session_id)

    def test_write_helpers_use_savepoints_when_session_owns_transaction(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        try:
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()

            conn = Mock()
            runtime.begin_write(conn, session)
            runtime.commit_write(conn, session)
            runtime.begin_write(conn, session)
            runtime.rollback_write(conn, session)

            self.assertEqual(
                [call.args[0] for call in conn.execute.call_args_list],
                [
                    "SAVEPOINT mongoeco_write_1",
                    "RELEASE SAVEPOINT mongoeco_write_1",
                    "SAVEPOINT mongoeco_write_2",
                    "ROLLBACK TO SAVEPOINT mongoeco_write_2",
                    "RELEASE SAVEPOINT mongoeco_write_2",
                ],
            )
            conn.commit.assert_not_called()
            conn.rollback.assert_not_called()

            runtime.begin_write(conn, None)
            runtime.commit_write(conn, None)
            runtime.rollback_write(conn, None)

            self.assertEqual(
                conn.execute.call_args_list[-1].args[0],
                "BEGIN IMMEDIATE",
            )
            conn.commit.assert_called_once_with()
            conn.rollback.assert_called_once_with()
        finally:
            asyncio.run(engine.disconnect())

    def test_failed_savepoint_rollback_preserves_stack_and_clears_caches(self):
        engine = SQLiteEngine()
        runtime = engine._session_runtime
        engine._connection = Mock()
        session = ClientSession()
        runtime.create_session_state(session)
        session.start_transaction()
        conn = Mock()

        runtime.begin_write(conn, session)
        engine._ensured_multikey_physical_indexes.add("mkidx")
        conn.execute.reset_mock()
        conn.execute.side_effect = RuntimeError("rollback to boom")

        with self.assertRaisesRegex(RuntimeError, "rollback to boom"):
            runtime.rollback_write(conn, session)

        self.assertEqual(runtime._write_savepoint_stack(), ["mongoeco_write_1"])
        self.assertNotIn("mkidx", engine._ensured_multikey_physical_indexes)

        session.abort_transaction()

        self.assertEqual(runtime._write_savepoint_stack(), [])
        self.assertFalse(session.in_transaction)
        self.assertIsNone(engine._transaction_owner_session_id)

    def test_failed_session_write_rolls_back_to_operation_savepoint(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        session: ClientSession | None = None
        try:
            conn = engine._require_connection()
            conn.execute(
                "INSERT INTO documents (db_name, coll_name, storage_key, document) VALUES (?, ?, ?, ?)",
                ("db", "coll", "1", "{'_id': '1', 'kind': 'view'}"),
            )
            conn.commit()
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()

            class _CompiledPlan:
                def apply(self, document, *, variables):
                    del variables
                    document["kind"] = "note"
                    return True

            semantics = SimpleNamespace(
                dialect=MONGODB_DIALECT_70,
                collation=None,
                query_plan=MatchAll(),
                variables={},
                compiled_update_plan=_CompiledPlan(),
                compiled_upsert_plan=SimpleNamespace(
                    apply=lambda _document, **_kwargs: None,
                ),
            )

            with self.assertRaisesRegex(OperationFailure, "boom"):
                modify_ops.update_with_operation(
                    db_name="db",
                    coll_name="coll",
                    operation=SimpleNamespace(array_filters=None),
                    upsert=False,
                    upsert_seed=None,
                    selector_filter=None,
                    dialect=None,
                    bypass_document_validation=True,
                    compile_update_semantics=lambda *_args, **_kwargs: semantics,
                    require_connection=lambda: conn,
                    purge_expired_documents=lambda *_args: None,
                    collection_options_or_empty=lambda *_args: {},
                    dialect_requires_python_fallback=lambda _dialect: False,
                    select_first_document_for_plan=lambda *_args: ("1", {"_id": "1", "kind": "view"}),
                    load_documents=lambda *_args: [],
                    match_plan=lambda *_args: False,
                    enforce_collection_document_validation=lambda *_args, **_kwargs: None,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    load_indexes=lambda *_args: [],
                    load_search_index_rows=lambda *_args: [],
                    begin_write=lambda current: runtime.begin_write(current, session),
                    commit_write=lambda current: runtime.commit_write(current, session),
                    rollback_write=lambda current: runtime.rollback_write(current, session),
                    translate_compiled_update_plan=lambda *_args: (
                        "?",
                        ("{'_id': '1', 'kind': 'note'}",),
                    ),
                    compiled_update_plan_type=_CompiledPlan,
                    rebuild_multikey_entries_for_document=lambda *_args: None,
                    rebuild_scalar_entries_for_document=lambda *_args: None,
                    replace_search_entries_for_document=lambda *_args: (_ for _ in ()).throw(OperationFailure("boom")),
                    serialize_document=lambda document: str(document),
                    storage_key_for_id=lambda value: str(value),
                    new_object_id=lambda: "new-id",
                    invalidate_collection_features_cache=lambda *_args: None,
                )

            self.assertTrue(conn.in_transaction)
            self.assertEqual(
                conn.execute("SELECT document FROM documents").fetchone(),
                ("{'_id': '1', 'kind': 'view'}",),
            )
            session.commit_transaction()
            self.assertEqual(
                conn.execute("SELECT document FROM documents").fetchone(),
                ("{'_id': '1', 'kind': 'view'}",),
            )
        finally:
            if session is not None and engine._transaction_owner_session_id is not None:
                session.abort_transaction()
            asyncio.run(engine.disconnect())

    def test_failed_post_commit_session_callback_still_rolls_back_operation(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        session: ClientSession | None = None
        try:
            conn = engine._require_connection()
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()

            with self.assertRaisesRegex(RuntimeError, "cache boom"):
                write_ops.put_document(
                    conn,
                    db_name="db",
                    coll_name="coll",
                    document={"_id": "1"},
                    overwrite=False,
                    bypass_document_validation=True,
                    storage_key="1",
                    serialized_document="{'_id': '1'}",
                    purge_expired_documents=lambda *_args: None,
                    begin_write=lambda current: runtime.begin_write(current, session),
                    rollback_write=lambda current: runtime.rollback_write(current, session),
                    commit_write=lambda current: runtime.commit_write(current, session),
                    collection_options_or_empty=lambda *_args: {},
                    load_existing_document_for_storage_key=lambda *_args: None,
                    ensure_collection_row=lambda *_args, **_kwargs: None,
                    validate_document_against_unique_indexes=lambda *_args: None,
                    load_indexes=lambda *_args: [],
                    rebuild_multikey_entries_for_document=lambda *_args: None,
                    supports_scalar_index=lambda _index: False,
                    rebuild_scalar_entries_for_document=lambda *_args: None,
                    load_search_index_rows=lambda *_args: [],
                    replace_search_entries_for_document=lambda *_args: None,
                    invalidate_collection_features_cache=lambda *_args: (_ for _ in ()).throw(RuntimeError("cache boom")),
                )

            self.assertTrue(conn.in_transaction)
            self.assertEqual(
                conn.execute(
                    "SELECT document FROM documents WHERE db_name = ? AND coll_name = ?",
                    ("db", "coll"),
                ).fetchall(),
                [],
            )
            session.commit_transaction()
            self.assertEqual(
                conn.execute(
                    "SELECT document FROM documents WHERE db_name = ? AND coll_name = ?",
                    ("db", "coll"),
                ).fetchall(),
                [],
            )
        finally:
            if session is not None and engine._transaction_owner_session_id is not None:
                session.abort_transaction()
            asyncio.run(engine.disconnect())

    def test_create_index_purge_after_savepoint_release_is_best_effort(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        session: ClientSession | None = None
        try:
            conn = engine._require_connection()
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()

            name = index_admin.create_index(
                conn,
                db_name="db",
                coll_name="coll",
                keys=[("kind", 1)],
                unique=False,
                name="kind_idx",
                sparse=False,
                hidden=False,
                collation=None,
                partial_filter_expression=None,
                expire_after_seconds=None,
                deadline=None,
                enforce_deadline_fn=lambda _deadline: None,
                begin_write=lambda current: runtime.begin_write(current, session),
                commit_write=lambda current: runtime.commit_write(current, session),
                rollback_write=lambda current: runtime.rollback_write(current, session),
                purge_expired_documents=lambda *_args: (_ for _ in ()).throw(RuntimeError("purge boom")),
                mark_index_metadata_changed=lambda *_args: None,
                invalidate_collection_features_cache=lambda *_args: None,
                load_indexes=lambda *_args: [],
                supports_multikey_index=lambda *_args: False,
                physical_index_name=lambda *_args: "physical_kind_idx",
                physical_multikey_index_name=lambda *_args: "multikey_kind_idx",
                physical_scalar_index_name=lambda *_args: None,
                is_builtin_id_index=lambda _keys: False,
                replace_multikey_entries_for_document=lambda *_args: None,
                replace_scalar_entries_for_document=lambda *_args: None,
                load_documents=lambda *_args: [],
                validate_compound_multikey_document=lambda *_args: None,
                unique_index_conflict=lambda *_args: None,
                quote_identifier=lambda value: f'"{value}"',
            )

            self.assertEqual(name, "kind_idx")
            self.assertEqual(
                conn.execute(
                    "SELECT name FROM indexes WHERE db_name = ? AND coll_name = ?",
                    ("db", "coll"),
                ).fetchall(),
                [("kind_idx",)],
            )
            session.commit_transaction()
            self.assertEqual(
                conn.execute(
                    "SELECT name FROM indexes WHERE db_name = ? AND coll_name = ?",
                    ("db", "coll"),
                ).fetchall(),
                [("kind_idx",)],
            )
        finally:
            if session is not None and engine._transaction_owner_session_id is not None:
                session.abort_transaction()
            asyncio.run(engine.disconnect())

    def test_savepoint_rollback_clears_multikey_physical_index_cache(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        session: ClientSession | None = None
        try:
            conn = engine._require_connection()
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()
            index = EngineIndexRecord(
                name="tags_1",
                physical_name="idx_tags",
                fields=["tags"],
                key=[("tags", 1)],
                unique=False,
                multikey=True,
                multikey_physical_name="mkidx_tags",
            )

            runtime.begin_write(conn, session)
            engine._ensure_multikey_physical_indexes_sync(conn, [index])
            engine._compound_rank_cache = {
                ("db", "coll", "phys", 0, "query"): {(("1",), None): ("1",)}
            }
            runtime.rollback_write(conn, session)

            self.assertNotIn("mkidx_tags", engine._ensured_multikey_physical_indexes)
            self.assertEqual(engine._compound_rank_cache, {})
            self.assertEqual(conn.execute("PRAGMA index_list(multikey_entries)").fetchall(), [])

            engine._ensure_multikey_physical_indexes_sync(conn, [index])
            self.assertIn("mkidx_tags", engine._ensured_multikey_physical_indexes)
            index_names = {
                row[1]
                for row in conn.execute("PRAGMA index_list(multikey_entries)").fetchall()
            }
            self.assertIn("mkidx_tags", index_names)
        finally:
            if session is not None and engine._transaction_owner_session_id is not None:
                session.abort_transaction()
            asyncio.run(engine.disconnect())

    def test_session_abort_clears_collection_id_cache_for_rolled_back_collection(self):
        engine = SQLiteEngine()
        asyncio.run(engine.connect())
        session: ClientSession | None = None
        try:
            conn = engine._require_connection()
            runtime = engine._session_runtime
            session = ClientSession()
            runtime.create_session_state(session)
            session.start_transaction()

            collection_id = engine._lookup_collection_id(conn, "db", "coll", create=True)
            self.assertEqual(engine._collection_id_cache[("db", "coll")], collection_id)
            engine._compound_should_score_cache = {
                ("db", "coll", "phys", 0, "query"): {"1": {"matchedShould": 1.0}}
            }
            engine._compound_topk_prefilter_cache = {
                ("db", "coll", "phys", 0, "query"): {(("1",), 1, None): (("1",), {})}
            }

            session.abort_transaction()

            self.assertNotIn(("db", "coll"), engine._collection_id_cache)
            self.assertEqual(engine._compound_should_score_cache, {})
            self.assertEqual(engine._compound_topk_prefilter_cache, {})
            self.assertIsNone(engine._lookup_collection_id(conn, "db", "coll", create=False))
        finally:
            if session is not None and engine._transaction_owner_session_id is not None:
                session.abort_transaction()
            asyncio.run(engine.disconnect())
