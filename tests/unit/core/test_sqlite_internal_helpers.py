import sqlite3
import unittest
from unittest.mock import Mock, patch

from mongoeco.engines import _sqlite_index_admin as index_admin
from mongoeco.engines import _sqlite_write_ops as write_ops
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.types import EngineIndexRecord


class SQLiteInternalHelperTests(unittest.TestCase):
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
