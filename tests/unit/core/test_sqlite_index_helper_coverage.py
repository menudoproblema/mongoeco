from __future__ import annotations

import sqlite3
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.query_plan import MatchAll
from mongoeco.engines import _sqlite_index_admin as index_admin
from mongoeco.engines import _sqlite_index_runtime as index_runtime
from mongoeco.engines import _sqlite_modify_ops as modify_ops
from mongoeco.errors import DuplicateKeyError


class SQLiteIndexHelperCoverageTests(unittest.TestCase):
    @staticmethod
    def _base_create_index_kwargs() -> dict[str, object]:
        return {
            "db_name": "db",
            "coll_name": "coll",
            "keys": [("kind", 1)],
            "unique": False,
            "name": None,
            "sparse": False,
            "hidden": False,
            "collation": None,
            "partial_filter_expression": None,
            "expire_after_seconds": None,
            "deadline": None,
            "enforce_deadline_fn": lambda _deadline: None,
            "begin_write": lambda _conn: None,
            "commit_write": lambda _conn: None,
            "rollback_write": lambda _conn: None,
            "purge_expired_documents": lambda *_args: None,
            "mark_index_metadata_changed": lambda *_args: None,
            "invalidate_collection_features_cache": lambda *_args: None,
            "load_indexes": lambda *_args: [],
            "field_traverses_array_in_collection": lambda *_args: False,
            "supports_multikey_index": lambda *_args: False,
            "physical_index_name": lambda *_args: "idx",
            "physical_multikey_index_name": lambda *_args: "idx_multi",
            "physical_scalar_index_name": lambda *_args: "idx_scalar",
            "is_builtin_id_index": lambda _keys: False,
            "replace_multikey_entries_for_document": lambda *_args: None,
            "replace_scalar_entries_for_document": lambda *_args: None,
            "load_documents": lambda *_args: [],
            "quote_identifier": lambda value: f'"{value}"',
        }

    def test_create_index_rejects_non_boolean_hidden_flag(self):
        with self.assertRaisesRegex(TypeError, "hidden must be a bool"):
            index_admin.create_index(
                Mock(),
                db_name="db",
                coll_name="coll",
                keys=[("kind", 1)],
                unique=False,
                name=None,
                sparse=False,
                hidden="yes",
                collation=None,
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
                physical_index_name=lambda *_args: "idx",
                physical_multikey_index_name=lambda *_args: "idx_multi",
                physical_scalar_index_name=lambda *_args: "idx_scalar",
                is_builtin_id_index=lambda _keys: False,
                replace_multikey_entries_for_document=lambda *_args: None,
                replace_scalar_entries_for_document=lambda *_args: None,
                load_documents=lambda *_args: [],
                quote_identifier=lambda value: f'"{value}"',
            )

    def test_create_index_validates_ttl_hidden_and_collation_after_definition_init(self):
        kwargs = self._base_create_index_kwargs()
        kwargs["expire_after_seconds"] = "bad"
        with patch(
            "mongoeco.engines._sqlite_index_admin.IndexDefinition",
            return_value=SimpleNamespace(weights=None, default_language=None, language_override=None),
        ):
            with self.assertRaisesRegex(TypeError, "expire_after_seconds must be a non-negative int or None"):
                index_admin.create_index(Mock(), **kwargs)

        kwargs = self._base_create_index_kwargs()
        kwargs["hidden"] = "yes"
        with patch(
            "mongoeco.engines._sqlite_index_admin.IndexDefinition",
            return_value=SimpleNamespace(weights=None, default_language=None, language_override=None),
        ):
            with self.assertRaisesRegex(TypeError, "hidden must be a bool"):
                index_admin.create_index(Mock(), **kwargs)

        kwargs = self._base_create_index_kwargs()
        kwargs["collation"] = "en"
        with patch(
            "mongoeco.engines._sqlite_index_admin.IndexDefinition",
            return_value=SimpleNamespace(weights=None, default_language=None, language_override=None),
        ):
            with self.assertRaisesRegex(TypeError, "collation must be a dict or None"):
                index_admin.create_index(Mock(), **kwargs)

    def test_drop_index_rolls_back_when_sql_drop_fails(self):
        conn = Mock()
        conn.execute.side_effect = RuntimeError("boom")
        rollback = Mock()

        with self.assertRaisesRegex(RuntimeError, "boom"):
            index_admin.drop_index(
                conn,
                db_name="db",
                coll_name="coll",
                index_or_name="idx_kind",
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda current: rollback(current),
                load_indexes=lambda *_args: [{"name": "idx_kind", "physical_name": "idx_kind"}],
                lookup_collection_id=lambda *_args: 1,
                quote_identifier=lambda value: f'"{value}"',
                is_builtin_id_index=lambda _keys: False,
                mark_index_metadata_changed=lambda *_args: None,
                invalidate_collection_features_cache=lambda *_args: None,
            )

        rollback.assert_called_once_with(conn)

    def test_replace_scalar_entries_for_index_for_document_returns_when_index_is_not_scalar_supported(self):
        conn = Mock()
        engine = SimpleNamespace(
            _lookup_collection_id=lambda *_args, **_kwargs: 1,
            _supports_scalar_index=lambda _index: False,
        )

        index_runtime.replace_scalar_entries_for_index_for_document(
            engine,
            conn,
            "db",
            "coll",
            "1",
            {"_id": "1", "kind": "view"},
            {"name": "kind_1"},
        )

        conn.execute.assert_called_once()

    def test_backfill_scalar_indexes_sync_skips_namespaces_without_collection_id(self):
        conn = Mock()
        first_cursor = Mock()
        first_cursor.fetchall.return_value = [
            (
                "db",
                "coll",
                "kind_1",
                "idx_kind",
                '["kind"]',
                '[["kind", 1]]',
                0,
                0,
                0,
                None,
                None,
                0,
                None,
                None,
                "idx_kind_scalar",
            )
        ]
        conn.execute.side_effect = [first_cursor]
        engine = SimpleNamespace(
            _physical_scalar_index_name=lambda *_args: "generated_scalar",
            _physical_index_name=lambda *_args: "generated_idx",
            _physical_multikey_index_name=lambda *_args: "generated_multi",
            _lookup_collection_id=lambda *_args: None,
            _load_documents=lambda *_args: [("1", {"_id": "1"})],
        )

        with patch("mongoeco.engines._sqlite_index_runtime.ensure_scalar_physical_indexes_sync"):
            index_runtime.backfill_scalar_indexes_sync(engine, conn)

        self.assertEqual(conn.execute.call_count, 1)

    def test_update_with_operation_converts_fast_path_integrity_errors_to_duplicate_key(self):
        conn = Mock()
        rollback = Mock()

        class _CompiledPlan:
            def apply(self, document):
                document["kind"] = "note"
                return True

        semantics = SimpleNamespace(
            dialect=MONGODB_DIALECT_70,
            collation=None,
            query_plan=MatchAll(),
            compiled_update_plan=_CompiledPlan(),
            compiled_upsert_plan=SimpleNamespace(apply=lambda _document: None),
        )

        with self.assertRaisesRegex(DuplicateKeyError, "dup"):
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
                begin_write=lambda _conn: None,
                commit_write=lambda _conn: None,
                rollback_write=lambda current: rollback(current),
                translate_compiled_update_plan=lambda *_args: ("?", ()),
                compiled_update_plan_type=_CompiledPlan,
                rebuild_multikey_entries_for_document=lambda *_args: None,
                rebuild_scalar_entries_for_document=lambda *_args: None,
                replace_search_entries_for_document=lambda *_args: (_ for _ in ()).throw(sqlite3.IntegrityError("dup")),
                serialize_document=lambda document: str(document),
                storage_key_for_id=lambda value: str(value),
                new_object_id=lambda: "new-id",
                invalidate_collection_features_cache=lambda *_args: None,
            )

        rollback.assert_called_once_with(conn)
