from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from mongoeco.core.query_plan import EqualsCondition
from mongoeco.core.search import ClassicTextQuery
from mongoeco.engines._active_operations import LocalActiveOperationRegistry
from mongoeco.engines._sqlite_namespace_admin import drop_collection
from mongoeco.engines._sqlite_plan_heuristics import plan_requires_python_for_array_comparisons
from mongoeco.engines._sqlite_read_ops import build_search_explain
from mongoeco.engines._sqlite_read_runtime import plan_find_semantics_sync
from mongoeco.engines._sqlite_session_runtime import SQLiteSessionRuntime
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.types import SearchIndexDefinition


class SQLiteRuntimeSmallHelperTests(unittest.TestCase):
    def test_active_operation_registry_cancel_invokes_task_cancel(self):
        registry = LocalActiveOperationRegistry()
        task = Mock()

        from mongoeco.engines import _active_operations as active_ops

        with unittest.mock.patch.object(active_ops.asyncio, "current_task", return_value=task):
            record = registry.begin(command_name="find", operation_type="read", namespace="db.coll")

        self.assertTrue(registry.cancel(record.opid))
        task.cancel.assert_called_once_with()

    def test_plan_requires_python_for_array_comparisons_returns_false_when_no_fields_require_it(self):
        self.assertFalse(
            plan_requires_python_for_array_comparisons(
                db_name="db",
                coll_name="coll",
                plan=EqualsCondition(field="kind", value="view"),
                comparison_fields=lambda _plan: {"kind"},
                field_is_top_level_array_in_collection=lambda *_args: False,
            )
        )

    def test_plan_find_semantics_sync_adds_slice_step_for_text_queries(self):
        semantics = compile_find_semantics(
            text_query=ClassicTextQuery(raw_query="ada", terms=("ada",)),
            skip=1,
            limit=2,
        )
        plan = plan_find_semantics_sync(SimpleNamespace(), "db", "coll", semantics)

        self.assertEqual(plan.strategy, "python-text")
        self.assertTrue(any(step.phase == "slice" for step in plan.execution_lineage))

    def test_sqlite_session_runtime_sync_session_state_returns_when_context_is_missing(self):
        engine = SimpleNamespace(
            _engine_key=lambda: "sqlite:memory",
            _connection=None,
            _mvcc_version=3,
        )
        runtime = SQLiteSessionRuntime(engine)
        session = Mock()
        session.get_engine_context.return_value = None

        runtime.sync_session_state(session, transaction_active=True)

        session.get_engine_context.assert_called_once_with("sqlite:memory")

    def test_namespace_drop_collection_drops_search_backend_rows_even_without_physical_name(self):
        conn = Mock()
        dropped: list[str | None] = []

        drop_collection(
            conn=conn,
            db_name="db",
            coll_name="coll",
            begin_write=lambda _conn: None,
            commit_write=lambda _conn: None,
            rollback_write=lambda _conn: None,
            load_indexes=lambda *_args: [],
            load_search_index_rows=lambda *_args: [
                (SearchIndexDefinition({"mappings": {"dynamic": True}}, name="search"), None, None)
            ],
            lookup_collection_id=lambda *_args: None,
            quote_identifier=lambda value: f'"{value}"',
            drop_search_backend=lambda _conn, physical_name: dropped.append(physical_name),
            mark_index_metadata_changed=lambda *_args: None,
            invalidate_collection_id_cache=lambda *_args: None,
            invalidate_collection_features_cache=lambda *_args: None,
        )

        self.assertEqual(dropped, [None])

    def test_build_search_explain_compiles_query_details(self):
        definition = SearchIndexDefinition(
            {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2, "similarity": "cosine"}]},
            name="vec",
            index_type="vectorSearch",
        )

        explain = build_search_explain(
            operator="$vectorSearch",
            spec={"index": "vec", "path": "embedding", "queryVector": [1.0, 0.0], "limit": 2},
            definition=definition,
            physical_name="vec_idx",
            fts5_match=None,
        )

        self.assertEqual(explain["index"], "vec")
        self.assertEqual(explain["physicalName"], "vec_idx")
        self.assertEqual(explain["vector_paths"], ["embedding"])
