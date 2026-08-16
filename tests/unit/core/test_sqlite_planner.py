import asyncio
import unittest

from types import SimpleNamespace
from unittest.mock import Mock

from mongoeco.engines._runtime_metrics import LocalRuntimeMetrics
from mongoeco.engines._sqlite_explain_contract import sqlite_pushdown_details
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.engines.sqlite_planner import (
    SQLitePlanOwner,
    SQLitePushdownMode,
    SQLiteReadExecutionPlan,
    compile_sqlite_read_execution_plan,
)


class SQLitePlannerUnitTests(unittest.TestCase):
    def test_legacy_pushdown_details_and_invalid_metric_mode_are_explicit(self):
        details = sqlite_pushdown_details(
            SimpleNamespace(strategy="python", fallback_reason="legacy"),
        )
        metrics = LocalRuntimeMetrics()
        metrics.record_planner("")

        self.assertEqual(
            details,
            {
                "mode": "python",
                "usesSqlRuntime": False,
                "pythonSort": False,
                "fallbackReason": "legacy",
            },
        )
        self.assertEqual(metrics.planner_snapshot(), {"modes": {}, "fallbacks": {}})

    def test_sqlite_plan_rejects_impossible_pushdown_contracts(self):
        base = {
            "semantics": compile_find_semantics({}),
            "strategy": "python",
            "execution_lineage": (),
            "physical_plan": (),
        }
        cases = (
            lambda: SQLiteReadExecutionPlan(**base, use_sql=1),
            lambda: SQLiteReadExecutionPlan(**base, apply_python_sort=1),
            lambda: SQLiteReadExecutionPlan(**base, mode="python"),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                mode=SQLitePushdownMode.PYTHON,
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                mode=SQLitePushdownMode.SQL_EXACT,
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                mode=SQLitePushdownMode.SQL_PREFILTER,
                sql="SELECT 1",
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                sql="SELECT 1",
                apply_python_residual=True,
                mode=SQLitePushdownMode.SQL_EXACT,
            ),
            lambda: SQLiteReadExecutionPlan(**base, filter_owner="python"),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                mode=SQLitePushdownMode.SQL_EXACT,
            ),
            lambda: SQLiteReadExecutionPlan(**base, sql="SELECT 1"),
            lambda: SQLiteReadExecutionPlan(**base, params=[]),
            lambda: SQLiteReadExecutionPlan(**base, sql=1),
            lambda: SQLiteReadExecutionPlan(**base, collectors=["count"]),
            lambda: SQLiteReadExecutionPlan(**base, collectors=("count", "count")),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                sql="SELECT 1",
                filter_owner=SQLitePlanOwner.PYTHON,
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                sort_owner=SQLitePlanOwner.PYTHON,
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                sql="SELECT 1",
                apply_python_residual=True,
                mode=SQLitePushdownMode.SQL_PREFILTER,
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                use_sql=True,
                sql="SELECT 1",
                residual_query_plan=compile_find_semantics({}).query_plan,
                mode=SQLitePushdownMode.SQL_EXACT,
            ),
            lambda: SQLiteReadExecutionPlan(
                **{**base, "semantics": compile_find_semantics({}, sort=[("x", 1)])},
                use_sql=True,
                sql="SELECT 1",
                apply_python_sort=True,
                sort_owner=SQLitePlanOwner.SQL,
            ),
            lambda: SQLiteReadExecutionPlan(
                **{**base, "semantics": compile_find_semantics({}, limit=1)},
                use_sql=True,
                sql="SELECT 1",
                apply_python_residual=True,
                residual_query_plan=compile_find_semantics({}).query_plan,
                window_owner=SQLitePlanOwner.SQL,
            ),
            lambda: SQLiteReadExecutionPlan(
                **base,
                window_owner=SQLitePlanOwner.SQL,
            ),
        )
        for factory in cases:
            with (
                self.subTest(factory=factory),
                self.assertRaises((TypeError, ValueError)),
            ):
                factory()

        prefilter = SQLiteReadExecutionPlan(
            **{**base, "strategy": "hybrid"},
            use_sql=True,
            sql="SELECT 1",
            apply_python_residual=True,
            residual_query_plan=compile_find_semantics({"kind": "note"}).query_plan,
            collectors=("count",),
        )
        self.assertIs(prefilter.mode, SQLitePushdownMode.SQL_PREFILTER)
        self.assertIs(prefilter.filter_owner, SQLitePlanOwner.SQL_THEN_PYTHON)
        self.assertEqual(prefilter.require_sql(), ("SELECT 1", ()))
        self.assertEqual(prefilter.to_document()["collectors"], ["count"])
        self.assertTrue(prefilter.to_document()["residualPlanPresent"])
        self.assertEqual(prefilter.to_document()["parameterCount"], 0)

    def test_sqlite_read_execution_plan_is_typed_pushdown_boundary(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                semantics = compile_find_semantics({"name": "Ada"})
                plan = compile_sqlite_read_execution_plan(
                    db_name="db",
                    coll_name="users",
                    semantics=semantics,
                    hint=None,
                    dialect_requires_python_fallback=engine._dialect_requires_python_fallback,
                    plan_has_array_traversing_paths=engine._plan_has_array_traversing_paths,
                    plan_requires_python_for_dbref_paths=engine._plan_requires_python_for_dbref_paths,
                    plan_requires_python_for_array_comparisons=engine._plan_requires_python_for_array_comparisons,
                    plan_requires_python_for_undefined=engine._plan_requires_python_for_undefined,
                    plan_requires_python_for_bytes=engine._plan_requires_python_for_bytes,
                    sort_requires_python=engine._sort_requires_python,
                    build_select_sql=engine._build_select_sql,
                )
            finally:
                await engine.disconnect()

            self.assertIsInstance(plan, SQLiteReadExecutionPlan)
            self.assertTrue(plan.use_sql)
            self.assertEqual(plan.strategy, "sql")
            self.assertTrue(plan.execution_lineage)

        asyncio.run(_run())

    def test_sqlite_read_execution_plan_uses_hybrid_strategy_for_python_sort_only(self):
        async def _run() -> None:
            engine = SQLiteEngine()
            await engine.connect()
            try:
                await engine.put_document(
                    "db", "users", {"_id": "1", "kind": "view", "payload": b"\x02"}
                )
                await engine.put_document(
                    "db", "users", {"_id": "2", "kind": "view", "payload": b"\x01"}
                )
                semantics = compile_find_semantics(
                    {"kind": "view"}, sort=[("payload", 1)]
                )
                plan = compile_sqlite_read_execution_plan(
                    db_name="db",
                    coll_name="users",
                    semantics=semantics,
                    hint=None,
                    dialect_requires_python_fallback=engine._dialect_requires_python_fallback,
                    plan_has_array_traversing_paths=engine._plan_has_array_traversing_paths,
                    plan_requires_python_for_dbref_paths=engine._plan_requires_python_for_dbref_paths,
                    plan_requires_python_for_array_comparisons=engine._plan_requires_python_for_array_comparisons,
                    plan_requires_python_for_undefined=engine._plan_requires_python_for_undefined,
                    plan_requires_python_for_bytes=engine._plan_requires_python_for_bytes,
                    sort_requires_python=engine._sort_requires_python,
                    build_select_sql=engine._build_select_sql,
                )
            finally:
                await engine.disconnect()

            self.assertIsInstance(plan, SQLiteReadExecutionPlan)
            self.assertTrue(plan.use_sql)
            self.assertTrue(plan.apply_python_sort)
            self.assertEqual(plan.strategy, "hybrid")
            self.assertEqual(plan.fallback_reason, "Sort requires Python fallback")
            self.assertTrue(
                any(
                    step.runtime == "sql" and step.phase == "filter"
                    for step in plan.execution_lineage
                )
            )
            self.assertTrue(
                any(
                    step.runtime == "python" and step.phase == "sort"
                    for step in plan.execution_lineage
                )
            )

        asyncio.run(_run())

    def test_sqlite_read_execution_plan_falls_back_to_python_when_sort_sql_is_not_translatable(  # noqa: E501
        self,
    ):
        semantics = compile_find_semantics({"kind": "view"}, sort=[("payload", 1)])
        plan = compile_sqlite_read_execution_plan(
            db_name="db",
            coll_name="users",
            semantics=semantics,
            hint=None,
            dialect_requires_python_fallback=lambda _dialect: False,
            plan_has_array_traversing_paths=lambda *_args: False,
            plan_requires_python_for_dbref_paths=lambda *_args: False,
            plan_requires_python_for_array_comparisons=lambda *_args: False,
            plan_requires_python_for_undefined=lambda *_args: False,
            plan_requires_python_for_bytes=lambda *_args: False,
            sort_requires_python=lambda *_args: True,
            build_select_sql=Mock(side_effect=NotImplementedError("fallback")),
        )

        self.assertFalse(plan.use_sql)
        self.assertEqual(plan.strategy, "python")
        self.assertEqual(plan.fallback_reason, "Sort requires Python fallback")
