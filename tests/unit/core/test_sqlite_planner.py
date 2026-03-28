import asyncio
import unittest

from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.engines.sqlite_planner import (
    SQLiteReadExecutionPlan,
    compile_sqlite_read_execution_plan,
)


class SQLitePlannerUnitTests(unittest.TestCase):
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
                await engine.put_document("db", "users", {"_id": "1", "kind": "view", "payload": b"\x02"})
                await engine.put_document("db", "users", {"_id": "2", "kind": "view", "payload": b"\x01"})
                semantics = compile_find_semantics({"kind": "view"}, sort=[("payload", 1)])
                plan = compile_sqlite_read_execution_plan(
                    db_name="db",
                    coll_name="users",
                    semantics=semantics,
                    hint=None,
                    dialect_requires_python_fallback=engine._dialect_requires_python_fallback,
                    plan_has_array_traversing_paths=engine._plan_has_array_traversing_paths,
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
            self.assertTrue(any(step.runtime == "sql" and step.phase == "filter" for step in plan.execution_lineage))
            self.assertTrue(any(step.runtime == "python" and step.phase == "sort" for step in plan.execution_lineage))

        asyncio.run(_run())
