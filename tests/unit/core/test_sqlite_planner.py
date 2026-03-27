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

        asyncio.run(_run())
