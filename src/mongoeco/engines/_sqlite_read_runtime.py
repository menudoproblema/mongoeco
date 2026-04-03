from __future__ import annotations

import sqlite3
from typing import Any, Protocol

from mongoeco.compat import MongoDialect
from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.core.query_plan import QueryNode
from mongoeco.engines._sqlite_read_execution import (
    compile_read_execution_plan as _sqlite_compile_read_execution_plan,
    plan_find_semantics_sync as _sqlite_plan_find_semantics_sync,
)
from mongoeco.engines.semantic_core import EngineFindSemantics, compile_find_semantics
from mongoeco.engines.sqlite_planner import SQLiteReadExecutionPlan
from mongoeco.types import (
    ExecutionLineageStep,
    Filter,
    IndexKeySpec,
    PhysicalPlanStep,
    SortSpec,
)


class _SQLiteReadRuntimeEngine(Protocol):
    def _dialect_requires_python_fallback(self, dialect: MongoDialect) -> bool: ...
    def _plan_has_array_traversing_paths(self, db_name: str, coll_name: str, plan: object) -> bool: ...
    def _plan_requires_python_for_dbref_paths(self, db_name: str, coll_name: str, plan: object) -> bool: ...
    def _plan_requires_python_for_array_comparisons(self, db_name: str, coll_name: str, plan: object) -> bool: ...
    def _plan_requires_python_for_undefined(self, db_name: str, coll_name: str, plan: object) -> bool: ...
    def _plan_requires_python_for_bytes(self, db_name: str, coll_name: str, plan: object) -> bool: ...
    def _sort_requires_python(self, db_name: str, coll_name: str, plan: object, sort: object | None) -> bool: ...
    def _build_select_sql(self, db_name: str, coll_name: str, plan: QueryNode, *, select_clause: str, sort: SortSpec | None = None, skip: int = 0, limit: int | None = None, hint: str | IndexKeySpec | None = None, dialect: MongoDialect) -> tuple[str, list[object]]: ...
    def _storage_key(self, value: Any) -> str: ...
    def _find_scalar_fast_path_index(self, db_name: str, coll_name: str, field: str): ...
    def _field_is_top_level_array_in_collection(self, db_name: str, coll_name: str, field: str) -> bool: ...
    def _field_contains_real_numeric_in_collection(self, db_name: str, coll_name: str, field: str) -> bool: ...
    def _field_contains_non_ascii_text_in_collection(self, db_name: str, coll_name: str, field: str) -> bool: ...
    def _build_scalar_indexed_top_level_equals_sql(self, db_name: str, coll_name: str, *, field: str, value: Any, index_name: str, physical_name: str, limit: int | None = None, null_matches_undefined: bool = False) -> tuple[str, tuple[object, ...]] | None: ...
    def _build_scalar_indexed_top_level_range_sql(self, db_name: str, coll_name: str, *, field: str, value: Any, operator: str, index_name: str, physical_name: str, limit: int | None = None) -> tuple[str, tuple[object, ...]] | None: ...
    def _require_connection(self) -> sqlite3.Connection: ...
    def _plan_find_semantics_sync(self, db_name: str, coll_name: str, semantics: EngineFindSemantics): ...


def _python_text_execution_plan(semantics: EngineFindSemantics) -> SQLiteReadExecutionPlan:
    return SQLiteReadExecutionPlan(
        semantics=semantics,
        strategy="python-text",
        execution_lineage=(
            ExecutionLineageStep(runtime="python", phase="scan", detail="sqlite collection scan"),
            ExecutionLineageStep(runtime="python", phase="text", detail="classic text filter"),
            ExecutionLineageStep(runtime="python", phase="filter", detail="semantic core"),
        ),
        physical_plan=(
            PhysicalPlanStep(runtime="python", operation="scan"),
            PhysicalPlanStep(runtime="python", operation="text"),
            PhysicalPlanStep(runtime="python", operation="filter"),
        ),
        use_sql=False,
        fallback_reason="classic $text local runtime executes in Python fallback",
    )


def compile_read_execution_plan(
    engine: _SQLiteReadRuntimeEngine,
    db_name: str,
    coll_name: str,
    semantics: EngineFindSemantics,
    *,
    select_clause: str = "document",
    hint: str | IndexKeySpec | None = None,
) -> SQLiteReadExecutionPlan:
    if semantics.text_query is not None:
        return _python_text_execution_plan(semantics)
    return _sqlite_compile_read_execution_plan(
        db_name=db_name,
        coll_name=coll_name,
        semantics=semantics,
        select_clause=select_clause,
        hint=hint,
        dialect_requires_python_fallback=engine._dialect_requires_python_fallback,
        plan_has_array_traversing_paths=engine._plan_has_array_traversing_paths,
        plan_requires_python_for_dbref_paths=engine._plan_requires_python_for_dbref_paths,
        plan_requires_python_for_array_comparisons=engine._plan_requires_python_for_array_comparisons,
        plan_requires_python_for_undefined=engine._plan_requires_python_for_undefined,
        plan_requires_python_for_bytes=engine._plan_requires_python_for_bytes,
        sort_requires_python=engine._sort_requires_python,
        build_select_sql=engine._build_select_sql,
    )


def plan_find_semantics_sync(
    engine: _SQLiteReadRuntimeEngine,
    db_name: str,
    coll_name: str,
    semantics: EngineFindSemantics,
):
    if semantics.text_query is not None:
        lineage = [
            ExecutionLineageStep(runtime="python", phase="scan", detail="sqlite collection scan"),
            ExecutionLineageStep(runtime="python", phase="text", detail="classic text filter"),
            ExecutionLineageStep(runtime="python", phase="filter", detail="semantic core"),
        ]
        if semantics.sort:
            lineage.append(ExecutionLineageStep(runtime="python", phase="sort", detail="semantic core"))
        if semantics.projection is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="project", detail="semantic core"))
        if semantics.skip or semantics.limit is not None:
            lineage.append(ExecutionLineageStep(runtime="python", phase="slice", detail="semantic core"))
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python-text",
            execution_lineage=tuple(lineage),
            physical_plan=(
                PhysicalPlanStep(runtime="python", operation="scan"),
                PhysicalPlanStep(runtime="python", operation="text"),
                PhysicalPlanStep(runtime="python", operation="filter"),
                *((PhysicalPlanStep(runtime="python", operation="sort"),) if semantics.sort else ()),
                *((PhysicalPlanStep(runtime="python", operation="project"),) if semantics.projection is not None else ()),
                *((PhysicalPlanStep(runtime="python", operation="slice"),) if semantics.skip or semantics.limit is not None else ()),
            ),
            use_sql=False,
            fallback_reason="classic $text local runtime executes in Python fallback",
        )
    return _sqlite_plan_find_semantics_sync(
        db_name=db_name,
        coll_name=coll_name,
        semantics=semantics,
        storage_key_for_id=engine._storage_key,
        find_scalar_fast_path_index=lambda current_db_name, current_coll_name, field: engine._find_scalar_fast_path_index(
            current_db_name,
            current_coll_name,
            field,
        ),
        field_is_top_level_array_in_collection=lambda current_db_name, current_coll_name, field: engine._field_is_top_level_array_in_collection(
            current_db_name,
            current_coll_name,
            field,
        ),
        field_contains_real_numeric_in_collection=lambda current_db_name, current_coll_name, field: engine._field_contains_real_numeric_in_collection(
            current_db_name,
            current_coll_name,
            field,
        ),
        field_contains_non_ascii_text_in_collection=lambda current_db_name, current_coll_name, field: engine._field_contains_non_ascii_text_in_collection(
            current_db_name,
            current_coll_name,
            field,
        ),
        build_equals_sql=lambda current_db_name, current_coll_name, value, index_name, physical_name, limit, null_matches_undefined: engine._build_scalar_indexed_top_level_equals_sql(
            current_db_name,
            current_coll_name,
            field=semantics.query_plan.field,
            value=value,
            index_name=index_name,
            physical_name=physical_name,
            limit=limit,
            null_matches_undefined=null_matches_undefined,
        ),
        build_range_sql=lambda current_db_name, current_coll_name, value, operator, index_name, physical_name, limit: engine._build_scalar_indexed_top_level_range_sql(
            current_db_name,
            current_coll_name,
            field=semantics.query_plan.field,
            value=value,
            operator=operator,
            index_name=index_name,
            physical_name=physical_name,
            limit=limit,
        ),
        compile_read_plan=lambda current_semantics, current_hint: engine._compile_read_execution_plan(
            db_name,
            coll_name,
            current_semantics,
            hint=current_hint,
        ),
    )


def explain_query_plan_sync(
    engine: _SQLiteReadRuntimeEngine,
    db_name: str,
    coll_name: str,
    semantics_or_filter_spec: EngineFindSemantics | Filter | None = None,
    *,
    plan: QueryNode | None = None,
    sort: SortSpec | None = None,
    skip: int = 0,
    limit: int | None = None,
    hint: str | IndexKeySpec | None = None,
    max_time_ms: int | None = None,
    dialect: MongoDialect | None = None,
) -> list[str]:
    semantics = (
        semantics_or_filter_spec
        if isinstance(semantics_or_filter_spec, EngineFindSemantics)
        else compile_find_semantics(
            semantics_or_filter_spec,
            plan=plan,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            max_time_ms=max_time_ms,
            dialect=dialect,
        )
    )
    deadline = semantics.deadline
    conn = engine._require_connection()
    enforce_deadline(deadline)
    execution_plan = engine._plan_find_semantics_sync(
        db_name,
        coll_name,
        semantics,
    )
    sql, params = execution_plan.require_sql()
    rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", tuple(params)).fetchall()
    enforce_deadline(deadline)
    return [str(row[3]) for row in rows]
