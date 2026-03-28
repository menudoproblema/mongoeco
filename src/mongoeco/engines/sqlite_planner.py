from dataclasses import dataclass
from typing import Any, Callable

from mongoeco.engines.semantic_core import EngineFindSemantics, EngineReadExecutionPlan
from mongoeco.types import ExecutionLineageStep, IndexKeySpec, PhysicalPlanStep


@dataclass(frozen=True, slots=True, kw_only=True)
class SQLiteReadExecutionPlan(EngineReadExecutionPlan):
    use_sql: bool
    sql: str | None = None
    params: tuple[object, ...] = ()
    apply_python_sort: bool = False

    def require_sql(self) -> tuple[str, tuple[object, ...]]:
        if not self.use_sql or self.sql is None:
            raise NotImplementedError(self.fallback_reason or "Python fallback required")
        return self.sql, self.params


def compile_sqlite_read_execution_plan(
    *,
    db_name: str,
    coll_name: str,
    semantics: EngineFindSemantics,
    select_clause: str = "document",
    hint: str | IndexKeySpec | None,
    dialect_requires_python_fallback: Callable[[object], bool],
    plan_has_array_traversing_paths: Callable[[str, str, object], bool],
    plan_requires_python_for_array_comparisons: Callable[[str, str, object], bool],
    plan_requires_python_for_undefined: Callable[[str, str, object], bool],
    plan_requires_python_for_bytes: Callable[[str, str, object], bool],
    sort_requires_python: Callable[[str, str, object, object], bool],
    build_select_sql: Callable[..., tuple[str, list[object]]],
) -> SQLiteReadExecutionPlan:
    if semantics.collation is not None:
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, "Collation requires Python fallback"),
            physical_plan=_python_physical_plan(semantics, "collation"),
            use_sql=False,
            fallback_reason="Collation requires Python fallback",
        )
    if dialect_requires_python_fallback(semantics.dialect):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, "Custom dialect requires Python fallback"),
            physical_plan=_python_physical_plan(semantics, "dialect"),
            use_sql=False,
            fallback_reason="Custom dialect requires Python fallback",
        )
    if plan_has_array_traversing_paths(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, "Array traversal requires Python fallback"),
            physical_plan=_python_physical_plan(semantics, "array-traversal"),
            use_sql=False,
            fallback_reason="Array traversal requires Python fallback",
        )
    if plan_requires_python_for_array_comparisons(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, "Top-level array comparisons require Python fallback"),
            physical_plan=_python_physical_plan(semantics, "array-comparison"),
            use_sql=False,
            fallback_reason="Top-level array comparisons require Python fallback",
        )
    if plan_requires_python_for_undefined(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, "Tagged undefined requires Python fallback"),
            physical_plan=_python_physical_plan(semantics, "undefined"),
            use_sql=False,
            fallback_reason="Tagged undefined requires Python fallback",
        )
    if plan_requires_python_for_bytes(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, "Tagged bytes require Python fallback"),
            physical_plan=_python_physical_plan(semantics, "bytes"),
            use_sql=False,
            fallback_reason="Tagged bytes require Python fallback",
        )
    if sort_requires_python(db_name, coll_name, semantics.query_plan, semantics.sort):
        try:
            sql, params = build_select_sql(
                db_name,
                coll_name,
                semantics.query_plan,
                select_clause=select_clause,
                sort=None,
                skip=0,
                limit=None,
                hint=hint,
                dialect=semantics.dialect,
            )
        except NotImplementedError:
            return SQLiteReadExecutionPlan(
                semantics=semantics,
                strategy="python",
                execution_lineage=_python_lineage(semantics, "Sort requires Python fallback"),
                physical_plan=_python_physical_plan(semantics, "sort"),
                use_sql=False,
                fallback_reason="Sort requires Python fallback",
            )
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="hybrid",
            execution_lineage=_hybrid_lineage(semantics, "Sort requires Python fallback"),
            physical_plan=_hybrid_physical_plan(semantics, "sort"),
            use_sql=True,
            sql=sql,
            params=tuple(params),
            apply_python_sort=True,
            fallback_reason="Sort requires Python fallback",
        )
    try:
        sql, params = build_select_sql(
            db_name,
            coll_name,
            semantics.query_plan,
            select_clause=select_clause,
            sort=semantics.sort,
            skip=semantics.skip,
            limit=semantics.limit,
            hint=hint,
            dialect=semantics.dialect,
        )
    except NotImplementedError as exc:
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, str(exc)),
            physical_plan=_python_physical_plan(semantics, "translator"),
            use_sql=False,
            fallback_reason=str(exc),
        )
    return SQLiteReadExecutionPlan(
        semantics=semantics,
        strategy="sql",
        execution_lineage=_sql_lineage(semantics),
        physical_plan=_sql_physical_plan(semantics),
        use_sql=True,
        sql=sql,
        params=tuple(params),
    )


def _sql_lineage(semantics: EngineFindSemantics) -> tuple[ExecutionLineageStep, ...]:
    lineage = [
        ExecutionLineageStep(runtime="sql", phase="scan", detail="engine pushdown"),
        ExecutionLineageStep(runtime="sql", phase="filter", detail="engine pushdown"),
    ]
    if semantics.sort:
        lineage.append(ExecutionLineageStep(runtime="sql", phase="sort", detail="engine pushdown"))
    if semantics.skip or semantics.limit is not None:
        lineage.append(ExecutionLineageStep(runtime="sql", phase="slice", detail="engine pushdown"))
    if semantics.projection is not None:
        lineage.append(ExecutionLineageStep(runtime="python", phase="project", detail="semantic core projection"))
    return tuple(lineage)


def _python_lineage(semantics: EngineFindSemantics, reason: str) -> tuple[ExecutionLineageStep, ...]:
    lineage = [
        ExecutionLineageStep(runtime="python", phase="scan", detail=reason),
        ExecutionLineageStep(runtime="python", phase="filter", detail="semantic core"),
    ]
    if semantics.sort:
        lineage.append(ExecutionLineageStep(runtime="python", phase="sort", detail="semantic core"))
    if semantics.skip or semantics.limit is not None:
        lineage.append(ExecutionLineageStep(runtime="python", phase="slice", detail="semantic core"))
    if semantics.projection is not None:
        lineage.append(ExecutionLineageStep(runtime="python", phase="project", detail="semantic core"))
    return tuple(lineage)


def _hybrid_lineage(semantics: EngineFindSemantics, reason: str) -> tuple[ExecutionLineageStep, ...]:
    lineage = [
        ExecutionLineageStep(runtime="sql", phase="scan", detail="engine pushdown"),
        ExecutionLineageStep(runtime="sql", phase="filter", detail="engine pushdown"),
    ]
    if semantics.sort:
        lineage.append(ExecutionLineageStep(runtime="python", phase="sort", detail=reason))
    if semantics.skip or semantics.limit is not None:
        lineage.append(ExecutionLineageStep(runtime="python", phase="slice", detail="semantic core"))
    if semantics.projection is not None:
        lineage.append(ExecutionLineageStep(runtime="python", phase="project", detail="semantic core projection"))
    return tuple(lineage)


def _sql_physical_plan(semantics: EngineFindSemantics) -> tuple[PhysicalPlanStep, ...]:
    steps: list[PhysicalPlanStep] = [
        PhysicalPlanStep(runtime="sql", operation="scan"),
        PhysicalPlanStep(runtime="sql", operation="filter"),
    ]
    if semantics.sort:
        steps.append(PhysicalPlanStep(runtime="sql", operation="sort"))
    if semantics.skip or semantics.limit is not None:
        steps.append(PhysicalPlanStep(runtime="sql", operation="slice"))
    if semantics.projection is not None:
        steps.append(PhysicalPlanStep(runtime="python", operation="project"))
    return tuple(steps)


def _python_physical_plan(
    semantics: EngineFindSemantics,
    reason: str,
) -> tuple[PhysicalPlanStep, ...]:
    steps: list[PhysicalPlanStep] = [
        PhysicalPlanStep(runtime="python", operation="scan", detail=reason),
        PhysicalPlanStep(runtime="python", operation="filter"),
    ]
    if semantics.sort:
        steps.append(PhysicalPlanStep(runtime="python", operation="sort"))
    if semantics.projection is not None:
        steps.append(PhysicalPlanStep(runtime="python", operation="project"))
    if semantics.skip or semantics.limit is not None:
        steps.append(PhysicalPlanStep(runtime="python", operation="slice"))
    return tuple(steps)


def _hybrid_physical_plan(
    semantics: EngineFindSemantics,
    reason: str,
) -> tuple[PhysicalPlanStep, ...]:
    steps: list[PhysicalPlanStep] = [
        PhysicalPlanStep(runtime="sql", operation="scan"),
        PhysicalPlanStep(runtime="sql", operation="filter"),
        PhysicalPlanStep(runtime="python", operation="sort", detail=reason),
    ]
    if semantics.projection is not None:
        steps.append(PhysicalPlanStep(runtime="python", operation="project"))
    if semantics.skip or semantics.limit is not None:
        steps.append(PhysicalPlanStep(runtime="python", operation="slice"))
    return tuple(steps)
