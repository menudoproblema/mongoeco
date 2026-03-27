from dataclasses import dataclass
from typing import Any, Callable

from mongoeco.engines.semantic_core import EngineFindSemantics
from mongoeco.types import IndexKeySpec


@dataclass(frozen=True, slots=True)
class SQLiteReadExecutionPlan:
    semantics: EngineFindSemantics
    use_sql: bool
    sql: str | None = None
    params: tuple[object, ...] = ()
    fallback_reason: str | None = None

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
    if dialect_requires_python_fallback(semantics.dialect):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
            fallback_reason="Custom dialect requires Python fallback",
        )
    if plan_has_array_traversing_paths(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
            fallback_reason="Array traversal requires Python fallback",
        )
    if plan_requires_python_for_array_comparisons(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
            fallback_reason="Top-level array comparisons require Python fallback",
        )
    if plan_requires_python_for_undefined(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
            fallback_reason="Tagged undefined requires Python fallback",
        )
    if plan_requires_python_for_bytes(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
            fallback_reason="Tagged bytes require Python fallback",
        )
    if sort_requires_python(db_name, coll_name, semantics.query_plan, semantics.sort):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
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
        )
    except NotImplementedError as exc:
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            use_sql=False,
            fallback_reason=str(exc),
        )
    return SQLiteReadExecutionPlan(
        semantics=semantics,
        use_sql=True,
        sql=sql,
        params=tuple(params),
    )
