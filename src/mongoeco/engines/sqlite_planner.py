from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum

from mongoeco.core.query_plan import QueryNode
from mongoeco.engines.semantic_core import EngineFindSemantics, EngineReadExecutionPlan
from mongoeco.types import ExecutionLineageStep, IndexKeySpec, PhysicalPlanStep


class SQLitePushdownMode(StrEnum):
    SQL_EXACT = "sql-exact"
    SQL_PREFILTER = "sql-prefilter"
    PYTHON = "python"


class SQLitePlanOwner(StrEnum):
    SQL = "sql"
    SQL_THEN_PYTHON = "sql-then-python"
    PYTHON = "python"
    NONE = "none"


@dataclass(frozen=True, slots=True, kw_only=True)
class SQLiteReadExecutionPlan(EngineReadExecutionPlan):
    use_sql: bool = False
    sql: str | None = None
    params: tuple[object, ...] = ()
    apply_python_sort: bool = False
    apply_python_residual: bool = False
    mode: SQLitePushdownMode | None = None
    residual_query_plan: QueryNode | None = None
    filter_owner: SQLitePlanOwner | None = None
    sort_owner: SQLitePlanOwner | None = None
    window_owner: SQLitePlanOwner | None = None
    collectors: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        self._validate_field_types()
        mode = self._resolved_mode()
        self._validate_mode_contract(mode)
        object.__setattr__(self, "mode", mode)
        sort = getattr(self.semantics, "sort", None)
        skip = getattr(self.semantics, "skip", 0)
        limit = getattr(self.semantics, "limit", None)
        self._assign_default_owners(mode, sort=sort, skip=skip, limit=limit)
        self._validate_owner_contract(mode, sort=sort, skip=skip, limit=limit)

    def _validate_field_types(self) -> None:
        if not isinstance(self.use_sql, bool):
            message = "SQLite plan use_sql must be a bool"
            raise TypeError(message)
        if not isinstance(self.apply_python_sort, bool) or not isinstance(
            self.apply_python_residual,
            bool,
        ):
            message = "SQLite Python phase flags must be bools"
            raise TypeError(message)
        if self.sql is not None and (not isinstance(self.sql, str) or not self.sql):
            message = "SQLite plan SQL must be a non-empty string or None"
            raise TypeError(message)
        if not isinstance(self.params, tuple):
            message = "SQLite plan params must be a tuple"
            raise TypeError(message)
        if not isinstance(self.collectors, tuple) or not all(
            isinstance(item, str) and item for item in self.collectors
        ):
            message = "SQLite plan collectors must be non-empty string identifiers"
            raise TypeError(message)
        if len(set(self.collectors)) != len(self.collectors):
            message = "SQLite plan collector identifiers must be unique"
            raise ValueError(message)

    def _resolved_mode(self) -> SQLitePushdownMode:
        mode = self.mode
        if mode is None:
            mode = (
                SQLitePushdownMode.PYTHON
                if not self.use_sql
                else SQLitePushdownMode.SQL_PREFILTER
                if self.apply_python_residual
                else SQLitePushdownMode.SQL_EXACT
            )
        if not isinstance(mode, SQLitePushdownMode):
            message = "SQLite pushdown mode must be SQLitePushdownMode"
            raise TypeError(message)
        return mode

    def _validate_mode_contract(self, mode: SQLitePushdownMode) -> None:
        if mode is SQLitePushdownMode.PYTHON and self.use_sql:
            message = "Python SQLite plan cannot use SQL"
            raise ValueError(message)
        if mode is not SQLitePushdownMode.PYTHON and not self.use_sql:
            message = "SQL SQLite plan must use SQL"
            raise ValueError(message)
        if mode is SQLitePushdownMode.PYTHON and self.sql is not None:
            message = "Python SQLite plan cannot carry SQL"
            raise ValueError(message)
        if mode is not SQLitePushdownMode.PYTHON and self.sql is None:
            message = "SQL SQLite plan requires an SQL fragment"
            raise ValueError(message)
        if mode is SQLitePushdownMode.SQL_PREFILTER and not self.apply_python_residual:
            message = "SQLite prefilter plan requires a Python residual"
            raise ValueError(message)
        if (
            mode is SQLitePushdownMode.SQL_PREFILTER
            and self.residual_query_plan is None
        ):
            message = "SQLite prefilter plan requires a residual query plan"
            raise ValueError(message)
        if mode is SQLitePushdownMode.SQL_EXACT and self.apply_python_residual:
            message = "exact SQLite plan cannot carry a residual"
            raise ValueError(message)
        if (
            mode is not SQLitePushdownMode.SQL_PREFILTER
            and self.residual_query_plan is not None
        ):
            message = "only SQLite prefilter plans may carry a residual query plan"
            raise ValueError(message)

    def _assign_default_owners(
        self,
        mode: SQLitePushdownMode,
        *,
        sort: object,
        skip: int,
        limit: int | None,
    ) -> None:
        object.__setattr__(
            self,
            "filter_owner",
            self.filter_owner
            or (
                SQLitePlanOwner.PYTHON
                if mode is SQLitePushdownMode.PYTHON
                else SQLitePlanOwner.SQL_THEN_PYTHON
                if mode is SQLitePushdownMode.SQL_PREFILTER
                else SQLitePlanOwner.SQL
            ),
        )
        object.__setattr__(
            self,
            "sort_owner",
            self.sort_owner
            or (
                SQLitePlanOwner.NONE
                if not sort
                else SQLitePlanOwner.PYTHON
                if mode is SQLitePushdownMode.PYTHON
                or self.apply_python_sort
                or self.apply_python_residual
                else SQLitePlanOwner.SQL
            ),
        )
        object.__setattr__(
            self,
            "window_owner",
            self.window_owner
            or (
                SQLitePlanOwner.NONE
                if not skip and limit is None
                else SQLitePlanOwner.PYTHON
                if mode is SQLitePushdownMode.PYTHON
                or self.apply_python_sort
                or self.apply_python_residual
                else SQLitePlanOwner.SQL
            ),
        )

    def _validate_owner_contract(
        self,
        mode: SQLitePushdownMode,
        *,
        sort: object,
        skip: int,
        limit: int | None,
    ) -> None:
        for owner in (self.filter_owner, self.sort_owner, self.window_owner):
            if not isinstance(owner, SQLitePlanOwner):
                message = "SQLite plan owner must be SQLitePlanOwner"
                raise TypeError(message)
        expected_filter_owner = {
            SQLitePushdownMode.PYTHON: SQLitePlanOwner.PYTHON,
            SQLitePushdownMode.SQL_PREFILTER: SQLitePlanOwner.SQL_THEN_PYTHON,
            SQLitePushdownMode.SQL_EXACT: SQLitePlanOwner.SQL,
        }[mode]
        if self.filter_owner is not expected_filter_owner:
            message = "SQLite filter owner contradicts pushdown mode"
            raise ValueError(message)
        if (
            (self.apply_python_sort or self.apply_python_residual)
            and sort
            and self.sort_owner is not SQLitePlanOwner.PYTHON
        ):
            message = "SQLite Python finalization requires Python sort ownership"
            raise ValueError(message)
        if (
            (self.apply_python_sort or self.apply_python_residual)
            and (skip or limit is not None)
            and self.window_owner is not SQLitePlanOwner.PYTHON
        ):
            message = "SQLite residual work requires Python window ownership"
            raise ValueError(message)
        if not sort and self.sort_owner is not SQLitePlanOwner.NONE:
            message = "SQLite plan without sort cannot assign sort ownership"
            raise ValueError(message)
        if not skip and limit is None and self.window_owner is not SQLitePlanOwner.NONE:
            message = "SQLite plan without a window cannot assign window ownership"
            raise ValueError(message)

    def require_sql(self) -> tuple[str, tuple[object, ...]]:
        if not self.use_sql or self.sql is None:
            raise NotImplementedError(
                self.fallback_reason or "Python fallback required"
            )
        return self.sql, self.params

    def to_document(self) -> dict[str, object | None]:
        return {
            "mode": self.strategy,
            "contractMode": self.mode.value,
            "usesSqlRuntime": self.use_sql,
            "sqlPredicateExact": self.mode is not SQLitePushdownMode.PYTHON,
            "residualRequired": self.apply_python_residual,
            "pythonSort": self.apply_python_sort,
            "filterOwner": self.filter_owner.value,
            "sortOwner": self.sort_owner.value,
            "windowOwner": self.window_owner.value,
            "sqlFragment": self.sql,
            "parameterCount": len(self.params),
            "residualPlanPresent": self.residual_query_plan is not None,
            "collectors": list(self.collectors),
            "fallbackReason": self.fallback_reason,
        }


def compile_sqlite_read_execution_plan(  # noqa: PLR0911, PLR0913
    *,
    db_name: str,
    coll_name: str,
    semantics: EngineFindSemantics,
    select_clause: str = "document",
    hint: str | IndexKeySpec | None,
    dialect_requires_python_fallback: Callable[[object], bool],
    plan_has_array_traversing_paths: Callable[[str, str, object], bool],
    plan_requires_python_for_dbref_paths: Callable[[str, str, object], bool],
    plan_requires_python_for_array_comparisons: Callable[[str, str, object], bool],
    plan_requires_python_for_undefined: Callable[[str, str, object], bool],
    plan_requires_python_for_bytes: Callable[[str, str, object], bool],
    plan_requires_python_for_decimal_numeric: Callable[
        [str, str, object], bool
    ] = lambda *_args: False,
    sort_requires_python: Callable[[str, str, object, object], bool],
    build_select_sql: Callable[..., tuple[str, list[object]]],
) -> SQLiteReadExecutionPlan:
    if semantics.collation is not None:
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "Collation requires Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "collation"),
            use_sql=False,
            fallback_reason="Collation requires Python fallback",
        )
    if dialect_requires_python_fallback(semantics.dialect):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "Custom dialect requires Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "dialect"),
            use_sql=False,
            fallback_reason="Custom dialect requires Python fallback",
        )
    if plan_has_array_traversing_paths(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "Array traversal requires Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "array-traversal"),
            use_sql=False,
            fallback_reason="Array traversal requires Python fallback",
        )
    if plan_requires_python_for_dbref_paths(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "DBRef subfield access requires Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "dbref-subfield"),
            use_sql=False,
            fallback_reason="DBRef subfield access requires Python fallback",
        )
    if plan_requires_python_for_array_comparisons(
        db_name, coll_name, semantics.query_plan
    ):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "Top-level array comparisons require Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "array-comparison"),
            use_sql=False,
            fallback_reason="Top-level array comparisons require Python fallback",
        )
    if plan_requires_python_for_undefined(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "Tagged undefined requires Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "undefined"),
            use_sql=False,
            fallback_reason="Tagged undefined requires Python fallback",
        )
    if plan_requires_python_for_bytes(db_name, coll_name, semantics.query_plan):
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(
                semantics, "Tagged bytes require Python fallback"
            ),
            physical_plan=_python_physical_plan(semantics, "bytes"),
            use_sql=False,
            fallback_reason="Tagged bytes require Python fallback",
        )
    if plan_requires_python_for_decimal_numeric(
        db_name, coll_name, semantics.query_plan
    ):
        reason = "Decimal BSON equality requires Python fallback"
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="python",
            execution_lineage=_python_lineage(semantics, reason),
            physical_plan=_python_physical_plan(semantics, "decimal-numeric"),
            use_sql=False,
            fallback_reason=reason,
        )
    try:
        requires_python_sort = sort_requires_python(
            db_name,
            coll_name,
            semantics.query_plan,
            semantics.sort,
        )
    except (NotImplementedError, TypeError):
        requires_python_sort = True
    if requires_python_sort:
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
                execution_lineage=_python_lineage(
                    semantics, "Sort requires Python fallback"
                ),
                physical_plan=_python_physical_plan(semantics, "sort"),
                use_sql=False,
                fallback_reason="Sort requires Python fallback",
            )
        return SQLiteReadExecutionPlan(
            semantics=semantics,
            strategy="hybrid",
            execution_lineage=_hybrid_lineage(
                semantics, "Sort requires Python fallback"
            ),
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
        lineage.append(
            ExecutionLineageStep(runtime="sql", phase="sort", detail="engine pushdown")
        )
    if semantics.skip or semantics.limit is not None:
        lineage.append(
            ExecutionLineageStep(runtime="sql", phase="slice", detail="engine pushdown")
        )
    if semantics.projection is not None:
        lineage.append(
            ExecutionLineageStep(
                runtime="python", phase="project", detail="semantic core projection"
            )
        )
    return tuple(lineage)


def _python_lineage(
    semantics: EngineFindSemantics, reason: str
) -> tuple[ExecutionLineageStep, ...]:
    lineage = [
        ExecutionLineageStep(runtime="python", phase="scan", detail=reason),
        ExecutionLineageStep(runtime="python", phase="filter", detail="semantic core"),
    ]
    if semantics.sort:
        lineage.append(
            ExecutionLineageStep(runtime="python", phase="sort", detail="semantic core")
        )
    if semantics.skip or semantics.limit is not None:
        lineage.append(
            ExecutionLineageStep(
                runtime="python", phase="slice", detail="semantic core"
            )
        )
    if semantics.projection is not None:
        lineage.append(
            ExecutionLineageStep(
                runtime="python", phase="project", detail="semantic core"
            )
        )
    return tuple(lineage)


def _hybrid_lineage(
    semantics: EngineFindSemantics, reason: str
) -> tuple[ExecutionLineageStep, ...]:
    lineage = [
        ExecutionLineageStep(runtime="sql", phase="scan", detail="engine pushdown"),
        ExecutionLineageStep(runtime="sql", phase="filter", detail="engine pushdown"),
    ]
    if semantics.sort:
        lineage.append(
            ExecutionLineageStep(runtime="python", phase="sort", detail=reason)
        )
    if semantics.skip or semantics.limit is not None:
        lineage.append(
            ExecutionLineageStep(
                runtime="python", phase="slice", detail="semantic core"
            )
        )
    if semantics.projection is not None:
        lineage.append(
            ExecutionLineageStep(
                runtime="python", phase="project", detail="semantic core projection"
            )
        )
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
