from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mongoeco.compat import MongoDialect
from mongoeco.core.query_plan import (
    EqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
)
from mongoeco.engines.semantic_core import EngineFindSemantics
from mongoeco.engines.sqlite_planner import SQLiteReadExecutionPlan, compile_sqlite_read_execution_plan
from mongoeco.types import IndexKeySpec


def build_scalar_indexed_top_level_equals_sql(
    *,
    db_name: str,
    coll_name: str,
    field: str,
    value: Any,
    index_name: str,
    physical_name: str,
    limit: int | None,
    null_matches_undefined: bool,
    lookup_collection_id: Callable[[str, str], int | None],
    multikey_signatures_for_query_value: Callable[[object, bool], tuple[tuple[str, str], ...]],
    multikey_type_score: Callable[[str], int],
    quote_identifier: Callable[[str], str],
) -> tuple[str, tuple[object, ...]] | None:
    collection_id = lookup_collection_id(db_name, coll_name)
    if collection_id is None:
        return None
    try:
        signatures = multikey_signatures_for_query_value(value, null_matches_undefined)
    except NotImplementedError:
        return None
    if not signatures:
        return None
    filters = " OR ".join(
        "(scalar_index_entries.type_score = ? AND scalar_index_entries.element_key = ?)"
        for _ in signatures
    )
    params: list[object] = [db_name, coll_name, collection_id, index_name]
    for element_type, element_key in signatures:
        params.extend([multikey_type_score(element_type), element_key])
    sql = (
        "SELECT documents.document "
        f"FROM scalar_index_entries INDEXED BY {quote_identifier(physical_name)} "
        "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
        "AND documents.storage_key = scalar_index_entries.storage_key "
        "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
        f"AND ({filters})"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql, tuple(params)


def build_scalar_indexed_top_level_range_sql(
    *,
    db_name: str,
    coll_name: str,
    value: Any,
    operator: str,
    index_name: str,
    physical_name: str,
    limit: int | None,
    can_use_scalar_range_fast_path: Callable[[str, str, object], tuple[object, int, str] | None],
    lookup_collection_id: Callable[[str, str], int | None],
    quote_identifier: Callable[[str], str],
) -> tuple[str, tuple[object, ...]] | None:
    scalar_path = can_use_scalar_range_fast_path(db_name, coll_name, value)
    if scalar_path is None:
        return None
    _index, type_score, element_key = scalar_path
    collection_id = lookup_collection_id(db_name, coll_name)
    if collection_id is None:
        return None
    sql = (
        "SELECT documents.document "
        f"FROM scalar_index_entries INDEXED BY {quote_identifier(physical_name)} "
        "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
        "AND documents.storage_key = scalar_index_entries.storage_key "
        "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
        "AND scalar_index_entries.type_score = ? "
        f"AND scalar_index_entries.element_key {operator} ?"
    )
    if limit is not None:
        sql += f" LIMIT {int(limit)}"
    return sql, (db_name, coll_name, collection_id, index_name, type_score, element_key)


def compile_read_execution_plan(
    *,
    db_name: str,
    coll_name: str,
    semantics: EngineFindSemantics,
    select_clause: str,
    hint: str | IndexKeySpec | None,
    dialect_requires_python_fallback: Callable[[MongoDialect], bool],
    plan_has_array_traversing_paths: Callable[[str, str, object], bool],
    plan_requires_python_for_dbref_paths: Callable[[str, str, object], bool],
    plan_requires_python_for_array_comparisons: Callable[[str, str, object], bool],
    plan_requires_python_for_undefined: Callable[[str, str, object], bool],
    plan_requires_python_for_bytes: Callable[[str, str, object], bool],
    sort_requires_python: Callable[[str, str, object, object | None], bool],
    build_select_sql: Callable[..., tuple[str, tuple[object, ...]]],
) -> SQLiteReadExecutionPlan:
    return compile_sqlite_read_execution_plan(
        db_name=db_name,
        coll_name=coll_name,
        semantics=semantics,
        select_clause=select_clause,
        hint=hint,
        dialect_requires_python_fallback=dialect_requires_python_fallback,
        plan_has_array_traversing_paths=plan_has_array_traversing_paths,
        plan_requires_python_for_dbref_paths=plan_requires_python_for_dbref_paths,
        plan_requires_python_for_array_comparisons=plan_requires_python_for_array_comparisons,
        plan_requires_python_for_undefined=plan_requires_python_for_undefined,
        plan_requires_python_for_bytes=plan_requires_python_for_bytes,
        sort_requires_python=sort_requires_python,
        build_select_sql=build_select_sql,
    )


def plan_find_semantics_sync(
    *,
    db_name: str,
    coll_name: str,
    semantics: EngineFindSemantics,
    storage_key_for_id: Callable[[object], str],
    find_scalar_fast_path_index: Callable[[str, str], object | None],
    field_is_top_level_array_in_collection: Callable[[str, str], bool],
    build_equals_sql: Callable[[str, str, object, str, str, int | None, bool], tuple[str, tuple[object, ...]] | None],
    build_range_sql: Callable[[str, str, object, str, str, str, int | None], tuple[str, tuple[object, ...]] | None],
    compile_read_plan: Callable[[EngineFindSemantics, str | IndexKeySpec | None], SQLiteReadExecutionPlan],
) -> SQLiteReadExecutionPlan:
    query_plan = semantics.query_plan
    if semantics.collation is None and semantics.sort is None and semantics.skip == 0:
        if (
            isinstance(query_plan, EqualsCondition)
            and "." not in query_plan.field
            and (semantics.limit is None or semantics.limit >= 1)
        ):
            field = query_plan.field
            if field == "_id":
                storage_key = storage_key_for_id(query_plan.value)
                sql = (
                    "SELECT document FROM documents "
                    "WHERE db_name = ? AND coll_name = ? AND storage_key = ?"
                )
                if semantics.limit is not None:
                    sql += f" LIMIT {int(semantics.limit)}"
                params = (db_name, coll_name, storage_key)
                return SQLiteReadExecutionPlan(
                    semantics=semantics,
                    strategy="sql",
                    execution_lineage=(),
                    physical_plan=(),
                    use_sql=True,
                    sql=sql,
                    params=params,
                )

            index = find_scalar_fast_path_index(db_name, coll_name, field)
            if index is not None and not field_is_top_level_array_in_collection(db_name, coll_name, field):
                indexed_sql = build_equals_sql(
                    db_name,
                    coll_name,
                    query_plan.value,
                    str(index["name"]),
                    str(index["scalar_physical_name"]),
                    semantics.limit,
                    query_plan.null_matches_undefined,
                )
                if indexed_sql is not None:
                    sql, params = indexed_sql
                    return SQLiteReadExecutionPlan(
                        semantics=semantics,
                        strategy="sql",
                        execution_lineage=(),
                        physical_plan=(),
                        use_sql=True,
                        sql=sql,
                        params=params,
                    )

        if isinstance(query_plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)):
            operator = (
                ">" if isinstance(query_plan, GreaterThanCondition)
                else ">=" if isinstance(query_plan, GreaterThanOrEqualCondition)
                else "<" if isinstance(query_plan, LessThanCondition)
                else "<="
            )
            index = find_scalar_fast_path_index(db_name, coll_name, query_plan.field)
            if index is not None:
                indexed_sql = build_range_sql(
                    db_name,
                    coll_name,
                    query_plan.value,
                    operator,
                    str(index["name"]),
                    str(index["scalar_physical_name"]),
                    semantics.limit,
                )
                if indexed_sql is not None:
                    sql, params = indexed_sql
                    return SQLiteReadExecutionPlan(
                        semantics=semantics,
                        strategy="sql",
                        execution_lineage=(),
                        physical_plan=(),
                        use_sql=True,
                        sql=sql,
                        params=params,
                    )

    return compile_read_plan(semantics, semantics.hint)
