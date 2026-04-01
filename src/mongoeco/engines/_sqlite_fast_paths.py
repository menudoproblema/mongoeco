from __future__ import annotations

from collections.abc import Callable
from typing import Any

from mongoeco.core.query_plan import (
    EqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    NotCondition,
    OrCondition,
    AndCondition,
    QueryNode,
)
from mongoeco.types import Document, IndexKeySpec


def plan_fields(plan: QueryNode) -> set[str]:
    if isinstance(plan, MatchAll):
        return set()
    if hasattr(plan, "field"):
        field = getattr(plan, "field")
        if isinstance(field, str):
            return {field}
    if hasattr(plan, "clause"):
        return plan_fields(getattr(plan, "clause"))
    if hasattr(plan, "clauses"):
        fields: set[str] = set()
        for clause in getattr(plan, "clauses"):
            fields.update(plan_fields(clause))
        return fields
    return set()


def comparison_fields(plan: QueryNode) -> set[str]:
    if isinstance(plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)):
        return {plan.field}
    if isinstance(plan, NotCondition):
        return comparison_fields(plan.clause)
    if isinstance(plan, (AndCondition, OrCondition)):
        fields: set[str] = set()
        for clause in plan.clauses:
            fields.update(comparison_fields(clause))
        return fields
    return set()


def resolve_select_clause_for_scalar_sort(select_clause: str) -> str:
    if select_clause == "document":
        return "documents.document"
    if select_clause == "storage_key, document":
        return "documents.storage_key, documents.document"
    return select_clause


def build_select_statement_with_custom_order(
    *,
    select_clause: str,
    from_clause: str,
    namespace_sql: str,
    namespace_params: tuple[object, ...],
    where_fragment: tuple[str, list[object]],
    order_sql: str,
    skip: int = 0,
    limit: int | None = None,
) -> tuple[str, list[object]]:
    where_sql, where_params = where_fragment
    sql = (
        f"SELECT {select_clause} "
        f"FROM {from_clause} "
        f"WHERE {namespace_sql} AND ({where_sql}) "
        f"{order_sql}"
    )
    params: list[object] = [*namespace_params, *where_params]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(limit)
    elif skip:
        sql += " LIMIT -1"
    if skip:
        sql += " OFFSET ?"
        params.append(skip)
    return sql, params


def build_scalar_sort_select_sql(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    select_clause: str,
    sort: object | None,
    skip: int,
    limit: int | None,
    hint: str | IndexKeySpec | None,
    field_traverses_array_in_collection: Callable[[str, str, str], bool],
    field_contains_tagged_bytes_in_collection: Callable[[str, str, str], bool],
    field_contains_tagged_undefined_in_collection: Callable[[str, str, str], bool],
    field_has_uniform_scalar_sort_type_in_collection: Callable[[str, str, str], str | None],
    translate_query_plan_with_multikey: Callable[[str, str, QueryNode], tuple[str, list[object]]],
    find_scalar_sort_index: Callable[[str, str, str], object | None],
    lookup_collection_id: Callable[[str, str], int | None],
    quote_identifier: Callable[[str], str],
    value_expression_sql: Callable[[str], str],
) -> tuple[str, list[object]] | None:
    if hint is not None or not sort or len(sort) != 1:
        return None

    field, direction = sort[0]
    if direction not in (1, -1):
        return None
    if field_traverses_array_in_collection(db_name, coll_name, field):
        return None
    if field_contains_tagged_bytes_in_collection(db_name, coll_name, field):
        return None
    if field_contains_tagged_undefined_in_collection(db_name, coll_name, field):
        return None
    if field_has_uniform_scalar_sort_type_in_collection(db_name, coll_name, field) is None:
        return None

    order = "ASC" if direction == 1 else "DESC"
    where_fragment = translate_query_plan_with_multikey(db_name, coll_name, plan)

    index = find_scalar_sort_index(db_name, coll_name, field)
    if index is not None and index.get("scalar_physical_name"):
        collection_id = lookup_collection_id(db_name, coll_name)
        if collection_id is not None:
            return build_select_statement_with_custom_order(
                select_clause=resolve_select_clause_for_scalar_sort(select_clause),
                from_clause=(
                    f"scalar_index_entries INDEXED BY {quote_identifier(str(index['scalar_physical_name']))} "
                    "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
                    "AND documents.storage_key = scalar_index_entries.storage_key"
                ),
                namespace_sql="scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ?",
                namespace_params=(db_name, coll_name, collection_id, index["name"]),
                where_fragment=where_fragment,
                order_sql=(
                    f" ORDER BY scalar_index_entries.element_key {order}, "
                    "documents.storage_key ASC"
                ),
                skip=skip,
                limit=limit,
            )

    return build_select_statement_with_custom_order(
        select_clause=select_clause,
        from_clause="documents",
        namespace_sql="db_name = ? AND coll_name = ?",
        namespace_params=(db_name, coll_name),
        where_fragment=where_fragment,
        order_sql=f" ORDER BY {value_expression_sql(field)} {order}",
        skip=skip,
        limit=limit,
    )


def build_select_sql(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    select_clause: str,
    sort: object | None,
    skip: int,
    limit: int | None,
    hint: str | IndexKeySpec | None,
    build_scalar_sort_select_sql_fn: Callable[..., tuple[str, list[object]] | None],
    resolve_hint_index: Callable[[str, str, str | IndexKeySpec | None], object | None],
    translate_query_plan_with_multikey: Callable[[str, str, QueryNode], tuple[str, list[object]]],
    build_select_statement: Callable[..., object],
    quote_identifier: Callable[[str], str],
) -> tuple[str, list[object]]:
    scalar_sort_sql = build_scalar_sort_select_sql_fn(
        db_name,
        coll_name,
        plan,
        select_clause=select_clause,
        sort=sort,
        skip=skip,
        limit=limit,
        hint=hint,
    )
    if scalar_sort_sql is not None:
        return scalar_sort_sql
    hinted_index = resolve_hint_index(db_name, coll_name, hint)
    where_fragment = translate_query_plan_with_multikey(db_name, coll_name, plan)
    from_clause = "documents"
    if hinted_index is not None and hinted_index.get("physical_name") is not None:
        from_clause = (
            "documents INDEXED BY "
            f"{quote_identifier(str(hinted_index['physical_name']))}"
        )
    statement = build_select_statement(
        select_clause=select_clause,
        from_clause=from_clause,
        namespace_sql="db_name = ? AND coll_name = ?",
        namespace_params=(db_name, coll_name),
        where_fragment=where_fragment,
        sort=sort,
        skip=skip,
        limit=limit,
    )
    return statement.sql, list(statement.params)


def select_first_document_for_scalar_index(
    *,
    db_name: str,
    coll_name: str,
    field: str,
    value: Any,
    null_matches_undefined: bool,
    field_is_top_level_array_in_collection: Callable[[str, str, str], bool],
    find_scalar_index: Callable[[str, str, str], object | None],
    lookup_collection_id: Callable[[str, str], int | None],
    multikey_signatures_for_query_value: Callable[[object, bool], tuple[tuple[str, str], ...]],
    multikey_type_score: Callable[[str], int],
    quote_identifier: Callable[[str], str],
    fetchone: Callable[[str, tuple[object, ...]], tuple[str, object] | None],
    deserialize_document: Callable[[object], Document],
) -> tuple[str, Document] | None:
    if field_is_top_level_array_in_collection(db_name, coll_name, field):
        return None
    index = find_scalar_index(db_name, coll_name, field)
    if index is None or not index.get("scalar_physical_name"):
        return None
    collection_id = lookup_collection_id(db_name, coll_name)
    if collection_id is None:
        return None
    try:
        signatures = multikey_signatures_for_query_value(value, null_matches_undefined)
    except NotImplementedError:
        return None
    filters = " OR ".join(
        "(scalar_index_entries.type_score = ? AND scalar_index_entries.element_key = ?)"
        for _ in signatures
    )
    params: list[object] = [db_name, coll_name, collection_id, index["name"]]
    for element_type, element_key in signatures:
        params.extend([multikey_type_score(element_type), element_key])
    row = fetchone(
        "SELECT documents.storage_key, documents.document "
        f"FROM scalar_index_entries INDEXED BY {quote_identifier(str(index['scalar_physical_name']))} "
        "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
        "AND documents.storage_key = scalar_index_entries.storage_key "
        "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
        f"AND ({filters}) LIMIT 1",
        tuple(params),
    )
    if row is None:
        return None
    storage_key, document = row
    return storage_key, deserialize_document(document)


def select_first_document_for_scalar_range(
    *,
    db_name: str,
    coll_name: str,
    field: str,
    value: Any,
    operator: str,
    can_use_scalar_range_fast_path: Callable[[str, str, str, object], tuple[object, int, str] | None],
    lookup_collection_id: Callable[[str, str], int | None],
    quote_identifier: Callable[[str], str],
    fetchone: Callable[[str, tuple[object, ...]], tuple[str, object] | None],
    deserialize_document: Callable[[object], Document],
) -> tuple[str, Document] | None:
    scalar_path = can_use_scalar_range_fast_path(db_name, coll_name, field, value)
    if scalar_path is None:
        return None
    index, type_score, element_key = scalar_path
    if not index.get("scalar_physical_name"):
        return None
    collection_id = lookup_collection_id(db_name, coll_name)
    if collection_id is None:
        return None
    row = fetchone(
        "SELECT documents.storage_key, documents.document "
        f"FROM scalar_index_entries INDEXED BY {quote_identifier(str(index['scalar_physical_name']))} "
        "JOIN documents ON documents.db_name = ? AND documents.coll_name = ? "
        "AND documents.storage_key = scalar_index_entries.storage_key "
        "WHERE scalar_index_entries.collection_id = ? AND scalar_index_entries.index_name = ? "
        "AND scalar_index_entries.type_score = ? "
        f"AND scalar_index_entries.element_key {operator} ? "
        "LIMIT 1",
        (db_name, coll_name, collection_id, index["name"], type_score, element_key),
    )
    if row is None:
        return None
    storage_key, document = row
    return storage_key, deserialize_document(document)


def select_first_document_for_plan(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    storage_key_for_id: Callable[[Any], str],
    fetchone: Callable[[str, tuple[object, ...]], tuple[str, object] | None],
    deserialize_document: Callable[[object], Document],
    select_first_document_for_scalar_index_fn: Callable[[str, str, str, Any, bool], tuple[str, Document] | None],
    select_first_document_for_scalar_range_fn: Callable[[str, str, str, Any, str], tuple[str, Document] | None],
) -> tuple[str, Document] | None:
    if isinstance(plan, EqualsCondition) and "." not in plan.field:
        if plan.field == "_id":
            storage_key = storage_key_for_id(plan.value)
            row = fetchone(
                "SELECT storage_key, document FROM documents "
                "WHERE db_name = ? AND coll_name = ? AND storage_key = ?",
                (db_name, coll_name, storage_key),
            )
            if row is None:
                return None
            row_storage_key, document = row
            return row_storage_key, deserialize_document(document)
        selected = select_first_document_for_scalar_index_fn(
            db_name,
            coll_name,
            plan.field,
            plan.value,
            plan.null_matches_undefined,
        )
        if selected is not None:
            return selected
    if isinstance(plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)) and "." not in plan.field:
        operator = (
            ">" if isinstance(plan, GreaterThanCondition)
            else ">=" if isinstance(plan, GreaterThanOrEqualCondition)
            else "<" if isinstance(plan, LessThanCondition)
            else "<="
        )
        return select_first_document_for_scalar_range_fn(
            db_name,
            coll_name,
            plan.field,
            plan.value,
            operator,
        )
    return None
