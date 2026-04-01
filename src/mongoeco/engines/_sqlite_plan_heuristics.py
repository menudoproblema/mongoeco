from __future__ import annotations

from collections.abc import Callable

from mongoeco.core.query_plan import QueryNode


def plan_has_array_traversing_paths(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    plan_fields: Callable[[QueryNode], set[str]],
    field_traverses_array_in_collection: Callable[[str, str, str], bool],
) -> bool:
    return any(
        field_traverses_array_in_collection(db_name, coll_name, field)
        for field in plan_fields(plan)
    )


def plan_requires_python_for_dbref_paths(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    plan_fields: Callable[[QueryNode], set[str]],
    field_traverses_dbref_in_collection: Callable[[str, str, str], bool],
) -> bool:
    return any(
        field_traverses_dbref_in_collection(db_name, coll_name, field)
        for field in plan_fields(plan)
    )


def plan_requires_python_for_tagged_type(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    comparison_fields: Callable[[QueryNode], set[str]],
    field_contains_tagged_type_in_collection: Callable[[str, str, str], bool],
) -> bool:
    return any(
        field_contains_tagged_type_in_collection(db_name, coll_name, field)
        for field in comparison_fields(plan)
    )


def plan_requires_python_for_array_comparisons(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    comparison_fields: Callable[[QueryNode], set[str]],
    field_is_top_level_array_in_collection: Callable[[str, str, str], bool],
) -> bool:
    return any(
        field_is_top_level_array_in_collection(db_name, coll_name, field)
        for field in comparison_fields(plan)
    )


def sort_requires_python(
    *,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    sort: object | None,
    plan_has_array_traversing_paths: Callable[[str, str, QueryNode], bool],
    translate_query_plan: Callable[[QueryNode], tuple[str, list[object]]],
    field_traverses_array_in_collection: Callable[[str, str, str], bool],
    field_contains_tagged_bytes_in_collection: Callable[[str, str, str], bool],
    field_contains_tagged_undefined_in_collection: Callable[[str, str, str], bool],
    fetchone: Callable[[str, tuple[object, ...]], object | None],
    type_expression_sql: Callable[[str], str],
    json_path_for_field: Callable[[str], str],
) -> bool:
    if not sort:
        return False
    if plan_has_array_traversing_paths(db_name, coll_name, plan):
        return True
    where_sql, params = translate_query_plan(plan)
    for field, _direction in sort:
        if field_traverses_array_in_collection(db_name, coll_name, field):
            return True
        if field_contains_tagged_bytes_in_collection(db_name, coll_name, field):
            return True
        if field_contains_tagged_undefined_in_collection(db_name, coll_name, field):
            return True
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = fetchone(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ? AND ({where_sql})
              AND (
                json_type(document, {path_literal}) = 'array'
                OR (
                    json_type(document, {path_literal}) = 'object'
                    AND {type_expression_sql(field)} = ''
                )
              )
            LIMIT 1
            """,
            (db_name, coll_name, *params),
        )
        if row is not None:
            return True
    return False
