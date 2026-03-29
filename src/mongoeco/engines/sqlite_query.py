import datetime
from typing import Any, assert_never
import uuid

from mongoeco.core.bson_ordering import SQLITE_SORT_BUCKET_WEIGHTS
from mongoeco.core.operators import CompiledUpdatePlan
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.json_compat import json_dumps_compact
from mongoeco.core.sql_translation import BaseSQLTranslator
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NotEqualsCondition,
    NotCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    BitwiseCondition,
    DeferredQueryNode,
    ExprCondition,
    is_concrete_query_node,
)
from mongoeco.types import SortSpec, Update


type SqlFragment = tuple[str, list[object]]


HANDLED_SQL_QUERY_NODE_TYPES: tuple[type[QueryNode], ...] = (
    MatchAll,
    DeferredQueryNode,
    EqualsCondition,
    NotEqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    InCondition,
    NotInCondition,
    ExistsCondition,
    TypeCondition,
    NotCondition,
    AllCondition,
    SizeCondition,
    ModCondition,
    RegexCondition,
    ElemMatchCondition,
    BitwiseCondition,
    ExprCondition,
    AndCondition,
    OrCondition,
)


class SQLiteQueryTranslator(BaseSQLTranslator):
    def translate_query_plan(self, plan: QueryNode) -> SqlFragment:
        return translate_query_plan(plan)

    def translate_sort_spec(self, sort: SortSpec | None) -> str:
        return translate_sort_spec(sort)


def _quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def json_path_for_field(field: str) -> str:
    parts = field.split(".")
    path = "$"
    for part in parts:
        if part.isdigit():
            path += f"[{part}]"
        else:
            path += f".{part}"
    return path


def _path_literal(field: str) -> str:
    return _quote_sql_string(json_path_for_field(field))


def _path_has_numeric_segment(path: str) -> bool:
    return any(part.isdigit() for part in path.split("."))


def path_array_prefixes(path: str) -> tuple[str, ...]:
    parts = path.split(".")
    prefixes: list[str] = []
    current: list[str] = []
    for index, part in enumerate(parts[:-1]):
        current.append(part)
        next_part = parts[index + 1]
        if not part.isdigit() and not next_part.isdigit():
            prefixes.append(".".join(current))
    return tuple(prefixes)


def _path_crosses_scalar_parent(document: object, path: str) -> bool:
    current = document
    parts = path.split(".")
    for part in parts[:-1]:
        if isinstance(current, dict):
            if part not in current:
                return False
            current = current[part]
            if current is None:
                return False
            if not isinstance(current, (dict, list)):
                return True
            continue
        if isinstance(current, list):
            if not part.isdigit():
                return True
            index = int(part)
            if index >= len(current):
                return False
            current = current[index]
            if current is None:
                return False
            if not isinstance(current, (dict, list)):
                return True
            continue
        return True
    return False


def _is_supported_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _is_codec_aware_value(value: object) -> bool:
    return DocumentCodec._is_tagged_value(DocumentCodec.encode(value))


def _is_translatable_equality_value(value: object) -> bool:
    return _is_supported_scalar(value) or _is_codec_aware_value(value)


def _normalize_comparable_value(value: object) -> tuple[str, object]:
    encoded = DocumentCodec.encode(value)
    if DocumentCodec._is_tagged_value(encoded):
        payload = encoded[DocumentCodec._MARKER]
        comparable = payload[DocumentCodec._VALUE]
        if isinstance(comparable, (str, int, float)):
            return payload[DocumentCodec._TYPE], comparable
        raise NotImplementedError("Unsupported tagged comparison value for SQL translation")
    if isinstance(value, bool):
        return "bool", int(value)
    if isinstance(value, str):
        return "string", value
    if isinstance(value, (int, float)):
        return "number", value
    raise NotImplementedError("Unsupported comparison value for SQL translation")


def _tagged_paths(field: str) -> tuple[str, str]:
    base = json_path_for_field(field)
    marker = DocumentCodec._MARKER
    return f'{base}."{marker}".{DocumentCodec._TYPE}', f'{base}."{marker}".{DocumentCodec._VALUE}'


def _json_each_tagged_paths() -> tuple[str, str]:
    marker = DocumentCodec._MARKER
    return f'$."{marker}".{DocumentCodec._TYPE}', f'$."{marker}".{DocumentCodec._VALUE}'


def type_expression_sql(field: str) -> str:
    type_path, _ = _tagged_paths(field)
    return f"COALESCE(json_extract(document, {_quote_sql_string(type_path)}), '')"


def value_expression_sql(field: str) -> str:
    _, value_path = _tagged_paths(field)
    return (
        f"COALESCE("
        f"json_extract(document, {_quote_sql_string(value_path)}), "
        f"json_extract(document, {_path_literal(field)})"
        f")"
    )


def index_expressions_sql(field: str) -> tuple[str, str]:
    return type_expression_sql(field), value_expression_sql(field)


def sort_type_expression_sql(field: str) -> str:
    path = _path_literal(field)
    tagged_type_path, _ = _tagged_paths(field)
    return (
        "CASE "
        f"WHEN json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null' THEN {SQLITE_SORT_BUCKET_WEIGHTS['null']} "
        f"WHEN json_type(document, {path}) IN ('integer', 'real') THEN {SQLITE_SORT_BUCKET_WEIGHTS['number']} "
        f"WHEN json_type(document, {path}) = 'text' THEN {SQLITE_SORT_BUCKET_WEIGHTS['string']} "
        f"WHEN json_type(document, {path}) = 'object' AND json_extract(document, {_quote_sql_string(tagged_type_path)}) IS NULL THEN {SQLITE_SORT_BUCKET_WEIGHTS['plain-object']} "
        f"WHEN json_type(document, {path}) = 'array' THEN {SQLITE_SORT_BUCKET_WEIGHTS['array']} "
        # bytes tagged values intentionally stay out of the SQL fast-path bucket:
        # SQLite cannot preserve the same relative ordering/range semantics as the
        # in-memory BSONComparator when bytes and UUID share bracket 6, so the
        # engine falls back to Python whenever tagged bytes are present.
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'uuid' THEN {SQLITE_SORT_BUCKET_WEIGHTS['uuid']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'objectid' THEN {SQLITE_SORT_BUCKET_WEIGHTS['objectid']} "
        f"WHEN json_type(document, {path}) IN ('true', 'false') THEN {SQLITE_SORT_BUCKET_WEIGHTS['bool']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'datetime' THEN {SQLITE_SORT_BUCKET_WEIGHTS['datetime']} "
        f"ELSE {SQLITE_SORT_BUCKET_WEIGHTS['fallback']} END"
    )


def _translate_codec_aware_equals(field: str, value: object) -> SqlFragment | None:
    encoded = DocumentCodec.encode(value)
    if not DocumentCodec._is_tagged_value(encoded):
        return None

    payload = encoded[DocumentCodec._MARKER]
    return (
        f"{type_expression_sql(field)} = ? AND {value_expression_sql(field)} = ?",
        [payload[DocumentCodec._TYPE], payload[DocumentCodec._VALUE]],
    )


def _translate_array_contains_scalar(
    field: str,
    value: object,
    *,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    path = _path_literal(field)
    if value is None:
        each_type_path, _ = _json_each_tagged_paths()
        undefined_clause = (
            f" OR (json_each.type = 'object' "
            f"AND COALESCE(json_extract(json_each.value, {_quote_sql_string(each_type_path)}), '') = 'undefined')"
            if null_matches_undefined
            else ""
        )
        return (
            f"(json_type(document, {path}) = 'array' "
            f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) "
            f"WHERE json_each.type = 'null'{undefined_clause}))",
            [],
        )
    if isinstance(value, bool):
        return (
            f"(json_type(document, {path}) = 'array' "
            f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) "
            f"WHERE json_each.type IN ('true', 'false') AND json_each.value = ?))",
            [int(value)],
        )
    if isinstance(value, str):
        return (
            f"(json_type(document, {path}) = 'array' "
            f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) "
            f"WHERE json_each.type = 'text' AND json_each.value = ?))",
            [value],
        )
    if isinstance(value, (int, float)):
        return (
            f"(json_type(document, {path}) = 'array' "
            f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) "
            f"WHERE json_each.type IN ('integer', 'real') AND json_each.value = ?))",
            [value],
        )
    return (
        f"(json_type(document, {path}) = 'array' "
        f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.value = ?))",
        [value],
    )


def _translate_array_contains_codec_aware(field: str, value: object) -> SqlFragment:
    path = _path_literal(field)
    each_type_path, each_value_path = _json_each_tagged_paths()
    payload = DocumentCodec.encode(value)[DocumentCodec._MARKER]
    return (
        f"(json_type(document, {path}) = 'array' "
        f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) "
        f"WHERE json_each.type = 'object' "
        f"AND COALESCE(json_extract(json_each.value, {_quote_sql_string(each_type_path)}), '') = ? "
        f"AND COALESCE(json_extract(json_each.value, {_quote_sql_string(each_value_path)}), json_extract(json_each.value, '$')) = ?))",
        [payload[DocumentCodec._TYPE], payload[DocumentCodec._VALUE]],
    )


def _translate_scalar_equals(
    field: str,
    value: object,
    *,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    path = _path_literal(field)
    tagged = _translate_codec_aware_equals(field, value)
    if tagged is not None:
        return tagged
    if value is None:
        undefined_clause = (
            f" OR {type_expression_sql(field)} = 'undefined'"
            if null_matches_undefined
            else ""
        )
        return (
            f"(json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null'{undefined_clause})",
            [],
        )
    if isinstance(value, bool):
        return f"(json_type(document, {path}) IN ('true', 'false') AND json_extract(document, {path}) = ?)", [int(value)]
    if isinstance(value, str):
        return f"(json_type(document, {path}) = 'text' AND json_extract(document, {path}) = ?)", [value]
    if isinstance(value, (int, float)):
        return f"(json_type(document, {path}) IN ('integer', 'real') AND json_extract(document, {path}) = ?)", [value]
    return f"({type_expression_sql(field)} = ? AND {value_expression_sql(field)} = ?)", ["", value]


def _translate_equals(
    field: str,
    value: object,
    *,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    if not _is_translatable_equality_value(value):
        raise NotImplementedError("Unsupported equality value for SQL translation")
    scalar_sql, scalar_params = _translate_scalar_equals(
        field,
        value,
        null_matches_undefined=null_matches_undefined,
    )
    array_sql, array_params = (
        _translate_array_contains_codec_aware(field, value)
        if _is_codec_aware_value(value)
        else _translate_array_contains_scalar(
            field,
            value,
            null_matches_undefined=null_matches_undefined,
        )
    )
    return f"({scalar_sql} OR {array_sql})", [*scalar_params, *array_params]


def _translate_equals_scalar_only(
    field: str,
    value: object,
    *,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    if not _is_translatable_equality_value(value):
        raise NotImplementedError("Unsupported equality value for SQL translation")
    return _translate_scalar_equals(
        field,
        value,
        null_matches_undefined=null_matches_undefined,
    )


def _translate_not_equals(field: str, value: object) -> SqlFragment:
    if not _is_translatable_equality_value(value):
        raise NotImplementedError("Unsupported equality value for SQL translation")
    path = _path_literal(field)
    if value is None:
        array_sql, array_params = _translate_array_contains_scalar(field, None)
        return (
            f"(json_type(document, {path}) IS NOT NULL "
            f"AND json_type(document, {path}) != 'null' "
            f"AND NOT {array_sql})",
            array_params,
        )
    scalar_sql, scalar_params = _translate_scalar_equals(field, value)
    array_sql, array_params = (
        _translate_array_contains_codec_aware(field, value)
        if _is_codec_aware_value(value)
        else _translate_array_contains_scalar(field, value)
    )
    return (
        f"(json_type(document, {path}) IS NULL "
        f"OR json_type(document, {path}) = 'null' "
        f"OR NOT ({scalar_sql} OR {array_sql}))",
        [*scalar_params, *array_params],
    )


def _comparison_type_order(value_type: str) -> int:
    if value_type == "number":
        return 2
    if value_type == "string":
        return 3
    if value_type == "bool":
        return 8
    # bytes tagged values deliberately use Python fallback in SQLite; see
    # SQLiteEngine._plan_requires_python_for_bytes.
    if value_type == "uuid":
        return 6
    if value_type == "objectid":
        return 7
    if value_type == "datetime":
        return 9
    raise NotImplementedError("Unsupported comparison type for SQL translation")


def _translate_same_type_comparison(operator: str, field: str, value: object) -> SqlFragment:
    value_type, comparable = _normalize_comparable_value(value)
    sql_operator = {
        ">": ">",
        ">=": ">=",
        "<": "<",
        "<=": "<=",
    }[operator]
    path = _path_literal(field)
    if value_type == "number":
        return (
            f"(json_type(document, {path}) IN ('integer', 'real') AND json_extract(document, {path}) {sql_operator} ?)",
            [comparable],
        )
    if value_type == "string":
        return (
            f"(json_type(document, {path}) = 'text' AND json_extract(document, {path}) {sql_operator} ?)",
            [comparable],
        )
    if value_type == "bool":
        return (
            f"(json_type(document, {path}) IN ('true', 'false') AND json_extract(document, {path}) {sql_operator} ?)",
            [comparable],
        )
    return (
        f"({type_expression_sql(field)} = ? AND {value_expression_sql(field)} {sql_operator} ?)",
        [value_type, comparable],
    )


def _translate_comparison(operator: str, field: str, value: object) -> SqlFragment:
    value_type, comparable = _normalize_comparable_value(value)
    sql_operator = {
        ">": ">",
        ">=": ">=",
        "<": "<",
        "<=": "<=",
    }[operator]
    path = _path_literal(field)
    document_order_sql = (
        "CASE "
        f"WHEN json_type(document, {path}) IS NULL THEN NULL "
        f"WHEN json_type(document, {path}) = 'null' THEN 1 "
        f"WHEN json_type(document, {path}) IN ('integer', 'real') THEN 2 "
        f"WHEN json_type(document, {path}) = 'text' THEN 3 "
        f"WHEN json_type(document, {path}) = 'object' AND json_extract(document, {_quote_sql_string(_tagged_paths(field)[0])}) IS NULL THEN 4 "
        f"WHEN json_type(document, {path}) = 'array' THEN 5 "
        f"WHEN json_extract(document, {_quote_sql_string(_tagged_paths(field)[0])}) = 'uuid' THEN 6 "
        f"WHEN json_extract(document, {_quote_sql_string(_tagged_paths(field)[0])}) = 'objectid' THEN 7 "
        f"WHEN json_type(document, {path}) IN ('true', 'false') THEN 8 "
        f"WHEN json_extract(document, {_quote_sql_string(_tagged_paths(field)[0])}) = 'datetime' THEN 9 "
        "ELSE 100 END"
    )
    if value_type == "number":
        same_type_sql = f"(json_type(document, {path}) IN ('integer', 'real') AND json_extract(document, {path}) {sql_operator} ?)"
    elif value_type == "string":
        same_type_sql = f"(json_type(document, {path}) = 'text' AND json_extract(document, {path}) {sql_operator} ?)"
    elif value_type == "bool":
        same_type_sql = f"(json_type(document, {path}) IN ('true', 'false') AND json_extract(document, {path}) {sql_operator} ?)"
    else:
        same_type_sql = f"({type_expression_sql(field)} = ? AND {value_expression_sql(field)} {sql_operator} ?)"
    cross_sql = ">" if operator in {">", ">="} else "<"
    if value_type in {"number", "string", "bool"}:
        return (
            f"(({document_order_sql} {cross_sql} ?) OR {same_type_sql})",
            [_comparison_type_order(value_type), comparable],
        )
    return (
        f"(({document_order_sql} {cross_sql} ?) OR {same_type_sql})",
        [_comparison_type_order(value_type), value_type, comparable],
    )


def _translate_membership(
    field: str,
    values: tuple[object, ...],
    *,
    negated: bool,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    if values and all(_is_codec_aware_value(value) for value in values):
        path = _path_literal(field)
        params: list[object] = []
        clauses: list[str] = []
        for value in values:
            scalar_sql, scalar_sql_params = _translate_scalar_equals(field, value)
            array_sql, array_sql_params = _translate_array_contains_codec_aware(field, value)
            clauses.append(f"({scalar_sql})")
            params.extend(scalar_sql_params)
            clauses.append(array_sql)
            params.extend(array_sql_params)
        wrapped = " OR ".join(clauses)
        if negated:
            return (
                f"(json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null' OR NOT ({wrapped}))",
                params,
            )
        return wrapped, params
    if not values or any(not _is_supported_scalar(value) and value is not None for value in values):
        raise NotImplementedError("Unsupported membership values for SQL translation")
    path = _path_literal(field)
    params: list[object] = []
    clauses: list[str] = []
    for value in values:
        scalar_sql, scalar_sql_params = _translate_scalar_equals(
            field,
            value,
            null_matches_undefined=null_matches_undefined,
        )
        array_sql, array_sql_params = _translate_array_contains_scalar(
            field,
            value,
            null_matches_undefined=null_matches_undefined,
        )
        clauses.append(f"({scalar_sql})")
        params.extend(scalar_sql_params)
        clauses.append(array_sql)
        params.extend(array_sql_params)
    wrapped = " OR ".join(clauses)
    if negated:
        return (
            f"(json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null' OR NOT ({wrapped}))",
            params,
        )
    return wrapped, params


def _translate_type_condition(plan: TypeCondition) -> SqlFragment:
    path = _path_literal(plan.field)
    tagged_type_sql = type_expression_sql(plan.field)
    tagged_value_sql = value_expression_sql(plan.field)
    scalar_clauses: list[str] = []
    array_clauses: list[str] = []
    params: list[object] = []

    for spec in plan.values:
        if isinstance(spec, bool):
            raise NotImplementedError("Unsupported $type spec for SQL translation")
        if isinstance(spec, int):
            if spec == 1:
                scalar_clauses.append(f"json_type(document, {path}) = 'real'")
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type = 'real')"
                )
            elif spec in {2}:
                scalar_clauses.append(f"json_type(document, {path}) = 'text'")
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type = 'text')"
                )
            elif spec == 3:
                scalar_clauses.append(
                    f"(json_type(document, {path}) = 'object' AND {tagged_type_sql} = '')"
                )
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) "
                    f"WHERE json_each.type = 'object' AND COALESCE(json_extract(json_each.value, '$.\"$mongoeco\".type'), '') = '')"
                )
            elif spec == 4:
                scalar_clauses.append(f"json_type(document, {path}) = 'array'")
            elif spec == 5:
                scalar_clauses.append(f"{tagged_type_sql} IN ('bytes', 'uuid')")
            elif spec == 7:
                scalar_clauses.append(f"{tagged_type_sql} = 'objectid'")
            elif spec == 8:
                scalar_clauses.append(f"json_type(document, {path}) IN ('true', 'false')")
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type IN ('true', 'false'))"
                )
            elif spec == 9:
                scalar_clauses.append(f"{tagged_type_sql} = 'datetime'")
            elif spec == 10:
                scalar_clauses.append(f"json_type(document, {path}) = 'null'")
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type = 'null')"
                )
            elif spec == 11:
                raise NotImplementedError("Regex $type is not translatable to SQLite")
            elif spec == 16:
                scalar_clauses.append(
                    f"(json_type(document, {path}) = 'integer' AND json_extract(document, {path}) BETWEEN -2147483648 AND 2147483647)"
                )
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) "
                    f"WHERE json_each.type = 'integer' AND json_each.value BETWEEN -2147483648 AND 2147483647)"
                )
            elif spec == 18:
                scalar_clauses.append(
                    f"(json_type(document, {path}) = 'integer' AND (json_extract(document, {path}) < -2147483648 OR json_extract(document, {path}) > 2147483647))"
                )
                array_clauses.append(
                    f"EXISTS (SELECT 1 FROM json_each(document, {path}) "
                    f"WHERE json_each.type = 'integer' AND (json_each.value < -2147483648 OR json_each.value > 2147483647))"
                )
            else:
                raise NotImplementedError("Unsupported numeric $type code for SQL translation")
            continue

        if not isinstance(spec, str):
            raise NotImplementedError("Unsupported $type spec for SQL translation")
        normalized = spec.strip()
        if normalized == "number":
            scalar_clauses.append(f"json_type(document, {path}) IN ('integer', 'real')")
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type IN ('integer', 'real'))"
            )
        elif normalized == "double":
            scalar_clauses.append(f"json_type(document, {path}) = 'real'")
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type = 'real')"
            )
        elif normalized == "string":
            scalar_clauses.append(f"json_type(document, {path}) = 'text'")
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type = 'text')"
            )
        elif normalized == "object":
            scalar_clauses.append(
                f"(json_type(document, {path}) = 'object' AND {tagged_type_sql} = '')"
            )
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) "
                f"WHERE json_each.type = 'object' AND COALESCE(json_extract(json_each.value, '$.\"$mongoeco\".type'), '') = '')"
            )
        elif normalized == "array":
            scalar_clauses.append(f"json_type(document, {path}) = 'array'")
        elif normalized == "binData":
            scalar_clauses.append(f"{tagged_type_sql} IN ('bytes', 'uuid')")
        elif normalized == "objectId":
            scalar_clauses.append(f"{tagged_type_sql} = 'objectid'")
        elif normalized == "bool":
            scalar_clauses.append(f"json_type(document, {path}) IN ('true', 'false')")
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type IN ('true', 'false'))"
            )
        elif normalized == "date":
            scalar_clauses.append(f"{tagged_type_sql} = 'datetime'")
        elif normalized == "null":
            scalar_clauses.append(f"json_type(document, {path}) = 'null'")
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.type = 'null')"
            )
        elif normalized == "int":
            scalar_clauses.append(
                f"(json_type(document, {path}) = 'integer' AND json_extract(document, {path}) BETWEEN -2147483648 AND 2147483647)"
            )
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) "
                f"WHERE json_each.type = 'integer' AND json_each.value BETWEEN -2147483648 AND 2147483647)"
            )
        elif normalized == "long":
            scalar_clauses.append(
                f"(json_type(document, {path}) = 'integer' AND (json_extract(document, {path}) < -2147483648 OR json_extract(document, {path}) > 2147483647))"
            )
            array_clauses.append(
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) "
                f"WHERE json_each.type = 'integer' AND (json_each.value < -2147483648 OR json_each.value > 2147483647))"
            )
        elif normalized == "undefined":
            scalar_clauses.append(f"{tagged_type_sql} = 'undefined'")
        elif normalized == "regex":
            raise NotImplementedError("Regex $type is not translatable to SQLite")
        else:
            raise NotImplementedError("Unsupported $type alias for SQL translation")

    clauses = [f"({clause})" for clause in scalar_clauses]
    if array_clauses:
        clauses.extend(
            f"(json_type(document, {path}) = 'array' AND ({clause}))"
            for clause in array_clauses
        )
    return " OR ".join(clauses), params


def translate_sort_spec(sort: SortSpec | None) -> str:
    if not sort:
        return ""
    parts: list[str] = []
    for field, direction in sort:
        order = "ASC" if direction == 1 else "DESC"
        parts.append(f"{sort_type_expression_sql(field)} {order}")
        parts.append(f"{value_expression_sql(field)} {order}")
    return " ORDER BY " + ", ".join(parts)


def translate_update_spec(update_spec: Update, *, current_document: dict[str, Any] | None = None) -> SqlFragment:
    from mongoeco.core.operators import UpdateEngine

    try:
        plan = UpdateEngine.compile_update_plan(update_spec)
    except Exception as exc:  # pragma: no cover - compatibility shim
        raise NotImplementedError(str(exc)) from exc
    return translate_compiled_update_plan(plan, current_document=current_document)


def translate_compiled_update_plan(
    plan: CompiledUpdatePlan,
    *,
    current_document: dict[str, Any] | None = None,
) -> SqlFragment:
    supported_operators = {"$set", "$unset"}
    if any(operator.operator not in supported_operators for operator in plan.compiled_operators):
        raise NotImplementedError("Unsupported update operator for SQL translation")

    expression = "document"
    params: list[object] = []

    set_instructions = next(
        (operator.instructions for operator in plan.compiled_operators if operator.operator == "$set"),
        (),
    )
    if set_instructions:
        args: list[str] = []
        for instruction in set_instructions:
            path = instruction.path.raw
            value = instruction.value
            if _path_has_numeric_segment(path):
                raise NotImplementedError("Array index paths require Python update fallback")
            if current_document is not None and "." in path and _path_crosses_scalar_parent(current_document, path):
                raise NotImplementedError("Scalar parent requires Python update fallback")
            args.append(_path_literal(path))
            args.append("json(?)")
            params.append(json_dumps_compact(DocumentCodec.encode(value), sort_keys=False))
        expression = f"json_set({expression}, {', '.join(args)})"

    unset_instructions = next(
        (operator.instructions for operator in plan.compiled_operators if operator.operator == "$unset"),
        (),
    )
    if unset_instructions:
        paths: list[str] = []
        for instruction in unset_instructions:
            path = instruction.path.raw
            if _path_has_numeric_segment(path):
                raise NotImplementedError("Array index paths require Python update fallback")
            paths.append(_path_literal(path))
        expression = f"json_remove({expression}, {', '.join(paths)})"

    return expression, params


def translate_query_plan(plan: QueryNode) -> SqlFragment:
    if not is_concrete_query_node(plan):
        raise TypeError(f"Unsupported query plan node: {type(plan)!r}")
    match plan:
        case MatchAll():
            return "1 = 1", []
        case DeferredQueryNode(issue=issue):
            raise NotImplementedError(f"Deferred query nodes cannot be translated to SQL: {issue.message}")
        case EqualsCondition(field=field, value=value, null_matches_undefined=null_matches_undefined):
            if not _is_translatable_equality_value(value):
                raise NotImplementedError("Unsupported equality value for SQL translation")
            return _translate_equals(
                field,
                value,
                null_matches_undefined=null_matches_undefined,
            )
        case NotEqualsCondition(field=field, value=value):
            if not _is_translatable_equality_value(value):
                raise NotImplementedError("Unsupported inequality value for SQL translation")
            return _translate_not_equals(field, value)
        case GreaterThanCondition(field=field, value=value):
            return _translate_comparison(">", field, value)
        case GreaterThanOrEqualCondition(field=field, value=value):
            return _translate_comparison(">=", field, value)
        case LessThanCondition(field=field, value=value):
            return _translate_comparison("<", field, value)
        case LessThanOrEqualCondition(field=field, value=value):
            return _translate_comparison("<=", field, value)
        case InCondition(field=field, values=values, null_matches_undefined=null_matches_undefined):
            return _translate_membership(
                field,
                values,
                negated=False,
                null_matches_undefined=null_matches_undefined,
            )
        case NotInCondition(field=field, values=values):
            return _translate_membership(field, values, negated=True)
        case ExistsCondition(field=field, value=value):
            path = _path_literal(field)
            return (
                f"json_type(document, {path}) IS NOT NULL" if value else f"json_type(document, {path}) IS NULL",
                [],
            )
        case TypeCondition():
            return _translate_type_condition(plan)
        case NotCondition(clause=clause):
            clause_sql, clause_params = translate_query_plan(clause)
            return f"NOT COALESCE(({clause_sql}), 0)", clause_params
        case AllCondition() | SizeCondition() | ModCondition() | RegexCondition() | ElemMatchCondition():
            raise NotImplementedError("Query operator not yet translated to SQL")
        case BitwiseCondition() | ExprCondition():
            raise NotImplementedError("Query operator not yet translated to SQL")
        case AndCondition(clauses=clauses):
            sql_clauses: list[str] = []
            params: list[object] = []
            for clause in clauses:
                clause_sql, clause_params = translate_query_plan(clause)
                sql_clauses.append(f"({clause_sql})")
                params.extend(clause_params)
            return " AND ".join(sql_clauses), params
        case OrCondition(clauses=clauses):
            sql_clauses: list[str] = []
            params: list[object] = []
            for clause in clauses:
                clause_sql, clause_params = translate_query_plan(clause)
                sql_clauses.append(f"({clause_sql})")
                params.extend(clause_params)
            return " OR ".join(sql_clauses), params
        case _:
            assert_never(plan)
