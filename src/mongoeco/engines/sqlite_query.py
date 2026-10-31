from __future__ import annotations

import math

from typing import TYPE_CHECKING, Any

from mongoeco.core.bson_ordering import SQLITE_SORT_BUCKET_WEIGHTS
from mongoeco.core.bson_values import binary_parts
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.identity import root_id_needs_semantic_lookup
from mongoeco.core.json_compat import json_dumps_compact
from mongoeco.core.operators import CompiledUpdatePlan, UpdateEngine
from mongoeco.core.query_plan import (
    AllCondition,
    AndCondition,
    BitwiseCondition,
    DeferredQueryNode,
    ElemMatchCondition,
    EqualsCondition,
    ExistsCondition,
    ExprCondition,
    GeoIntersectsCondition,
    GeoWithinCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
    JsonSchemaCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    ModCondition,
    NearCondition,
    NotCondition,
    NotEqualsCondition,
    NotInCondition,
    OrCondition,
    QueryNode,
    RegexCondition,
    SizeCondition,
    TypeCondition,
    WhereCondition,
    _normalize_type_specifier,
    is_concrete_query_node,
)
from mongoeco.core.sql_translation import BaseSQLTranslator
from mongoeco.errors import OperationFailure


if TYPE_CHECKING:
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
    GeoWithinCondition,
    GeoIntersectsCondition,
    NearCondition,
    ElemMatchCondition,
    BitwiseCondition,
    ExprCondition,
    WhereCondition,
    JsonSchemaCondition,
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
    parts = field.split('.')
    path = '$'
    for part in parts:
        if part.isdigit():
            path += f'[{part}]'
        else:
            path += f'.{part}'
    return path


def _path_literal(field: str) -> str:
    return _quote_sql_string(json_path_for_field(field))


def parse_safe_literal_regex(
    pattern: str, options: str
) -> tuple[str, str, bool] | None:
    ignore_case = False
    if options:
        if options != 'i':
            return None
        ignore_case = True
    anchored_start = pattern.startswith('^')
    body = pattern[1:] if anchored_start else pattern
    anchored_end = body.endswith('$')
    if anchored_end:
        body = body[:-1]
    if anchored_start and body.endswith('.*'):
        body = body[:-2]
        anchored_end = False
    if not body:
        return None

    literal_chars: list[str] = []
    index = 0
    metacharacters = set('.^$*+?{}[]()|')
    while index < len(body):
        char = body[index]
        if char == '\\':
            index += 1
            if index >= len(body):
                return None
            literal_chars.append(body[index])
        elif char in metacharacters:
            return None
        else:
            literal_chars.append(char)
        index += 1

    literal = ''.join(literal_chars)
    if anchored_start and anchored_end and literal:
        return ('exact', literal, ignore_case)
    if anchored_start and literal:
        return ('prefix', literal, ignore_case)
    if anchored_end and literal:
        return ('suffix', literal, ignore_case)
    return ('contains', literal, ignore_case) if literal else None


def _path_has_numeric_segment(path: str) -> bool:
    return any(part.isdigit() for part in path.split('.'))


def _path_uses_dbref_special_segment(path: str) -> bool:
    return any(part in {'$ref', '$id', '$db'} for part in path.split('.'))


def path_array_prefixes(path: str) -> tuple[str, ...]:
    parts = path.split('.')
    prefixes: list[str] = []
    current: list[str] = []
    for part in parts[:-1]:
        current.append(part)
        if not part.isdigit():
            prefixes.append('.'.join(current))
    return tuple(prefixes)


def _path_crosses_scalar_parent(document: object, path: str) -> bool:
    current = document
    parts = path.split('.')
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
    # bytes and Binary(…, 0) have different historical storage tags. Tag-only
    # predicates omit equal BSON values; retain canonical Python evaluation.
    if binary_parts(value) is not None:
        return False
    encoded = DocumentCodec.encode(value)
    if DocumentCodec._is_tagged_value(encoded):
        payload = encoded[DocumentCodec._MARKER]
        return payload[DocumentCodec._VALUE] is None or isinstance(
            payload[DocumentCodec._VALUE], (str, int, float, bool)
        )
    return _is_supported_scalar(value)


def _normalize_comparable_value(value: object) -> tuple[str, object]:
    encoded = DocumentCodec.encode(value)
    if DocumentCodec._is_tagged_value(encoded):
        payload = encoded[DocumentCodec._MARKER]
        comparable = payload[DocumentCodec._VALUE]
        if isinstance(comparable, (str, int, float)):
            return payload[DocumentCodec._TYPE], comparable
        raise NotImplementedError(
            'Unsupported tagged comparison value for SQL translation'
        )
    if isinstance(value, bool):
        return 'bool', int(value)
    if type(value) is str:
        return 'string', value
    if isinstance(value, (int, float)):
        return 'number', value
    raise NotImplementedError(
        'Unsupported comparison value for SQL translation'
    )


def _tagged_paths(field: str) -> tuple[str, str]:
    base = json_path_for_field(field)
    marker = DocumentCodec._MARKER
    return (
        f'{base}."{marker}".{DocumentCodec._TYPE}',
        f'{base}."{marker}".{DocumentCodec._VALUE}',
    )


def _json_each_tagged_paths() -> tuple[str, str]:
    marker = DocumentCodec._MARKER
    return (
        f'$."{marker}".{DocumentCodec._TYPE}',
        f'$."{marker}".{DocumentCodec._VALUE}',
    )


def type_expression_sql(field: str) -> str:
    type_path, _ = _tagged_paths(field)
    return (
        f"COALESCE(json_extract(document, {_quote_sql_string(type_path)}), '')"
    )


def value_expression_sql(field: str) -> str:
    _, value_path = _tagged_paths(field)
    return (
        f'COALESCE('
        f'json_extract(document, {_quote_sql_string(value_path)}), '
        f'json_extract(document, {_path_literal(field)})'
        f')'
    )


def index_expressions_sql(field: str) -> tuple[str, str]:
    return type_expression_sql(field), value_expression_sql(field)


def sort_type_expression_sql(field: str) -> str:
    path = _path_literal(field)
    tagged_type_path, _ = _tagged_paths(field)
    return (
        'CASE '
        f"WHEN json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null' THEN {SQLITE_SORT_BUCKET_WEIGHTS['null']} "
        f"WHEN json_type(document, {path}) IN ('integer', 'real') THEN {SQLITE_SORT_BUCKET_WEIGHTS['number']} "
        f"WHEN json_type(document, {path}) = 'text' THEN {SQLITE_SORT_BUCKET_WEIGHTS['string']} "
        f"WHEN json_type(document, {path}) = 'object' AND json_extract(document, {_quote_sql_string(tagged_type_path)}) IS NULL THEN {SQLITE_SORT_BUCKET_WEIGHTS['plain-object']} "
        f"WHEN json_type(document, {path}) = 'array' THEN {SQLITE_SORT_BUCKET_WEIGHTS['array']} "
        # bytes tagged values intentionally stay out of the SQL fast-path bucket:
        # SQLite cannot preserve the same relative ordering/range semantics as the
        # in-memory BSONComparator when bytes and UUID share bracket 6, so the
        # engine falls back to Python whenever tagged bytes are present.
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'binary' THEN {SQLITE_SORT_BUCKET_WEIGHTS['binary']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'uuid' THEN {SQLITE_SORT_BUCKET_WEIGHTS['uuid']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'objectid' THEN {SQLITE_SORT_BUCKET_WEIGHTS['objectid']} "
        f"WHEN json_type(document, {path}) IN ('true', 'false') THEN {SQLITE_SORT_BUCKET_WEIGHTS['bool']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'datetime' THEN {SQLITE_SORT_BUCKET_WEIGHTS['datetime']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'timestamp' THEN {SQLITE_SORT_BUCKET_WEIGHTS['timestamp']} "
        f"WHEN json_extract(document, {_quote_sql_string(tagged_type_path)}) = 'regex' THEN {SQLITE_SORT_BUCKET_WEIGHTS['regex']} "
        f'ELSE {SQLITE_SORT_BUCKET_WEIGHTS["fallback"]} END'
    )


def _translate_codec_aware_equals(
    field: str, value: object
) -> SqlFragment | None:
    encoded = DocumentCodec.encode(value)
    if not DocumentCodec._is_tagged_value(encoded):
        return None

    payload = encoded[DocumentCodec._MARKER]
    return (
        f'{type_expression_sql(field)} = ? AND {value_expression_sql(field)} = ?',
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
            f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) '
            f"WHERE json_each.type = 'null'{undefined_clause}))",
            [],
        )
    if isinstance(value, bool):
        return (
            f"(json_type(document, {path}) = 'array' "
            f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) '
            f"WHERE json_each.type IN ('true', 'false') AND json_each.value = ?))",
            [int(value)],
        )
    if type(value) is str:
        return (
            f"(json_type(document, {path}) = 'array' "
            f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) '
            f"WHERE json_each.type = 'text' AND json_each.value = ?))",
            [value],
        )
    if isinstance(value, (int, float)):
        return (
            f"(json_type(document, {path}) = 'array' "
            f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) '
            f"WHERE json_each.type IN ('integer', 'real') AND json_each.value = ?))",
            [value],
        )
    return (
        f"(json_type(document, {path}) = 'array' "
        f"AND EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE json_each.value = ?))",
        [value],
    )


def _translate_json_each_scalar_match(
    path: str, value: object
) -> SqlFragment | None:
    path_literal = _quote_sql_string(path)
    if value is None:
        return (
            f"EXISTS (SELECT 1 FROM json_each(document, {path_literal}) WHERE json_each.type = 'null')",
            [],
        )
    if isinstance(value, bool):
        return (
            f'EXISTS (SELECT 1 FROM json_each(document, {path_literal}) '
            f"WHERE json_each.type IN ('true', 'false') AND json_each.value = ?)",
            [int(value)],
        )
    if type(value) is str:
        return (
            f'EXISTS (SELECT 1 FROM json_each(document, {path_literal}) '
            f"WHERE json_each.type = 'text' AND json_each.value = ?)",
            [value],
        )
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return (
            f'EXISTS (SELECT 1 FROM json_each(document, {path_literal}) '
            f"WHERE json_each.type IN ('integer', 'real') AND json_each.value = ?)",
            [value],
        )
    return None


def _translate_json_each_scalar_comparison(
    operator: str, value: object
) -> SqlFragment | None:
    expected_type, comparable = _normalize_comparable_value(value)
    if expected_type == 'number':
        return (
            f"json_each.type IN ('integer', 'real') AND json_each.value {operator} ?",
            [comparable],
        )
    if expected_type == 'string':
        return (
            f"json_each.type = 'text' AND json_each.value {operator} ?",
            [comparable],
        )
    if expected_type == 'bool':
        return (
            f"json_each.type IN ('true', 'false') AND json_each.value {operator} ?",
            [comparable],
        )
    return None


def _translate_array_contains_codec_aware(
    field: str, value: object
) -> SqlFragment:
    path = _path_literal(field)
    each_type_path, each_value_path = _json_each_tagged_paths()
    payload = DocumentCodec.encode(value)[DocumentCodec._MARKER]
    return (
        f"(json_type(document, {path}) = 'array' "
        f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) '
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
    if not _is_translatable_equality_value(value):
        message = "Complex BSON equality requires semantic fallback"
        raise NotImplementedError(message)
    path = _path_literal(field)
    tagged = _translate_codec_aware_equals(field, value)
    if tagged is not None:
        return tagged
    if value is None:
        undefined_clause = (
            f" OR {type_expression_sql(field)} = 'undefined'"
            if null_matches_undefined
            else ''
        )
        return (
            f"(json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null'{undefined_clause})",
            [],
        )
    if isinstance(value, bool):
        return (
            f"(json_type(document, {path}) IN ('true', 'false') AND json_extract(document, {path}) = ?)",
            [int(value)],
        )
    if type(value) is str:
        return (
            f"(json_type(document, {path}) = 'text' AND json_extract(document, {path}) = ?)",
            [value],
        )
    if isinstance(value, (int, float)):
        return (
            f"(json_type(document, {path}) IN ('integer', 'real') AND json_extract(document, {path}) = ?)",
            [value],
        )
    return (
        f'({type_expression_sql(field)} = ? AND {value_expression_sql(field)} = ?)',
        ['', value],
    )


def _translate_equals(
    field: str,
    value: object,
    *,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    if not _is_translatable_equality_value(value):
        raise NotImplementedError(
            'Unsupported equality value for SQL translation'
        )
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
    return f'({scalar_sql} OR {array_sql})', [*scalar_params, *array_params]


def _translate_all_condition(
    field: str, values: tuple[object, ...]
) -> SqlFragment:
    if '.' in field:
        raise NotImplementedError(
            'Only top-level $all paths are translated to SQL'
        )
    if not values:
        raise NotImplementedError(
            'Empty $all values are not translated to SQL'
        )
    path = json_path_for_field(field)
    path_literal = _quote_sql_string(path)
    clauses: list[str] = []
    params: list[object] = []
    for value in values:
        if isinstance(value, dict):
            raise NotImplementedError(
                '$all with operator values is not translated to SQL'
            )
        fragment = _translate_json_each_scalar_match(path, value)
        if fragment is None:
            raise NotImplementedError(
                '$all only supports simple scalar values for SQL translation'
            )
        sql, sql_params = fragment
        clauses.append(sql)
        params.extend(sql_params)
    array_clause = (
        f"(json_type(document, {path_literal}) = 'array' AND "
        + ' AND '.join(f'({clause})' for clause in clauses)
        + ')'
    )
    if len(values) == 1:
        scalar_sql, scalar_params = _translate_scalar_equals(
            field, values[0], null_matches_undefined=False
        )
        return f'({array_clause} OR ({scalar_sql}))', [*params, *scalar_params]
    return array_clause, params


def _translate_json_each_value_plan(plan: QueryNode) -> SqlFragment | None:
    match plan:
        case EqualsCondition(field='value', value=value):
            fragment = _translate_json_each_scalar_match('$', value)
            if fragment is None:
                return None
            sql, params = fragment
            prefix = "EXISTS (SELECT 1 FROM json_each(document, '$') WHERE "
            suffix = ')'
            if sql.startswith(prefix) and sql.endswith(suffix):
                return sql[len(prefix) : -len(suffix)], params
            return None
        case GreaterThanCondition(field='value', value=value):
            return _translate_json_each_scalar_comparison('>', value)
        case GreaterThanOrEqualCondition(field='value', value=value):
            return _translate_json_each_scalar_comparison('>=', value)
        case LessThanCondition(field='value', value=value):
            return _translate_json_each_scalar_comparison('<', value)
        case LessThanOrEqualCondition(field='value', value=value):
            return _translate_json_each_scalar_comparison('<=', value)
        case RegexCondition(field='value', pattern=pattern, options=options):
            safe_regex = parse_safe_literal_regex(pattern, options)
            if safe_regex is None:
                return None
            mode, literal, ignore_case = safe_regex
            if ignore_case and not literal.isascii():
                return None
            value_expr = (
                'lower(json_each.value)' if ignore_case else 'json_each.value'
            )
            literal_param = literal.lower() if ignore_case else literal
            if mode == 'exact':
                return (
                    f"json_each.type = 'text' AND {value_expr} = ?",
                    [literal_param],
                )
            if mode == 'prefix':
                return (
                    f"json_each.type = 'text' AND substr({value_expr}, 1, length(?)) = ?",
                    [literal_param, literal_param],
                )
            if mode == 'suffix':
                return (
                    f"json_each.type = 'text' AND substr({value_expr}, -length(?)) = ?",
                    [literal_param, literal_param],
                )
            if mode == 'contains':
                return (
                    f"json_each.type = 'text' AND instr({value_expr}, ?) > 0",
                    [literal_param],
                )
            return None
        case _:
            return None


def _translate_elem_match_condition(
    field: str,
    compiled_plan: QueryNode | None,
    *,
    wrap_value: bool,
) -> SqlFragment:
    if '.' in field:
        raise NotImplementedError(
            'Only top-level $elemMatch paths are translated to SQL'
        )
    if compiled_plan is None or not wrap_value:
        raise NotImplementedError(
            'Only scalar $elemMatch shapes are translated to SQL'
        )
    path = _quote_sql_string(json_path_for_field(field))
    predicate = _translate_json_each_value_plan(compiled_plan)
    if predicate is None:
        raise NotImplementedError(
            'Unsupported $elemMatch scalar predicate for SQL translation'
        )
    predicate_sql, predicate_params = predicate
    return (
        f"(json_type(document, {path}) = 'array' "
        f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE {predicate_sql}))',
        predicate_params,
    )


def _require_sql_identity_comparison(field: object, *values: object) -> None:
    if (
        isinstance(field, str)
        and (field == "_id" or field.startswith("_id."))
        and any(root_id_needs_semantic_lookup(value) for value in values)
    ):
        message = "Identity aliases require BSON equality"
        raise NotImplementedError(message)


def _translate_equals_scalar_only(
    field: str,
    value: object,
    *,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    _require_sql_identity_comparison(field, value)
    if not _is_translatable_equality_value(value):
        raise NotImplementedError(
            'Unsupported equality value for SQL translation'
        )
    return _translate_scalar_equals(
        field,
        value,
        null_matches_undefined=null_matches_undefined,
    )


def _translate_not_equals(field: str, value: object) -> SqlFragment:
    if not _is_translatable_equality_value(value):
        raise NotImplementedError(
            'Unsupported equality value for SQL translation'
        )
    path = _path_literal(field)
    if value is None:
        array_sql, array_params = _translate_array_contains_scalar(field, None)
        return (
            f'(json_type(document, {path}) IS NOT NULL '
            f"AND json_type(document, {path}) != 'null' "
            f'AND NOT {array_sql})',
            array_params,
        )
    scalar_sql, scalar_params = _translate_scalar_equals(field, value)
    array_sql, array_params = (
        _translate_array_contains_codec_aware(field, value)
        if _is_codec_aware_value(value)
        else _translate_array_contains_scalar(field, value)
    )
    return (
        f'(json_type(document, {path}) IS NULL '
        f"OR json_type(document, {path}) = 'null' "
        f'OR NOT ({scalar_sql} OR {array_sql}))',
        [*scalar_params, *array_params],
    )


def _translate_size_condition(field: str, value: int) -> SqlFragment:
    path = _path_literal(field)
    return (
        f"(json_type(document, {path}) = 'array' AND json_array_length(document, {path}) = ?)",
        [value],
    )


def _translate_same_type_comparison(
    operator: str, field: str, value: object
) -> SqlFragment:
    value_type, comparable = _normalize_comparable_value(value)
    sql_operator = {
        '>': '>',
        '>=': '>=',
        '<': '<',
        '<=': '<=',
    }[operator]
    path = _path_literal(field)
    if value_type == 'number':
        return (
            f"(json_type(document, {path}) IN ('integer', 'real') AND json_extract(document, {path}) {sql_operator} ?)",
            [comparable],
        )
    if value_type == 'string':
        return (
            f"(json_type(document, {path}) = 'text' AND json_extract(document, {path}) {sql_operator} ?)",
            [comparable],
        )
    if value_type == 'bool':
        return (
            f"(json_type(document, {path}) IN ('true', 'false') AND json_extract(document, {path}) {sql_operator} ?)",
            [comparable],
        )
    return (
        f'({type_expression_sql(field)} = ? AND {value_expression_sql(field)} {sql_operator} ?)',
        [value_type, comparable],
    )


def _translate_scalar_or_array_same_type_comparison(
    operator: str, field: str, value: object
) -> SqlFragment:
    scalar_sql, scalar_params = _translate_same_type_comparison(
        operator, field, value
    )
    array_predicate = _translate_json_each_scalar_comparison(operator, value)
    if array_predicate is None:
        raise NotImplementedError(
            'Unsupported array-aware comparison value for SQL translation'
        )
    array_predicate_sql, array_predicate_params = array_predicate
    path = _path_literal(field)
    array_sql = (
        f"(json_type(document, {path}) = 'array' "
        f'AND EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE {array_predicate_sql}))'
    )
    return f'({scalar_sql} OR {array_sql})', [
        *scalar_params,
        *array_predicate_params,
    ]


def _translate_comparison(
    operator: str, field: str, value: object
) -> SqlFragment:
    # MQL comparisons are type-bracketed. The array-aware leaf uses the same
    # family for scalars and terminal elements; unsupported representations
    # retain the engine's guarded Python fallback.
    return _translate_scalar_or_array_same_type_comparison(operator, field, value)


def _translate_membership(
    field: str,
    values: tuple[object, ...],
    *,
    negated: bool,
    null_matches_undefined: bool = False,
) -> SqlFragment:
    if any(binary_parts(value) is not None for value in values):
        message = 'Binary membership requires semantic fallback'
        raise NotImplementedError(message)
    if values and all(_is_codec_aware_value(value) for value in values):
        path = _path_literal(field)
        params: list[object] = []
        clauses: list[str] = []
        for value in values:
            scalar_sql, scalar_sql_params = _translate_scalar_equals(
                field, value
            )
            array_sql, array_sql_params = (
                _translate_array_contains_codec_aware(field, value)
            )
            clauses.append(f'({scalar_sql})')
            params.extend(scalar_sql_params)
            clauses.append(array_sql)
            params.extend(array_sql_params)
        wrapped = ' OR '.join(clauses)
        if negated:
            return (
                f"(json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null' OR NOT ({wrapped}))",
                params,
            )
        return wrapped, params
    if not values or any(
        not _is_supported_scalar(value) and value is not None
        for value in values
    ):
        raise NotImplementedError(
            'Unsupported membership values for SQL translation'
        )
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
        clauses.append(f'({scalar_sql})')
        params.extend(scalar_sql_params)
        clauses.append(array_sql)
        params.extend(array_sql_params)
    wrapped = ' OR '.join(clauses)
    if negated:
        if any(value is None for value in values):
            return f'NOT ({wrapped})', params
        return (
            f"(json_type(document, {path}) IS NULL OR json_type(document, {path}) = 'null' OR NOT ({wrapped}))",
            params,
        )
    return wrapped, params


def _type_alias_predicate(alias, *, json_type_sql, value_sql, tag_sql):
    """One BSON family predicate, usable for a field and one array element."""
    simple = {
        'string': "= 'text'",
        'bool': "IN ('true', 'false')",
        'null': "= 'null'",
        'array': "= 'array'",
    }
    if alias in simple:
        return f"{json_type_sql} {simple[alias]}"
    tagged = {
        'binData': "IN ('bytes', 'binary', 'uuid')",
        'objectId': "= 'objectid'",
        'date': "= 'datetime'",
        'undefined': "= 'undefined'",
        'decimal': "IN ('decimal', 'decimal128', 'decimal128_public')",
    }
    if alias in tagged:
        return f"{tag_sql} {tagged[alias]}"
    if alias == 'object':
        return (
            f"({json_type_sql} = 'object' AND "
            f"{tag_sql} IN ('', 'dbref', 'son', 'dict'))"
        )
    if alias == 'double':
        return f"({json_type_sql} = 'real' OR {tag_sql} = 'double')"
    if alias == 'int':
        return (
            f"(({json_type_sql} = 'integer' AND {value_sql} "
            f"BETWEEN -2147483648 AND 2147483647) OR {tag_sql} = 'int32')"
        )
    if alias == 'long':
        return (
            f"(({json_type_sql} = 'integer' AND ({value_sql} < -2147483648 "
            f"OR {value_sql} > 2147483647)) OR {tag_sql} = 'int64')"
        )
    message = 'Unsupported $type alias for SQL translation'
    raise NotImplementedError(message)


def _translate_type_condition(plan: TypeCondition) -> SqlFragment:
    path = _path_literal(plan.field)
    try:
        aliases = plan.aliases or frozenset(
            alias
            for spec in plan.values
            for alias in _normalize_type_specifier(spec)
        )
    except (OperationFailure, ValueError) as error:
        raise NotImplementedError(str(error)) from error
    clauses = []
    element_tag_path, _ = _json_each_tagged_paths()
    element_tag = (
        "CASE WHEN json_each.type = 'object' THEN "
        f"COALESCE(json_extract(json_each.value, {_quote_sql_string(element_tag_path)}), '') "
        "ELSE '' END"
    )
    for alias in sorted(aliases):
        scalar = _type_alias_predicate(
            alias,
            json_type_sql=f'json_type(document, {path})',
            value_sql=f'json_extract(document, {path})',
            tag_sql=type_expression_sql(plan.field),
        )
        clauses.append(f'({scalar})')
        if alias != 'array':
            element = _type_alias_predicate(
                alias,
                json_type_sql='json_each.type',
                value_sql='json_each.value',
                tag_sql=element_tag,
            )
            clauses.append(
                f"(json_type(document, {path}) = 'array' AND "
                f"EXISTS (SELECT 1 FROM json_each(document, {path}) WHERE {element}))"
            )
    return ' OR '.join(clauses), []


def translate_sort_spec(sort: SortSpec | None) -> str:
    if not sort:
        return ''
    parts: list[str] = []
    for field, direction in sort:
        order = 'ASC' if direction == 1 else 'DESC'
        parts.append(f'{sort_type_expression_sql(field)} {order}')
        parts.append(f'{value_expression_sql(field)} {order}')
    return ' ORDER BY ' + ', '.join(parts)


def translate_update_spec(
    update_spec: Update, *, current_document: dict[str, Any] | None = None
) -> SqlFragment:
    try:
        plan = UpdateEngine.compile_update_plan(update_spec)
    except Exception as exc:  # pragma: no cover - compatibility shim
        raise NotImplementedError(str(exc)) from exc
    if not isinstance(plan, CompiledUpdatePlan):
        raise NotImplementedError(
            'Aggregation pipeline updates require Python update fallback'
        )
    return translate_compiled_update_plan(
        plan, current_document=current_document
    )


def translate_compiled_update_plan(
    plan: CompiledUpdatePlan,
    *,
    current_document: dict[str, Any] | None = None,
) -> SqlFragment:
    if _json_mutation_loses_nonfinite(current_document):
        message = 'Non-finite values require Python JSON update fallback'
        raise NotImplementedError(message)
    if any(
        segment.kind in {"positional", "all_positional", "filtered_positional"}
        for operator in plan.compiled_operators
        for instruction in operator.instructions
        for segment in instruction.path.segments
    ):
        message = 'Positional update paths require Python update fallback'
        raise NotImplementedError(message)
    supported_operators = {'$set', '$unset'}
    if any(
        operator.operator not in supported_operators
        for operator in plan.compiled_operators
    ):
        raise NotImplementedError(
            'Unsupported update operator for SQL translation'
        )

    expression = 'document'
    params: list[object] = []

    set_instructions = next(
        (
            operator.instructions
            for operator in plan.compiled_operators
            if operator.operator == '$set'
        ),
        (),
    )
    if set_instructions:
        args: list[str] = []
        for instruction in set_instructions:
            path = instruction.path.raw
            value = instruction.value
            if _json_mutation_loses_nonfinite(value):
                message = 'Non-finite values require Python JSON update fallback'
                raise NotImplementedError(message)
            if _path_has_numeric_segment(path):
                raise NotImplementedError(
                    'Array index paths require Python update fallback'
                )
            if (
                current_document is not None
                and '.' in path
                and _path_crosses_scalar_parent(current_document, path)
            ):
                raise NotImplementedError(
                    'Scalar parent requires Python update fallback'
                )
            args.append(_path_literal(path))
            args.append('json(?)')
            params.append(
                json_dumps_compact(
                    DocumentCodec.encode(value), sort_keys=False
                )
            )
        expression = f'json_set({expression}, {", ".join(args)})'

    unset_instructions = next(
        (
            operator.instructions
            for operator in plan.compiled_operators
            if operator.operator == '$unset'
        ),
        (),
    )
    if unset_instructions:
        paths: list[str] = []
        for instruction in unset_instructions:
            path = instruction.path.raw
            if _path_has_numeric_segment(path):
                raise NotImplementedError(
                    'Array index paths require Python update fallback'
                )
            paths.append(_path_literal(path))
        expression = f'json_remove({expression}, {", ".join(paths)})'

    return expression, params


def _json_mutation_loses_nonfinite(value: object) -> bool:
    # JSON1 accepts JSON5 NaN tokens but rewrites them as null, including values
    # outside the modified path. Inspect codec output for BSON scalar containers
    # (Code scopes, DBRefs, numeric wrappers) without maintaining another registry.
    if isinstance(value, dict):
        return any(_json_mutation_loses_nonfinite(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(_json_mutation_loses_nonfinite(item) for item in value)
    if isinstance(value, float):
        return not math.isfinite(value)
    if value is None or type(value) in (str, int, bool):
        return False
    encoded = DocumentCodec.encode(value)
    return encoded is not value and _json_mutation_loses_nonfinite(encoded)


def translate_query_plan(plan: QueryNode) -> SqlFragment:
    if not is_concrete_query_node(plan):
        raise TypeError(f'Unsupported query plan node: {type(plan)!r}')
    _require_sql_identity_comparison(
        field := getattr(plan, 'field', None),
        *(getattr(plan, name, None) for name in ('value', 'values', 'lower', 'upper')),
    )
    if isinstance(field, str) and _path_uses_dbref_special_segment(field):
        raise NotImplementedError(
            'DBRef special subfields require Python query fallback'
        )
    match plan:
        case MatchAll():
            return '1 = 1', []
        case DeferredQueryNode(issue=issue):
            raise NotImplementedError(
                f'Deferred query nodes cannot be translated to SQL: {issue.message}'
            )
        case EqualsCondition(
            field=field,
            value=value,
            null_matches_undefined=null_matches_undefined,
        ):
            if not _is_translatable_equality_value(value):
                raise NotImplementedError(
                    'Unsupported equality value for SQL translation'
                )
            return _translate_equals(
                field,
                value,
                null_matches_undefined=null_matches_undefined,
            )
        case NotEqualsCondition(field=field, value=value):
            if not _is_translatable_equality_value(value):
                raise NotImplementedError(
                    'Unsupported inequality value for SQL translation'
                )
            return _translate_not_equals(field, value)
        case GreaterThanCondition(field=field, value=value):
            return _translate_comparison('>', field, value)
        case GreaterThanOrEqualCondition(field=field, value=value):
            return _translate_comparison('>=', field, value)
        case LessThanCondition(field=field, value=value):
            return _translate_comparison('<', field, value)
        case LessThanOrEqualCondition(field=field, value=value):
            return _translate_comparison('<=', field, value)
        case InCondition(
            field=field,
            values=values,
            null_matches_undefined=null_matches_undefined,
        ):
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
                f'json_type(document, {path}) IS NOT NULL'
                if value
                else f'json_type(document, {path}) IS NULL',
                [],
            )
        case TypeCondition():
            return _translate_type_condition(plan)
        case NotCondition(clause=clause):
            clause_sql, clause_params = translate_query_plan(clause)
            return f'NOT COALESCE(({clause_sql}), 0)', clause_params
        case SizeCondition(field=field, value=value):
            return _translate_size_condition(field, value)
        case AllCondition(field=field, values=values):
            return _translate_all_condition(field, values)
        case ElemMatchCondition(
            field=field, compiled_plan=compiled_plan, wrap_value=wrap_value
        ):
            return _translate_elem_match_condition(
                field, compiled_plan, wrap_value=wrap_value
            )
        case ModCondition() | RegexCondition():
            raise NotImplementedError(
                'Query operator not yet translated to SQL'
            )
        case GeoWithinCondition() | GeoIntersectsCondition() | NearCondition():
            raise NotImplementedError(
                'Geospatial operators require Python query fallback'
            )
        case (
            BitwiseCondition()
            | ExprCondition()
            | WhereCondition()
            | JsonSchemaCondition()
        ):
            raise NotImplementedError(
                'Query operator not yet translated to SQL'
            )
        case AndCondition(clauses=clauses):
            if not clauses:
                return '1 = 1', []
            sql_clauses: list[str] = []
            params: list[object] = []
            for clause in clauses:
                clause_sql, clause_params = translate_query_plan(clause)
                sql_clauses.append(f'({clause_sql})')
                params.extend(clause_params)
            return ' AND '.join(sql_clauses), params
        case OrCondition(clauses=clauses):
            if not clauses:
                return '0 = 1', []
            sql_clauses: list[str] = []
            params: list[object] = []
            for clause in clauses:
                clause_sql, clause_params = translate_query_plan(clause)
                sql_clauses.append(f'({clause_sql})')
                params.extend(clause_params)
            return ' OR '.join(sql_clauses), params
    raise AssertionError(
        'translate_query_plan exhausted supported query nodes'
    )  # pragma: no cover
