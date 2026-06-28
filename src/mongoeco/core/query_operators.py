from collections.abc import Mapping
from typing import Any, TypeGuard

from mongoeco.errors import OperationFailure


QUERY_LOGICAL_OPERATORS = frozenset({'$and', '$or', '$nor'})
QUERY_TOP_LEVEL_OPERATORS = frozenset(
    {
        '$and',
        '$comment',
        '$expr',
        '$jsonSchema',
        '$nor',
        '$or',
        '$where',
    }
)


def is_query_operator_key(key: object) -> TypeGuard[str]:
    return isinstance(key, str) and key.startswith('$')


def query_operator_keys(document: Mapping[object, object]) -> tuple[str, ...]:
    return tuple(key for key in document if is_query_operator_key(key))


def has_query_top_level_operator(document: Mapping[object, object]) -> bool:
    return any(key in QUERY_TOP_LEVEL_OPERATORS for key in document)


def is_non_empty_document_clause_list(
    value: object,
) -> TypeGuard[list[dict[str, Any]]]:
    return (
        isinstance(value, list)
        and bool(value)
        and all(isinstance(item, dict) for item in value)
    )


def require_non_empty_document_clause_list(
    value: object,
    *,
    operator: str,
    context: str,
) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise OperationFailure(
            f'{operator} in {context} requires a non-empty list'
        )
    if not all(isinstance(item, dict) for item in value):
        raise OperationFailure(
            f'{operator} in {context} requires document clauses'
        )
    return value
