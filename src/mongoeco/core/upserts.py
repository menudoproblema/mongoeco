from mongoeco.core.filtering import BSONComparator
from mongoeco.core.query_operators import (
    is_query_operator_key,
    query_operator_keys,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter


def _seed_candidate_from_condition(value: object) -> object:
    if not isinstance(value, dict):
        return value

    operators = query_operator_keys(value)
    if not operators:
        return value
    if len(operators) != len(value):
        raise KeyError('condition is not seedable')
    if operators == ('$eq',):
        return value['$eq']
    if operators == ('$in',):
        in_values = value['$in']
        if isinstance(in_values, list | tuple) and len(in_values) == 1:
            return in_values[0]
    raise KeyError('condition is not seedable')


def _iter_seedable_filters(filter_spec: Filter):
    for key, value in filter_spec.items():
        if key == '$and':
            if not isinstance(value, list):
                continue
            for clause in value:
                if isinstance(clause, dict):
                    yield from _iter_seedable_filters(clause)
            continue
        if not isinstance(key, str) or is_query_operator_key(key):
            continue
        try:
            yield key, _seed_candidate_from_condition(value)
        except KeyError:
            continue


def _seed_filter_value(document: Document, path: str, value: object) -> None:
    if '.' not in path:
        if path not in document:
            document[path] = value
            return

        if BSONComparator.compare(document[path], value) != 0:
            raise OperationFailure(f'Conflicting upsert seed path: {path}')
        return

    first, rest = path.split('.', 1)
    if first not in document:
        document[first] = {}
    elif not isinstance(document[first], dict):
        raise OperationFailure(f'Conflicting upsert seed path: {path}')

    _seed_filter_value(document[first], rest, value)


def seed_upsert_document(document: Document, filter_spec: Filter) -> None:
    for key, candidate in _iter_seedable_filters(filter_spec):
        _seed_filter_value(document, key, candidate)
