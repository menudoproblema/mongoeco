from mongoeco.core.filtering import BSONComparator
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter


def _seed_candidate_from_condition(value: object) -> object:
    if not isinstance(value, dict):
        return value

    operators = [operator for operator in value if isinstance(operator, str) and operator.startswith("$")]
    if not operators:
        return value
    if operators == ["$eq"]:
        return value["$eq"]
    if operators == ["$in"]:
        in_values = value["$in"]
        if isinstance(in_values, list | tuple) and len(in_values) == 1:
            return in_values[0]
    raise KeyError("condition is not seedable")


def _iter_seedable_filters(filter_spec: Filter):
    for key, value in filter_spec.items():
        if key == "$and":
            if not isinstance(value, list):
                continue
            for clause in value:
                if isinstance(clause, dict):
                    yield from _iter_seedable_filters(clause)
            continue
        if key.startswith("$"):
            continue
        try:
            yield key, _seed_candidate_from_condition(value)
        except KeyError:
            continue


def _seed_filter_value(document: Document, path: str, value: object) -> None:
    if "." not in path:
        if path not in document:
            document[path] = value
            return

        if BSONComparator.compare(document[path], value) != 0:
            raise OperationFailure(f"Conflicting upsert seed path: {path}")
        return

    first, rest = path.split(".", 1)
    if first not in document:
        document[first] = {}
    elif not isinstance(document[first], dict):
        raise OperationFailure(f"Conflicting upsert seed path: {path}")

    _seed_filter_value(document[first], rest, value)


def seed_upsert_document(document: Document, filter_spec: Filter) -> None:
    for key, candidate in _iter_seedable_filters(filter_spec):
        _seed_filter_value(document, key, candidate)
