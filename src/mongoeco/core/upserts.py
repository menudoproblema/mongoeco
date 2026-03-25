from mongoeco.core.filtering import BSONComparator
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter


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
    for key, value in filter_spec.items():
        if key.startswith("$"):
            continue

        candidate = value
        if isinstance(value, dict):
            operators = [operator for operator in value if isinstance(operator, str) and operator.startswith("$")]
            if operators != ["$eq"]:
                continue
            candidate = value["$eq"]

        _seed_filter_value(document, key, candidate)
