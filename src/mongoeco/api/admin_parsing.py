from mongoeco.api._async.cursor import (
    _validate_batch_size,
    _validate_hint_spec,
    _validate_max_time_ms,
    _validate_sort_spec,
)
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter, IndexModel


def normalize_command_document(command: object, kwargs: dict[str, object]) -> dict[str, object]:
    if isinstance(command, str):
        if not command:
            raise TypeError("command name must be a non-empty string")
        return {command: 1, **kwargs}
    if not isinstance(command, dict) or not command:
        raise TypeError("command must be a non-empty string or dict")
    if kwargs:
        raise TypeError("keyword arguments are only supported when command is a string")
    return command


def require_collection_name(value: object, field_name: str) -> str:
    if not isinstance(value, str) or not value:
        raise TypeError(f"{field_name} must be a non-empty string")
    return value


def resolve_collection_reference(value: object, field_name: str) -> str:
    if isinstance(value, str):
        return require_collection_name(value, field_name)
    name = getattr(value, "name", None)
    if isinstance(name, str) and name:
        return name
    raise TypeError(f"{field_name} must be a collection name or collection object")


def normalize_index_models_from_command(indexes: object) -> list[IndexModel]:
    if not isinstance(indexes, list) or not indexes:
        raise TypeError("indexes must be a non-empty list")

    normalized: list[IndexModel] = []
    for raw_index in indexes:
        if not isinstance(raw_index, dict):
            raise TypeError("each index specification must be a dict")
        if "key" not in raw_index:
            raise OperationFailure("index specification must contain 'key'")

        unsupported = set(raw_index) - {"key", "name", "unique"}
        if unsupported:
            unsupported_names = ", ".join(sorted(unsupported))
            raise TypeError(
                f"unsupported createIndexes options in command: {unsupported_names}"
            )

        kwargs: dict[str, object] = {}
        if "name" in raw_index:
            kwargs["name"] = raw_index["name"]
        if "unique" in raw_index:
            kwargs["unique"] = raw_index["unique"]
        normalized.append(IndexModel(raw_index["key"], **kwargs))
    return normalized


def normalize_command_sort_document(sort: object | None) -> list[tuple[str, int]] | None:
    if sort is None:
        return None
    if not isinstance(sort, dict):
        raise TypeError("sort must be a document")
    normalized = list(sort.items())
    _validate_sort_spec(normalized)
    return normalized


def normalize_command_hint(hint: object | None) -> object | None:
    if hint is None:
        return None
    if isinstance(hint, dict):
        hint = list(hint.items())
    _validate_hint_spec(hint)
    return hint


def normalize_command_max_time_ms(max_time_ms: object | None) -> int | None:
    if max_time_ms is None:
        return None
    _validate_max_time_ms(max_time_ms)
    return max_time_ms


def normalize_command_projection(projection: object | None) -> dict[str, object] | None:
    if projection is None:
        return None
    if not is_projection(projection):
        raise TypeError("projection must be a dict")
    return projection


def normalize_command_batch_size(batch_size: object | None) -> int | None:
    if batch_size is None:
        return None
    _validate_batch_size(batch_size)
    return batch_size


def normalize_command_ordered(value: object | None) -> bool:
    if value is None:
        return True
    if not isinstance(value, bool):
        raise TypeError("ordered must be a bool")
    return value


def normalize_command_scale(scale: object | None) -> int:
    if scale is None:
        return 1
    if not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0:
        raise TypeError("scale must be a positive integer")
    return scale


def normalize_namespace(value: object, field_name: str) -> tuple[str, str]:
    if not isinstance(value, str) or "." not in value:
        raise TypeError(f"{field_name} must be a fully qualified namespace string")
    db_name, coll_name = value.split(".", 1)
    if not db_name or not coll_name:
        raise TypeError(f"{field_name} must be a fully qualified namespace string")
    return db_name, coll_name


def normalize_insert_documents(spec: object) -> list[Document]:
    if not isinstance(spec, list) or not spec:
        raise TypeError("documents must be a non-empty list of documents")
    if not all(isinstance(item, dict) for item in spec):
        raise TypeError("documents must be a non-empty list of documents")
    return spec


def normalize_update_specs(spec: object) -> list[dict[str, object]]:
    if not isinstance(spec, list) or not spec:
        raise TypeError("updates must be a non-empty list of update documents")
    if not all(isinstance(item, dict) for item in spec):
        raise TypeError("updates must be a non-empty list of update documents")
    return spec


def normalize_delete_specs(spec: object) -> list[dict[str, object]]:
    if not isinstance(spec, list) or not spec:
        raise TypeError("deletes must be a non-empty list of delete documents")
    if not all(isinstance(item, dict) for item in spec):
        raise TypeError("deletes must be a non-empty list of delete documents")
    return spec


def normalize_filter_document(filter_spec: object | None) -> Filter:
    if filter_spec is None:
        return {}
    if not is_filter(filter_spec):
        raise TypeError("filter_spec must be a dict")
    return filter_spec
