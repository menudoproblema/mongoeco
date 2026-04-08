from dataclasses import dataclass

from mongoeco.api.argument_validation import (
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_hint_spec as _validate_hint_spec,
    validate_max_time_ms as _validate_max_time_ms,
    validate_sort_spec as _validate_sort_spec,
)
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter, IndexModel


@dataclass(frozen=True, slots=True)
class ListCollectionsCommandOptions:
    name_only: bool
    authorized_collections: bool
    filter_spec: Filter
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class ListDatabasesCommandOptions:
    name_only: bool
    filter_spec: Filter
    comment: str | None = None


@dataclass(frozen=True, slots=True)
class ValidationCommandOptions:
    scandata: bool
    full: bool
    background: bool | None
    comment: str | None


@dataclass(frozen=True, slots=True)
class FindAndModifyCommandOptions:
    collection_name: str
    query: Filter
    collation: dict[str, object] | None
    sort: list[tuple[str, int]] | None
    fields: dict[str, object] | None
    remove: bool
    return_new: bool
    upsert: bool
    bypass_document_validation: bool
    array_filters: list[Filter] | None
    hint: object | None
    max_time_ms: int | None
    let: dict[str, object] | None
    comment: object | None
    update_spec: dict[str, object] | None


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

        unsupported = set(raw_index) - {
            "key",
            "name",
            "unique",
            "sparse",
            "background",
            "hidden",
            "collation",
            "partialFilterExpression",
            "expireAfterSeconds",
            "weights",
            "wildcardProjection",
            "defaultLanguage",
            "languageOverride",
            "min",
            "max",
            "bucketSize",
            "wildcard_projection",
            "default_language",
            "language_override",
            "min_value",
            "max_value",
            "bucket_size",
        }
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
        if "sparse" in raw_index:
            kwargs["sparse"] = raw_index["sparse"]
        if "background" in raw_index:
            kwargs["background"] = raw_index["background"]
        if "hidden" in raw_index:
            kwargs["hidden"] = raw_index["hidden"]
        if "collation" in raw_index:
            kwargs["collation"] = raw_index["collation"]
        if "partialFilterExpression" in raw_index:
            kwargs["partialFilterExpression"] = raw_index["partialFilterExpression"]
        if "expireAfterSeconds" in raw_index:
            kwargs["expireAfterSeconds"] = raw_index["expireAfterSeconds"]
        if "weights" in raw_index:
            kwargs["weights"] = raw_index["weights"]
        if "wildcardProjection" in raw_index:
            kwargs["wildcardProjection"] = raw_index["wildcardProjection"]
        if "defaultLanguage" in raw_index:
            kwargs["defaultLanguage"] = raw_index["defaultLanguage"]
        if "languageOverride" in raw_index:
            kwargs["languageOverride"] = raw_index["languageOverride"]
        if "min" in raw_index:
            kwargs["min"] = raw_index["min"]
        if "max" in raw_index:
            kwargs["max"] = raw_index["max"]
        if "bucketSize" in raw_index:
            kwargs["bucketSize"] = raw_index["bucketSize"]
        if "wildcard_projection" in raw_index:
            kwargs["wildcard_projection"] = raw_index["wildcard_projection"]
        if "default_language" in raw_index:
            kwargs["default_language"] = raw_index["default_language"]
        if "language_override" in raw_index:
            kwargs["language_override"] = raw_index["language_override"]
        if "min_value" in raw_index:
            kwargs["min_value"] = raw_index["min_value"]
        if "max_value" in raw_index:
            kwargs["max_value"] = raw_index["max_value"]
        if "bucket_size" in raw_index:
            kwargs["bucket_size"] = raw_index["bucket_size"]
        normalized.append(IndexModel(raw_index["key"], **kwargs))
    return normalized


def normalize_command_sort_document(sort: object | None) -> list[tuple[str, int]] | None:
    if sort is None:
        return None
    if not isinstance(sort, dict):
        raise TypeError("sort must be a document")
    normalized = _normalize_sort_spec(sort)
    if normalized is None:
        return None
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


def normalize_command_bool(value: object | None, field_name: str, *, default: bool = False) -> bool:
    if value is None:
        return default
    if not isinstance(value, bool):
        raise TypeError(f"{field_name} must be a bool")
    return value


def normalize_command_scale(scale: object | None) -> int:
    if scale is None:
        return 1
    if not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0:
        raise TypeError("scale must be a positive integer")
    return scale


def normalize_list_collections_options(
    spec: dict[str, object],
) -> ListCollectionsCommandOptions:
    comment = spec.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise TypeError("comment must be a string")
    return ListCollectionsCommandOptions(
        name_only=normalize_command_bool(spec.get("nameOnly"), "nameOnly"),
        authorized_collections=normalize_command_bool(
            spec.get("authorizedCollections"),
            "authorizedCollections",
        ),
        filter_spec=normalize_filter_document(spec.get("filter")),
        comment=comment,
    )


def normalize_list_databases_options(
    spec: dict[str, object],
) -> ListDatabasesCommandOptions:
    comment = spec.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise TypeError("comment must be a string")
    return ListDatabasesCommandOptions(
        name_only=normalize_command_bool(spec.get("nameOnly"), "nameOnly"),
        filter_spec=normalize_filter_document(spec.get("filter")),
        comment=comment,
    )


def normalize_validate_command_options(
    *,
    scandata: object | None = None,
    full: object | None = None,
    background: object | None = None,
    comment: object | None = None,
) -> ValidationCommandOptions:
    if background is not None and not isinstance(background, bool):
        raise TypeError("background must be a bool or None")
    if comment is not None and not isinstance(comment, str):
        raise TypeError("comment must be a string")
    return ValidationCommandOptions(
        scandata=normalize_command_bool(scandata, "scandata"),
        full=normalize_command_bool(full, "full"),
        background=background,
        comment=comment,
    )


def normalize_find_and_modify_options(
    spec: dict[str, object],
) -> FindAndModifyCommandOptions:
    remove = normalize_command_bool(spec.get("remove"), "remove")
    return_new = normalize_command_bool(spec.get("new"), "new")
    upsert = normalize_command_bool(spec.get("upsert"), "upsert")
    array_filters = spec.get("arrayFilters")
    if array_filters is not None:
        if not isinstance(array_filters, list) or not all(is_filter(item) for item in array_filters):
            raise TypeError("arrayFilters must be a list of dicts")
    let = spec.get("let")
    if let is not None and not isinstance(let, dict):
        raise TypeError("let must be a dict")
    collation = spec.get("collation")
    if collation is not None and not isinstance(collation, dict):
        raise TypeError("collation must be a document")
    bypass_document_validation = spec.get("bypassDocumentValidation", False)
    if not isinstance(bypass_document_validation, bool):
        raise TypeError("bypassDocumentValidation must be a bool")
    update_spec = spec.get("update")
    if update_spec is not None and not isinstance(update_spec, (dict, list)):
        raise TypeError("update must be a document or pipeline")
    return FindAndModifyCommandOptions(
        collection_name=require_collection_name(spec.get("findAndModify"), "findAndModify"),
        query=normalize_filter_document(spec.get("query")),
        collation=collation,
        sort=normalize_command_sort_document(spec.get("sort")),
        fields=normalize_command_projection(spec.get("fields")),
        remove=remove,
        return_new=return_new,
        upsert=upsert,
        bypass_document_validation=bypass_document_validation,
        array_filters=array_filters,
        hint=normalize_command_hint(spec.get("hint")),
        max_time_ms=normalize_command_max_time_ms(spec.get("maxTimeMS")),
        let=let,
        comment=spec.get("comment"),
        update_spec=update_spec,
    )


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
