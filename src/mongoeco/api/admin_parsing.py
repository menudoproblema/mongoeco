from dataclasses import dataclass
from types import MappingProxyType

from mongoeco.api._command_bson import (
    command_boolean,
    legacy_command_truth_value,
    validate_command_array_filters,
    validate_command_let,
    validate_write_fields,
)
from mongoeco.api.argument_validation import (
    normalize_sort_spec as _normalize_sort_spec,
    validate_batch_size as _validate_batch_size,
    validate_hint_spec as _validate_hint_spec,
    validate_max_time_ms as _validate_max_time_ms,
    validate_sort_spec as _validate_sort_spec,
)
from mongoeco.core.bson_raw import decode_bson_document
from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.core.validation import is_filter, is_projection
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, Filter, IndexModel


CREATE_INDEXES_SPEC_FIELDS = frozenset(
    {
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
)
CREATE_INDEXES_SPEC_ALIASES = MappingProxyType(
    {
        "wildcard_projection": "wildcardProjection",
        "default_language": "defaultLanguage",
        "language_override": "languageOverride",
        "min_value": "min",
        "max_value": "max",
        "bucket_size": "bucketSize",
    }
)
CREATE_INDEXES_ACCEPTED_NOOP_SPEC_FIELDS = frozenset(
    {"background", "wildcardProjection", "wildcard_projection"}
)


@dataclass(frozen=True, slots=True)
class ListCollectionsCommandOptions:
    name_only: bool
    authorized_collections: bool
    filter_spec: Filter
    comment: object | None = None


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
    comment: object | None


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


def normalize_command_document(
    command: object, kwargs: dict[str, object]
) -> dict[str, object]:
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
    if not isinstance(indexes, list):
        raise TypeError("indexes must be a non-empty list")
    if not indexes:
        raise server_error(
            "Must specify at least one index to create",
            code=2,
            scope=ErrorScope.COMMAND,
        )

    normalized: list[IndexModel] = []
    for raw_index in indexes:
        if not isinstance(raw_index, dict):
            raise TypeError("each index specification must be a dict")
        if "key" not in raw_index:
            message = "index specification must contain 'key'"
            raise OperationFailure(message)

        unsupported = set(raw_index) - CREATE_INDEXES_SPEC_FIELDS
        if unsupported:
            unsupported_names = ", ".join(sorted(unsupported))
            raise TypeError(
                f"unsupported createIndexes options in command: {unsupported_names}"
            )

        kwargs = {name: value for name, value in raw_index.items() if name != "key"}
        normalized.append(IndexModel(raw_index["key"], **kwargs))
    return normalized


def normalize_command_sort_document(
    sort: object | None,
) -> list[tuple[str, int]] | None:
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
    return command_boolean(value, "ordered")


def normalize_command_bool(
    value: object | None, field_name: str, *, default: bool = False
) -> bool:
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
    filter_spec = spec.get("filter")
    if filter_spec is not None and not isinstance(filter_spec, dict):
        message = "listCollections filter must be a document"
        raise server_error(message, code=14, scope=ErrorScope.COMMAND)
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
    normalized_full = legacy_command_truth_value(full)
    normalized_background = (
        None if background is None else legacy_command_truth_value(background)
    )
    if normalized_full and normalized_background:
        message = "Cannot specify both full and background validation"
        raise server_error(message, code=72, scope=ErrorScope.COMMAND)
    return ValidationCommandOptions(
        scandata=legacy_command_truth_value(scandata),
        full=normalized_full,
        background=normalized_background,
        comment=comment,
    )


def normalize_drop_collection_name(value, *, dialect):
    """Parse drop's version-specific legacy/IDL namespace grammar."""
    if not matches_bson_type(value, "string"):
        message = "collection name must be a string"
        code = {"7.0": 2, "8.0": 14, "9.0": 73}[dialect.server_version]
        raise server_error(message, code=code, scope=ErrorScope.COMMAND)
    if not value or "\x00" in value:
        message = "Invalid namespace for drop"
        raise server_error(message, code=73, scope=ErrorScope.COMMAND)
    return value


def normalize_validate_collection_name(value):
    """Parse an already encoded validate namespace without client coercions."""
    if not matches_bson_type(value, "string"):
        message = "validate requires a BSON string collection name"
        raise server_error(message, code=14, scope=ErrorScope.COMMAND)
    if not value:
        message = "Invalid namespace for validate"
        raise server_error(message, code=73, scope=ErrorScope.COMMAND)
    return value


def normalize_find_and_modify_options(
    spec: dict[str, object],
    *, dialect: str = "7.0",
) -> FindAndModifyCommandOptions:
    validate_write_fields(spec, "findAndModify")
    validate_command_array_filters(spec, dialect=dialect)
    remove = command_boolean(spec.get("remove"), "remove", numeric=True)
    return_new = command_boolean(spec.get("new"), "new", numeric=True)
    upsert = command_boolean(spec.get("upsert"), "upsert", numeric=True)
    array_filters = spec.get("arrayFilters")
    if array_filters is not None:
        if not isinstance(array_filters, list) or not all(
            is_filter(item) for item in array_filters
        ):
            message = "arrayFilters must be a list of documents"
            raise server_error(
                message,
                code=14,
                scope=ErrorScope.COMMAND,
            )
    let = spec.get("let")
    validate_command_let(let)
    collation = spec.get("collation")
    if collation is not None and not isinstance(collation, dict):
        message = "collation must be a document"
        raise server_error(message, code=14, scope=ErrorScope.COMMAND)
    bypass_document_validation = spec.get("bypassDocumentValidation", False)
    bypass_document_validation = command_boolean(
        bypass_document_validation, "bypassDocumentValidation", numeric=True
    )
    update_spec = spec.get("update")
    if remove and "update" in spec:
        message = "Cannot specify both an update and remove=true"
        raise server_error(message, code=9, scope=ErrorScope.COMMAND)
    if update_spec is not None and not isinstance(update_spec, (dict, list)):
        message = "update must be a document or pipeline"
        raise server_error(message, code=14, scope=ErrorScope.COMMAND)
    return FindAndModifyCommandOptions(
        collection_name=require_collection_name(
            spec.get("findAndModify"), "findAndModify"
        ),
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
    if all(isinstance(item, dict) for item in spec):
        return spec
    try:
        from bson.raw_bson import RawBSONDocument  # noqa: PLC0415 - optional SDK
    except ImportError:
        message = "documents must be a non-empty list of documents"
        raise TypeError(message) from None
    if not all(isinstance(item, (dict, RawBSONDocument)) for item in spec):
        raise TypeError("documents must be a non-empty list of documents")
    return [
        decode_bson_document(item.raw) if isinstance(item, RawBSONDocument) else item
        for item in spec
    ]


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
