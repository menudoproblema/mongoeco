from collections.abc import Callable
from copy import deepcopy
from itertools import count
from re import Pattern
from typing import Any

from mongoeco.core.bson_identity import same_bson_update_value
from mongoeco.core.bson_scalars import (
    _is_nan_numeric_value,
    bson_datetime_milliseconds,
    is_bson_numeric,
    wrap_bson_numeric,
)
from mongoeco.core.bson_values import binary_parts, document_mapping
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.server_errors import retain_diagnostic, server_error
from mongoeco.errors import WriteError
from mongoeco.types import Regex, Timestamp


try:
    from bson.regex import Regex as BsonRegex
except ImportError:  # pragma: no cover - optional wire dependency
    BsonRegex = None


SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE = (
    "Cannot target a selected document whose _id does not match its storage key"
)
UPDATED_DOCUMENT_STORAGE_MISMATCH_MESSAGE = "After applying the update, the (immutable) field '_id' was found to have been altered"


def document_with_id_first(document):
    """MongoDB stores the root _id first without reordering embedded documents."""
    if next(iter(document)) == "_id":
        return document
    return {"_id": document["_id"], **document}


def substitute_empty_root_timestamps(document, now, *, increments=None):
    """Insert/replacement policy; modifier and pipeline values stay literal."""
    if not any(
        key != "_id" and isinstance(value, Timestamp) and value == Timestamp(0, 0)
        for key, value in document.items()
    ):
        return document
    increments = count(1) if increments is None else increments
    seconds = bson_datetime_milliseconds(now) // 1000
    return {
        key: Timestamp(seconds, next(increments))
        if key != "_id" and isinstance(value, Timestamp) and value == Timestamp(0, 0)
        else value
        for key, value in document.items()
    }


def canonical_document_id(value: Any) -> Any:
    if isinstance(value, dict):
        return (
            "dict",
            tuple((key, canonical_document_id(item)) for key, item in value.items()),
        )
    if isinstance(value, list):
        return ("list", tuple(canonical_document_id(item) for item in value))
    try:
        hash(value)
        return (type(value), value)
    except TypeError:
        return ("repr", repr(value))


def root_id_needs_semantic_lookup(value: Any) -> bool:
    """Numeric and binary aliases need equality beyond physical storage keys."""
    if isinstance(value, dict):
        return any(root_id_needs_semantic_lookup(item) for item in value.values())
    if isinstance(value, (list, tuple)):
        return any(root_id_needs_semantic_lookup(item) for item in value)
    return is_bson_numeric(value) or binary_parts(value) is not None


def is_valid_root_document_id(value: Any) -> bool:
    return (
        not isinstance(value, (list, Regex, Pattern))
        and not (BsonRegex is not None and isinstance(value, BsonRegex))
        and not _id_contains_dollar_field(value)
    )


def _id_contains_dollar_field(value):
    mapping = document_mapping(value)
    if mapping is not None:
        permitted = (
            {"$ref", "$id", "$db"}
            if (
                isinstance(mapping.get("$ref"), str)
                and "$id" in mapping
                and (mapping.get("$db") is None or isinstance(mapping["$db"], str))
            )
            else ()
        )
        return any(
            (key.startswith("$") and key not in permitted)
            or (key not in permitted and _id_contains_dollar_field(item))
            for key, item in mapping.items()
        )
    return isinstance(value, list) and any(
        _id_contains_dollar_field(item) for item in value
    )


def assert_valid_root_document_id(value: Any) -> None:
    if not isinstance(value, list) and _id_contains_dollar_field(value):
        message = "The _id field cannot contain a dollar-prefixed field name"
        raise WriteError(message, code=52)
    if is_valid_root_document_id(value):
        return
    kind = "array" if isinstance(value, list) else "regex"
    message = f"The '_id' value cannot be of type {kind}"
    raise WriteError(message, code=53)


def assert_valid_replacement_fields(document):
    for key in document:
        if key.startswith("$") and key not in {"$ref", "$id", "$db"}:
            message = (
                f"The dollar-prefixed field '{key}' is not allowed in a replacement"
            )
            raise WriteError(message, code=52)


def assert_valid_upsert_document_id(value: Any, *, is_replacement: bool) -> None:
    if is_replacement and isinstance(value, list):
        message = "After applying the update, the '_id' field was found to be an array"
        raise WriteError(message, code=54)
    assert_valid_root_document_id(value)


def _storage_keys_equal(left: Any, right: Any) -> bool:
    """Keep key equality, making cloned NaNs reflexive without coercion."""
    # Tuple equality can itself signal InvalidOperation for Decimal sNaN.
    # Inspect numeric leaves first rather than swallowing an equality error.
    if isinstance(left, tuple) and isinstance(right, tuple):
        return len(left) == len(right) and all(
            _storage_keys_equal(a, b) for a, b in zip(left, right, strict=True)
        )
    numeric = wrap_bson_numeric(left)
    if numeric is not None and _is_nan_numeric_value(numeric.value):
        return type(left) is type(right) and same_bson_update_value(left, right)
    return left == right


def _storage_key_matches_id(value, storage_key, storage_key_for_id):
    if _storage_keys_equal(storage_key_for_id(value), storage_key):
        return True
    # Existing SQLite rows can retain the owned Binary class in their key,
    # while decoding yields its public local class. Verify physical BSON
    # identity before accepting the owned representation; never coerce numbers
    # or rewrite a persisted key to make a selected document fit it.
    owned = DocumentCodec.to_internal(value)
    return same_bson_update_value(value, owned) and _storage_keys_equal(
        storage_key_for_id(owned), storage_key
    )


def updated_document_storage_key(
    before: dict[str, Any],
    after: dict[str, Any],
    storage_key: Any,
    *,
    storage_key_for_id: Callable[[Any], Any],
) -> Any:
    """Keep the selected persisted key when its BSON identifier is unchanged."""
    if (
        "_id" in before
        and "_id" in after
        and same_bson_update_value(before["_id"], after["_id"])
    ):
        return storage_key
    return storage_key_for_id(after.get("_id"))


def assert_document_matches_storage_key(
    document: dict[str, Any],
    storage_key: Any,
    *,
    storage_key_for_id: Callable[[Any], Any],
) -> None:
    if "_id" in document:
        assert_valid_root_document_id(document["_id"])
    if not _storage_key_matches_id(
        document.get("_id"), storage_key, storage_key_for_id
    ):
        raise WriteError(SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)


def assert_document_matches_stored_lookup(
    document: dict[str, Any],
    stored_document: dict[str, Any] | None,
    *,
    dialect: Any,
) -> None:
    if "_id" in document:
        assert_valid_root_document_id(document["_id"])
    if stored_document is None:
        raise WriteError(SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)
    if "_id" in document:
        if "_id" not in stored_document or not dialect.values_equal(
            stored_document["_id"],
            document["_id"],
        ):
            raise WriteError(
                SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE,
                code=66,
            )
        return
    if not document_matches_root_id_lookup(
        stored_document,
        None,
        dialect=dialect,
    ):
        raise WriteError(SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)


def assert_document_kept_storage_key(
    document: dict[str, Any],
    storage_key: Any,
    *,
    storage_key_for_id: Callable[[Any], Any],
) -> None:
    if not _storage_key_matches_id(
        document.get("_id"), storage_key, storage_key_for_id
    ):
        raise WriteError(UPDATED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)


def replacement_document_changed(original, materialized, replacement):
    """Native replacement no-op rule, including implicit _id insertion."""
    return (
        "_id" not in replacement and "_id" in original
    ) or not same_bson_update_value(original, materialized)


def materialize_replacement_document(
    original_document: dict[str, Any],
    replacement: dict[str, Any],
) -> dict[str, Any]:
    """Store root _id first, retaining the authored order of embedded values."""
    if "_id" in replacement and not _id_contains_dollar_field(replacement["_id"]):
        assert_valid_root_document_id(replacement["_id"])
    materialized = {key: deepcopy(value) for key, value in replacement.items()}
    if "_id" not in materialized and "_id" in original_document:
        materialized["_id"] = deepcopy(original_document["_id"])
    return (
        document_with_id_first(materialized) if "_id" in materialized else materialized
    )


def materialize_merge_insert_document(
    document: dict[str, Any], *, dialect: Any = None
) -> dict[str, Any]:
    """Apply MongoDB's update-style field ordering for a merge upsert."""
    materialized = {"_id": deepcopy(document["_id"])}
    keys = [key for key in document if key != "_id"]
    if dialect is None or not dialect.behavior_flag(
        "merge_insert_preserves_incoming_field_order", default=False
    ):
        keys.sort()
    for key in keys:
        materialized[key] = deepcopy(document[key])
    return materialized


def document_matches_root_id_lookup(
    document: dict[str, Any],
    document_id: Any,
    *,
    dialect: Any,
) -> bool:
    if "_id" not in document:
        return True
    return dialect.values_equal(document["_id"], document_id)


def assert_classic_update_preserves_id(
    original_doc: dict[str, Any],
    updated_doc: dict[str, Any],
) -> None:
    if "_id" in original_doc:
        original_id = original_doc["_id"]
        if "_id" not in updated_doc or not same_bson_update_value(
            updated_doc["_id"], original_id
        ):
            message = (
                "Performing an update on the path '_id' would modify "
                "the immutable field '_id'"
            )
            raise retain_diagnostic(
                server_error(message, code=66, reason="immutable_id"),
                WriteError(message, code=66),
            )
        assert_valid_root_document_id(updated_doc["_id"])
        updated_doc["_id"] = deepcopy(original_id)
        return
    if "_id" in updated_doc:
        assert_valid_root_document_id(updated_doc["_id"])


def preserve_and_validate_pipeline_id(
    original_doc: dict[str, Any],
    replacement_doc: dict[str, Any],
    *,
    dialect: Any,
    is_upsert_insert: bool = False,
) -> None:
    replacement_id = replacement_doc.get("_id")
    mapping = document_mapping(replacement_id)
    invalid_before_immutable = (
        any(key.startswith("$") for key in mapping)
        if mapping is not None and dialect.key == "7.0"
        else mapping is not None and _id_contains_dollar_field(replacement_id)
    )
    if not is_upsert_insert and invalid_before_immutable:
        assert_valid_root_document_id(replacement_id)
    if "_id" in original_doc:
        original_id = original_doc["_id"]
        if "_id" not in replacement_doc:
            materialized = materialize_replacement_document(
                original_doc,
                replacement_doc,
            )
            replacement_doc.clear()
            replacement_doc.update(materialized)
            assert_valid_root_document_id(replacement_doc["_id"])
            return
        if not dialect.values_equal(replacement_doc["_id"], original_id):
            raise WriteError(
                UPDATED_DOCUMENT_STORAGE_MISMATCH_MESSAGE,
                code=66,
            )
        assert_valid_root_document_id(replacement_doc["_id"])
        if same_bson_update_value(original_id, replacement_doc["_id"]):
            replacement_doc["_id"] = deepcopy(original_id)
        return
    if "_id" in replacement_doc:
        assert_valid_root_document_id(replacement_doc["_id"])
