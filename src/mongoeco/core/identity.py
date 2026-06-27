from collections.abc import Callable
from copy import deepcopy
from typing import Any

from mongoeco.errors import WriteError

SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE = (
    "Cannot target a selected document whose _id does not match its storage key"
)
UPDATED_DOCUMENT_STORAGE_MISMATCH_MESSAGE = (
    "After applying the update, the (immutable) field '_id' was found to have been altered"
)


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


def is_valid_root_document_id(value: Any) -> bool:
    return not isinstance(value, list)


def assert_valid_root_document_id(value: Any) -> None:
    if is_valid_root_document_id(value):
        return
    raise WriteError("The '_id' value cannot be of type array", code=53)


def assert_document_matches_storage_key(
    document: dict[str, Any],
    storage_key: Any,
    *,
    storage_key_for_id: Callable[[Any], Any],
) -> None:
    if "_id" in document:
        assert_valid_root_document_id(document["_id"])
    if storage_key_for_id(document.get("_id")) != storage_key:
        raise WriteError(SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)


def assert_document_matches_stored_lookup(
    document: dict[str, Any],
    stored_document: dict[str, Any] | None,
    *,
    dialect: Any,
) -> None:
    if "_id" in document:
        assert_valid_root_document_id(document["_id"])
    if stored_document is None or not dialect.values_equal(stored_document, document):
        raise WriteError(SELECTED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)


def assert_document_kept_storage_key(
    document: dict[str, Any],
    storage_key: Any,
    *,
    storage_key_for_id: Callable[[Any], Any],
) -> None:
    if storage_key_for_id(document.get("_id")) != storage_key:
        raise WriteError(UPDATED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)


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
    *,
    dialect: Any,
) -> None:
    if "_id" in original_doc:
        original_id = original_doc["_id"]
        if "_id" not in updated_doc or not dialect.values_equal(
            updated_doc["_id"],
            original_id,
        ):
            raise WriteError(
                "Performing an update on the path '_id' would modify the immutable field '_id'",
                code=66,
            )
        assert_valid_root_document_id(updated_doc["_id"])
        return
    if "_id" in updated_doc:
        assert_valid_root_document_id(updated_doc["_id"])


def preserve_and_validate_pipeline_id(
    original_doc: dict[str, Any],
    replacement_doc: dict[str, Any],
    *,
    dialect: Any,
) -> None:
    if "_id" in original_doc:
        original_id = original_doc["_id"]
        if "_id" not in replacement_doc:
            replacement_doc["_id"] = deepcopy(original_id)
            assert_valid_root_document_id(replacement_doc["_id"])
            return
        if not dialect.values_equal(replacement_doc["_id"], original_id):
            raise WriteError(UPDATED_DOCUMENT_STORAGE_MISMATCH_MESSAGE, code=66)
        assert_valid_root_document_id(replacement_doc["_id"])
        return
    if "_id" in replacement_doc:
        assert_valid_root_document_id(replacement_doc["_id"])
