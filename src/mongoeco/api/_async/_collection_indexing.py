from __future__ import annotations

import time

from typing import TYPE_CHECKING

from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.search_index_cursor import AsyncSearchIndexCursor
from mongoeco.core.collation import normalize_collation
from mongoeco.core.expression_context import execution_now_scope
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.errors import OperationFailure
from mongoeco.types import SearchIndexModel
from mongoeco._types.indexes import default_index_name


if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.session import ClientSession
    from mongoeco.types import Document, IndexInformation, IndexKeySpec


def _validate_wildcard_projection(collection, projection):
    if projection is not None and collection._mongodb_dialect.behavior_flag(
        "rejects_unimplemented_wildcard_projection", default=False
    ):
        message = "wildcardProjection is outside Mongoeco's supported index subset"
        raise OperationFailure(message)


def _project_index_document(collection, document):
    if collection._mongodb_dialect.behavior_flag(
        "list_indexes_includes_simple_collation", default=False
    ):
        document = dict(document)
        document.setdefault("collation", {"locale": "simple"})
    return collection._apply_codec_options_to_document(document)


async def _index_collation_for_create(collection, keys, collation, session):
    if keys == [("_id", 1)] and (
        collation is None or normalize_collation(collation).locale == "simple"
    ):
        # The SPI's builtin definition is immutable and intrinsically unique.
        return None
    if not collection._mongodb_dialect.behavior_flag(
        "list_indexes_includes_simple_collation", default=False
    ):
        return collation
    if collation is not None and normalize_collation(collation).locale != "simple":
        return collation
    # Reuse the stored spelling for an equivalent existing simple index.
    # Listing metadata never rewrites catalog entries or changes identity.
    existing = await collection._engine.list_indexes(
        collection._db_name, collection._collection_name, context=session
    )
    for index in existing:
        stored = index.get("collation")
        if index["key"] == dict(keys) and (
            stored is None or normalize_collation(stored).locale == "simple"
        ):
            return stored
    return collation


async def create_index(
    collection: AsyncCollection,
    keys: object,
    *,
    unique: bool = False,
    name: str | None = None,
    sparse: bool = False,
    background: bool = False,
    hidden: bool = False,
    collation: dict[str, object] | None = None,
    partial_filter_expression: dict[str, object] | None = None,
    expire_after_seconds: int | None = None,
    weights: dict[str, int] | None = None,
    wildcard_projection: dict[str, object] | None = None,
    default_language: str | None = None,
    language_override: str | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> str:
    collection._ensure_session_active(session)
    normalized_keys = collection._normalize_index_keys(keys)
    if not isinstance(background, bool):
        raise TypeError("background must be a bool")
    if wildcard_projection is not None and not isinstance(wildcard_projection, dict):
        raise TypeError("wildcard_projection must be a dict or None")
    _validate_wildcard_projection(collection, wildcard_projection)
    normalized_partial_filter = (
        None
        if partial_filter_expression is None
        else collection._normalize_filter(partial_filter_expression)
    )
    expire_after_seconds = collection._normalize_expire_after_seconds(expire_after_seconds)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    collation = await _index_collation_for_create(
        collection, normalized_keys, collation, session
    )
    builtin_id = normalized_keys == [("_id", 1)]
    if builtin_id:
        if not isinstance(unique, bool):
            message = "unique must be a bool"
            raise TypeError(message)
        if name is not None and (not isinstance(name, str) or not name):
            message = "name must be a non-empty string"
            code, code_name = (
                (14, "TypeMismatch") if not isinstance(name, str)
                else (67, "CannotCreateIndex")
            )
            raise OperationFailure(message, code=code, details={"codeName": code_name})
    requested_name = name or default_index_name(normalized_keys)
    with execution_now_scope(collection._resolve_now()):
        created_name = await collection._engine.create_index(
        collection._db_name,
        collection._collection_name,
        normalized_keys,
        unique=True if builtin_id else unique,
        name="_id_" if builtin_id else name,
        sparse=sparse,
        hidden=hidden,
        collation=collation,
        partial_filter_expression=normalized_partial_filter,
        expire_after_seconds=expire_after_seconds,
        weights=weights,
        default_language=default_language,
        language_override=language_override,
        max_time_ms=max_time_ms,
            context=session,
        )
    collection._record_operation_metadata(
        operation="create_index",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    return requested_name if builtin_id else created_name


async def create_indexes(
    collection: AsyncCollection,
    indexes: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> list[str]:
    collection._ensure_session_active(session)
    models = collection._normalize_index_models(indexes)
    for model in models:
        _validate_wildcard_projection(collection, model.wildcard_projection)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    deadline = operation_deadline(max_time_ms)
    existing = await collection._engine.index_information(
        collection._db_name,
        collection._collection_name,
        context=session,
    )
    names: list[str] = []
    created_names: list[str] = []
    execution_now = collection._resolve_now()
    for index in models:
        try:
            enforce_deadline(deadline)
            normalized_partial_filter = (
                None
                if index.partial_filter_expression is None
                else collection._normalize_filter(
                    index.partial_filter_expression
                )
            )
            with execution_now_scope(execution_now):
                collation = await _index_collation_for_create(
                    collection, index.keys, index.collation, session
                )
                builtin_id = index.keys == [("_id", 1)]
                name = await collection._engine.create_index(
                collection._db_name,
                collection._collection_name,
                index.keys,
                unique=True if builtin_id else index.unique,
                name="_id_" if builtin_id else index.name,
                sparse=index.sparse,
                hidden=index.hidden,
                collation=collation,
                partial_filter_expression=normalized_partial_filter,
                expire_after_seconds=index.expire_after_seconds,
                weights=index.weights,
                default_language=index.default_language,
                language_override=index.language_override,
                min_value=index.min_value,
                max_value=index.max_value,
                bucket_size=index.bucket_size,
                max_time_ms=None if deadline is None else max(
                    1,
                    int((deadline - time.monotonic()) * 1000),
                ),
                    context=session,
                )
        except Exception:
            for created_name in reversed(created_names):
                try:
                    await collection._engine.drop_index(
                        collection._db_name,
                        collection._collection_name,
                        created_name,
                        context=session,
                    )
                except Exception:
                    pass
            raise
        names.append(index.resolved_name if builtin_id else name)
        if name not in existing and name not in created_names:
            created_names.append(name)
    collection._record_operation_metadata(
        operation="create_indexes",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    return names


def list_indexes(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> AsyncIndexCursor:
    collection._ensure_session_active(session)
    collection._record_operation_metadata(
        operation="list_indexes",
        comment=comment,
        session=session,
    )
    async def _load_indexes() -> list[Document]:
        documents = await collection._engine.list_indexes(
            collection._db_name,
            collection._collection_name,
            context=session,
        )
        return [
            _project_index_document(collection, document)
            for document in documents
        ]

    return AsyncIndexCursor(_load_indexes)


async def index_information(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> IndexInformation:
    collection._ensure_session_active(session)
    collection._record_operation_metadata(
        operation="index_information",
        comment=comment,
        session=session,
    )
    information = await collection._engine.index_information(
        collection._db_name,
        collection._collection_name,
        context=session,
    )
    return {
        name: _project_index_document(collection, document)
        for name, document in information.items()
    }


async def drop_index(
    collection: AsyncCollection,
    index_or_name: str | object,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> None:
    collection._ensure_session_active(session)
    target: str | IndexKeySpec
    if isinstance(index_or_name, str):
        target = index_or_name
    else:
        target = collection._normalize_index_keys(index_or_name)
    await collection._engine.drop_index(
        collection._db_name,
        collection._collection_name,
        target,
        context=session,
    )
    collection._record_operation_metadata(
        operation="drop_index",
        comment=comment,
        session=session,
    )


async def drop_indexes(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> None:
    collection._ensure_session_active(session)
    await collection._engine.drop_indexes(
        collection._db_name,
        collection._collection_name,
        context=session,
    )
    collection._record_operation_metadata(
        operation="drop_indexes",
        comment=comment,
        session=session,
    )


async def create_search_index(
    collection: AsyncCollection,
    model: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> str:
    collection._ensure_session_active(session)
    normalized_model = collection._normalize_search_index_model(model)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    created_name = await collection._engine.create_search_index(
        collection._db_name,
        collection._collection_name,
        normalized_model.definition_snapshot,
        max_time_ms=max_time_ms,
        context=session,
    )
    collection._record_operation_metadata(
        operation="create_search_index",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    return created_name


async def create_search_indexes(
    collection: AsyncCollection,
    indexes: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> list[str]:
    collection._ensure_session_active(session)
    models = collection._normalize_search_index_models(indexes)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    deadline = operation_deadline(max_time_ms)
    existing = {
        document["name"]
        for document in await collection._engine.list_search_indexes(
            collection._db_name,
            collection._collection_name,
            context=session,
        )
        if isinstance(document.get("name"), str)
    }
    names: list[str] = []
    created_names: list[str] = []
    for model in models:
        try:
            enforce_deadline(deadline)
            remaining = None if deadline is None else max(1, int((deadline - time.monotonic()) * 1000))
            name = await collection._engine.create_search_index(
                collection._db_name,
                collection._collection_name,
                model.definition_snapshot,
                max_time_ms=remaining,
                context=session,
            )
        except Exception:
            for created_name in reversed(created_names):
                try:
                    await collection._engine.drop_search_index(
                        collection._db_name,
                        collection._collection_name,
                        created_name,
                        context=session,
                    )
                except Exception:
                    pass
            raise
        names.append(name)
        if name not in existing and name not in created_names:
            created_names.append(name)
    collection._record_operation_metadata(
        operation="create_search_indexes",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    return names


def list_search_indexes(
    collection: AsyncCollection,
    name: str | None = None,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> AsyncSearchIndexCursor:
    collection._ensure_session_active(session)
    if name is not None:
        name = collection._normalize_search_index_name(name)
    collection._record_operation_metadata(
        operation="list_search_indexes",
        comment=comment,
        session=session,
    )
    return AsyncSearchIndexCursor(
        lambda: collection._engine.list_search_indexes(
            collection._db_name,
            collection._collection_name,
            name=name,
            context=session,
        )
    )


async def update_search_index(
    collection: AsyncCollection,
    name: str,
    definition: Document,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> None:
    collection._ensure_session_active(session)
    name = collection._normalize_search_index_name(name)
    definition = collection._require_document(definition)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    await collection._engine.update_search_index(
        collection._db_name,
        collection._collection_name,
        name,
        definition,
        max_time_ms=max_time_ms,
        context=session,
    )
    collection._record_operation_metadata(
        operation="update_search_index",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )


async def drop_search_index(
    collection: AsyncCollection,
    name: str,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> None:
    collection._ensure_session_active(session)
    name = collection._normalize_search_index_name(name)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    await collection._engine.drop_search_index(
        collection._db_name,
        collection._collection_name,
        name,
        max_time_ms=max_time_ms,
        context=session,
    )
    collection._record_operation_metadata(
        operation="drop_search_index",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
