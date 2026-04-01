from __future__ import annotations

from typing import TYPE_CHECKING
import time

from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.search_index_cursor import AsyncSearchIndexCursor
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.session import ClientSession
from mongoeco.types import (
    Document,
    IndexInformation,
    IndexKeySpec,
    SearchIndexModel,
)

if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection


async def create_index(
    collection: AsyncCollection,
    keys: object,
    *,
    unique: bool = False,
    name: str | None = None,
    sparse: bool = False,
    partial_filter_expression: dict[str, object] | None = None,
    expire_after_seconds: int | None = None,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> str:
    normalized_keys = collection._normalize_index_keys(keys)
    expire_after_seconds = collection._normalize_expire_after_seconds(expire_after_seconds)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    created_name = await collection._engine.create_index(
        collection._db_name,
        collection._collection_name,
        normalized_keys,
        unique=unique,
        name=name,
        sparse=sparse,
        partial_filter_expression=partial_filter_expression,
        expire_after_seconds=expire_after_seconds,
        max_time_ms=max_time_ms,
        context=session,
    )
    collection._record_operation_metadata(
        operation="create_index",
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    return created_name


async def create_indexes(
    collection: AsyncCollection,
    indexes: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> list[str]:
    models = collection._normalize_index_models(indexes)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    deadline = operation_deadline(max_time_ms)
    existing = await collection._engine.index_information(
        collection._db_name,
        collection._collection_name,
        context=session,
    )
    names: list[str] = []
    created_names: list[str] = []
    for index in models:
        try:
            enforce_deadline(deadline)
            name = await collection._engine.create_index(
                collection._db_name,
                collection._collection_name,
                index.keys,
                unique=index.unique,
                name=index.name,
                sparse=index.sparse,
                partial_filter_expression=index.partial_filter_expression,
                expire_after_seconds=index.expire_after_seconds,
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
        names.append(name)
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
    collection._record_operation_metadata(
        operation="list_indexes",
        comment=comment,
        session=session,
    )
    return AsyncIndexCursor(
        lambda: collection._engine.list_indexes(
            collection._db_name,
            collection._collection_name,
            context=session,
        )
    )


async def index_information(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> IndexInformation:
    collection._record_operation_metadata(
        operation="index_information",
        comment=comment,
        session=session,
    )
    return await collection._engine.index_information(
        collection._db_name,
        collection._collection_name,
        context=session,
    )


async def drop_index(
    collection: AsyncCollection,
    index_or_name: str | object,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> None:
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
    models = collection._normalize_search_index_models(indexes)
    max_time_ms = collection._normalize_max_time_ms(max_time_ms)
    deadline = operation_deadline(max_time_ms)
    names: list[str] = []
    for model in models:
        enforce_deadline(deadline)
        remaining = None if deadline is None else max(1, int((deadline - time.monotonic()) * 1000))
        name = await collection._engine.create_search_index(
            collection._db_name,
            collection._collection_name,
            model.definition_snapshot,
            max_time_ms=remaining,
            context=session,
        )
        names.append(name)
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
