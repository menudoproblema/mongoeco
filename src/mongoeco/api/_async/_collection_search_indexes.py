"""Search request admission shared by single and multiple index operations."""

from __future__ import annotations

import time

from collections.abc import Mapping
from contextlib import suppress
from typing import TYPE_CHECKING

from mongoeco.api._async.search_index_cursor import AsyncSearchIndexCursor
from mongoeco.api.bson_input import encode_bson_request
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.session_guards import ensure_session_can_use_engine
from mongoeco.types import CodecOptions, SearchIndexModel, UuidRepresentation


if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.session import ClientSession
    from mongoeco.types import Document


_SEARCH_OPTIONS = CodecOptions(uuid_representation=UuidRepresentation.UNSPECIFIED)


def _model_fields(definition, name=None, **kwargs):
    # Match the SDK constructor's order, including omission of explicit None.
    document = {} if name is None else {"name": name}
    document["definition"] = definition
    index_type = kwargs.pop("type", None)
    if index_type is not None:
        document["type"] = index_type
    document.update(kwargs)
    return document


def _model_document(model):
    if isinstance(model, SearchIndexModel):
        return _model_fields(model.definition, name=model.name, type=model.index_type)
    if isinstance(model, Mapping):
        # Retain the local definition shorthand alongside the SDK request shape.
        return (
            _model_fields(**model) if "definition" in model else {"definition": model}
        )
    document = getattr(model, "document", None)
    if isinstance(document, Mapping):
        return document
    message = "model must be a SearchIndexModel or a mapping definition"
    raise TypeError(message)


def _admit(collection, command, *, comment, session, max_time_ms=None):
    ensure_session_can_use_engine(collection._engine, session)
    if max_time_ms is not None:
        command["maxTimeMS"] = max_time_ms
    if comment is not None:
        command["comment"] = comment
    return encode_bson_request(command, codec_options=_SEARCH_OPTIONS)


def _prepared_models(documents):
    if not documents:
        message = "indexes must not be empty"
        raise ValueError(message)
    return [SearchIndexModel(**document) for document in documents]


async def create_search_index(
    collection: AsyncCollection,
    model: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> str:
    names = await _create_search_indexes(
        collection,
        [_model_document(model)],
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
        operation="create_search_index",
    )
    return names[0]


async def create_search_indexes(
    collection: AsyncCollection,
    indexes: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> list[str]:
    if not isinstance(indexes, list):
        message = "indexes must be a list of SearchIndexModel instances"
        raise TypeError(message)
    return await _create_search_indexes(
        collection,
        [_model_document(model) for model in indexes],
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
        operation="create_search_indexes",
    )


async def _create_search_indexes(  # noqa: PLR0913 - prepared command and metadata
    collection,
    documents,
    *,
    comment,
    max_time_ms,
    session,
    operation,
):
    admitted = _admit(
        collection,
        {"createSearchIndexes": collection.name, "indexes": documents},
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    models = _prepared_models(admitted["indexes"])
    max_time_ms = collection._normalize_max_time_ms(admitted.get("maxTimeMS"))
    comment = admitted.get("comment")
    deadline = operation_deadline(max_time_ms)
    existing = set()
    if len(models) > 1:
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
            remaining = (
                None
                if deadline is None
                else max(1, int((deadline - time.monotonic()) * 1000))
            )
            name = await collection._engine.create_search_index(
                collection._db_name,
                collection._collection_name,
                model.definition_snapshot,
                max_time_ms=remaining,
                context=session,
            )
        except Exception:
            for created_name in reversed(created_names):
                with suppress(Exception):
                    await collection._engine.drop_search_index(
                        collection._db_name,
                        collection._collection_name,
                        created_name,
                        context=session,
                    )
            raise
        names.append(name)
        if name not in existing and name not in created_names:
            created_names.append(name)
    collection._record_operation_metadata(
        operation=operation,
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
    spec = {} if name is None else {"name": name}
    admitted = _admit(
        collection,
        {
            "aggregate": collection.name,
            "pipeline": [{"$listSearchIndexes": spec}],
            "cursor": {},
        },
        comment=comment,
        session=session,
    )
    spec = admitted["pipeline"][0]["$listSearchIndexes"]
    name = spec.get("name")
    if name is not None:
        name = collection._normalize_search_index_name(name)
    comment = admitted.get("comment")
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


async def update_search_index(  # noqa: PLR0913 - public API signature
    collection: AsyncCollection,
    name: str,
    definition: Document,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> None:
    admitted = _admit(
        collection,
        {"updateSearchIndex": collection.name, "name": name, "definition": definition},
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    name = collection._normalize_search_index_name(admitted["name"])
    definition = collection._require_document(admitted["definition"])
    max_time_ms = collection._normalize_max_time_ms(admitted.get("maxTimeMS"))
    comment = admitted.get("comment")
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
    admitted = _admit(
        collection,
        {"dropSearchIndex": collection.name, "name": name},
        comment=comment,
        max_time_ms=max_time_ms,
        session=session,
    )
    name = collection._normalize_search_index_name(admitted["name"])
    max_time_ms = collection._normalize_max_time_ms(admitted.get("maxTimeMS"))
    comment = admitted.get("comment")
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
