from __future__ import annotations

import time

from contextlib import suppress
from typing import TYPE_CHECKING

from mongoeco._types.indexes import default_index_name
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api.admin_parsing import normalize_index_models_from_command
from mongoeco.api.bson_input import encode_bson_request
from mongoeco.core._public_materialization import materialize_command_response
from mongoeco.core.collation import normalize_collation
from mongoeco.core.error_documents import command_errors
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.expression_context import execution_now_scope
from mongoeco.errors import OperationFailure
from mongoeco.session_guards import ensure_session_can_use_engine
from mongoeco.types import (
    SON,
    CodecOptions,
    DropIndexesCommandResult,
    IndexModel,
    UuidRepresentation,
)


if TYPE_CHECKING:
    from mongoeco.api._async.collection import AsyncCollection
    from mongoeco.session import ClientSession
    from mongoeco.types import Document, IndexInformation


_INDEX_NOT_FOUND_CODE = 27

_INDEX_REQUEST_OPTIONS = CodecOptions(
    uuid_representation=UuidRepresentation.UNSPECIFIED
)
_INDEX_LIST_OPTIONS = CodecOptions(
    document_class=SON, uuid_representation=UuidRepresentation.UNSPECIFIED
)
_INDEX_MODEL_FIELDS = {
    "unique": "unique",
    "sparse": "sparse",
    "background": "background",
    "hidden": "hidden",
    "collation": "collation",
    "partial_filter_expression": "partialFilterExpression",
    "expire_after_seconds": "expireAfterSeconds",
    "weights": "weights",
    "wildcard_projection": "wildcardProjection",
    "default_language": "default_language",
    "language_override": "language_override",
    "min_value": "min",
    "max_value": "max",
    "bucket_size": "bucketSize",
}


def index_model_document(model):
    """View a model's request without copying values before BSON admission."""
    if isinstance(model, IndexModel):
        document = {"name": model.resolved_name, "key": dict(model.keys)}
        for attribute, field in _INDEX_MODEL_FIELDS.items():
            value = getattr(model, attribute)
            if value is not None and (
                value is not False
                or field not in {"unique", "sparse", "background", "hidden"}
            ):
                document[field] = value
        return document
    document = getattr(model, "document", None)
    if not isinstance(document, dict):
        message = "indexes must contain only IndexModel instances"
        raise TypeError(message)
    # PyMongo gathers every name before encoding any model in the command.
    document["name"]
    return document


def _admit_index_request(collection, documents, *, comment, max_time_ms):
    command = {"createIndexes": collection.name, "indexes": documents}
    if max_time_ms is not None:
        command["maxTimeMS"] = max_time_ms
    if comment is not None:
        command["comment"] = comment
    return encode_bson_request(command, codec_options=_INDEX_REQUEST_OPTIONS)


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
    return document


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


@command_errors
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
    normalized_keys = collection._normalize_index_keys(keys)
    if collation is not None and not isinstance(collation, dict):
        message = "collation must be a dict or None"
        raise TypeError(message)
    ensure_session_can_use_engine(collection._engine, session)
    index = {
        "name": default_index_name(normalized_keys) if name is None else name,
        "key": dict(normalized_keys),
    }
    options = (
        ("unique", unique),
        ("sparse", sparse),
        ("background", background),
        ("hidden", hidden),
        ("collation", collation),
        ("partialFilterExpression", partial_filter_expression),
        ("expireAfterSeconds", expire_after_seconds),
        ("weights", weights),
        ("wildcardProjection", wildcard_projection),
        ("default_language", default_language),
        ("language_override", language_override),
    )
    index.update({field: value for field, value in options if value is not None})
    admitted = _admit_index_request(
        collection, [index], comment=comment, max_time_ms=max_time_ms
    )
    index = admitted["indexes"][0]
    normalized_keys = list(index["key"].items())
    partial_filter_expression = index.get("partialFilterExpression")
    collation = index.get("collation")
    expire_after_seconds = index.get("expireAfterSeconds")
    weights = index.get("weights")
    wildcard_projection = index.get("wildcardProjection")
    comment = admitted.get("comment")
    max_time_ms = admitted.get("maxTimeMS")
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
    expire_after_seconds = collection._normalize_expire_after_seconds(
        expire_after_seconds
    )
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
                (14, "TypeMismatch")
                if not isinstance(name, str)
                else (67, "CannotCreateIndex")
            )
            raise OperationFailure(message, code=code, details={"codeName": code_name})
    requested_name = name or default_index_name(normalized_keys)
    with (
        collection._runtime._engine_change_delivery_scope(session=session),
        execution_now_scope(collection._resolve_now()),
    ):
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


@command_errors
async def create_indexes(
    collection: AsyncCollection,
    indexes: object,
    *,
    comment: object | None = None,
    max_time_ms: int | None = None,
    session: ClientSession | None = None,
) -> list[str]:
    if not isinstance(indexes, list):
        message = "indexes must be a list of IndexModel instances"
        raise TypeError(message)
    ensure_session_can_use_engine(collection._engine, session)
    documents = [index_model_document(index) for index in indexes]
    admitted = _admit_index_request(
        collection, documents, comment=comment, max_time_ms=max_time_ms
    )
    models = normalize_index_models_from_command(admitted["indexes"])
    return await execute_create_indexes(
        collection,
        models,
        comment=admitted.get("comment"),
        max_time_ms=admitted.get("maxTimeMS"),
        session=session,
    )


async def execute_create_indexes(
    collection, models, *, comment=None, max_time_ms=None, session=None
):
    collection._ensure_session_active(session)
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
                else collection._normalize_filter(index.partial_filter_expression)
            )
            with (
                collection._runtime._engine_change_delivery_scope(session=session),
                execution_now_scope(execution_now),
            ):
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
                    max_time_ms=None
                    if deadline is None
                    else max(
                        1,
                        int((deadline - time.monotonic()) * 1000),
                    ),
                    context=session,
                )
        except Exception:
            for created_name in reversed(created_names):
                with suppress(Exception):
                    await collection._engine.drop_index(
                        collection._db_name,
                        collection._collection_name,
                        created_name,
                        context=session,
                    )
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


async def load_index_documents(collection, *, session=None):
    """Read prepared catalog metadata without public request or response codecs."""
    documents = await collection._engine.list_indexes(
        collection._db_name, collection._collection_name, context=session
    )
    return [_project_index_document(collection, document) for document in documents]


def _admit_list_indexes(collection, *, comment, session):
    ensure_session_can_use_engine(collection._engine, session)
    command = {"listIndexes": collection.name, "cursor": {}}
    if comment is not None:
        command["comment"] = comment
    return encode_bson_request(command, codec_options=_INDEX_LIST_OPTIONS)


async def _public_index_documents(collection, *, session):
    return [
        materialize_command_response(document, codec_options=_INDEX_LIST_OPTIONS)
        for document in await load_index_documents(collection, session=session)
    ]


def list_indexes(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> AsyncIndexCursor:
    admitted = _admit_list_indexes(collection, comment=comment, session=session)
    collection._record_operation_metadata(
        operation="list_indexes", comment=admitted.get("comment"), session=session,
    )

    async def _load_indexes() -> list[Document]:
        return await _public_index_documents(collection, session=session)

    return AsyncIndexCursor(_load_indexes)


async def index_information(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> IndexInformation:
    admitted = _admit_list_indexes(collection, comment=comment, session=session)
    collection._record_operation_metadata(
        operation="index_information", comment=admitted.get("comment"), session=session,
    )
    information = {}
    for document in await _public_index_documents(collection, session=session):
        entry = dict(document)
        name = entry.pop("name")
        entry["key"] = list(entry["key"].items())
        information[name] = entry
    return information


@command_errors
async def execute_drop_index(collection, target, *, comment=None, session=None):
    """Execute an already admitted index removal without client codecs."""
    before = await collection._engine.index_information(
        collection._db_name, collection._collection_name, context=session,
    )
    if target == "*":
        await collection._engine.drop_indexes(
            collection._db_name, collection._collection_name, context=session,
        )
    else:
        try:
            await collection._engine.drop_index(
                collection._db_name, collection._collection_name, target,
                context=session,
            )
        except OperationFailure as error:
            if (
                error.code != _INDEX_NOT_FOUND_CODE
                or collection._mongodb_dialect.key != "9.0"
            ):
                raise
    collection._record_operation_metadata(
        operation="drop_indexes" if target == "*" else "drop_index",
        comment=comment, session=session,
    )
    return DropIndexesCommandResult(
        previous_index_count=len(before),
        message=(
            "non-_id indexes dropped for collection" if target == "*" else None
        ),
    )


async def _drop_index_request(collection, target, *, comment, session):
    if isinstance(target, list):
        target = default_index_name(target)
    if not isinstance(target, str):
        message = "index_or_name must be a string or list of index keys"
        raise TypeError(message)
    ensure_session_can_use_engine(collection._engine, session)
    command = {"dropIndexes": collection.name, "index": target}
    if comment is not None:
        command["comment"] = comment
    admitted = encode_bson_request(command, codec_options=collection.codec_options)
    response = await execute_drop_index(
        collection, admitted["index"], comment=admitted.get("comment"), session=session,
    )
    # The driver decodes this otherwise discarded response after the write.
    materialize_command_response(
        response.to_document(), codec_options=collection.codec_options,
    )


@command_errors
async def drop_index(
    collection: AsyncCollection,
    index_or_name: str | object,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> None:
    await _drop_index_request(
        collection, index_or_name, comment=comment, session=session,
    )


@command_errors
async def drop_indexes(
    collection: AsyncCollection,
    *,
    comment: object | None = None,
    session: ClientSession | None = None,
) -> None:
    await _drop_index_request(collection, "*", comment=comment, session=session)
