from __future__ import annotations

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.preparation import PreparedPipeline, prepare_pipeline
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.server_errors import server_error
from mongoeco.errors import OperationFailure
from mongoeco.types import (
    ChangeEventDocument,  # noqa: TC001 - introspectable helper signature
)


ALLOWED_CHANGE_STREAM_STAGES = frozenset(
    {
        "$match",
        "$project",
        "$addFields",
        "$set",
        "$unset",
        "$replaceRoot",
        "$replaceWith",
    }
)
ALLOWED_FULL_DOCUMENT_MODES = frozenset(
    {"default", "updateLookup", "whenAvailable", "required"}
)


def compile_change_stream_pipeline(
    pipeline: object | None,
    *,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collection: str | None = None,
) -> PreparedPipeline | None:
    if pipeline is None:
        return None
    if not isinstance(pipeline, list):
        raise TypeError("pipeline must be a list of stages")
    for stage in pipeline:
        if not isinstance(stage, dict):
            message = "pipeline stages must be documents"
            raise TypeError(message)
    prepared = prepare_pipeline(
        DocumentCodec.to_internal(pipeline),
        dialect=dialect,
        collection=collection,
        scope="watch",
    )
    for stage in prepared:
        operator = next(iter(stage))
        if operator not in ALLOWED_CHANGE_STREAM_STAGES:
            message = f"{operator} is not permitted in a change stream pipeline"
            raise server_error(message, code=20)
    return prepared


def normalize_full_document_mode(mode: str) -> str:
    if not isinstance(mode, str):
        raise TypeError("full_document must be a string")
    if mode not in ALLOWED_FULL_DOCUMENT_MODES:
        raise OperationFailure(
            "full_document must be one of default, updateLookup, whenAvailable, or required"
        )
    return mode


def apply_full_document_mode(
    document: ChangeEventDocument, mode: str
) -> ChangeEventDocument:
    if document.get("operationType") != "update":
        return document
    normalized = dict(document)
    full_document = normalized.get("fullDocument")
    if mode == "default":
        normalized.pop("fullDocument", None)
        return normalized
    if mode in {"updateLookup", "whenAvailable"}:
        return normalized
    if full_document is None:
        raise OperationFailure(
            "change stream fullDocument is required but was not available"
        )
    return normalized
