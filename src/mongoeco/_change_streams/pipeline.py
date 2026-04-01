from __future__ import annotations

from mongoeco.errors import OperationFailure
from mongoeco.types import ChangeEventDocument


ALLOWED_CHANGE_STREAM_STAGES = frozenset(
    {"$match", "$project", "$addFields", "$set", "$unset", "$replaceRoot", "$replaceWith"}
)
ALLOWED_FULL_DOCUMENT_MODES = frozenset({"default", "updateLookup", "whenAvailable", "required"})


def compile_change_stream_pipeline(
    pipeline: object | None,
) -> list[dict[str, object]] | None:
    if pipeline is None:
        return None
    if not isinstance(pipeline, list):
        raise TypeError("pipeline must be a list of stages")
    normalized: list[dict[str, object]] = []
    for stage in pipeline:
        if not isinstance(stage, dict) or len(stage) != 1:
            raise TypeError("pipeline stages must be single-key dicts")
        operator, spec = next(iter(stage.items()))
        if operator not in ALLOWED_CHANGE_STREAM_STAGES:
            raise OperationFailure(
                "watch only supports $match, $project, $addFields, $set, $unset, $replaceRoot, and $replaceWith stages"
            )
        if operator in {"$match", "$project", "$addFields", "$set", "$replaceRoot", "$replaceWith"} and not isinstance(spec, dict):
            raise TypeError(f"{operator} stage must be a dict")
        normalized.append(stage)
    return normalized


def normalize_full_document_mode(mode: str) -> str:
    if not isinstance(mode, str):
        raise TypeError("full_document must be a string")
    if mode not in ALLOWED_FULL_DOCUMENT_MODES:
        raise OperationFailure("full_document must be one of default, updateLookup, whenAvailable, or required")
    return mode


def apply_full_document_mode(document: ChangeEventDocument, mode: str) -> ChangeEventDocument:
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
        raise OperationFailure("change stream fullDocument is required but was not available")
    return normalized
