"""Validate classic write grammar before publishing any batch effects."""

from __future__ import annotations

from typing import TYPE_CHECKING, NoReturn

from mongoeco.core.bson_scalars import wrap_bson_numeric
from mongoeco.core.codec import DocumentCodec
from mongoeco.errors import OperationFailure


if TYPE_CHECKING:
    from mongoeco.core.bson_scalars import BsonNumeric

_MAX_WRITE_CONCERN_NODES = 50
_WRITE_CONCERN_FIELDS = frozenset(
    {
        "w",
        "j",
        "fsync",
        "wtimeout",
        "provenance",
        "getLastError",
        "wOpTime",
        "wElectionId",
    }
)


def command_shape_error(message: str, *, code: int = 14) -> NoReturn:
    raise OperationFailure(
        message,
        code=code,
        details={
            "codeName": {
                2: "BadValue",
                9: "FailedToParse",
                40414: "IDLFailedToParse",
                40415: "IDLUnknownField",
            }.get(code, "TypeMismatch"),
        },
    )


def validate_selection_shapes(spec: dict[str, object], *, update: bool = False) -> None:
    if "hint" in spec and not isinstance(spec["hint"], (str, dict)):
        command_shape_error("hint must be a string or document", code=9)
    for field in ("collation", "sort") if update else ("collation",):
        if spec.get(field) is not None and not isinstance(spec[field], dict):
            command_shape_error(f"{field} must be a document")


def _bson_number(value: object) -> BsonNumeric | None:
    # Wire decoding converts this scalar already; direct commands retain it.
    numeric = wrap_bson_numeric(value)
    if numeric is None:
        numeric = wrap_bson_numeric(DocumentCodec.to_internal(value))
    return numeric


def _sync_requested(value: object, field: str) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    numeric = _bson_number(value)
    if numeric is not None:
        return bool(numeric.value)
    command_shape_error(f"writeConcern.{field} must be boolean or numeric")


def _write_concern_w(value: object) -> bool:
    numeric = _bson_number(value)
    if numeric is not None:
        try:
            count = int(numeric.value)
        except (ValueError, OverflowError):
            command_shape_error("writeConcern.w must be a finite number", code=9)
        if not 0 <= count <= _MAX_WRITE_CONCERN_NODES:
            command_shape_error("writeConcern.w must be between 0 and 50", code=9)
        if count > 1:
            command_shape_error("w > 1 requires a replicated host", code=2)
        return count != 0
    if value is None or isinstance(value, str):
        if value != "majority":
            command_shape_error("non-majority w mode requires a replica set", code=2)
        return True
    if isinstance(value, dict):
        if not value or any(_bson_number(count) is None for count in value.values()):
            command_shape_error("tagged write concern requires numeric tags", code=9)
        return True
    command_shape_error("writeConcern.w must be numeric, string or document", code=9)


def write_command_acknowledged(spec: dict[str, object], *, dialect: str) -> bool:
    if "writeConcern" not in spec:
        return True
    concern = spec["writeConcern"]
    if concern is None and dialect == "9.0":
        return True
    if not isinstance(concern, dict):
        command_shape_error("writeConcern must be a document")
    if set(concern) - _WRITE_CONCERN_FIELDS:
        command_shape_error("unknown writeConcern field", code=40415)
    journal = _sync_requested(concern.get("j"), "j")
    fsync = _sync_requested(concern.get("fsync"), "fsync")
    if journal and fsync:
        command_shape_error("fsync and j cannot both be enabled", code=9)
    acknowledged = _write_concern_w(concern.get("w", 1))
    return acknowledged or journal or fsync
