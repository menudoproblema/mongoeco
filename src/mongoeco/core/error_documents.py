"""Serialize server diagnostics without embedding Python exception formatting."""

from __future__ import annotations

import json
import math
import re
import uuid

from datetime import UTC, datetime
from functools import wraps

from mongoeco.core.server_errors import operation_error, retain_diagnostic
from mongoeco.error_catalog import SERVER_CODE_NAMES as CODE_NAMES
from mongoeco.errors import OperationFailure


def error_message(error):
    """Return the diagnostic, never OperationFailure's rendered ``full error``."""
    details = getattr(error, "details", None)
    if isinstance(details, dict) and isinstance(details.get("errmsg"), str):
        return details["errmsg"]
    return getattr(error, "_mongoeco_error_message", str(error))


def write_error_details(error):
    context = (
        getattr(error, "_duplicate_key_context", None)
        or getattr(error, "details", None)
        or {}
    )
    return {
        key: context[key]
        for key in ("keyPattern", "keyValue", "hexEncoded", "collation", "errInfo")
        if key in context
    }


def command_error_document(error):
    diagnostic = getattr(error, "_server_diagnostic", None)
    if diagnostic is not None:
        return diagnostic.command_document()
    details = dict(error.details or {})
    details.update(ok=0.0, errmsg=error_message(error))
    if error.code is not None:
        details["code"] = error.code
    name = (
        details.get("codeName")
        or getattr(error, "code_name", None)
        or (
            CODE_NAMES.get(error.code, f"Location{error.code}")
            if error.code is not None
            else None
        )
    )
    if name is not None:
        details["codeName"] = name
    document = {
        key: details[key]
        for key in ("ok", "errmsg", "code", "codeName")
        if key in details
    }
    document.update(details)
    return document


def _complete_command_error(error):
    if error.code is None and error.details is None:
        return error
    completed = type(error)(
        error_message(error), code=error.code, details=command_error_document(error)
    )
    name = getattr(error, "code_name", None)
    if name is not None:
        completed.code_name = name
    return retain_diagnostic(error, completed)


def command_errors(method):
    """Complete exceptions escaping a command, preserving their public family."""

    @wraps(method)
    async def invoke(*args, **kwargs):
        try:
            return await method(*args, **kwargs)
        except OperationFailure as error:
            dialect = (
                getattr(getattr(args[0], "_mongodb_dialect", None), "key", None)
                if args
                else None
            )
            resolved = operation_error(
                error,
                method.__name__,
                dialect,
                namespace=getattr(args[0], "full_name", None) if args else None,
            )
            raise _complete_command_error(resolved) from error

    return invoke


def sync_command_errors(method):
    """Complete synchronous command preparation before returning a cursor."""

    @wraps(method)
    def invoke(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except OperationFailure as error:
            dialect = (
                getattr(getattr(args[0], "_mongodb_dialect", None), "key", None)
                if args
                else None
            )
            resolved = operation_error(
                error,
                method.__name__,
                dialect,
                namespace=getattr(args[0], "full_name", None) if args else None,
            )
            raise _complete_command_error(resolved) from error

    return invoke


_UUID_SUBTYPE = 4
_UUID_BYTES = 16
_BINARY_DISPLAY_LIMIT = 80


def render_bson_diagnostic(value):  # noqa: PLR0911, PLR0912 - explicit BSON display dispatch
    kind = type(value).__name__
    if kind == "Code":
        code = str(value)
        if value.scope is not None:
            return f"CodeWScope( {code}, {render_bson_diagnostic(value.scope)})"
        return _truncate_text(code, 79, 70)
    if kind in {"MinKey", "MaxKey"}:
        return kind
    if isinstance(value, uuid.UUID):
        return f'UUID("{value}")'
    if isinstance(value, str):
        return f'"{_truncate_text(value, 159, 150)}"'
    if isinstance(value, bytes):
        subtype = getattr(value, "subtype", 0)
        if subtype == _UUID_SUBTYPE and len(value) == _UUID_BYTES:
            return f'UUID("{uuid.UUID(bytes=bytes(value))}")'
        binary = (
            value[:70].hex().upper() + "..."
            if len(value) > _BINARY_DISPLAY_LIMIT
            else value.hex().upper()
        )
        return f"BinData({subtype}, {binary})"
    if isinstance(value, float) and not math.isfinite(value):
        return "nan.0" if math.isnan(value) else "-inf.0" if value < 0 else "inf.0"
    if isinstance(value, datetime):
        normalized = value.replace(tzinfo=UTC) if value.tzinfo is None else value
        delta = normalized - datetime(1970, 1, 1, tzinfo=UTC)
        milliseconds = (
            delta.days * 86400000 + delta.seconds * 1000 + delta.microseconds // 1000
        )
        return f"new Date({milliseconds})"
    kind = type(value).__name__
    renderers = {
        "ObjectId": lambda: f"ObjectId('{value}')",
        "Binary": lambda: f"BinData({value.subtype}, {bytes(value).hex().upper()})",
        "Timestamp": lambda: f"Timestamp({value.time}, {value.inc})",
        "Decimal128": lambda: str(value),
        "Regex": lambda: _regex_diagnostic(value),
    }
    if kind in renderers:
        return renderers[kind]()
    if isinstance(value, dict):
        if not value:
            return "{}"
        return (
            "{ "
            + ", ".join(
                f"{key}: {render_bson_diagnostic(item)}" for key, item in value.items()
            )
            + " }"
        )
    if isinstance(value, list):
        if not value:
            return "[]"
        return "[ " + ", ".join(render_bson_diagnostic(item) for item in value) + " ]"
    try:
        rendered = json.dumps(value, ensure_ascii=False)
    except TypeError:
        rendered = str(value)
    return rendered


def _regex_diagnostic(value):
    flags = "".join(
        letter
        for flag, letter in (
            (re.IGNORECASE, "i"),
            (re.LOCALE, "l"),
            (re.MULTILINE, "m"),
            (re.DOTALL, "s"),
            (re.UNICODE, "u"),
            (re.VERBOSE, "x"),
        )
        if value.flags & flag
    )
    return f"/{value.pattern}/{flags}"


def _truncate_text(value, threshold, length):
    encoded = value.encode("utf-8")
    if len(encoded) <= threshold:
        return value
    return encoded[:length].decode("utf-8", errors="replace") + "..."
