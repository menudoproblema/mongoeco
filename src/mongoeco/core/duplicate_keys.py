"""Carry a rejected index key to the command boundary without changing item errors."""

from __future__ import annotations

import json
import math
import re
import uuid

from datetime import UTC, datetime
from itertools import product

from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import (
    index_collation_document,
    index_collation_key,
    normalize_collation,
)
from mongoeco.core.error_documents import error_message
from mongoeco.core.filtering import QueryEngine
from mongoeco.errors import DuplicateKeyError


def duplicate_key_error(outcome, message):
    error = DuplicateKeyError(message)
    context = getattr(outcome, "_duplicate_key_context", None)
    if context is not None:
        error._duplicate_key_context = context
    return error


def duplicate_key_context(*, namespace, index, values, conflict, equality):
    """Attach the actual rejected key while the engine still owns its selection."""
    key = next(
        (
            row
            for row in values
            if row == conflict or tuple(equality(value) for value in row) == conflict
        ),
        values[0],
    )
    context = {
        "namespace": namespace,
        "diagnosticKey": DocumentCodec.to_pymongo(
            dict(zip(index["fields"], key, strict=True))
        ),
        "index": index["name"],
        "keyPattern": dict(index["key"]),
        "keyValue": DocumentCodec.to_pymongo(
            {
                field: None if value == [] else value
                for field, value in zip(index["fields"], key, strict=True)
            }
        ),
    }
    collation = normalize_collation(index.get("collation"))
    if collation is not None and collation.locale != "simple":
        context["collation"] = index_collation_document(collation)
        if any(type(value) is str for value in key):
            context["hexEncoded"] = [type(value) is str for value in key]
        for field, value in zip(index["fields"], key, strict=True):
            if type(value) is str:
                encoded = index_collation_key(value, collation).hex()
                context["keyValue"][field] = encoded
                context["diagnosticKey"][field] = f"CollationKey(0x{encoded})"
            else:
                transformed = _collated_container(context["keyValue"][field], collation)
                context["keyValue"][field] = transformed
                if value != []:
                    context["diagnosticKey"][field] = transformed
    return context


def duplicate_key_message(context, *, command=True, update_dialect=None):
    """Render the stable MongoDB duplicate-key diagnostic for command failures."""
    values = ", ".join(
        f"{field}: {'undefined' if value == [] else _diagnostic_value(value)}"
        for field, value in context["diagnosticKey"].items()
    )
    collation = (
        f"collation: {_diagnostic_value(context['collation'])} "
        if "collation" in context
        else ""
    )
    return (
        (
            "Plan executor error during findAndModify :: caused by :: "
            if command
            else "Plan executor error during update :: caused by :: "
            if update_dialect == "9.0"
            else ""
        )
        + f"E11000 duplicate key error collection: {context['namespace']} "
        f"index: {context['index']} {collation}dup key: {{ {values} }}"
    )


def write_error_message(error, *, update_dialect=None):
    """Render item duplicates from their rejected key, retaining other diagnostics."""
    context = getattr(error, "_duplicate_key_context", None)
    if context is None:
        return error_message(error)
    return duplicate_key_message(context, command=False, update_dialect=update_dialect)


_UUID_SUBTYPE = 4
_UUID_BYTES = 16
_BINARY_DISPLAY_LIMIT = 80


def _diagnostic_value(value):  # noqa: PLR0911, PLR0912 - explicit BSON display dispatch
    kind = type(value).__name__
    if kind == "Code":
        code = str(value)
        if value.scope is not None:
            return f"CodeWScope( {code}, {_diagnostic_value(value.scope)})"
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
                f"{key}: {_diagnostic_value(item)}" for key, item in value.items()
            )
            + " }"
        )
    if isinstance(value, list):
        if not value:
            return "[]"
        return "[ " + ", ".join(_diagnostic_value(item) for item in value) + " ]"
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


def _collated_container(value, collation):
    if type(value) is str:
        return index_collation_key(value, collation).decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {
            key: _collated_container(item, collation) for key, item in value.items()
        }
    if isinstance(value, list):
        return [_collated_container(item, collation) for item in value]
    return value


def index_build_duplicate_error(*, namespace, index, document, conflict):
    """Build both engines' index diagnostics from BSON values before encoding loss."""
    context = duplicate_key_context(
        namespace=namespace,
        index=index,
        values=list(
            product(
                *(
                    QueryEngine.extract_values(document, field) or [None]
                    for field in index["fields"]
                )
            )
        ),
        conflict=conflict,
        equality=bson_equality_key,
    )
    message = "Index build failed: " + duplicate_key_message(context, command=False)
    return DuplicateKeyError(
        message,
        code=11000,
        details={
            "ok": 0.0,
            "code": 11000,
            "codeName": "DuplicateKey",
            "errmsg": message,
            **{
                key: context[key]
                for key in ("keyPattern", "keyValue", "collation", "hexEncoded")
                if key in context
            },
        },
    )
