"""Carry a rejected index key to the command boundary without changing item errors."""

from __future__ import annotations

from itertools import product

from mongoeco.core._public_materialization import materialize_public_value
from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import (
    index_collation_document,
    index_collation_key,
    normalize_collation,
)
from mongoeco.core.error_documents import error_message, render_bson_diagnostic
from mongoeco.core.filtering import QueryEngine
from mongoeco.errors import DuplicateKeyError
from mongoeco.types import UndefinedType


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
        "keyValue": materialize_public_value(
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
                if value != [] and not isinstance(value, UndefinedType):
                    context["diagnosticKey"][field] = transformed
    return context


def duplicate_key_message(context, *, command=True, update_dialect=None):
    """Render the stable MongoDB duplicate-key diagnostic for command failures."""
    values = ", ".join(
        f"{field}: "
        + (
            "undefined"
            if value == [] or isinstance(value, UndefinedType)
            else render_bson_diagnostic(value)
        )
        for field, value in context["diagnosticKey"].items()
    )
    collation = (
        f"collation: {render_bson_diagnostic(context['collation'])} "
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
