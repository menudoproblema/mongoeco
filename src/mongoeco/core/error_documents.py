"""Serialize server diagnostics without embedding Python exception formatting."""

from __future__ import annotations

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
    return details


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
            resolved = operation_error(error, method.__name__, dialect)
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
            resolved = operation_error(error, method.__name__, dialect)
            raise _complete_command_error(resolved) from error

    return invoke
