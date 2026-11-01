"""Adapt per-item write failures at the collection command boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
from functools import wraps

from mongoeco.core._public_materialization import (
    materialize_find_modify_response,
    materialize_public_document,
)
from mongoeco.core.duplicate_keys import duplicate_key_message, write_error_message
from mongoeco.core.error_documents import (
    CODE_NAMES,
    command_error_document,
    error_message,
    write_error_details,
)
from mongoeco.core.server_errors import (
    ErrorScope,
    error_scope,
    operation_error,
    retain_diagnostic,
    server_error,
)
from mongoeco.errors import (
    BulkWriteError,
    DuplicateKeyError,
    ExecutionTimeout,
    OperationFailure,
    WriteError,
)
from mongoeco.types import CodecOptions


def materialize_write_response(document, *, codec_options):
    """Write replies retain builtin UUID/date options, without user decoders."""
    options = CodecOptions(
        tz_aware=codec_options.tz_aware,
        tzinfo=codec_options.tzinfo,
        uuid_representation=codec_options.uuid_representation,
    )
    return materialize_public_document(document, codec_options=options)


def materialize_write_error(error, *, codec_options):
    """Decode the write reply with builtin options, preserving client bulk ops."""
    source = error.details or {}
    if not isinstance(error, BulkWriteError):
        source = {
            "index": 0,
            "code": error.code,
            "errmsg": error_message(error),
            **write_error_details(error),
        }
    if isinstance(error, BulkWriteError):
        source = {
            **source,
            "writeErrors": [
                {key: value for key, value in entry.items() if key != "op"}
                for entry in source.get("writeErrors", ())
            ],
        }
    details = materialize_write_response(source, codec_options=codec_options)
    if isinstance(error, BulkWriteError):
        # PyMongo adds the original client operation after decoding the reply.
        for public, original in zip(
            details.get("writeErrors", ()),
            (error.details or {}).get("writeErrors", ()),
            strict=True,
        ):
            if "op" in original:
                public["op"] = original["op"]
    converted = (
        BulkWriteError(error_message(error), details=details)
        if isinstance(error, BulkWriteError)
        else type(error)._from_server_document(
            error_message(error), code=error.code, details=details
        )
    )
    return retain_diagnostic(error, converted)


def validate_classic_update_sort(dialect_key: str, *, has_sort: bool) -> None:
    if dialect_key == "7.0" and has_sort:
        message = "BSON field 'update.updates.sort' is an unknown field."
        raise server_error(message, code=40415, scope=ErrorScope.COMMAND)


def write_command_errors[**P, T](
    method: Callable[P, Awaitable[T]],
) -> Callable[P, Awaitable[T]]:
    """Collection write helpers expose item errors as WriteError, like PyMongo."""

    @wraps(method)
    async def invoke(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await method(*args, **kwargs)
        except OperationFailure as error:
            dialect = (
                getattr(getattr(args[0], "_mongodb_dialect", None), "key", None)
                if args
                else None
            )
            error = operation_error(error, method.__name__, dialect)
            if (
                isinstance(error, DuplicateKeyError)
                and getattr(error, "_duplicate_key_context", None) is not None
            ):
                message = write_error_message(
                    error, update_dialect=args[0]._mongodb_dialect.key
                )
                converted = DuplicateKeyError._from_server_document(
                    message,
                    code=error.code,
                    details={
                        "index": 0,
                        "code": error.code,
                        "errmsg": message,
                        **write_error_details(error),
                    },
                )
                if not kwargs.get("server_command"):
                    converted = materialize_write_error(
                        converted,
                        codec_options=args[0]._codec_options,
                    )
                raise retain_diagnostic(error, converted) from error
            if error_scope(error) is ErrorScope.COMMAND or isinstance(
                error, ExecutionTimeout
            ):
                raise
            error_type = type(error) if isinstance(error, WriteError) else WriteError
            converted = error_type._from_server_document(
                error_message(error),
                code=error.code,
                details={
                    "index": 0,
                    "code": error.code,
                    "errmsg": error_message(error),
                    **write_error_details(error),
                },
            )
            if not kwargs.get("server_command"):
                converted = materialize_write_error(
                    converted,
                    codec_options=args[0]._codec_options,
                )
            raise retain_diagnostic(error, converted) from error

    return invoke


def find_modify_errors[**P, T](
    method: Callable[P, Awaitable[T]],
) -> Callable[P, Awaitable[T]]:
    """FindAndModify returns a command failure with a stable server codeName."""

    @wraps(method)
    async def invoke(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await method(*args, **kwargs)
        except OperationFailure as error:
            dialect = (
                getattr(getattr(args[0], "_mongodb_dialect", None), "key", None)
                if args
                else None
            )
            error = operation_error(error, method.__name__, dialect)
            name = CODE_NAMES.get(error.code) or (error.details or {}).get("codeName")
            error_type = (
                DuplicateKeyError
                if isinstance(error, DuplicateKeyError)
                else ExecutionTimeout
                if isinstance(error, ExecutionTimeout)
                else OperationFailure
            )
            details = command_error_document(error)
            context = getattr(error, "_duplicate_key_context", None)
            message = error_message(error)
            if context is not None:
                message = duplicate_key_message(context)
                details.update(
                    keyPattern=context["keyPattern"], keyValue=context["keyValue"]
                )
                for field in ("hexEncoded", "collation"):
                    if field in context:
                        details[field] = context[field]
            details.update(ok=0.0, code=error.code, errmsg=message)
            if name is not None:
                details["codeName"] = name
            if not kwargs.get("server_command"):
                details = materialize_find_modify_response(
                    details,
                    codec_options=args[0]._codec_options,
                )
            raise retain_diagnostic(
                error, error_type(message, code=error.code, details=details)
            ) from error

    return invoke
