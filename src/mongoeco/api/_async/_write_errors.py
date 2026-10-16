"""Adapt per-item write failures at the collection command boundary."""

from __future__ import annotations

from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable
from functools import wraps

from mongoeco.errors import DuplicateKeyError, OperationFailure, WriteError


_CODE_NAMES = {
    2: "BadValue",
    14: "TypeMismatch",
    28: "PathNotViable",
    40: "ConflictingUpdateOperators",
}
_FIND_MODIFY_CODE_NAMES = {
    **_CODE_NAMES,
    53: "InvalidIdField",
    54: "NotSingleValueField",
    66: "ImmutableField",
    121: "DocumentValidationFailure",
    11000: "DuplicateKey",
}


def validate_classic_update_sort(dialect_key: str, *, has_sort: bool) -> None:
    if dialect_key == "7.0" and has_sort:
        message = "BSON field 'update.updates.sort' is an unknown field"
        raise OperationFailure(
            message, code=40415, details={"codeName": "IDLUnknownField"}
        )


def write_command_errors[**P, T](
    method: Callable[P, Awaitable[T]],
) -> Callable[P, Awaitable[T]]:
    """Collection write helpers expose item errors as WriteError, like PyMongo."""

    @wraps(method)
    async def invoke(*args: P.args, **kwargs: P.kwargs) -> T:
        try:
            return await method(*args, **kwargs)
        except OperationFailure as error:
            if isinstance(error, WriteError) or error.code not in _CODE_NAMES:
                raise
            raise WriteError(
                str(error), code=error.code, details=error.details
            ) from error

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
            name = _FIND_MODIFY_CODE_NAMES.get(error.code)
            if name is None:
                raise
            error_type = (
                DuplicateKeyError
                if isinstance(error, DuplicateKeyError)
                else OperationFailure
            )
            raise error_type(
                str(error),
                code=error.code,
                details={**(error.details or {}), "codeName": name},
            ) from error

    return invoke
