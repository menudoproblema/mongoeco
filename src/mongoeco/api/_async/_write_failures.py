"""Shared failure accumulation for classic command and bulk execution."""

from mongoeco.core.error_documents import error_message, write_error_details
from mongoeco.core.server_errors import ErrorScope, error_scope
from mongoeco.types import WriteErrorEntry


def record_write_failure(errors, error, *, index, operation=None, first_failure=None):
    """Command failures retain scope while unordered execution finishes its batch."""
    if error_scope(error) is ErrorScope.COMMAND:
        return first_failure if first_failure is not None else error
    errors.append(
        WriteErrorEntry(
            index=index,
            code=getattr(error, "code", None),
            errmsg=error_message(error),
            details=write_error_details(error),
            operation=operation,
        )
    )
    return first_failure
