"""Typed internal diagnostics; public exception constructors remain permissive."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from types import MappingProxyType

from mongoeco.error_catalog import SERVER_CODE_NAMES
from mongoeco.errors import OperationFailure


def _freeze_context(value):
    if isinstance(value, Mapping):
        return MappingProxyType(
            {key: _freeze_context(item) for key, item in value.items()}
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze_context(item) for item in value)
    return deepcopy(value)


def _copy_context(value):
    if isinstance(value, Mapping):
        return {key: _copy_context(item) for key, item in value.items()}
    if isinstance(value, tuple):
        return [_copy_context(item) for item in value]
    return deepcopy(value)


class ErrorScope(Enum):
    COMMAND = "command"
    ITEM = "item"


@dataclass(frozen=True, slots=True)
class ServerDiagnostic:
    code: int
    message: str
    scope: ErrorScope = ErrorScope.ITEM
    context: Mapping | None = None
    code_name: str | None = None
    reason: str | None = None

    def __post_init__(self):
        if not isinstance(self.code, int) or isinstance(self.code, bool):
            message = "server diagnostic code must be an integer"
            raise TypeError(message)
        if not isinstance(self.message, str) or not self.message:
            message = "server diagnostic message must be nonempty"
            raise ValueError(message)
        if not isinstance(self.scope, ErrorScope):
            message = "server diagnostic scope must be explicit"
            raise TypeError(message)
        object.__setattr__(self, "context", _freeze_context(self.context or {}))

    def command_document(self):
        return {
            **_copy_context(self.context),
            "ok": 0.0,
            "code": self.code,
            "codeName": self.code_name
            or SERVER_CODE_NAMES.get(self.code, f"Location{self.code}"),
            "errmsg": self.message,
        }

    def item_document(self, index=0):
        return {
            **_copy_context(self.context),
            "index": index,
            "code": self.code,
            "errmsg": self.message,
        }


def server_error(message, *, code, scope=ErrorScope.ITEM, context=None, reason=None):
    diagnostic = ServerDiagnostic(code, message, scope, context, reason=reason)
    error = OperationFailure(message, code=code, details=diagnostic.command_document())
    error._server_diagnostic = diagnostic
    return error


def error_scope(error):
    diagnostic = getattr(error, "_server_diagnostic", None)
    return (
        diagnostic.scope
        if isinstance(diagnostic, ServerDiagnostic)
        else ErrorScope.ITEM
    )


def retain_diagnostic(source, target):
    diagnostic = getattr(source, "_server_diagnostic", None)
    if diagnostic is not None:
        target._server_diagnostic = diagnostic
    for note in getattr(source, "__notes__", ()):
        target.add_note(note)
    return target


def _expression_operation_error(diagnostic, operation, dialect, namespace):
    if (
        diagnostic.reason == "expression_optimization"
        and operation == "aggregate_match"
    ):
        return server_error(
            "Failed to optimize pipeline :: caused by :: " + diagnostic.message,
            code=diagnostic.code,
            reason="expression_optimized_pipeline",
        )
    if diagnostic.reason != "expression_type":
        return None
    if operation in {"aggregate_runtime", "count_documents"}:
        prefix = (
            f"Executor error during aggregate command on namespace: {namespace} "
            ":: caused by :: "
            if dialect == "9.0"
            else "PlanExecutor error during aggregation :: caused by :: "
        )
    elif operation in {"find_runtime", "find_one"}:
        prefix = (
            "Executor error during find command :: caused by :: "
            if dialect == "7.0"
            else f"Executor error during find command: {namespace} :: caused by :: "
        )
    elif operation == "distinct":
        prefix = (
            "Executor error during distinct command on namespace: "
            f"{namespace} :: caused by :: "
            if dialect == "9.0"
            else "Executor error during distinct command :: caused by :: "
        )
    elif dialect == "9.0" and operation in {"update_one", "update_many"}:
        prefix = "Plan executor error during update :: caused by :: "
    else:
        return None
    return server_error(
        prefix + diagnostic.message, code=diagnostic.code, reason="expression_execution"
    )


def operation_error(error, operation, dialect, *, namespace=None):
    """Resolve a semantic diagnostic using the operation's explicit server context."""
    diagnostic = getattr(error, "_server_diagnostic", None)
    if diagnostic is not None and diagnostic.reason in {
        "expression_type",
        "expression_optimization",
    }:
        resolved = _expression_operation_error(
            diagnostic, operation, dialect, namespace
        )
        if resolved is not None:
            error = resolved
    if diagnostic is not None and diagnostic.reason == "arithmetic_overflow":
        if operation.startswith("find_one_and_"):
            prefix = "Plan executor error during findAndModify :: caused by :: "
        elif dialect == "9.0" and operation in {"update_one", "update_many"}:
            prefix = "Plan executor error during update :: caused by :: "
        else:
            return error
        if diagnostic.message.startswith(prefix):
            return error
        return server_error(
            prefix + diagnostic.message,
            code=diagnostic.code, scope=diagnostic.scope,
            context=diagnostic.context, reason=diagnostic.reason,
        )
    if diagnostic is None or diagnostic.reason != "cluster_time_unavailable":
        return error
    code = diagnostic.code
    if dialect == "8.0" and operation in {"count_documents", "aggregate_match"}:
        code = 5611301
    elif dialect == "9.0" and operation in {
        "delete_one",
        "delete_many",
        "find_one_and_delete",
        "distinct",
    }:
        code = 51144
    if code == diagnostic.code:
        return error
    return server_error(
        diagnostic.message,
        code=code,
        scope=diagnostic.scope,
        context=diagnostic.context,
        reason=diagnostic.reason,
    )
