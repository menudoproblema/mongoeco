from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from mongoeco.driver.requests import PreparedRequestExecution, RequestExecutionPlan
from mongoeco.errors import OperationFailure

_DEFAULT_RETRYABLE_MESSAGE = "mock retryable command failure"
_DEFAULT_TRANSIENT_TRANSACTION_MESSAGE = "mock transient transaction failure"
_DEFAULT_WRITE_CONCERN_TIMEOUT_MESSAGE = "mock write concern timeout"
_DEFAULT_SERVER_SELECTION_TIMEOUT_REASON = "mock server selection timeout"
_WRITE_CONCERN_TIMEOUT_CODE = 64


def _normalize_command_name(command_name: str) -> str:
    return command_name.replace("-", "_").replace(" ", "_").casefold()


def _normalize_command_names(
    command_names: Iterable[str] | None,
) -> frozenset[str] | None:
    if command_names is None:
        return None
    normalized = {
        _normalize_command_name(command_name)
        for command_name in command_names
        if isinstance(command_name, str) and command_name.strip()
    }
    if not normalized:
        return None
    return frozenset(normalized)


@dataclass(slots=True)
class _CommandFailureRule:
    kind: str
    remaining: int
    command_names: frozenset[str] | None
    message: str
    include_unknown_commit_result: bool = False

    def matches(self, plan: RequestExecutionPlan) -> bool:
        if self.command_names is None:
            return True
        command_name = _normalize_command_name(plan.request.command_name)
        return command_name in self.command_names


@dataclass(slots=True)
class _SelectionTimeoutRule:
    remaining: int
    command_names: frozenset[str] | None
    reason: str

    def matches(self, plan: RequestExecutionPlan) -> bool:
        if self.command_names is None:
            return True
        command_name = _normalize_command_name(plan.request.command_name)
        return command_name in self.command_names


class DriverFailpointController:
    """Inject high-level driver failures for deterministic mock/runtime tests."""

    def __init__(self) -> None:
        self._command_failure_rules: list[_CommandFailureRule] = []
        self._selection_timeout_rules: list[_SelectionTimeoutRule] = []

    def clear(self) -> None:
        self._command_failure_rules.clear()
        self._selection_timeout_rules.clear()

    def inject_retryable_command_failure(
        self,
        *,
        command_names: Iterable[str] | None = None,
        times: int = 1,
        message: str = _DEFAULT_RETRYABLE_MESSAGE,
    ) -> None:
        self._command_failure_rules.append(
            _CommandFailureRule(
                kind="retryable",
                remaining=max(times, 0),
                command_names=_normalize_command_names(command_names),
                message=message,
            )
        )

    def inject_transient_transaction_failure(
        self,
        *,
        command_names: Iterable[str] | None = ("commitTransaction", "abortTransaction"),
        times: int = 1,
        message: str = _DEFAULT_TRANSIENT_TRANSACTION_MESSAGE,
        include_unknown_commit_result: bool = False,
    ) -> None:
        self._command_failure_rules.append(
            _CommandFailureRule(
                kind="transient-transaction",
                remaining=max(times, 0),
                command_names=_normalize_command_names(command_names),
                message=message,
                include_unknown_commit_result=include_unknown_commit_result,
            )
        )

    def inject_write_concern_timeout(
        self,
        *,
        command_names: Iterable[str] | None = ("insert", "update", "delete", "findAndModify"),
        times: int = 1,
        message: str = _DEFAULT_WRITE_CONCERN_TIMEOUT_MESSAGE,
    ) -> None:
        self._command_failure_rules.append(
            _CommandFailureRule(
                kind="write-concern-timeout",
                remaining=max(times, 0),
                command_names=_normalize_command_names(command_names),
                message=message,
            )
        )

    def inject_server_selection_timeout(
        self,
        *,
        command_names: Iterable[str] | None = None,
        times: int = 1,
        reason: str = _DEFAULT_SERVER_SELECTION_TIMEOUT_REASON,
    ) -> None:
        self._selection_timeout_rules.append(
            _SelectionTimeoutRule(
                remaining=max(times, 0),
                command_names=_normalize_command_names(command_names),
                reason=reason,
            )
        )

    def consume_server_selection_timeout(
        self,
        plan: RequestExecutionPlan,
    ) -> str | None:
        for rule in tuple(self._selection_timeout_rules):
            if rule.remaining <= 0:
                continue
            if not rule.matches(plan):
                continue
            rule.remaining -= 1
            return rule.reason
        return None

    def consume_command_failure(
        self,
        execution: PreparedRequestExecution,
    ) -> Exception | None:
        plan = execution.plan
        for rule in tuple(self._command_failure_rules):
            if rule.remaining <= 0:
                continue
            if not rule.matches(plan):
                continue
            rule.remaining -= 1
            if rule.kind == "retryable":
                label = "RetryableReadError" if plan.request.read_only else "RetryableWriteError"
                return OperationFailure(rule.message, error_labels=(label,))
            if rule.kind == "transient-transaction":
                labels = ["TransientTransactionError"]
                command_name = _normalize_command_name(plan.request.command_name)
                if rule.include_unknown_commit_result and command_name == "committransaction":
                    labels.append("UnknownTransactionCommitResult")
                return OperationFailure(rule.message, error_labels=tuple(labels))
            if rule.kind == "write-concern-timeout":
                return OperationFailure(
                    rule.message,
                    code=_WRITE_CONCERN_TIMEOUT_CODE,
                    details={
                        "writeConcernError": {
                            "code": _WRITE_CONCERN_TIMEOUT_CODE,
                            "errmsg": rule.message,
                            "codeName": "WriteConcernTimeout",
                            "errInfo": {"wtimeout": True},
                        }
                    },
                )
        return None
