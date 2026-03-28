from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol

from mongoeco.driver.requests import PreparedRequestExecution, RequestExecutionPlan, RequestOutcome
from mongoeco.errors import ConnectionFailure, ExecutionTimeout, OperationFailure, PyMongoError


class AsyncCommandTransport(Protocol):
    async def send(self, execution: PreparedRequestExecution) -> dict[str, Any]:
        ...


@dataclass(frozen=True, slots=True)
class RequestAttempt:
    attempt_number: int
    server_address: str
    connection_id: str
    outcome: RequestOutcome


@dataclass(frozen=True, slots=True)
class RequestExecutionTrace:
    attempts: tuple[RequestAttempt, ...] = field(default_factory=tuple)

    @property
    def final_outcome(self) -> RequestOutcome | None:
        return self.attempts[-1].outcome if self.attempts else None


@dataclass(frozen=True, slots=True)
class RequestExecutionResult:
    outcome: RequestOutcome
    trace: RequestExecutionTrace


def classify_request_exception(
    exc: Exception,
    *,
    plan: RequestExecutionPlan,
) -> RequestOutcome:
    retryable = _is_retryable_exception(exc, plan=plan)
    if isinstance(exc, OperationFailure):
        return RequestOutcome(
            server_address=None,
            ok=False,
            error=str(exc),
            retryable=retryable,
        )
    if isinstance(exc, PyMongoError):
        return RequestOutcome(
            server_address=None,
            ok=False,
            error=str(exc),
            retryable=retryable,
        )
    return RequestOutcome(
        server_address=None,
        ok=False,
        error=f"{type(exc).__name__}: {exc}",
        retryable=retryable,
    )


async def execute_request_pipeline(
    *,
    plan: RequestExecutionPlan,
    prepare_execution,
    complete_execution,
    transport: AsyncCommandTransport,
) -> RequestExecutionResult:
    attempts: list[RequestAttempt] = []
    max_attempts = len(plan.candidate_servers) or 1
    if _retry_enabled(plan):
        max_attempts = max(max_attempts, 2)
    for attempt_number in range(1, max_attempts + 1):
        execution = prepare_execution(plan, attempt_number=attempt_number)
        try:
            response = await _send_with_timeout(transport, execution, plan=plan)
            outcome = RequestOutcome(
                server_address=execution.selected_server.address,
                ok=True,
                response=response,
                retryable=False,
            )
        except Exception as exc:  # noqa: BLE001
            outcome = classify_request_exception(exc, plan=plan)
            outcome = RequestOutcome(
                server_address=execution.selected_server.address,
                ok=outcome.ok,
                response=outcome.response,
                error=outcome.error,
                retryable=outcome.retryable,
            )
        finally:
            complete_execution(execution)
        attempts.append(
            RequestAttempt(
                attempt_number=attempt_number,
                server_address=execution.selected_server.address,
                connection_id=execution.connection.connection_id,
                outcome=outcome,
            )
        )
        if outcome.ok:
            trace = RequestExecutionTrace(tuple(attempts))
            return RequestExecutionResult(outcome=outcome, trace=trace)
        if not outcome.retryable or attempt_number >= max_attempts:
            trace = RequestExecutionTrace(tuple(attempts))
            return RequestExecutionResult(outcome=outcome, trace=trace)
    trace = RequestExecutionTrace(tuple(attempts))
    fallback = trace.final_outcome or RequestOutcome(server_address=None, ok=False, error="request execution failed")
    return RequestExecutionResult(outcome=fallback, trace=trace)


async def _send_with_timeout(
    transport: AsyncCommandTransport,
    execution: PreparedRequestExecution,
    *,
    plan: RequestExecutionPlan,
) -> dict[str, Any]:
    timeout_ms = plan.timeout_policy.socket_timeout_ms
    if timeout_ms is None:
        return await transport.send(execution)
    try:
        return await asyncio.wait_for(transport.send(execution), timeout=timeout_ms / 1000)
    except TimeoutError as exc:
        raise ExecutionTimeout("socket timeout expired during request execution") from exc


def _is_retryable_exception(exc: Exception, *, plan: RequestExecutionPlan) -> bool:
    if isinstance(exc, ConnectionFailure):
        return _retry_enabled(plan)
    if isinstance(exc, ExecutionTimeout):
        return False
    if isinstance(exc, OperationFailure):
        labels = set(exc.error_labels)
        if plan.request.read_only:
            return plan.retry_policy.retry_reads and "RetryableReadError" in labels
        return plan.retry_policy.retry_writes and "RetryableWriteError" in labels
    return False


def _retry_enabled(plan: RequestExecutionPlan) -> bool:
    return plan.retry_policy.retry_reads if plan.request.read_only else plan.retry_policy.retry_writes
