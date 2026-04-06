from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Protocol

from mongoeco.driver.monitoring import CommandFailedEvent, CommandStartedEvent, CommandSucceededEvent, DriverMonitor
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
    discard_execution=None,
    transport: AsyncCommandTransport,
    monitor: DriverMonitor | None = None,
) -> RequestExecutionResult:
    attempts: list[RequestAttempt] = []
    max_attempts = len(plan.candidate_servers) or 1
    if _retry_enabled(plan):
        max_attempts = max(max_attempts, 2)
    for attempt_number in range(1, max_attempts + 1):
        execution = await prepare_execution(plan, attempt_number=attempt_number)
        started_at = time.perf_counter()
        should_discard = False
        if monitor is not None:
            monitor.emit(
                CommandStartedEvent(
                    database=plan.request.database,
                    command_name=plan.request.command_name,
                    command=dict(plan.request.payload),
                    server_address=execution.selected_server.address,
                    connection_id=execution.connection.connection_id,
                    attempt_number=execution.attempt_number,
                    read_only=plan.request.read_only,
                    session_id=plan.request.session_id,
                    request_id=execution.request_id,
                )
            )
        try:
            response = await _send_with_timeout(transport, execution, plan=plan)
            outcome = RequestOutcome(
                server_address=execution.selected_server.address,
                ok=True,
                response=response,
                retryable=False,
            )
            if monitor is not None:
                monitor.emit(
                    CommandSucceededEvent(
                        database=plan.request.database,
                        command_name=plan.request.command_name,
                        reply=response,
                        server_address=execution.selected_server.address,
                        connection_id=execution.connection.connection_id,
                        attempt_number=execution.attempt_number,
                        duration_ms=(time.perf_counter() - started_at) * 1000,
                        session_id=plan.request.session_id,
                        request_id=execution.request_id,
                    )
                )
        except Exception as exc:  # noqa: BLE001
            should_discard = isinstance(exc, ConnectionFailure)
            outcome = classify_request_exception(exc, plan=plan)
            outcome = RequestOutcome(
                server_address=execution.selected_server.address,
                ok=outcome.ok,
                response=outcome.response,
                error=outcome.error,
                retryable=outcome.retryable,
            )
            if monitor is not None:
                monitor.emit(
                    CommandFailedEvent(
                        database=plan.request.database,
                        command_name=plan.request.command_name,
                        failure=outcome.error or str(exc),
                        server_address=execution.selected_server.address,
                        connection_id=execution.connection.connection_id,
                        attempt_number=execution.attempt_number,
                        duration_ms=(time.perf_counter() - started_at) * 1000,
                        retryable=outcome.retryable,
                        session_id=plan.request.session_id,
                        request_id=execution.request_id,
                    )
                )
        finally:
            if discard_execution is not None and should_discard:
                await discard_execution(execution)
            else:
                await complete_execution(execution)
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
