from __future__ import annotations

from typing import TYPE_CHECKING

from mongoeco.driver.execution import (
    RequestExecutionResult,
    RequestExecutionTrace,
    classify_request_exception,
    execute_request_pipeline,
)
from mongoeco.driver.monitoring import (
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    ServerSelectedEvent,
    ServerSelectionFailedEvent,
)
from mongoeco.errors import ServerSelectionTimeoutError

if TYPE_CHECKING:
    from mongoeco.driver.connections import ConnectionRegistry
    from mongoeco.driver.failpoints import DriverFailpointController
    from mongoeco.driver.monitoring import DriverMonitor
    from mongoeco.driver.requests import PreparedRequestExecution, RequestExecutionPlan
    from mongoeco.driver.transports import WireProtocolCommandTransport
    from mongoeco.driver.topology import TopologyDescription


class RuntimeAttemptLifecycle:
    def __init__(
        self,
        *,
        connections: "ConnectionRegistry",
        monitor: "DriverMonitor",
        resolve_plan,
        failpoints: "DriverFailpointController" | None = None,
    ):
        self._connections = connections
        self._monitor = monitor
        self._resolve_plan = resolve_plan
        self._failpoints = failpoints

    async def prepare(self, plan: "RequestExecutionPlan", *, attempt_number: int) -> "PreparedRequestExecution":
        from mongoeco.driver.requests import PreparedRequestExecution

        plan = self._resolve_plan(plan)
        if not plan.candidate_servers:
            raise RuntimeError("no candidate servers available for request execution")
        selected_server = plan.candidate_servers[min(attempt_number - 1, len(plan.candidate_servers) - 1)]
        lease = await self._connections.checkout_async(selected_server)
        execution = PreparedRequestExecution(
            plan=plan,
            selected_server=selected_server,
            connection=lease,
            attempt_number=attempt_number,
        )
        self._monitor.emit(
            ServerSelectedEvent(
                database=plan.request.database,
                command_name=plan.request.command_name,
                server_address=selected_server.address,
                attempt_number=attempt_number,
                read_only=plan.request.read_only,
                session_id=plan.request.session_id,
                request_id=execution.request_id,
            )
        )
        self._monitor.emit(
            ConnectionCheckedOutEvent(
                database=plan.request.database,
                command_name=plan.request.command_name,
                server_address=selected_server.address,
                connection_id=lease.connection_id,
                attempt_number=attempt_number,
                session_id=plan.request.session_id,
                request_id=execution.request_id,
            )
        )
        return execution

    async def complete(self, execution: "PreparedRequestExecution") -> None:
        await self._connections.checkin_async(execution.connection)
        self._monitor.emit(
            ConnectionCheckedInEvent(
                database=execution.plan.request.database,
                command_name=execution.plan.request.command_name,
                server_address=execution.selected_server.address,
                connection_id=execution.connection.connection_id,
                attempt_number=execution.attempt_number,
                session_id=execution.plan.request.session_id,
                request_id=execution.request_id,
            )
        )

    async def discard(self, execution: "PreparedRequestExecution") -> None:
        await self._connections.discard_async(execution.connection)

    async def execute(self, plan: "RequestExecutionPlan", *, transport) -> RequestExecutionResult:
        resolved_plan = self._resolve_plan(plan)
        forced_reason = None
        if self._failpoints is not None:
            forced_reason = self._failpoints.consume_server_selection_timeout(
                resolved_plan
            )
        if forced_reason is not None:
            self._monitor.emit(
                ServerSelectionFailedEvent(
                    database=resolved_plan.request.database,
                    command_name=resolved_plan.request.command_name,
                    reason=forced_reason,
                    topology_type=resolved_plan.topology.topology_type.value,
                    known_server_count=len(resolved_plan.topology.servers),
                    timeout_ms=resolved_plan.timeout_policy.server_selection_timeout_ms,
                    read_only=resolved_plan.request.read_only,
                    session_id=resolved_plan.request.session_id,
                    request_id=None,
                )
            )
            error = ServerSelectionTimeoutError(forced_reason)
            return RequestExecutionResult(
                outcome=classify_request_exception(error, plan=resolved_plan),
                trace=RequestExecutionTrace(),
            )
        if not resolved_plan.candidate_servers:
            reason = (
                f"no eligible servers found within "
                f"{resolved_plan.timeout_policy.server_selection_timeout_ms}ms"
            )
            self._monitor.emit(
                ServerSelectionFailedEvent(
                    database=resolved_plan.request.database,
                    command_name=resolved_plan.request.command_name,
                    reason=reason,
                    topology_type=resolved_plan.topology.topology_type.value,
                    known_server_count=len(resolved_plan.topology.servers),
                    timeout_ms=resolved_plan.timeout_policy.server_selection_timeout_ms,
                    read_only=resolved_plan.request.read_only,
                    session_id=resolved_plan.request.session_id,
                    request_id=None,
                )
            )
            error = ServerSelectionTimeoutError(reason)
            return RequestExecutionResult(
                outcome=classify_request_exception(error, plan=resolved_plan),
                trace=RequestExecutionTrace(),
            )
        return await execute_request_pipeline(
            plan=resolved_plan,
            prepare_execution=self.prepare,
            complete_execution=self.complete,
            discard_execution=self.discard,
            transport=transport,
            monitor=self._monitor,
            inject_failure=(
                None
                if self._failpoints is None
                else self._failpoints.consume_command_failure
            ),
        )
