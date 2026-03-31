from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from mongoeco.driver.connections import ConnectionLease, ConnectionPoolSnapshot, ConnectionRegistry
from mongoeco.driver.discovery import SrvResolution, materialize_srv_uri, resolve_srv_dns, resolve_srv_seeds
from mongoeco.driver.execution import (
    RequestExecutionResult,
    RequestExecutionTrace,
    classify_request_exception,
    execute_request_pipeline,
)
from mongoeco.driver.monitoring import (
    ConnectionCheckedInEvent,
    ConnectionCheckedOutEvent,
    DriverMonitor,
    ServerSelectedEvent,
)
from mongoeco.driver.policies import (
    ConcernPolicy,
    RetryPolicy,
    SelectionPolicy,
    TimeoutPolicy,
    build_concern_policy,
    build_retry_policy,
    build_selection_policy,
    build_timeout_policy,
)
from mongoeco.driver.requests import CommandRequest, PreparedRequestExecution, RequestExecutionPlan
from mongoeco.driver.security import AuthPolicy, TlsPolicy, build_auth_policy, build_tls_policy
from mongoeco.driver.topology_monitor import refresh_topology
from mongoeco.driver.topology import ServerDescription, TopologyDescription, build_local_topology_description
from mongoeco.driver.uri import (
    MongoUri,
    MongoUriSeed,
    build_read_concern_from_uri,
    build_read_preference_from_uri,
    build_write_concern_from_uri,
    parse_mongo_uri,
)
from mongoeco.session import ClientSession
from mongoeco.types import ReadConcern, ReadPreference, WriteConcern
from mongoeco.errors import ServerSelectionTimeoutError

if TYPE_CHECKING:
    from mongoeco.driver.transports import WireProtocolCommandTransport


class DriverRuntime:
    def __init__(
        self,
        *,
        uri: str | None,
        write_concern: WriteConcern,
        read_concern: ReadConcern,
        read_preference: ReadPreference,
        srv_records: tuple[tuple[str, int | None], ...] | None = None,
        srv_resolver=None,
    ):
        self._uri = parse_mongo_uri(uri)
        if srv_records is None:
            resolution = resolve_srv_dns(self._uri, resolver=srv_resolver) if self._uri.scheme == "mongodb+srv" else None
        else:
            resolution = resolve_srv_seeds(
                self._uri,
                srv_records=tuple(MongoUriSeed(host, port) for host, port in srv_records),
            )
        self._srv_resolution = resolution
        self._effective_uri = materialize_srv_uri(self._uri, resolution=resolution)
        effective_write_concern = build_write_concern_from_uri(self._effective_uri, write_concern)
        effective_read_concern = build_read_concern_from_uri(self._effective_uri, read_concern)
        effective_read_preference = build_read_preference_from_uri(self._effective_uri, read_preference)
        self._auth_policy = build_auth_policy(self._effective_uri)
        self._tls_policy = build_tls_policy(self._effective_uri)
        self._topology = build_local_topology_description(self._effective_uri)
        self._timeout_policy = build_timeout_policy(self._effective_uri)
        self._retry_policy = build_retry_policy(self._effective_uri)
        self._selection_policy = build_selection_policy(
            self._effective_uri,
            read_preference=effective_read_preference,
        )
        self._concern_policy = build_concern_policy(
            write_concern=effective_write_concern,
            read_concern=effective_read_concern,
            read_preference=effective_read_preference,
        )
        self._connections = ConnectionRegistry(self._effective_uri)
        self._monitor = DriverMonitor()
        self._topology_monitor_task: asyncio.Task[None] | None = None

    def plan_command_request(
        self,
        database: str,
        command_name: str,
        payload: dict[str, Any],
        *,
        session: ClientSession | None = None,
        read_only: bool = False,
    ) -> RequestExecutionPlan:
        return RequestExecutionPlan(
            request=CommandRequest(
                database=database,
                command_name=command_name,
                payload=payload,
                session=session,
                read_only=read_only,
            ),
            topology=self._topology,
            timeout_policy=self._timeout_policy,
            retry_policy=self._retry_policy,
            selection_policy=self._selection_policy,
            concern_policy=self._concern_policy,
            auth_policy=self._auth_policy,
            tls_policy=self._tls_policy,
            candidate_servers=self._selection_policy.select_servers(
                self._topology,
                for_writes=not read_only,
            ),
        )

    async def prepare_request_execution(self, plan: RequestExecutionPlan) -> PreparedRequestExecution:
        return await self.prepare_request_execution_attempt(plan, attempt_number=1)

    async def prepare_request_execution_attempt(
        self,
        plan: RequestExecutionPlan,
        *,
        attempt_number: int,
    ) -> PreparedRequestExecution:
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
            )
        )
        return execution

    async def complete_request_execution(self, execution: PreparedRequestExecution) -> None:
        await self._connections.checkin_async(execution.connection)
        self._monitor.emit(
            ConnectionCheckedInEvent(
                database=execution.plan.request.database,
                command_name=execution.plan.request.command_name,
                server_address=execution.selected_server.address,
                connection_id=execution.connection.connection_id,
                attempt_number=execution.attempt_number,
                session_id=execution.plan.request.session_id,
            )
        )

    async def discard_request_execution(self, execution: PreparedRequestExecution) -> None:
        await self._connections.discard_async(execution.connection)

    def clear_connections(self) -> None:
        self._connections.clear()

    async def execute_request(self, plan: RequestExecutionPlan, transport) -> RequestExecutionResult:
        if not plan.candidate_servers:
            error = ServerSelectionTimeoutError(
                f"no eligible servers found within {plan.timeout_policy.server_selection_timeout_ms}ms"
            )
            return RequestExecutionResult(
                outcome=classify_request_exception(error, plan=plan),
                trace=RequestExecutionTrace(),
            )
        return await execute_request_pipeline(
            plan=plan,
            prepare_execution=self.prepare_request_execution_attempt,
            complete_execution=self.complete_request_execution,
            discard_execution=self.discard_request_execution,
            transport=transport,
            monitor=self._monitor,
        )

    def create_network_transport(self) -> WireProtocolCommandTransport:
        from mongoeco.driver.transports import WireProtocolCommandTransport

        return WireProtocolCommandTransport(
            self._connections,
            tls_policy=self._tls_policy,
            connect_timeout_ms=self._timeout_policy.connect_timeout_ms,
        )

    async def refresh_topology(self, transport: WireProtocolCommandTransport | None = None) -> TopologyDescription:
        self._topology = await refresh_topology(
            current_topology=self._topology,
            prepare_execution=self.prepare_request_execution_attempt,
            complete_execution=self.complete_request_execution,
            transport=self.create_network_transport() if transport is None else transport,
        )
        return self._topology

    async def start_topology_monitoring(
        self,
        *,
        transport: WireProtocolCommandTransport | None = None,
    ) -> None:
        if self._topology_monitor_task is not None and not self._topology_monitor_task.done():
            return
        self._topology_monitor_task = asyncio.create_task(
            self._topology_monitor_loop(transport=transport),
            name="mongoeco-driver-topology-monitor",
        )

    async def stop_topology_monitoring(self) -> None:
        task = self._topology_monitor_task
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        self._topology_monitor_task = None

    async def _topology_monitor_loop(
        self,
        *,
        transport: WireProtocolCommandTransport | None = None,
    ) -> None:
        interval = self._effective_uri.options.heartbeat_frequency_ms / 1000
        active_transport = self.create_network_transport() if transport is None else transport
        while True:
            await self.refresh_topology(transport=active_transport)
            await asyncio.sleep(interval)

    @property
    def uri(self) -> MongoUri:
        return self._uri

    @property
    def topology(self) -> TopologyDescription:
        return self._topology

    @property
    def effective_uri(self) -> MongoUri:
        return self._effective_uri

    @property
    def timeout_policy(self) -> TimeoutPolicy:
        return self._timeout_policy

    @property
    def retry_policy(self) -> RetryPolicy:
        return self._retry_policy

    @property
    def selection_policy(self) -> SelectionPolicy:
        return self._selection_policy

    @property
    def concern_policy(self) -> ConcernPolicy:
        return self._concern_policy

    @property
    def auth_policy(self) -> AuthPolicy:
        return self._auth_policy

    @property
    def tls_policy(self) -> TlsPolicy:
        return self._tls_policy

    @property
    def srv_resolution(self) -> SrvResolution | None:
        return self._srv_resolution

    @property
    def connection_snapshots(self) -> tuple[ConnectionPoolSnapshot, ...]:
        return self._connections.snapshots()

    @property
    def monitor(self) -> DriverMonitor:
        return self._monitor
