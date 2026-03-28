from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from mongoeco.driver.connections import ConnectionLease, ConnectionPoolSnapshot, ConnectionRegistry
from mongoeco.driver.discovery import SrvResolution, materialize_srv_uri, resolve_srv_seeds
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
from mongoeco.driver.requests import CommandRequest, RequestExecutionPlan
from mongoeco.driver.security import AuthPolicy, TlsPolicy, build_auth_policy, build_tls_policy
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


@dataclass(frozen=True, slots=True)
class PreparedRequestExecution:
    plan: RequestExecutionPlan
    selected_server: ServerDescription
    connection: ConnectionLease


class DriverRuntime:
    def __init__(
        self,
        *,
        uri: str | None,
        write_concern: WriteConcern,
        read_concern: ReadConcern,
        read_preference: ReadPreference,
        srv_records: tuple[tuple[str, int | None], ...] | None = None,
    ):
        self._uri = parse_mongo_uri(uri)
        resolution = resolve_srv_seeds(
            self._uri,
            srv_records=None
            if srv_records is None
            else tuple(MongoUriSeed(host, port) for host, port in srv_records),
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
                session_id=None if session is None else session.session_id,
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

    def prepare_request_execution(self, plan: RequestExecutionPlan) -> PreparedRequestExecution:
        if not plan.candidate_servers:
            raise RuntimeError("no candidate servers available for request execution")
        selected_server = plan.candidate_servers[0]
        lease = self._connections.checkout(selected_server)
        return PreparedRequestExecution(
            plan=plan,
            selected_server=selected_server,
            connection=lease,
        )

    def complete_request_execution(self, execution: PreparedRequestExecution) -> None:
        self._connections.checkin(execution.connection)

    def clear_connections(self) -> None:
        self._connections.clear()

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
