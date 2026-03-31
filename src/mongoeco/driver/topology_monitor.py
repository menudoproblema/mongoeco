from __future__ import annotations

import time

from mongoeco.driver.requests import CommandRequest, RequestExecutionPlan
from mongoeco.driver.topology import ServerDescription, ServerType, TopologyDescription, TopologyType


def build_probe_plan(server: ServerDescription) -> RequestExecutionPlan:
    from mongoeco.driver.policies import (
        ConcernPolicy,
        RetryPolicy,
        SelectionPolicy,
        TimeoutPolicy,
    )
    from mongoeco.driver.security import AuthPolicy, TlsPolicy
    from mongoeco.types import ReadConcern, ReadPreference, ReadPreferenceMode, WriteConcern

    topology = TopologyDescription(
        topology_type=TopologyType.SINGLE,
        servers=(server,),
    )
    return RequestExecutionPlan(
        request=CommandRequest(
            database="admin",
            command_name="hello",
            payload={"hello": 1},
            read_only=True,
        ),
        topology=topology,
        timeout_policy=TimeoutPolicy(
            server_selection_timeout_ms=30_000,
            connect_timeout_ms=20_000,
            socket_timeout_ms=None,
            wait_queue_timeout_ms=None,
        ),
        retry_policy=RetryPolicy(retry_reads=False, retry_writes=False),
        selection_policy=SelectionPolicy(mode=ReadPreferenceMode.PRIMARY),
        concern_policy=ConcernPolicy(
            write_concern=WriteConcern(),
            read_concern=ReadConcern(),
            read_preference=ReadPreference(ReadPreferenceMode.PRIMARY),
        ),
        auth_policy=AuthPolicy(None, None, None, None, {}),
        tls_policy=TlsPolicy(False, True),
        candidate_servers=(server,),
    )


async def refresh_topology(
    *,
    current_topology: TopologyDescription,
    prepare_execution,
    complete_execution,
    transport,
) -> TopologyDescription:
    refreshed_servers: list[ServerDescription] = []
    for server in current_topology.servers:
        plan = build_probe_plan(server)
        execution = await prepare_execution(plan, attempt_number=1)
        started_at = time.perf_counter()
        try:
            response = await transport.send(execution)
            refreshed_servers.append(
                _server_description_from_hello(
                    server.address,
                    response,
                    round_trip_time_ms=(time.perf_counter() - started_at) * 1000,
                )
            )
        except Exception as exc:  # noqa: BLE001
            refreshed_servers.append(
                ServerDescription(
                    address=server.address,
                    server_type=ServerType.UNKNOWN,
                    error=f"{type(exc).__name__}: {exc}",
                )
            )
        finally:
            await complete_execution(execution)
    topology_type = _derive_topology_type(tuple(refreshed_servers), fallback=current_topology.topology_type)
    set_name = _derive_set_name(tuple(refreshed_servers), topology_type=topology_type)
    logical_session_timeout = min(
        (server.logical_session_timeout_minutes for server in refreshed_servers if server.logical_session_timeout_minutes),
        default=None,
    )
    return TopologyDescription(
        topology_type=topology_type,
        servers=tuple(refreshed_servers),
        set_name=set_name,
        compatible=all(server.error is None for server in refreshed_servers)
        and _topology_is_compatible(tuple(refreshed_servers), topology_type=topology_type),
        logical_session_timeout_minutes=logical_session_timeout,
    )


def _server_description_from_hello(
    address: str,
    response: dict[str, object],
    *,
    round_trip_time_ms: float,
) -> ServerDescription:
    if response.get("msg") == "isdbgrid":
        server_type = ServerType.MONGOS
    elif isinstance(response.get("setName"), str):
        if bool(response.get("secondary")):
            server_type = ServerType.RS_SECONDARY
        elif bool(response.get("isWritablePrimary")) or bool(response.get("ismaster")):
            server_type = ServerType.RS_PRIMARY
        else:
            server_type = ServerType.UNKNOWN
    elif bool(response.get("isWritablePrimary")) or bool(response.get("ismaster")):
        server_type = ServerType.STANDALONE
    else:
        server_type = ServerType.UNKNOWN
    tags = response.get("tags")
    return ServerDescription(
        address=address,
        server_type=server_type,
        round_trip_time_ms=round_trip_time_ms,
        set_name=response.get("setName") if isinstance(response.get("setName"), str) else None,
        tags=dict(tags) if isinstance(tags, dict) else {},
        wire_version_range=(
            int(response["minWireVersion"]),
            int(response["maxWireVersion"]),
        )
        if isinstance(response.get("minWireVersion"), int) and isinstance(response.get("maxWireVersion"), int)
        else None,
        logical_session_timeout_minutes=(
            int(response["logicalSessionTimeoutMinutes"])
            if isinstance(response.get("logicalSessionTimeoutMinutes"), int)
            else None
        ),
        hidden=bool(response.get("hidden")),
        arbiter_only=bool(response.get("arbiterOnly")),
        topology_version=dict(response["topologyVersion"])
        if isinstance(response.get("topologyVersion"), dict)
        else None,
        set_version=int(response["setVersion"]) if isinstance(response.get("setVersion"), int) else None,
        election_id=response.get("electionId"),
        last_update_time_monotonic=time.monotonic(),
    )


def _derive_topology_type(
    servers: tuple[ServerDescription, ...],
    *,
    fallback: TopologyType,
) -> TopologyType:
    families = {
        family
        for server in servers
        if (family := _server_family(server.server_type)) is not None
    }
    if not families:
        return fallback
    if len(families) > 1:
        return TopologyType.UNKNOWN
    family = next(iter(families))
    if family is TopologyType.SHARDED:
        return TopologyType.SHARDED
    if family is TopologyType.REPLICA_SET:
        return TopologyType.REPLICA_SET
    if family is TopologyType.SINGLE:
        return TopologyType.SINGLE
    return fallback


def _server_family(server_type: ServerType) -> TopologyType | None:
    if server_type is ServerType.MONGOS:
        return TopologyType.SHARDED
    if server_type in {ServerType.RS_PRIMARY, ServerType.RS_SECONDARY}:
        return TopologyType.REPLICA_SET
    if server_type is ServerType.STANDALONE:
        return TopologyType.SINGLE
    return None


def _derive_set_name(
    servers: tuple[ServerDescription, ...],
    *,
    topology_type: TopologyType,
) -> str | None:
    if topology_type is not TopologyType.REPLICA_SET:
        return None
    set_names = {server.set_name for server in servers if server.set_name}
    if len(set_names) != 1:
        return None
    return next(iter(set_names))


def _topology_is_compatible(
    servers: tuple[ServerDescription, ...],
    *,
    topology_type: TopologyType,
) -> bool:
    families = {
        family
        for server in servers
        if (family := _server_family(server.server_type)) is not None
    }
    if len(families) > 1:
        return False
    if topology_type is TopologyType.REPLICA_SET:
        set_names = {server.set_name for server in servers if server.set_name}
        if len(set_names) > 1:
            return False
    return True
