from __future__ import annotations

from dataclasses import replace
import time

from mongoeco.driver.topology import (
    ServerDescription,
    ServerState,
    ServerType,
    TopologyDescription,
    TopologyType,
)


def server_description_from_hello(
    address: str,
    response: dict[str, object],
    *,
    round_trip_time_ms: float,
) -> ServerDescription:
    observed_at = time.monotonic()
    if response.get("msg") == "isdbgrid":
        server_type = ServerType.MONGOS
    elif isinstance(response.get("setName"), str):
        if bool(response.get("arbiterOnly")):
            server_type = ServerType.RS_ARBITER
        elif bool(response.get("secondary")):
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
        state=ServerState.HEALTHY,
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
        last_update_time_monotonic=observed_at,
        last_success_time_monotonic=observed_at,
        hosts=string_tuple(response.get("hosts")),
        passives=string_tuple(response.get("passives")),
        arbiters=string_tuple(response.get("arbiters")),
        primary=response.get("primary") if isinstance(response.get("primary"), str) else None,
        me=response.get("me") if isinstance(response.get("me"), str) else None,
    )


def derive_topology_type(
    servers: tuple[ServerDescription, ...],
    *,
    fallback: TopologyType,
) -> TopologyType:
    families = {
        family
        for server in servers
        if (family := server_family(server.server_type)) is not None
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
    return TopologyType.SINGLE


def server_family(server_type: ServerType) -> TopologyType | None:
    if server_type is ServerType.MONGOS:
        return TopologyType.SHARDED
    if server_type in {ServerType.RS_PRIMARY, ServerType.RS_SECONDARY, ServerType.RS_ARBITER}:
        return TopologyType.REPLICA_SET
    if server_type is ServerType.STANDALONE:
        return TopologyType.SINGLE
    return None


def derive_set_name(
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


def topology_is_compatible(
    servers: tuple[ServerDescription, ...],
    *,
    topology_type: TopologyType,
) -> bool:
    families = {
        family
        for server in servers
        if (family := server_family(server.server_type)) is not None
    }
    if len(families) > 1:
        return False
    if topology_type is TopologyType.REPLICA_SET:
        set_names = {server.set_name for server in servers if server.set_name}
        if len(set_names) > 1:
            return False
    return True


def string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, str))


def discovered_addresses(server: ServerDescription) -> tuple[str, ...]:
    addresses: list[str] = []
    for candidate in (
        *((server.primary,) if server.primary is not None else ()),
        *server.hosts,
        *server.passives,
        *server.arbiters,
        *((server.me,) if server.me is not None else ()),
    ):
        if candidate not in addresses:
            addresses.append(candidate)
    return tuple(addresses)


def merge_successful_server_state(
    existing: ServerDescription | None,
    candidate: ServerDescription,
) -> ServerDescription:
    state = ServerState.HEALTHY
    if existing is not None and existing.consecutive_failures > 0:
        state = ServerState.RECOVERING
    return replace(
        candidate,
        state=state,
        consecutive_failures=0,
        last_success_time_monotonic=candidate.last_update_time_monotonic,
        last_error_time_monotonic=None,
        error=None,
    )


def mark_server_failure(
    *,
    address: str,
    existing: ServerDescription | None,
    error: str,
    reachable: bool = False,
    recovered: ServerDescription | None = None,
    seed_set_name: str | None = None,
) -> ServerDescription:
    now = time.monotonic()
    failures = 1 if existing is None else existing.consecutive_failures + 1
    if reachable:
        state = ServerState.DEGRADED
        server_type = recovered.server_type if recovered is not None else ServerType.UNKNOWN
        set_name = recovered.set_name if recovered is not None else seed_set_name
        round_trip_time_ms = recovered.round_trip_time_ms if recovered is not None else None
        topology_version = recovered.topology_version if recovered is not None else None
        set_version = recovered.set_version if recovered is not None else None
        election_id = recovered.election_id if recovered is not None else None
        hosts = recovered.hosts if recovered is not None else ()
        passives = recovered.passives if recovered is not None else ()
        arbiters = recovered.arbiters if recovered is not None else ()
        primary = recovered.primary if recovered is not None else None
        me = recovered.me if recovered is not None else None
        tags = recovered.tags if recovered is not None else {}
        wire_version_range = recovered.wire_version_range if recovered is not None else None
        logical_session_timeout_minutes = (
            recovered.logical_session_timeout_minutes if recovered is not None else None
        )
        hidden = recovered.hidden if recovered is not None else False
        arbiter_only = recovered.arbiter_only if recovered is not None else False
        staleness_seconds = recovered.staleness_seconds if recovered is not None else None
    else:
        state = (
            ServerState.DEGRADED
            if existing is not None and existing.last_success_time_monotonic is not None
            else ServerState.UNREACHABLE
        )
        server_type = ServerType.UNKNOWN
        set_name = existing.set_name if existing is not None else seed_set_name
        round_trip_time_ms = existing.round_trip_time_ms if existing is not None else None
        topology_version = existing.topology_version if existing is not None else None
        set_version = existing.set_version if existing is not None else None
        election_id = existing.election_id if existing is not None else None
        hosts = existing.hosts if existing is not None else ()
        passives = existing.passives if existing is not None else ()
        arbiters = existing.arbiters if existing is not None else ()
        primary = existing.primary if existing is not None else None
        me = existing.me if existing is not None else None
        tags = existing.tags if existing is not None else {}
        wire_version_range = existing.wire_version_range if existing is not None else None
        logical_session_timeout_minutes = (
            existing.logical_session_timeout_minutes if existing is not None else None
        )
        hidden = existing.hidden if existing is not None else False
        arbiter_only = existing.arbiter_only if existing is not None else False
        staleness_seconds = existing.staleness_seconds if existing is not None else None
    return ServerDescription(
        address=address,
        server_type=server_type,
        state=state,
        round_trip_time_ms=round_trip_time_ms,
        staleness_seconds=staleness_seconds,
        set_name=set_name,
        tags=tags,
        wire_version_range=wire_version_range,
        logical_session_timeout_minutes=logical_session_timeout_minutes,
        hidden=hidden,
        arbiter_only=arbiter_only,
        topology_version=topology_version,
        set_version=set_version,
        election_id=election_id,
        last_update_time_monotonic=now,
        last_success_time_monotonic=(
            recovered.last_success_time_monotonic
            if reachable and recovered is not None
            else (existing.last_success_time_monotonic if existing is not None else None)
        ),
        last_error_time_monotonic=now,
        consecutive_failures=failures,
        error=error,
        hosts=hosts,
        passives=passives,
        arbiters=arbiters,
        primary=primary,
        me=me,
    )


def is_stale_topology_update(
    existing: ServerDescription,
    candidate: ServerDescription,
) -> bool:
    existing_version = topology_version_signature(existing.topology_version)
    candidate_version = topology_version_signature(candidate.topology_version)
    if existing_version is not None and candidate_version is not None:
        existing_process_id, existing_counter = existing_version
        candidate_process_id, candidate_counter = candidate_version
        if existing_process_id == candidate_process_id and candidate_counter < existing_counter:
            return True
    if (
        existing.server_type is ServerType.RS_PRIMARY
        and candidate.server_type is ServerType.RS_PRIMARY
        and existing.set_version is not None
        and candidate.set_version is not None
        and candidate.set_version < existing.set_version
    ):
        return True
    return False


def topology_version_signature(value: dict[str, object] | None) -> tuple[str, int] | None:
    if not isinstance(value, dict):
        return None
    process_id = value.get("processId")
    counter = value.get("counter")
    if not isinstance(process_id, str):
        return None
    if not isinstance(counter, int) or isinstance(counter, bool):
        return None
    return process_id, counter


def build_refreshed_topology(
    *,
    current_topology: TopologyDescription,
    ordered_addresses: list[str],
    refreshed_by_address: dict[str, ServerDescription],
) -> TopologyDescription:
    refreshed_servers = tuple(refreshed_by_address[address] for address in ordered_addresses)
    topology_type = derive_topology_type(tuple(refreshed_servers), fallback=current_topology.topology_type)
    set_name = derive_set_name(tuple(refreshed_servers), topology_type=topology_type)
    logical_session_timeout = min(
        (server.logical_session_timeout_minutes for server in refreshed_servers if server.logical_session_timeout_minutes),
        default=None,
    )
    return TopologyDescription(
        topology_type=topology_type,
        servers=tuple(refreshed_servers),
        set_name=set_name,
        compatible=all(server.error is None for server in refreshed_servers)
        and topology_is_compatible(tuple(refreshed_servers), topology_type=topology_type),
        logical_session_timeout_minutes=logical_session_timeout,
    )
