from __future__ import annotations

import time

from mongoeco.driver.requests import CommandRequest, RequestExecutionPlan
from mongoeco.driver._topology_reducer import (
    build_refreshed_topology,
    discovered_addresses as _discovered_addresses,
    derive_topology_type as _derive_topology_type,
    is_stale_topology_update as _is_stale_topology_update,
    mark_server_failure as _mark_server_failure,
    merge_successful_server_state as _merge_successful_server_state,
    server_description_from_hello as _server_description_from_hello,
    topology_is_compatible as _topology_is_compatible,
)
from mongoeco.driver.topology import (
    ServerDescription,
    TopologyDescription,
    TopologyType,
)


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
    seed_set_name = current_topology.set_name
    current_by_address = {server.address: server for server in current_topology.servers}
    pending_addresses = [server.address for server in current_topology.servers]
    ordered_addresses = list(pending_addresses)
    seen_addresses = set(pending_addresses)
    refreshed_by_address: dict[str, ServerDescription] = {}

    while pending_addresses:
        address = pending_addresses.pop(0)
        server = current_by_address.get(address) or refreshed_by_address.get(address) or ServerDescription(address=address)
        plan = build_probe_plan(server)
        execution = await prepare_execution(plan, attempt_number=1)
        started_at = time.perf_counter()
        try:
            response = await transport.send(execution)
            description = _server_description_from_hello(
                address,
                response,
                round_trip_time_ms=(time.perf_counter() - started_at) * 1000,
            )
            existing = current_by_address.get(address)
            if existing is not None and _is_stale_topology_update(existing, description):
                description = existing
            else:
                description = _merge_successful_server_state(existing, description)
            if (
                seed_set_name is not None
                and description.set_name is not None
                and description.set_name != seed_set_name
            ):
                description = _mark_server_failure(
                    address=address,
                    existing=existing,
                    error=f"replica set name mismatch: expected {seed_set_name}, got {description.set_name}",
                    recovered=description,
                    reachable=True,
                )
            refreshed_by_address[address] = description
            for discovered_address in _discovered_addresses(description):
                if discovered_address not in seen_addresses:
                    seen_addresses.add(discovered_address)
                    ordered_addresses.append(discovered_address)
                    pending_addresses.append(discovered_address)
        except Exception as exc:  # noqa: BLE001
            refreshed_by_address[address] = _mark_server_failure(
                address=address,
                existing=current_by_address.get(address),
                error=f"{type(exc).__name__}: {exc}",
                seed_set_name=seed_set_name,
            )
        finally:
            await complete_execution(execution)
    return build_refreshed_topology(
        current_topology=current_topology,
        ordered_addresses=ordered_addresses,
        refreshed_by_address=refreshed_by_address,
    )
