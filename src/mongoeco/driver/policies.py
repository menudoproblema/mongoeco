from __future__ import annotations

from dataclasses import dataclass

from mongoeco.driver.topology import ServerDescription, ServerState, TopologyDescription, TopologyType
from mongoeco.driver.uri import MongoClientOptions, MongoUri
from mongoeco.types import ReadConcern, ReadPreference, ReadPreferenceMode, WriteConcern


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    server_selection_timeout_ms: int
    connect_timeout_ms: int
    socket_timeout_ms: int | None
    wait_queue_timeout_ms: int | None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    retry_reads: bool
    retry_writes: bool


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    mode: ReadPreferenceMode
    direct_connection: bool | None = None
    replica_set: str | None = None
    tag_sets: tuple[dict[str, str], ...] | None = None
    max_staleness_seconds: int | None = None

    def select_servers(
        self,
        topology: TopologyDescription,
        *,
        for_writes: bool = False,
    ) -> tuple[ServerDescription, ...]:
        if self.direct_connection:
            return topology.servers[:1]
        if topology.topology_type is TopologyType.UNKNOWN:
            provisional = self._order_nearest(self._provisional_replica_set_servers(topology))
            if for_writes:
                return provisional
            if self.mode is ReadPreferenceMode.NEAREST:
                return provisional
            return provisional
        if for_writes:
            writable = topology.writable_servers
            if writable or topology.topology_type is not TopologyType.REPLICA_SET:
                return writable
            return self._order_nearest(self._provisional_replica_set_servers(topology))
        if topology.topology_type is not TopologyType.REPLICA_SET:
            readable = topology.readable_servers
            return self._order_nearest(readable) if self.mode is ReadPreferenceMode.NEAREST else readable
        readable = topology.readable_servers
        secondaries = tuple(
            server for server in readable if server.server_type.name == "RS_SECONDARY"
        )
        primaries = tuple(
            server for server in readable if server.server_type.name == "RS_PRIMARY"
        )
        if not primaries and not secondaries:
            return self._order_nearest(self._provisional_replica_set_servers(topology))
        eligible_secondaries = self._apply_secondary_filters(secondaries)
        if self.mode is ReadPreferenceMode.PRIMARY:
            return primaries[:1]
        if self.mode is ReadPreferenceMode.PRIMARY_PREFERRED:
            return primaries[:1] or eligible_secondaries
        if self.mode is ReadPreferenceMode.SECONDARY:
            return eligible_secondaries
        if self.mode is ReadPreferenceMode.SECONDARY_PREFERRED:
            return eligible_secondaries or primaries[:1]
        nearest_candidates = self._apply_tag_sets(readable)
        return self._order_nearest(nearest_candidates)

    def _apply_secondary_filters(
        self,
        secondaries: tuple[ServerDescription, ...],
    ) -> tuple[ServerDescription, ...]:
        return self._apply_staleness(self._apply_tag_sets(secondaries))

    def _apply_tag_sets(
        self,
        servers: tuple[ServerDescription, ...],
    ) -> tuple[ServerDescription, ...]:
        if not self.tag_sets:
            return servers
        for tag_set in self.tag_sets:
            matches = tuple(server for server in servers if _server_matches_tag_set(server, tag_set))
            if matches:
                return matches
        return ()

    def _apply_staleness(
        self,
        servers: tuple[ServerDescription, ...],
    ) -> tuple[ServerDescription, ...]:
        if self.max_staleness_seconds is None:
            return servers
        return tuple(
            server
            for server in servers
            if server.staleness_seconds is None or server.staleness_seconds <= self.max_staleness_seconds
        )

    def _order_nearest(
        self,
        servers: tuple[ServerDescription, ...],
    ) -> tuple[ServerDescription, ...]:
        return tuple(
            sorted(
                servers,
                key=lambda server: (
                    _server_state_sort_weight(server.state),
                    float("inf") if server.round_trip_time_ms is None else server.round_trip_time_ms,
                    server.address,
                ),
            )
        )

    @staticmethod
    def _provisional_replica_set_servers(topology: TopologyDescription) -> tuple[ServerDescription, ...]:
        return tuple(
            server
            for server in topology.servers
            if server.error is None and not server.hidden and not server.arbiter_only
        )


def _server_matches_tag_set(server: ServerDescription, tag_set: dict[str, str]) -> bool:
    if not tag_set:
        return True
    return all(server.tags.get(key) == value for key, value in tag_set.items())


def _server_state_sort_weight(state: ServerState) -> int:
    if state is ServerState.HEALTHY:
        return 0
    if state is ServerState.RECOVERING:
        return 1
    if state is ServerState.DEGRADED:
        return 2
    if state is ServerState.UNKNOWN:
        return 3
    return 4


@dataclass(frozen=True, slots=True)
class ConcernPolicy:
    write_concern: WriteConcern
    read_concern: ReadConcern
    read_preference: ReadPreference


def build_timeout_policy(uri: MongoUri) -> TimeoutPolicy:
    options = uri.options
    return TimeoutPolicy(
        server_selection_timeout_ms=options.server_selection_timeout_ms,
        connect_timeout_ms=options.connect_timeout_ms,
        socket_timeout_ms=options.socket_timeout_ms,
        wait_queue_timeout_ms=options.wait_queue_timeout_ms,
    )


def build_retry_policy(uri: MongoUri) -> RetryPolicy:
    options = uri.options
    return RetryPolicy(
        retry_reads=options.retry_reads,
        retry_writes=options.retry_writes,
    )


def build_selection_policy(uri: MongoUri, *, read_preference: ReadPreference) -> SelectionPolicy:
    options: MongoClientOptions = uri.options
    return SelectionPolicy(
        mode=read_preference.mode,
        direct_connection=options.direct_connection,
        replica_set=options.replica_set,
        tag_sets=read_preference.tag_sets,
        max_staleness_seconds=read_preference.max_staleness_seconds,
    )


def build_concern_policy(
    *,
    write_concern: WriteConcern,
    read_concern: ReadConcern,
    read_preference: ReadPreference,
) -> ConcernPolicy:
    return ConcernPolicy(
        write_concern=write_concern,
        read_concern=read_concern,
        read_preference=read_preference,
    )
