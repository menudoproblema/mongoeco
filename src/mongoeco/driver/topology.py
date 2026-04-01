from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
import time

from mongoeco.driver.uri import MongoUri


class ServerType(Enum):
    STANDALONE = "standalone"
    MONGOS = "mongos"
    RS_PRIMARY = "rsPrimary"
    RS_SECONDARY = "rsSecondary"
    UNKNOWN = "unknown"


class TopologyType(Enum):
    SINGLE = "single"
    SHARDED = "sharded"
    REPLICA_SET = "replicaSet"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ServerDescription:
    address: str
    server_type: ServerType = ServerType.UNKNOWN
    round_trip_time_ms: float | None = None
    staleness_seconds: int | None = None
    set_name: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    wire_version_range: tuple[int, int] | None = None
    logical_session_timeout_minutes: int | None = None
    hidden: bool = False
    arbiter_only: bool = False
    topology_version: dict[str, object] | None = None
    set_version: int | None = None
    election_id: object | None = None
    last_update_time_monotonic: float | None = None
    error: str | None = None
    hosts: tuple[str, ...] = ()
    passives: tuple[str, ...] = ()
    arbiters: tuple[str, ...] = ()
    primary: str | None = None
    me: str | None = None

    @property
    def is_readable(self) -> bool:
        if self.error is not None or self.hidden or self.arbiter_only:
            return False
        return self.server_type in {
            ServerType.STANDALONE,
            ServerType.MONGOS,
            ServerType.RS_PRIMARY,
            ServerType.RS_SECONDARY,
        }

    @property
    def is_writable(self) -> bool:
        if self.error is not None or self.hidden or self.arbiter_only:
            return False
        return self.server_type in {
            ServerType.STANDALONE,
            ServerType.MONGOS,
            ServerType.RS_PRIMARY,
        }


@dataclass(frozen=True, slots=True)
class TopologyDescription:
    topology_type: TopologyType
    servers: tuple[ServerDescription, ...]
    set_name: str | None = None
    compatible: bool = True
    logical_session_timeout_minutes: int | None = None

    def get_server(self, address: str) -> ServerDescription | None:
        for server in self.servers:
            if server.address == address:
                return server
        return None

    @property
    def readable_servers(self) -> tuple[ServerDescription, ...]:
        return tuple(server for server in self.servers if server.is_readable)

    @property
    def writable_servers(self) -> tuple[ServerDescription, ...]:
        return tuple(server for server in self.servers if server.is_writable)


def build_local_topology_description(uri: MongoUri) -> TopologyDescription:
    if uri.options.load_balanced:
        topology_type = TopologyType.SHARDED
    elif uri.options.direct_connection:
        topology_type = TopologyType.SINGLE
    elif uri.options.replica_set:
        topology_type = TopologyType.REPLICA_SET
    else:
        topology_type = TopologyType.UNKNOWN

    servers: list[ServerDescription] = []
    for index, seed in enumerate(uri.seeds):
        if topology_type is TopologyType.SHARDED:
            server_type = ServerType.MONGOS
        elif topology_type is TopologyType.REPLICA_SET:
            server_type = ServerType.UNKNOWN
        elif topology_type is TopologyType.SINGLE:
            server_type = ServerType.STANDALONE
        else:
            server_type = ServerType.UNKNOWN
        servers.append(
            ServerDescription(
                address=seed.address,
                server_type=server_type,
                round_trip_time_ms=1.0 + index,
                staleness_seconds=None if server_type is ServerType.UNKNOWN else 0,
                set_name=uri.options.replica_set,
                logical_session_timeout_minutes=30,
                wire_version_range=(0, 20),
                last_update_time_monotonic=time.monotonic(),
            )
        )
    return TopologyDescription(
        topology_type=topology_type,
        servers=tuple(servers),
        set_name=uri.options.replica_set,
        logical_session_timeout_minutes=30,
    )
