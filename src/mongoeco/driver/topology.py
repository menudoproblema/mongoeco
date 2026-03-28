from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum

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
    set_name: str | None = None
    tags: dict[str, str] = field(default_factory=dict)
    wire_version_range: tuple[int, int] | None = None
    logical_session_timeout_minutes: int | None = None
    error: str | None = None

    @property
    def is_readable(self) -> bool:
        return self.server_type in {
            ServerType.STANDALONE,
            ServerType.MONGOS,
            ServerType.RS_PRIMARY,
            ServerType.RS_SECONDARY,
        }

    @property
    def is_writable(self) -> bool:
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
    topology_type = (
        TopologyType.REPLICA_SET
        if uri.options.replica_set
        else TopologyType.SINGLE if len(uri.seeds) == 1 else TopologyType.UNKNOWN
    )
    server_type = (
        ServerType.RS_PRIMARY
        if topology_type is TopologyType.REPLICA_SET
        else ServerType.STANDALONE if topology_type is TopologyType.SINGLE else ServerType.UNKNOWN
    )
    servers = tuple(
        ServerDescription(
            address=seed.address,
            server_type=server_type,
            set_name=uri.options.replica_set,
            logical_session_timeout_minutes=30,
            wire_version_range=(0, 20),
        )
        for seed in uri.seeds
    )
    return TopologyDescription(
        topology_type=topology_type,
        servers=servers,
        set_name=uri.options.replica_set,
        logical_session_timeout_minutes=30,
    )
