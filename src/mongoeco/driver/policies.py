from __future__ import annotations

from dataclasses import dataclass

from mongoeco.driver.topology import ServerDescription, TopologyDescription
from mongoeco.driver.uri import MongoClientOptions, MongoUri
from mongoeco.types import ReadConcern, ReadPreference, WriteConcern


@dataclass(frozen=True, slots=True)
class TimeoutPolicy:
    server_selection_timeout_ms: int
    connect_timeout_ms: int
    socket_timeout_ms: int | None


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    retry_reads: bool
    retry_writes: bool


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    mode: str
    direct_connection: bool | None = None
    replica_set: str | None = None

    def select_servers(
        self,
        topology: TopologyDescription,
        *,
        for_writes: bool = False,
    ) -> tuple[ServerDescription, ...]:
        if self.direct_connection:
            return topology.servers[:1]
        if for_writes:
            return topology.writable_servers
        return topology.readable_servers


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
        mode=options.read_preference or read_preference.name,
        direct_connection=options.direct_connection,
        replica_set=options.replica_set,
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
