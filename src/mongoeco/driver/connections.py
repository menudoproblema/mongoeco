from __future__ import annotations

import asyncio
import time

from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from enum import Enum
from uuid import uuid4

from mongoeco.driver.topology import (  # noqa: TC001 - exported annotations are introspectable
    ServerDescription,
)
from mongoeco.driver.uri import MongoUri  # noqa: TC001 - exported annotations are introspectable


class ConnectionState(Enum):
    READY = "ready"
    CHECKED_OUT = "checkedOut"
    CLOSED = "closed"


@dataclass(frozen=True, slots=True)
class PoolKey:
    address: str
    tls: bool
    replica_set: str | None = None
    auth_source: str | None = None
    compressors: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ConnectionPoolOptions:
    max_pool_size: int
    min_pool_size: int
    connect_timeout_ms: int
    socket_timeout_ms: int | None = None
    wait_queue_timeout_ms: int | None = None
    max_idle_time_ms: int | None = None


@dataclass(slots=True)
class DriverConnection:
    connection_id: str
    server: ServerDescription
    pool_key: PoolKey
    created_at_monotonic: float
    last_used_at_monotonic: float
    resource: object | None = None
    state: ConnectionState = ConnectionState.READY
    checkout_count: int = 0

    def mark_checked_out(self) -> None:
        self.state = ConnectionState.CHECKED_OUT
        self.checkout_count += 1
        self.last_used_at_monotonic = time.monotonic()

    def mark_ready(self) -> None:
        self.state = ConnectionState.READY
        self.last_used_at_monotonic = time.monotonic()

    def mark_closed(self) -> None:
        self.state = ConnectionState.CLOSED

    def attach_resource(self, resource: object) -> None:
        self.resource = resource

    def detach_resource(self) -> object | None:
        resource = self.resource
        self.resource = None
        return resource


@dataclass(frozen=True, slots=True)
class ConnectionLease:
    pool_key: PoolKey
    connection_id: str
    server: ServerDescription


@dataclass(frozen=True, slots=True)
class ConnectionPoolSnapshot:
    key: PoolKey
    total_size: int
    checked_out: int


def build_connection_pool_options(uri: MongoUri) -> ConnectionPoolOptions:
    options = uri.options
    return ConnectionPoolOptions(
        max_pool_size=options.max_pool_size,
        min_pool_size=options.min_pool_size,
        connect_timeout_ms=options.connect_timeout_ms,
        socket_timeout_ms=options.socket_timeout_ms,
        wait_queue_timeout_ms=options.wait_queue_timeout_ms,
        max_idle_time_ms=options.max_idle_time_ms,
    )


class ConnectionPool:
    def __init__(self, key: PoolKey, options: ConnectionPoolOptions):
        self._key = key
        self._options = options
        self._connections: dict[str, DriverConnection] = {}
        self._wait_condition = asyncio.Condition()
        self._waiters: deque[str] = deque()
        self._closed = False
        self._wait_loop: asyncio.AbstractEventLoop | None = None

    @property
    def key(self) -> PoolKey:
        return self._key

    @property
    def options(self) -> ConnectionPoolOptions:
        return self._options

    def checkout(self, server: ServerDescription) -> DriverConnection:
        if self._closed:
            raise RuntimeError("connection pool is closed")
        self._prune_idle()
        connection = self._checkout_if_available(server)
        if connection is None:
            message = "connection pool exhausted"
            raise RuntimeError(message)
        return connection

    async def checkout_async(self, server: ServerDescription) -> DriverConnection:
        self._wait_loop = asyncio.get_running_loop()
        deadline = None
        if self._options.wait_queue_timeout_ms is not None:
            deadline = time.monotonic() + (self._options.wait_queue_timeout_ms / 1000)
        waiter_id: str | None = None
        async with self._wait_condition:
            try:
                while True:
                    if self._closed:
                        raise RuntimeError("connection pool is closed")
                    self._prune_idle()
                    should_try_checkout = waiter_id is None and not self._waiters
                    if waiter_id is not None and self._waiters and self._waiters[0] == waiter_id:
                        should_try_checkout = True
                    if should_try_checkout:
                        connection = self._checkout_if_available(server)
                        if connection is not None:
                            if waiter_id is not None and self._waiters and self._waiters[0] == waiter_id:
                                self._waiters.popleft()
                                self._wait_condition.notify_all()
                            return connection
                    if waiter_id is None:
                        waiter_id = str(uuid4())
                        self._waiters.append(waiter_id)
                    if deadline is None:
                        await self._wait_condition.wait()
                        continue
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        message = "connection pool exhausted"
                        raise RuntimeError(message)
                    try:
                        await asyncio.wait_for(self._wait_condition.wait(), timeout=remaining)
                    except TimeoutError as exc:
                        raise RuntimeError("connection pool exhausted") from exc
            finally:
                if waiter_id is not None and waiter_id in self._waiters:
                    was_front = self._waiters[0] == waiter_id
                    self._waiters.remove(waiter_id)
                    if was_front:
                        self._wait_condition.notify_all()

    def checkin(self, connection_id: str) -> None:
        connection = self._connections.get(connection_id)
        if connection is None or connection.state is ConnectionState.CLOSED:
            return
        connection.mark_ready()

    async def checkin_async(self, connection_id: str) -> None:
        async with self._wait_condition:
            self.checkin(connection_id)
            self._wait_condition.notify_all()

    def clear(self) -> None:
        self._closed = True
        for connection in self._connections.values():
            _close_resource(connection.detach_resource())
            connection.mark_closed()
        self._connections.clear()
        loop = self._wait_loop
        if loop is not None and loop.is_running():
            with suppress(RuntimeError):
                loop.call_soon_threadsafe(
                    lambda: asyncio.create_task(self._notify_waiters())
                )

    async def _notify_waiters(self) -> None:
        async with self._wait_condition:
            self._wait_condition.notify_all()

    async def clear_async(self) -> None:
        resources: list[object] = []
        async with self._wait_condition:
            self._closed = True
            for connection in self._connections.values():
                resource = connection.detach_resource()
                if resource is not None:
                    resources.append(resource)
                connection.mark_closed()
            self._connections.clear()
            self._wait_condition.notify_all()
        await asyncio.gather(
            *(_close_resource_async(resource) for resource in resources),
            return_exceptions=True,
        )

    def get_connection(self, connection_id: str) -> DriverConnection | None:
        return self._connections.get(connection_id)

    def discard(self, connection_id: str) -> None:
        connection = self._connections.pop(connection_id, None)
        if connection is None:
            return
        _close_resource(connection.detach_resource())
        connection.mark_closed()

    async def discard_async(self, connection_id: str) -> None:
        resource = None
        async with self._wait_condition:
            connection = self._connections.pop(connection_id, None)
            if connection is not None:
                resource = connection.detach_resource()
                connection.mark_closed()
            self._wait_condition.notify_all()
        await _close_resource_async(resource)

    def _checkout_if_available(self, server: ServerDescription) -> DriverConnection | None:
        for connection in self._connections.values():
            if connection.state is ConnectionState.READY:
                connection.mark_checked_out()
                return connection
        if len(self._connections) >= self._options.max_pool_size:
            return None
        now = time.monotonic()
        connection = DriverConnection(
            connection_id=str(uuid4()),
            server=server,
            pool_key=self._key,
            created_at_monotonic=now,
            last_used_at_monotonic=now,
        )
        connection.mark_checked_out()
        self._connections[connection.connection_id] = connection
        return connection

    def _prune_idle(self) -> None:
        max_idle_time_ms = self._options.max_idle_time_ms
        if max_idle_time_ms is None:
            return
        now = time.monotonic()
        to_remove = [
            connection_id
            for connection_id, connection in self._connections.items()
            if connection.state is ConnectionState.READY
            and (now - connection.last_used_at_monotonic) * 1000 > max_idle_time_ms
        ]
        for connection_id in to_remove:
            self.discard(connection_id)

    def snapshot(self) -> ConnectionPoolSnapshot:
        checked_out = sum(
            1 for connection in self._connections.values() if connection.state is ConnectionState.CHECKED_OUT
        )
        return ConnectionPoolSnapshot(
            key=self._key,
            total_size=len(self._connections),
            checked_out=checked_out,
        )


class ConnectionRegistry:
    def __init__(self, uri: MongoUri):
        self._uri = uri
        self._options = build_connection_pool_options(uri)
        self._pools: dict[PoolKey, ConnectionPool] = {}
        self._closed = False

    def pool_key_for_server(self, server: ServerDescription) -> PoolKey:
        return PoolKey(
            address=server.address,
            tls=self._uri.options.tls.enabled,
            replica_set=self._uri.options.replica_set,
            auth_source=self._uri.options.auth.source,
            compressors=self._uri.options.compressors,
        )

    def pool_for_server(self, server: ServerDescription) -> ConnectionPool:
        if self._closed:
            message = "connection registry is closed"
            raise RuntimeError(message)
        key = self.pool_key_for_server(server)
        pool = self._pools.get(key)
        if pool is None:
            pool = ConnectionPool(key, self._options)
            self._pools[key] = pool
        return pool

    def checkout(self, server: ServerDescription) -> ConnectionLease:
        pool = self.pool_for_server(server)
        connection = pool.checkout(server)
        return ConnectionLease(
            pool_key=pool.key,
            connection_id=connection.connection_id,
            server=connection.server,
        )

    async def checkout_async(self, server: ServerDescription) -> ConnectionLease:
        pool = self.pool_for_server(server)
        connection = await pool.checkout_async(server)
        return ConnectionLease(
            pool_key=pool.key,
            connection_id=connection.connection_id,
            server=connection.server,
        )

    def checkin(self, lease: ConnectionLease) -> None:
        pool = self._pools.get(lease.pool_key)
        if pool is not None:
            pool.checkin(lease.connection_id)

    async def checkin_async(self, lease: ConnectionLease) -> None:
        pool = self._pools.get(lease.pool_key)
        if pool is not None:
            await pool.checkin_async(lease.connection_id)

    def get_connection(self, lease: ConnectionLease) -> DriverConnection | None:
        pool = self._pools.get(lease.pool_key)
        if pool is None:
            return None
        return pool.get_connection(lease.connection_id)

    def discard(self, lease: ConnectionLease) -> None:
        pool = self._pools.get(lease.pool_key)
        if pool is not None:
            pool.discard(lease.connection_id)

    async def discard_async(self, lease: ConnectionLease) -> None:
        pool = self._pools.get(lease.pool_key)
        if pool is not None:
            await pool.discard_async(lease.connection_id)

    def clear(self) -> None:
        self._closed = True
        for pool in self._pools.values():
            pool.clear()
        self._pools.clear()

    async def clear_async(self) -> None:
        self._closed = True
        pools = tuple(self._pools.values())
        self._pools.clear()
        await asyncio.gather(
            *(pool.clear_async() for pool in pools),
            return_exceptions=True,
        )

    def snapshots(self) -> tuple[ConnectionPoolSnapshot, ...]:
        return tuple(pool.snapshot() for pool in self._pools.values())


def _close_resource(resource: object | None) -> None:
    if resource is None:
        return
    writer = getattr(resource, "writer", None)
    close = getattr(writer, "close", None)
    if callable(close):
        close()


async def _close_resource_async(resource: object | None) -> None:
    if resource is None:
        return
    try:
        writer = getattr(resource, "writer", None)
        close = getattr(writer, "close", None)
        if callable(close):
            close()
        wait_closed = getattr(writer, "wait_closed", None)
        if callable(wait_closed):
            await asyncio.wait_for(wait_closed(), timeout=0.25)
    except (Exception, asyncio.CancelledError):
        # Cleanup is bounded and best-effort; it must not replace the command
        # failure or cancellation that caused the connection to be discarded.
        return
