from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable


@dataclass(frozen=True, slots=True)
class ServerSelectedEvent:
    database: str
    command_name: str
    server_address: str
    attempt_number: int
    read_only: bool
    session_id: str | None = None
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class ServerSelectionFailedEvent:
    database: str
    command_name: str
    reason: str
    topology_type: str
    known_server_count: int
    timeout_ms: int
    read_only: bool
    session_id: str | None = None
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectionCheckedOutEvent:
    database: str
    command_name: str
    server_address: str
    connection_id: str
    attempt_number: int
    session_id: str | None = None
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class ConnectionCheckedInEvent:
    database: str
    command_name: str
    server_address: str
    connection_id: str
    attempt_number: int
    session_id: str | None = None
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class CommandStartedEvent:
    database: str
    command_name: str
    command: dict[str, Any]
    server_address: str
    connection_id: str
    attempt_number: int
    read_only: bool
    session_id: str | None = None
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class CommandSucceededEvent:
    database: str
    command_name: str
    reply: dict[str, Any]
    server_address: str
    connection_id: str
    attempt_number: int
    duration_ms: float
    session_id: str | None = None
    request_id: str | None = None


@dataclass(frozen=True, slots=True)
class CommandFailedEvent:
    database: str
    command_name: str
    failure: str
    server_address: str
    connection_id: str
    attempt_number: int
    duration_ms: float
    retryable: bool
    session_id: str | None = None
    request_id: str | None = None


DriverEvent = (
    ServerSelectedEvent
    | ServerSelectionFailedEvent
    | ConnectionCheckedOutEvent
    | ConnectionCheckedInEvent
    | CommandStartedEvent
    | CommandSucceededEvent
    | CommandFailedEvent
)

DriverEventListener = Callable[[DriverEvent], None]


@dataclass(slots=True)
class DriverMonitor:
    _listeners: list[DriverEventListener] = field(default_factory=list)
    _history: list[DriverEvent] = field(default_factory=list)

    def add_listener(self, listener: DriverEventListener) -> None:
        self._listeners.append(listener)

    def remove_listener(self, listener: DriverEventListener) -> None:
        try:
            self._listeners.remove(listener)
        except ValueError:
            return

    def clear_listeners(self) -> None:
        self._listeners.clear()

    def emit(self, event: DriverEvent) -> None:
        self._history.append(event)
        for listener in tuple(self._listeners):
            listener(event)

    def clear_history(self) -> None:
        self._history.clear()

    @property
    def history(self) -> tuple[DriverEvent, ...]:
        return tuple(self._history)
