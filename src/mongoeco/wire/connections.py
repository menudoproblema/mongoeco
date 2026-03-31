from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class WireConnectionContext:
    connection_id: int
    peer_host: str = "127.0.0.1"
    peer_port: int = 0
    client_metadata: dict[str, Any] | None = None
    compression: tuple[str, ...] = ()
    hello_count: int = 0
    last_hello_command: str | None = None
    authenticated_users: list[dict[str, object]] = field(default_factory=list)
    authenticated_roles: list[dict[str, object]] = field(default_factory=list)
    auth_conversations: dict[int, object] = field(default_factory=dict)

    @property
    def peer_address(self) -> str:
        return f"{self.peer_host}:{self.peer_port}"

    def register_hello(self, command_name: str, body: dict[str, Any]) -> None:
        self.hello_count += 1
        self.last_hello_command = command_name
        client_metadata = body.get("client")
        if isinstance(client_metadata, dict):
            self.client_metadata = dict(client_metadata)
        compression = body.get("compression")
        if isinstance(compression, list) and all(isinstance(item, str) for item in compression):
            self.compression = tuple(compression)

    def authenticate(
        self,
        *,
        username: str,
        db: str,
        mechanism: str,
        roles: tuple[dict[str, object], ...] = (),
    ) -> None:
        self.authenticated_users = [
            {"user": username, "db": db, "mechanism": mechanism},
        ]
        self.authenticated_roles = list(roles)

    def logout(self) -> None:
        self.authenticated_users = []
        self.authenticated_roles = []
        self.auth_conversations.clear()


class WireConnectionRegistry:
    def __init__(self) -> None:
        self._next_connection_id = 1
        self._connections: dict[int, WireConnectionContext] = {}

    def create(self, peer_name: object) -> WireConnectionContext:
        host = "127.0.0.1"
        port = 0
        if isinstance(peer_name, tuple) and len(peer_name) >= 2:
            if isinstance(peer_name[0], str):
                host = peer_name[0]
            if isinstance(peer_name[1], int):
                port = peer_name[1]
        context = WireConnectionContext(
            connection_id=self._next_connection_id,
            peer_host=host,
            peer_port=port,
        )
        self._next_connection_id += 1
        self._connections[context.connection_id] = context
        return context

    def close(self, context: WireConnectionContext) -> None:
        self._connections.pop(context.connection_id, None)

    def register_hello(
        self,
        context: WireConnectionContext,
        command_name: str,
        body: dict[str, Any],
    ) -> None:
        context.register_hello(command_name, body)
