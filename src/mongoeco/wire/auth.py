from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.wire.connections import WireConnectionContext


@dataclass(frozen=True, slots=True)
class WireAuthUser:
    username: str
    password: str | None = None
    source: str = "admin"
    mechanisms: tuple[str, ...] = ("SCRAM-SHA-256", "SCRAM-SHA-1", "PLAIN")
    roles: tuple[dict[str, object], ...] = field(default_factory=tuple)


class WireAuthenticationService:
    def __init__(self, users: tuple[WireAuthUser, ...] = ()) -> None:
        self._users = users

    @property
    def enabled(self) -> bool:
        return bool(self._users)

    def authenticate(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
    ) -> dict[str, Any]:
        mechanism = body.get("mechanism")
        username = body.get("user")
        password = body.get("pwd")
        if "db" in body:
            database = body.get("db")
        elif "$db" in body:
            database = body.get("$db")
        else:
            database = "admin"
        if not isinstance(mechanism, str) or not mechanism:
            raise OperationFailure("authenticate requires mechanism")
        if not isinstance(username, str) or not username:
            raise OperationFailure("authenticate requires user")
        if not isinstance(database, str) or not database:
            raise OperationFailure("authenticate requires db")
        for user in self._users:
            if user.username != username or user.source != database:
                continue
            if mechanism not in user.mechanisms:
                continue
            if mechanism != "MONGODB-X509" and user.password != password:
                continue
            connection.authenticate(
                username=username,
                db=database,
                mechanism=mechanism,
                roles=user.roles,
            )
            return {"ok": 1.0}
        raise OperationFailure("Authentication failed")

    def logout(self, connection: WireConnectionContext) -> dict[str, Any]:
        connection.logout()
        return {"ok": 1.0}

    def require_authenticated(self, connection: WireConnectionContext, command_name: str) -> None:
        if not self.enabled:
            return
        if connection.authenticated_users:
            return
        raise OperationFailure(f"Authentication required for {command_name}")
