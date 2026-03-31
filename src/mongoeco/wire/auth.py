from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

try:  # pragma: no cover - optional dependency
    from bson.binary import Binary as BsonBinary
except Exception:  # pragma: no cover - bson is optional
    BsonBinary = type("_MissingBsonBinary", (bytes,), {})

from mongoeco.errors import OperationFailure
from mongoeco.types import Binary
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.scram import (
    build_scram_server_first,
    parse_scram_client_start,
    verify_scram_client_final,
)


@dataclass(frozen=True, slots=True)
class WireAuthUser:
    username: str
    password: str | None = None
    source: str = "admin"
    mechanisms: tuple[str, ...] = ("SCRAM-SHA-256", "SCRAM-SHA-1", "PLAIN")
    roles: tuple[dict[str, object], ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class _ScramConversation:
    conversation_id: int
    username: str
    database: str
    mechanism: str
    client_first_bare: str
    server_first_message: str
    roles: tuple[dict[str, object], ...]


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

    def sasl_start(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
    ) -> dict[str, Any]:
        _require_bson()
        mechanism = body.get("mechanism")
        if not isinstance(mechanism, str) or not mechanism:
            raise OperationFailure("saslStart requires mechanism")
        payload = self._payload_bytes(body.get("payload"), "saslStart")
        database = self._resolve_database(body)
        username, client_nonce, client_first_bare = parse_scram_client_start(payload, mechanism=mechanism)
        user = self._find_user(username=username, database=database, mechanism=mechanism)
        server_first = build_scram_server_first(
            client_nonce=client_nonce,
            mechanism=mechanism,
            salt=f"{database}:{username}".encode("utf-8"),
            iterations=15000,
        )
        conversation_id = max(connection.auth_conversations, default=0) + 1
        connection.auth_conversations[conversation_id] = _ScramConversation(
            conversation_id=conversation_id,
            username=user.username,
            database=database,
            mechanism=mechanism,
            client_first_bare=client_first_bare,
            server_first_message=server_first.message,
            roles=user.roles,
        )
        return {
            "conversationId": conversation_id,
            "done": False,
            "payload": BsonBinary(server_first.message.encode("utf-8")),
            "ok": 1.0,
        }

    def sasl_continue(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
    ) -> dict[str, Any]:
        _require_bson()
        conversation_id = body.get("conversationId")
        if not isinstance(conversation_id, int):
            raise OperationFailure("saslContinue requires conversationId")
        payload = self._payload_bytes(body.get("payload"), "saslContinue")
        conversation = connection.auth_conversations.get(conversation_id)
        if not isinstance(conversation, _ScramConversation):
            raise OperationFailure("Unknown SASL conversation")
        user = self._find_user(
            username=conversation.username,
            database=conversation.database,
            mechanism=conversation.mechanism,
        )
        if conversation.mechanism == "MONGODB-X509":
            raise OperationFailure("saslContinue does not support MONGODB-X509")
        if user.password is None:
            raise OperationFailure("Authentication failed")
        server_signature = verify_scram_client_final(
            payload,
            password=user.password,
            mechanism=conversation.mechanism,
            client_first_bare=conversation.client_first_bare,
            server_first_message=conversation.server_first_message,
        )
        connection.authenticate(
            username=user.username,
            db=conversation.database,
            mechanism=conversation.mechanism,
            roles=conversation.roles,
        )
        connection.auth_conversations.pop(conversation_id, None)
        return {
            "conversationId": conversation_id,
            "done": True,
            "payload": BsonBinary(f"v={server_signature}".encode("utf-8")),
            "ok": 1.0,
        }

    def require_authenticated(self, connection: WireConnectionContext, command_name: str) -> None:
        if not self.enabled:
            return
        if connection.authenticated_users:
            return
        raise OperationFailure(f"Authentication required for {command_name}")

    def _resolve_database(self, body: dict[str, Any]) -> str:
        if "db" in body:
            database = body.get("db")
        elif "$db" in body:
            database = body.get("$db")
        else:
            database = "admin"
        if not isinstance(database, str) or not database:
            raise OperationFailure("authenticate requires db")
        return database

    def _find_user(self, *, username: str, database: str, mechanism: str) -> WireAuthUser:
        for user in self._users:
            if user.username != username or user.source != database:
                continue
            if mechanism not in user.mechanisms:
                continue
            return user
        raise OperationFailure("Authentication failed")

    @staticmethod
    def _payload_bytes(payload: Any, command_name: str) -> bytes:
        if isinstance(payload, Binary):
            return bytes(payload)
        if isinstance(payload, BsonBinary):
            return bytes(payload)
        if isinstance(payload, bytes):
            return payload
        raise OperationFailure(f"{command_name} requires binary payload")


def _require_bson() -> None:
    if BsonBinary.__name__ == "_MissingBsonBinary":  # pragma: no cover - guarded by callers
        raise OperationFailure("wire authentication requires the optional 'pymongo'/'bson' dependency")
