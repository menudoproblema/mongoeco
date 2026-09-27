from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mongoeco.errors import OperationFailure
from mongoeco.wire._session_identity import wire_session_key

if TYPE_CHECKING:
    from mongoeco.api import AsyncMongoClient
    from mongoeco.session import ClientSession
    from mongoeco.wire.capabilities import WireCommandCapability


@dataclass(slots=True)
class WireSessionContext:
    key: str
    session: ClientSession


class WireSessionStore:
    def __init__(self) -> None:
        self._sessions: dict[str, WireSessionContext] = {}

    def clear(self) -> None:
        for context in self._sessions.values():
            context.session.close()
        self._sessions.clear()

    def resolve_for_command(
        self,
        client: AsyncMongoClient,
        body: dict[str, Any],
        *,
        capability: WireCommandCapability,
    ) -> ClientSession | None:
        if not capability.binds_session:
            return None
        lsid = body.get("lsid")
        if lsid is None:
            return None
        key = wire_session_key(lsid)
        context = self._sessions.get(key)
        if context is None or not context.session.active:
            context = WireSessionContext(key=key, session=client.start_session())
            self._sessions[key] = context
        self._apply_transaction_metadata(context.session, body)
        return context.session

    def end_sessions(self, session_specs: object) -> dict[str, Any]:
        if not isinstance(session_specs, list):
            raise TypeError("endSessions must be a list")
        for session_spec in session_specs:
            key = wire_session_key(session_spec)
            context = self._sessions.get(key)
            if context is not None:
                context.session.close()
                self._sessions.pop(key, None)
        return {"ok": 1.0}

    def commit_transaction(self, body: dict[str, Any]) -> dict[str, Any]:
        session = self._require_session(body)
        if session.transaction_active:
            session.commit_transaction()
        return {"ok": 1.0}

    def abort_transaction(self, body: dict[str, Any]) -> dict[str, Any]:
        session = self._require_session(body)
        if session.transaction_active:
            session.abort_transaction()
        return {"ok": 1.0}

    def _require_session(self, body: dict[str, Any]) -> ClientSession:
        lsid = body.get("lsid")
        if lsid is None:
            raise OperationFailure("wire transaction command requires lsid")
        key = wire_session_key(lsid)
        context = self._sessions.get(key)
        if context is None or not context.session.active:
            raise OperationFailure("wire transaction command requires an active session")
        return context.session

    @staticmethod
    def _apply_transaction_metadata(session: ClientSession, body: dict[str, Any]) -> None:
        start_transaction = body.get("startTransaction")
        autocommit = body.get("autocommit")
        if start_transaction is True:
            if autocommit not in (None, False):
                raise OperationFailure("startTransaction requires autocommit=false")
            if not session.transaction_active:
                session.start_transaction()
