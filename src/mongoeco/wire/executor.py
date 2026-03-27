from __future__ import annotations

from typing import Any

from mongoeco.api import AsyncMongoClient
from mongoeco.errors import MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.cursors import WireCursorStore
from mongoeco.wire.handshake import WireHandshakeService
from mongoeco.wire.requests import WireRequestContext
from mongoeco.wire.surface import WireSurface
from mongoeco.wire.sessions import WireSessionStore


_WIRE_INTERNAL_KEYS = frozenset(
    {
        "$db",
        "$readPreference",
        "$clusterTime",
        "lsid",
        "txnNumber",
        "autocommit",
        "startTransaction",
        "$audit",
    }
)


class WireCommandExecutor:
    def __init__(
        self,
        client: AsyncMongoClient,
        cursor_store: WireCursorStore,
        session_store: WireSessionStore,
        surface: WireSurface | None = None,
    ) -> None:
        self._client = client
        self._cursor_store = cursor_store
        self._session_store = session_store
        self._surface = surface or WireSurface()
        self._handshake = WireHandshakeService(client.mongodb_dialect, surface=self._surface)
        self._special_handlers = {
            "handshake": self._handle_handshake,
            "end_sessions": self._handle_end_sessions,
            "get_more": self._handle_get_more,
            "kill_cursors": self._handle_kill_cursors,
            "commit_transaction": self._handle_commit_transaction,
            "abort_transaction": self._handle_abort_transaction,
            "passthrough": self._handle_passthrough,
        }

    async def execute_command(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
        db_name: str | None = None,
    ) -> dict[str, Any]:
        context = self._build_request_context(body, connection=connection, db_name=db_name)
        handler = self._special_handlers[context.capability.kind]
        return await handler(context)

    async def execute_legacy_query(
        self,
        full_collection_name: str,
        query: dict[str, Any],
        *,
        connection: WireConnectionContext,
    ) -> dict[str, Any]:
        if not full_collection_name.endswith(".$cmd"):
            raise OperationFailure("legacy OP_QUERY only supports command namespaces")
        db_name = full_collection_name[:-5]
        return await self.execute_command(query, connection=connection, db_name=db_name)

    def _build_request_context(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
        db_name: str | None = None,
    ) -> WireRequestContext:
        if db_name is None:
            db_name = body.get("$db")
        if not isinstance(db_name, str) or not db_name:
            raise OperationFailure("$db must be a non-empty string")
        command_document = {
            key: value
            for key, value in body.items()
            if key not in _WIRE_INTERNAL_KEYS
        }
        if not command_document:
            raise OperationFailure("wire command document must contain an executable command")
        command_name = next(iter(command_document))
        if not self._surface.supports_command(command_name):
            raise OperationFailure(f"unsupported wire command: {command_name}")
        capability = resolve_wire_command_capability(command_name)
        session = self._session_store.resolve_for_command(
            self._client,
            body,
            capability=capability,
        )
        return WireRequestContext(
            db_name=db_name,
            command_name=command_name,
            command_document=command_document,
            raw_body=body,
            capability=capability,
            connection=connection,
            session=session,
        )

    async def _handle_handshake(self, context: WireRequestContext) -> dict[str, Any]:
        context.connection.register_hello(context.command_name, context.raw_body)
        return self._handshake.build_hello_response(
            command_name=context.command_name,
            body=context.raw_body,
            connection=context.connection,
        )

    async def _handle_end_sessions(self, context: WireRequestContext) -> dict[str, Any]:
        return self._session_store.end_sessions(context.command_document.get("endSessions"))

    async def _handle_get_more(self, context: WireRequestContext) -> dict[str, Any]:
        return self._cursor_store.get_more(context.command_document, db_name=context.db_name)

    async def _handle_kill_cursors(self, context: WireRequestContext) -> dict[str, Any]:
        return self._cursor_store.kill_cursors(context.command_document)

    async def _handle_commit_transaction(self, context: WireRequestContext) -> dict[str, Any]:
        return self._session_store.commit_transaction(context.raw_body)

    async def _handle_abort_transaction(self, context: WireRequestContext) -> dict[str, Any]:
        return self._session_store.abort_transaction(context.raw_body)

    async def _handle_passthrough(self, context: WireRequestContext) -> dict[str, Any]:
        database = self._client.get_database(context.db_name)
        result = await database.command(context.command_document, session=context.session)
        if not isinstance(result, dict):
            raise OperationFailure("wire command must resolve to a document response")
        return self._cursor_store.materialize_command_result(context.command_document, result)

    @staticmethod
    def error_document(exc: Exception) -> dict[str, Any]:
        if isinstance(exc, PyMongoError):
            document: dict[str, Any] = {
                "ok": 0.0,
                "errmsg": str(exc),
            }
            code = getattr(exc, "code", None)
            if code is not None:
                document["code"] = code
            code_name = getattr(exc, "code_name", None)
            if code_name is not None:
                document["codeName"] = code_name
            details = getattr(exc, "details", None)
            if isinstance(details, dict):
                document.update(details)
            error_labels = getattr(exc, "error_labels", ())
            if error_labels and "errorLabels" not in document:
                document["errorLabels"] = list(error_labels)
            return document
        if isinstance(exc, MongoEcoError):
            return {"ok": 0.0, "errmsg": str(exc)}
        return {"ok": 0.0, "errmsg": str(exc)}
