from __future__ import annotations

from typing import Any

from mongoeco.api import AsyncMongoClient
from mongoeco.errors import MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.cursors import WireCursorStore
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
    ) -> None:
        self._client = client
        self._cursor_store = cursor_store
        self._session_store = session_store

    async def execute_command(
        self,
        body: dict[str, Any],
        *,
        db_name: str | None = None,
    ) -> dict[str, Any]:
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
        capability = resolve_wire_command_capability(command_name)
        if capability.kind == "end_sessions":
            return self._session_store.end_sessions(command_document.get("endSessions"))
        if capability.kind == "get_more":
            return self._cursor_store.get_more(command_document, db_name=db_name)
        if capability.kind == "kill_cursors":
            return self._cursor_store.kill_cursors(command_document)
        if capability.kind == "commit_transaction":
            return self._session_store.commit_transaction(body)
        if capability.kind == "abort_transaction":
            return self._session_store.abort_transaction(body)
        database = self._client.get_database(db_name)
        session = self._session_store.resolve_for_command(
            self._client,
            body,
            capability=capability,
        )
        result = await database.command(command_document, session=session)
        if not isinstance(result, dict):
            raise OperationFailure("wire command must resolve to a document response")
        return self._cursor_store.materialize_command_result(command_document, result)

    async def execute_legacy_query(
        self,
        full_collection_name: str,
        query: dict[str, Any],
    ) -> dict[str, Any]:
        if not full_collection_name.endswith(".$cmd"):
            raise OperationFailure("legacy OP_QUERY only supports command namespaces")
        db_name = full_collection_name[:-5]
        return await self.execute_command(query, db_name=db_name)

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
