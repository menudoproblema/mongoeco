from __future__ import annotations

from typing import Any

from mongoeco.api import AsyncMongoClient
from mongoeco.errors import OperationFailure
from mongoeco.wire._executor_handlers import WireSpecialCommandHandlers
from mongoeco.wire._executor_support import (
    build_error_document,
    build_request_context,
    normalize_legacy_query_body,
)
from mongoeco.wire.auth import WireAuthenticationService
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.cursors import WireCursorStore
from mongoeco.wire.requests import WireRequestContext
from mongoeco.wire.surface import WireSurface
from mongoeco.wire.sessions import WireSessionStore


class WireCommandExecutor:
    def __init__(
        self,
        client: AsyncMongoClient,
        cursor_store: WireCursorStore,
        session_store: WireSessionStore,
        surface: WireSurface | None = None,
        auth: WireAuthenticationService | None = None,
    ) -> None:
        self._client = client
        self._cursor_store = cursor_store
        self._session_store = session_store
        self._surface = surface or WireSurface()
        self._auth = auth or WireAuthenticationService()
        self._handlers = WireSpecialCommandHandlers(
            client=client,
            cursor_store=cursor_store,
            session_store=session_store,
            surface=self._surface,
            auth=self._auth,
        )

    async def execute_command(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
        db_name: str | None = None,
    ) -> dict[str, Any]:
        context = self._build_request_context(body, connection=connection, db_name=db_name)
        return await self._handlers.dispatch(context)

    async def execute_legacy_query(
        self,
        full_collection_name: str,
        query: dict[str, Any],
        *,
        connection: WireConnectionContext,
        number_to_return: int | None = None,
    ) -> dict[str, Any]:
        if not full_collection_name.endswith(".$cmd"):
            raise OperationFailure("legacy OP_QUERY only supports command namespaces")
        db_name = full_collection_name[:-5]
        return await self.execute_command(
            self._normalize_legacy_query_body(query, number_to_return=number_to_return),
            connection=connection,
            db_name=db_name,
        )

    def _build_request_context(
        self,
        body: dict[str, Any],
        *,
        connection: WireConnectionContext,
        db_name: str | None = None,
    ) -> WireRequestContext:
        return build_request_context(
            body,
            connection=connection,
            db_name=db_name,
            client=self._client,
            surface=self._surface,
            session_store=self._session_store,
        )

    @staticmethod
    def _normalize_legacy_query_body(
        query: dict[str, Any],
        *,
        number_to_return: int | None,
    ) -> dict[str, Any]:
        return normalize_legacy_query_body(
            query,
            number_to_return=number_to_return,
        )

    @staticmethod
    def error_document(exc: Exception) -> dict[str, Any]:
        return build_error_document(exc)
