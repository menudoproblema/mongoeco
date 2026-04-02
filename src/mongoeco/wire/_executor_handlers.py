from __future__ import annotations

from mongoeco.api import AsyncMongoClient
from mongoeco.wire._executor_passthrough import execute_passthrough_command
from mongoeco.wire.auth import WireAuthenticationService
from mongoeco.wire.connections import WireConnectionContext
from mongoeco.wire.cursors import WireCursorStore
from mongoeco.wire.handshake import WireHandshakeService
from mongoeco.wire.requests import WireRequestContext
from mongoeco.wire.sessions import WireSessionStore
from mongoeco.wire.surface import WireSurface


class WireSpecialCommandHandlers:
    def __init__(
        self,
        *,
        client: AsyncMongoClient,
        cursor_store: WireCursorStore,
        session_store: WireSessionStore,
        surface: WireSurface,
        auth: WireAuthenticationService,
    ) -> None:
        self._client = client
        self._cursor_store = cursor_store
        self._session_store = session_store
        self._auth = auth
        self._handshake = WireHandshakeService(client.mongodb_dialect, surface=surface)
        self._handlers = {
            "authenticate": self._handle_authenticate,
            "sasl_start": self._handle_sasl_start,
            "sasl_continue": self._handle_sasl_continue,
            "handshake": self._handle_handshake,
            "end_sessions": self._handle_end_sessions,
            "get_more": self._handle_get_more,
            "kill_cursors": self._handle_kill_cursors,
            "logout": self._handle_logout,
            "commit_transaction": self._handle_commit_transaction,
            "abort_transaction": self._handle_abort_transaction,
            "passthrough": self._handle_passthrough,
        }

    async def dispatch(self, context: WireRequestContext) -> dict[str, object]:
        handler = self._handlers[context.capability.kind]
        return await handler(context)

    async def _handle_handshake(self, context: WireRequestContext) -> dict[str, object]:
        context.connection.register_hello(context.command_name, context.raw_body)
        return self._handshake.build_hello_response(
            command_name=context.command_name,
            body=context.raw_body,
            connection=context.connection,
        )

    async def _handle_end_sessions(self, context: WireRequestContext) -> dict[str, object]:
        return self._session_store.end_sessions(context.command_document.get("endSessions"))

    async def _handle_get_more(self, context: WireRequestContext) -> dict[str, object]:
        self._auth.require_authenticated(context.connection, context.command_name)
        return self._cursor_store.get_more(context.command_document, db_name=context.db_name)

    async def _handle_kill_cursors(self, context: WireRequestContext) -> dict[str, object]:
        self._auth.require_authenticated(context.connection, context.command_name)
        return self._cursor_store.kill_cursors(context.command_document)

    async def _handle_authenticate(self, context: WireRequestContext) -> dict[str, object]:
        return self._auth.authenticate(context.command_document, connection=context.connection)

    async def _handle_sasl_start(self, context: WireRequestContext) -> dict[str, object]:
        return self._auth.sasl_start(context.command_document, connection=context.connection)

    async def _handle_sasl_continue(self, context: WireRequestContext) -> dict[str, object]:
        return self._auth.sasl_continue(context.command_document, connection=context.connection)

    async def _handle_logout(self, context: WireRequestContext) -> dict[str, object]:
        return self._auth.logout(context.connection)

    async def _handle_commit_transaction(self, context: WireRequestContext) -> dict[str, object]:
        return self._session_store.commit_transaction(context.raw_body)

    async def _handle_abort_transaction(self, context: WireRequestContext) -> dict[str, object]:
        return self._session_store.abort_transaction(context.raw_body)

    async def _handle_passthrough(self, context: WireRequestContext) -> dict[str, object]:
        return await execute_passthrough_command(
            context,
            client=self._client,
            cursor_store=self._cursor_store,
            auth=self._auth,
        )
