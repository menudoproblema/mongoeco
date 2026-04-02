from __future__ import annotations

from typing import Any

from mongoeco.errors import OperationFailure
from mongoeco.wire._executor_support import patch_connection_status_auth_info


async def execute_passthrough_command(
    context,
    *,
    client,
    cursor_store,
    auth,
) -> dict[str, Any]:
    if context.command_name == "connectionStatus":
        return await _execute_connection_status_command(
            context,
            client=client,
        )
    return await _execute_authenticated_passthrough_command(
        context,
        client=client,
        cursor_store=cursor_store,
        auth=auth,
    )


async def _execute_connection_status_command(
    context,
    *,
    client,
) -> dict[str, Any]:
    database = client.get_database(context.db_name)
    result = await database.command(
        context.command_document,
        session=context.session,
    )
    if not isinstance(result, dict):
        raise OperationFailure("wire command must resolve to a document response")
    return patch_connection_status_auth_info(
        result,
        connection=context.connection,
    )


async def _execute_authenticated_passthrough_command(
    context,
    *,
    client,
    cursor_store,
    auth,
) -> dict[str, Any]:
    auth.require_authenticated(context.connection, context.command_name)
    database = client.get_database(context.db_name)
    result = await database.command(context.command_document, session=context.session)
    return _materialize_passthrough_result(
        context.command_document,
        result,
        cursor_store=cursor_store,
    )


def _materialize_passthrough_result(
    command_document: dict[str, Any],
    result: object,
    *,
    cursor_store,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise OperationFailure("wire command must resolve to a document response")
    return cursor_store.materialize_command_result(command_document, result)
