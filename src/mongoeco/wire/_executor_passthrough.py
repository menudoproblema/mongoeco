from __future__ import annotations

from typing import TYPE_CHECKING, Any

from mongoeco.errors import OperationFailure
from mongoeco.wire._executor_support import patch_connection_status_auth_info
from mongoeco.wire.capabilities import resolve_wire_command_capability


if TYPE_CHECKING:
    from mongoeco.wire.surface import WireSurface


async def execute_passthrough_command(
    context,
    *,
    client,
    cursor_store,
    auth,
    surface: WireSurface,
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
        surface=surface,
    )


async def _execute_connection_status_command(
    context,
    *,
    client,
) -> dict[str, Any]:
    database = client.get_database(context.db_name)
    result = await _execute_database_command(database, context)
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
    surface: WireSurface,
) -> dict[str, Any]:
    auth.require_authenticated(context.connection, context.command_name)
    database = client.get_database(context.db_name)
    result = await _execute_database_command(database, context)
    if context.command_name == "listCommands":
        result = _wire_list_commands_result(result, surface=surface)
    if context.command_name == "whatsmyuri":
        if not isinstance(result, dict):
            message = "wire command must resolve to a document response"
            raise OperationFailure(message)
        result = {**result, "you": context.connection.peer_address}
    return _materialize_passthrough_result(
        context.command_document,
        result,
        cursor_store=cursor_store,
        lsid=context.raw_body.get("lsid"),
        connection=context.connection,
    )


def _wire_list_commands_result(
    result: object, *, surface: WireSurface
) -> dict[str, Any]:
    if not isinstance(result, dict) or not isinstance(result.get("commands"), dict):
        message = "wire listCommands must resolve to a command catalog"
        raise OperationFailure(message)
    database_commands = result["commands"]
    wire_commands: dict[str, Any] = {}
    for name in dict.fromkeys(surface.supported_commands):
        if name in database_commands:
            wire_commands[name] = database_commands[name]
            continue
        capability = resolve_wire_command_capability(name)
        if capability.kind == "passthrough":
            continue
        wire_commands[name] = {
            "help": f"mongoeco local wire support for the {name} command",
            "adminFamily": capability.family,
            "supportsWire": True,
            "supportsExplain": False,
            "supportsComment": False,
            "note": "Wire-only command; its handler owns the contract.",
        }
    return {**result, "commands": wire_commands}


async def _execute_database_command(database, context):
    try:
        return await database.command(
            context.command_document,
            session=context.session,
            execution_context=context.execution_context,
        )
    except TypeError as exc:
        if "execution_context" not in str(exc):
            raise
        return await database.command(
            context.command_document,
            session=context.session,
        )


def _materialize_passthrough_result(
    command_document: dict[str, Any],
    result: object,
    *,
    cursor_store,
    lsid: object | None = None,
    connection=None,
) -> dict[str, Any]:
    if not isinstance(result, dict):
        raise OperationFailure("wire command must resolve to a document response")
    return cursor_store.materialize_command_result(
        command_document,
        result,
        lsid=lsid,
        connection=connection,
    )
