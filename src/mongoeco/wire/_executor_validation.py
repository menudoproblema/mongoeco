from __future__ import annotations

from typing import Any, Callable

from mongoeco.api.admin_parsing import (
    normalize_command_hint,
    normalize_command_max_time_ms,
    normalize_filter_document,
    normalize_index_models_from_command,
)
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.errors import OperationFailure


_COLLECTION_NAME_COMMANDS = frozenset(
    {
        "aggregate",
        "collStats",
        "count",
        "create",
        "createIndexes",
        "delete",
        "distinct",
        "drop",
        "dropIndexes",
        "find",
        "findAndModify",
        "insert",
        "listIndexes",
        "update",
        "validate",
    }
)

_FLAG_COMMANDS = frozenset(
    {
        "buildInfo",
        "connectionStatus",
        "currentOp",
        "dbHash",
        "dbStats",
        "getCmdLineOpts",
        "hostInfo",
        "listCommands",
        "listCollections",
        "listDatabases",
        "ping",
        "serverStatus",
        "whatsmyuri",
        "killOp",
    }
)



type _WireValidator = Callable[[dict[str, Any], dict[str, Any]], None]


def _raise_wire_shape_error(command_name: str, message: str) -> None:
    raise OperationFailure(f"wire {command_name} {message}")


def _validate_command_value_one(command_name: str, command_value: object) -> None:
    if command_value not in (True, 1):
        raise OperationFailure(f"wire {command_name} requires the command value 1")


def _require_non_empty_string(command_name: str, value: object, message: str) -> None:
    if not isinstance(value, str) or not value:
        raise OperationFailure(f"wire {command_name} {message}")




def _validate_authenticate(body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("authenticate", command_document.get("authenticate"))
    _require_non_empty_string("authenticate", command_document.get("mechanism"), "requires mechanism")
    _require_non_empty_string("authenticate", command_document.get("user"), "requires user")
    database = body.get("db", body.get("$db", "admin"))
    _require_non_empty_string("authenticate", database, "requires db")


def _validate_sasl_start(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("saslStart", command_document.get("saslStart"))
    _require_non_empty_string("saslStart", command_document.get("mechanism"), "requires mechanism")


def _validate_sasl_continue(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("saslContinue", command_document.get("saslContinue"))
    conversation_id = command_document.get("conversationId")
    if not isinstance(conversation_id, int) or isinstance(conversation_id, bool):
        raise OperationFailure("saslContinue requires conversationId")


def _validate_logout(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("logout", command_document.get("logout"))


def _validate_end_sessions(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    if not isinstance(command_document.get("endSessions"), list):
        raise OperationFailure("wire endSessions requires a list")


def _validate_get_more(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    command_value = command_document.get("getMore")
    if not isinstance(command_value, int) or isinstance(command_value, bool):
        raise OperationFailure("wire getMore cursor id must be an integer")
    _require_non_empty_string("getMore", command_document.get("collection"), "requires a non-empty collection name")
    batch_size = command_document.get("batchSize")
    if batch_size is not None and (
        not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0
    ):
        raise OperationFailure("wire getMore batchSize must be a non-negative integer")


def _validate_kill_cursors(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("killCursors", command_document.get("killCursors"), "requires a non-empty collection name")
    cursors = command_document.get("cursors")
    if not isinstance(cursors, list):
        raise OperationFailure("wire killCursors requires a cursors list")
    if any(not isinstance(cursor_id, int) or isinstance(cursor_id, bool) for cursor_id in cursors):
        raise OperationFailure("wire killCursors cursor ids must be integers")


def _validate_transaction_command(body: dict[str, Any], command_document: dict[str, Any]) -> None:
    command_name = next(iter(command_document))
    _validate_command_value_one(command_name, command_document.get(command_name))
    if body.get("lsid") is None:
        raise OperationFailure("wire transaction command requires lsid")






def _validate_current_op(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("currentOp", command_document.get("currentOp"))


def _validate_kill_op(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("killOp", command_document.get("killOp"))
    _require_non_empty_string("killOp", command_document.get("op"), "requires a non-empty string op")




def _validate_db_hash(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("dbHash", command_document.get("dbHash"))
    collections = command_document.get("collections")
    if collections is not None and (
        not isinstance(collections, list)
        or any(not isinstance(name, str) or not name for name in collections)
    ):
        raise OperationFailure("wire dbHash collections must be a list of non-empty strings")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire dbHash comment must be a string")




def _validate_drop_database(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    command_value = command_document.get("dropDatabase")
    if command_value is not True and (
        type(command_value) is not int or command_value != 1
    ):
        message = "wire dropDatabase requires the command value 1"
        raise OperationFailure(message)






def _validate_profile(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    command_value = command_document.get("profile")
    if not isinstance(command_value, int) or isinstance(command_value, bool):
        raise OperationFailure("wire profile level must be an integer")
    slow_ms = command_document.get("slowms")
    if slow_ms is not None and (
        not isinstance(slow_ms, int) or isinstance(slow_ms, bool)
    ):
        raise OperationFailure("wire profile slowms must be an integer")










def _validate_list_indexes(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("listIndexes", command_document.get("listIndexes"), "requires a non-empty collection name")


def _validate_create_indexes(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("createIndexes", command_document.get("createIndexes"), "requires a non-empty collection name")
    try:
        normalize_index_models_from_command(command_document.get("indexes"))
    except OperationFailure:
        raise
    except (TypeError, ValueError) as exc:
        _raise_wire_shape_error("createIndexes", str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("createIndexes", str(exc))


def _validate_drop_indexes(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("dropIndexes", command_document.get("dropIndexes"), "requires a non-empty collection name")


def _make_collection_name_validator(command_name: str) -> _WireValidator:
    def _validator(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
        _require_non_empty_string(
            command_name,
            command_document.get(command_name),
            "requires a non-empty collection name",
        )

    return _validator


def _make_flag_validator(command_name: str) -> _WireValidator:
    def _validator(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
        _validate_command_value_one(command_name, command_document.get(command_name))

    return _validator



def _validate_connection_status(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    _validate_command_value_one(
        "connectionStatus", command_document.get("connectionStatus")
    )


def _validate_list_collections(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    _validate_command_value_one(
        "listCollections", command_document.get("listCollections")
    )


def _validate_db_stats(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("dbStats", command_document.get("dbStats"))


def _validate_coll_stats(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    _require_non_empty_string(
        "collStats",
        command_document.get("collStats"),
        "requires a non-empty collection name",
    )


def _validate_count(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string(
        "count", command_document.get("count"), "requires a non-empty collection name"
    )


def _validate_distinct(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string(
        "distinct",
        command_document.get("distinct"),
        "requires a non-empty collection name",
    )


def _validate_find(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string(
        "find", command_document.get("find"), "requires a non-empty collection name"
    )
    try:
        if "filter" in command_document and command_document["filter"] is None:
            message = "filter_spec must be a dict"
            raise TypeError(message)
        normalize_filter_document(command_document.get("filter"))
    except TypeError as exc:
        message = f"wire find {exc}"
        raise server_error(message, code=14, scope=ErrorScope.COMMAND) from exc
    try:
        normalize_command_hint(command_document.get("hint"))
    except OperationFailure as exc:
        if exc.code is not None:
            raise
        _raise_wire_shape_error("find", str(exc))
    except TypeError as exc:
        _raise_wire_shape_error("find", str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("find", str(exc))


def _validate_list_databases(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    _validate_command_value_one("listDatabases", command_document.get("listDatabases"))
    filter_spec = command_document.get("filter")
    if filter_spec is not None and not isinstance(filter_spec, dict):
        raise OperationFailure("wire listDatabases filter must be a document")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire listDatabases comment must be a string")


def _validate_aggregate(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    _require_non_empty_string(
        "aggregate",
        command_document.get("aggregate"),
        "requires a non-empty collection name",
    )
    pipeline = command_document.get("pipeline")
    if not isinstance(pipeline, list):
        raise OperationFailure("wire aggregate requires a pipeline list")
    try:
        normalize_command_hint(command_document.get("hint"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("aggregate", str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("aggregate", str(exc))


def _validate_find_and_modify(
    _body: dict[str, Any], command_document: dict[str, Any]
) -> None:
    _require_non_empty_string(
        "findAndModify",
        command_document.get("findAndModify"),
        "requires a non-empty collection name",
    )
    try:
        normalize_filter_document(command_document.get("query"))
    except TypeError as exc:
        _raise_wire_shape_error("findAndModify", str(exc))
    try:
        normalize_command_hint(command_document.get("hint"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("findAndModify", str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("findAndModify", str(exc))



_WIRE_COMMAND_VALIDATORS: dict[str, _WireValidator] = {
    "findAndModify": _validate_find_and_modify,
    "aggregate": _validate_aggregate,
    "listDatabases": _validate_list_databases,
    "find": _validate_find,
    "distinct": _validate_distinct,
    "count": _validate_count,
    "collStats": _validate_coll_stats,
    "dbStats": _validate_db_stats,
    "listCollections": _validate_list_collections,
    "connectionStatus": _validate_connection_status,
    **{
        command_name: _make_flag_validator(command_name)
        for command_name in ("hello", "isMaster", "ismaster")
    },
    "authenticate": _validate_authenticate,
    "saslStart": _validate_sasl_start,
    "saslContinue": _validate_sasl_continue,
    "logout": _validate_logout,
    "endSessions": _validate_end_sessions,
    "getMore": _validate_get_more,
    "killCursors": _validate_kill_cursors,
    "commitTransaction": _validate_transaction_command,
    "abortTransaction": _validate_transaction_command,
    "currentOp": _validate_current_op,
    "killOp": _validate_kill_op,
    "dbHash": _validate_db_hash,
    "dropDatabase": _validate_drop_database,
    "profile": _validate_profile,
    "listIndexes": _validate_list_indexes,
    "createIndexes": _validate_create_indexes,
    "dropIndexes": _validate_drop_indexes,
    **{
        command_name: _make_collection_name_validator(command_name)
        for command_name in _COLLECTION_NAME_COMMANDS
        if command_name
        not in {
            "aggregate",
            "collStats",
            "count",
            "createIndexes",
            "distinct",
            "find",
            "findAndModify",
            "insert",
            "listIndexes",
            "update",
            "delete",
            "dropIndexes",
            "validate",
            "drop",
        }
    },
    **{
        command_name: _make_flag_validator(command_name)
        for command_name in _FLAG_COMMANDS
        if command_name
        not in {
            "connectionStatus",
            "currentOp",
            "dbHash",
            "dbStats",
            "killOp",
            "listCollections",
            "listDatabases",
            "profile",
        }
    },
}


def validate_wire_command_document(
    body: dict[str, Any],
    command_document: dict[str, Any],
    *,
    capability,
) -> None:
    validator = _WIRE_COMMAND_VALIDATORS.get(capability.name)
    if validator is None:
        return
    validator(body, command_document)
