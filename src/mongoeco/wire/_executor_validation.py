from __future__ import annotations

from typing import Any, Callable

from mongoeco.api.admin_parsing import (
    normalize_command_batch_size,
    normalize_command_hint,
    normalize_command_max_time_ms,
    normalize_command_projection,
    normalize_command_sort_document,
    normalize_filter_document,
    normalize_index_models_from_command,
)
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

_LIST_PAYLOAD_FIELDS = {
    "createIndexes": "indexes",
    "insert": "documents",
    "update": "updates",
    "delete": "deletes",
}

type _WireValidator = Callable[[dict[str, Any], dict[str, Any]], None]


def _raise_wire_shape_error(command_name: str, message: str) -> None:
    raise OperationFailure(f"wire {command_name} {message}")


def _validate_command_value_one(command_name: str, command_value: object) -> None:
    if command_value not in (True, 1):
        raise OperationFailure(f"wire {command_name} requires the command value 1")


def _require_non_empty_string(command_name: str, value: object, message: str) -> None:
    if not isinstance(value, str) or not value:
        raise OperationFailure(f"wire {command_name} {message}")


def _validate_wire_find_like_command(
    command_name: str,
    command_document: dict[str, Any],
    *,
    filter_field: str,
    require_batch_size: bool = False,
) -> None:
    try:
        normalize_filter_document(command_document.get(filter_field))
    except TypeError as exc:
        _raise_wire_shape_error(command_name, str(exc))
    try:
        normalize_command_projection(command_document.get("projection"))
    except TypeError as exc:
        _raise_wire_shape_error(command_name, str(exc))
    try:
        normalize_command_sort_document(command_document.get("sort"))
    except TypeError as exc:
        _raise_wire_shape_error(command_name, str(exc))
    try:
        normalize_command_hint(command_document.get("hint"))
    except (TypeError, OperationFailure) as exc:
        _raise_wire_shape_error(command_name, str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error(command_name, str(exc))
    for field_name in ("skip", "limit"):
        value = command_document.get(field_name)
        if value is not None and (
            not isinstance(value, int) or isinstance(value, bool) or value < 0
        ):
            _raise_wire_shape_error(command_name, f"{field_name} must be a non-negative integer")
    batch_size = command_document.get("batchSize")
    if batch_size is not None or require_batch_size:
        try:
            normalize_command_batch_size(batch_size)
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))


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


def _validate_explain(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    if not isinstance(command_document.get("explain"), dict) or not command_document.get("explain"):
        raise OperationFailure("wire explain requires a command document")
    verbosity = command_document.get("verbosity")
    if verbosity is not None and not isinstance(verbosity, str):
        raise OperationFailure("wire explain verbosity must be a string")


def _validate_connection_status(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("connectionStatus", command_document.get("connectionStatus"))
    show_privileges = command_document.get("showPrivileges")
    if show_privileges is not None and not isinstance(show_privileges, bool):
        raise OperationFailure("wire connectionStatus showPrivileges must be a bool")


def _validate_current_op(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("currentOp", command_document.get("currentOp"))


def _validate_kill_op(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("killOp", command_document.get("killOp"))
    _require_non_empty_string("killOp", command_document.get("op"), "requires a non-empty string op")


def _validate_coll_stats(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("collStats", command_document.get("collStats"), "requires a non-empty collection name")
    scale = command_document.get("scale")
    if scale is not None and (
        not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0
    ):
        raise OperationFailure("wire collStats scale must be a positive integer")


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


def _validate_db_stats(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("dbStats", command_document.get("dbStats"))
    scale = command_document.get("scale")
    if scale is not None and (
        not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0
    ):
        raise OperationFailure("wire dbStats scale must be a positive integer")


def _validate_list_collections(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("listCollections", command_document.get("listCollections"))
    name_only = command_document.get("nameOnly")
    if name_only is not None and not isinstance(name_only, bool):
        raise OperationFailure("wire listCollections nameOnly must be a bool")
    authorized = command_document.get("authorizedCollections")
    if authorized is not None and not isinstance(authorized, bool):
        raise OperationFailure("wire listCollections authorizedCollections must be a bool")
    filter_spec = command_document.get("filter")
    if filter_spec is not None and not isinstance(filter_spec, dict):
        raise OperationFailure("wire listCollections filter must be a document")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire listCollections comment must be a string")


def _validate_list_databases(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _validate_command_value_one("listDatabases", command_document.get("listDatabases"))
    name_only = command_document.get("nameOnly")
    if name_only is not None and not isinstance(name_only, bool):
        raise OperationFailure("wire listDatabases nameOnly must be a bool")
    filter_spec = command_document.get("filter")
    if filter_spec is not None and not isinstance(filter_spec, dict):
        raise OperationFailure("wire listDatabases filter must be a document")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire listDatabases comment must be a string")


def _validate_profile(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    command_value = command_document.get("profile")
    if not isinstance(command_value, int) or isinstance(command_value, bool):
        raise OperationFailure("wire profile level must be an integer")
    slow_ms = command_document.get("slowms")
    if slow_ms is not None and (
        not isinstance(slow_ms, int) or isinstance(slow_ms, bool)
    ):
        raise OperationFailure("wire profile slowms must be an integer")


def _validate_aggregate(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("aggregate", command_document.get("aggregate"), "requires a non-empty collection name")
    pipeline = command_document.get("pipeline")
    if not isinstance(pipeline, list):
        raise OperationFailure("wire aggregate requires a pipeline list")
    cursor = command_document.get("cursor")
    if cursor is not None and not isinstance(cursor, dict):
        raise OperationFailure("wire aggregate cursor must be a document")
    try:
        normalize_command_hint(command_document.get("hint"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("aggregate", str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("aggregate", str(exc))
    if command_document.get("allowDiskUse") is not None and not isinstance(command_document.get("allowDiskUse"), bool):
        raise OperationFailure("wire aggregate allowDiskUse must be a bool")
    let = command_document.get("let")
    if let is not None and not isinstance(let, dict):
        raise OperationFailure("wire aggregate let must be a document")
    if cursor is not None:
        try:
            normalize_command_batch_size(cursor.get("batchSize"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error("aggregate", str(exc))


def _validate_count(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("count", command_document.get("count"), "requires a non-empty collection name")
    _validate_wire_find_like_command("count", command_document, filter_field="query")


def _validate_distinct(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("distinct", command_document.get("distinct"), "requires a non-empty collection name")
    _require_non_empty_string("distinct", command_document.get("key"), "requires a non-empty key string")
    _validate_wire_find_like_command("distinct", command_document, filter_field="query")


def _validate_validate(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("validate", command_document.get("validate"), "requires a non-empty collection name")
    for field_name in ("scandata", "full"):
        value = command_document.get(field_name)
        if value is not None and not isinstance(value, bool):
            raise OperationFailure(f"wire validate {field_name} must be a bool")
    background = command_document.get("background")
    if background is not None and not isinstance(background, bool):
        raise OperationFailure("wire validate background must be a bool or null")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire validate comment must be a string")


def _validate_find(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("find", command_document.get("find"), "requires a non-empty collection name")
    _validate_wire_find_like_command("find", command_document, filter_field="filter")


def _validate_list_indexes(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("listIndexes", command_document.get("listIndexes"), "requires a non-empty collection name")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire listIndexes comment must be a string")


def _validate_create_indexes(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("createIndexes", command_document.get("createIndexes"), "requires a non-empty collection name")
    try:
        normalize_index_models_from_command(command_document.get("indexes"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("createIndexes", str(exc))
    try:
        normalize_command_max_time_ms(command_document.get("maxTimeMS"))
    except (TypeError, ValueError, OperationFailure) as exc:
        _raise_wire_shape_error("createIndexes", str(exc))


def _validate_drop_indexes(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("dropIndexes", command_document.get("dropIndexes"), "requires a non-empty collection name")
    target = command_document.get("index")
    if target != "*" and not isinstance(target, (str, list, tuple, dict)):
        raise OperationFailure("wire dropIndexes index must be '*', a name, or a key specification")
    comment = command_document.get("comment")
    if comment is not None and not isinstance(comment, str):
        raise OperationFailure("wire dropIndexes comment must be a string")


def _validate_find_and_modify(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
    _require_non_empty_string("findAndModify", command_document.get("findAndModify"), "requires a non-empty collection name")
    try:
        normalize_filter_document(command_document.get("query"))
    except TypeError as exc:
        _raise_wire_shape_error("findAndModify", str(exc))
    try:
        normalize_command_sort_document(command_document.get("sort"))
    except TypeError as exc:
        _raise_wire_shape_error("findAndModify", str(exc))
    try:
        normalize_command_projection(command_document.get("fields"))
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
    let = command_document.get("let")
    if let is not None and not isinstance(let, dict):
        raise OperationFailure("wire findAndModify let must be a dict")


def _make_collection_name_validator(command_name: str) -> _WireValidator:
    def _validator(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
        _require_non_empty_string(command_name, command_document.get(command_name), "requires a non-empty collection name")
    return _validator


def _make_payload_list_validator(command_name: str, field_name: str) -> _WireValidator:
    def _validator(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
        _require_non_empty_string(command_name, command_document.get(command_name), "requires a non-empty collection name")
        payload = command_document.get(field_name)
        if not isinstance(payload, list) or not payload:
            raise OperationFailure(f"wire {command_name} requires a non-empty {field_name} list")
    return _validator


def _make_flag_validator(command_name: str) -> _WireValidator:
    def _validator(_body: dict[str, Any], command_document: dict[str, Any]) -> None:
        _validate_command_value_one(command_name, command_document.get(command_name))
    return _validator


_WIRE_COMMAND_VALIDATORS: dict[str, _WireValidator] = {
    "authenticate": _validate_authenticate,
    "saslStart": _validate_sasl_start,
    "saslContinue": _validate_sasl_continue,
    "logout": _validate_logout,
    "endSessions": _validate_end_sessions,
    "getMore": _validate_get_more,
    "killCursors": _validate_kill_cursors,
    "commitTransaction": _validate_transaction_command,
    "abortTransaction": _validate_transaction_command,
    "explain": _validate_explain,
    "connectionStatus": _validate_connection_status,
    "currentOp": _validate_current_op,
    "killOp": _validate_kill_op,
    "collStats": _validate_coll_stats,
    "dbHash": _validate_db_hash,
    "dbStats": _validate_db_stats,
    "listCollections": _validate_list_collections,
    "listDatabases": _validate_list_databases,
    "profile": _validate_profile,
    "aggregate": _validate_aggregate,
    "count": _validate_count,
    "distinct": _validate_distinct,
    "validate": _validate_validate,
    "find": _validate_find,
    "listIndexes": _validate_list_indexes,
    "createIndexes": _validate_create_indexes,
    "dropIndexes": _validate_drop_indexes,
    "findAndModify": _validate_find_and_modify,
    **{
        command_name: _make_payload_list_validator(command_name, field_name)
        for command_name, field_name in _LIST_PAYLOAD_FIELDS.items()
        if command_name != "createIndexes"
    },
    **{
        command_name: _make_collection_name_validator(command_name)
        for command_name in _COLLECTION_NAME_COMMANDS
        if command_name not in {
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
        }
    },
    **{
        command_name: _make_flag_validator(command_name)
        for command_name in _FLAG_COMMANDS
        if command_name not in {
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
