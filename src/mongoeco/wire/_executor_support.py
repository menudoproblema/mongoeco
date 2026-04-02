from __future__ import annotations

from typing import Any

from mongoeco.api.admin_parsing import (
    normalize_command_batch_size,
    normalize_command_hint,
    normalize_command_max_time_ms,
    normalize_command_projection,
    normalize_command_sort_document,
    normalize_filter_document,
    normalize_index_models_from_command,
)
from mongoeco.api import AsyncMongoClient
from mongoeco.errors import MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire.capabilities import resolve_wire_command_capability
from mongoeco.wire.requests import WireRequestContext
from mongoeco.wire.surface import WireSurface


WIRE_INTERNAL_KEYS = frozenset(
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


def _raise_wire_shape_error(command_name: str, message: str) -> None:
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


def build_request_context(
    body: dict[str, Any],
    *,
    connection,
    db_name: str | None,
    client: AsyncMongoClient,
    surface: WireSurface,
    session_store,
) -> WireRequestContext:
    if db_name is None:
        db_name = body.get("$db")
    if not isinstance(db_name, str) or not db_name:
        raise OperationFailure("$db must be a non-empty string")

    command_document = {
        key: value
        for key, value in body.items()
        if key not in WIRE_INTERNAL_KEYS
    }
    if not command_document:
        raise OperationFailure("wire command document must contain an executable command")

    command_name = next(iter(command_document))
    if not isinstance(command_name, str) or not command_name:
        raise OperationFailure("wire command name must be a non-empty string")
    if not surface.supports_command(command_name):
        raise OperationFailure(f"unsupported wire command: {command_name}")

    capability = resolve_wire_command_capability(command_name)
    validate_wire_command_document(
        body,
        command_document,
        capability=capability,
    )
    session = session_store.resolve_for_command(
        client,
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


def normalize_legacy_query_body(
    query: dict[str, Any],
    *,
    number_to_return: int | None,
) -> dict[str, Any]:
    if not isinstance(query, dict):
        raise OperationFailure("legacy OP_QUERY command must be a document")
    if "$query" in query:
        wrapped_query = query.get("$query")
        if not isinstance(wrapped_query, dict):
            raise OperationFailure("legacy OP_QUERY $query wrapper must contain a document")
        body = dict(wrapped_query)
        for key, value in query.items():
            if key == "$query":
                continue
            if key == "$orderby" and "sort" not in body and "find" in body:
                body["sort"] = value
                continue
            body[key] = value
    else:
        body = dict(query)

    if not body:
        raise OperationFailure("legacy OP_QUERY command must contain a document")

    command_name = next(iter(body))
    if number_to_return is not None and number_to_return > 0:
        if command_name == "find":
            body.setdefault("batchSize", number_to_return)
        elif command_name in {"aggregate", "listCollections", "listIndexes"}:
            cursor_spec = body.get("cursor")
            if cursor_spec is None:
                body["cursor"] = {"batchSize": number_to_return}
            elif isinstance(cursor_spec, dict):
                cursor_spec.setdefault("batchSize", number_to_return)
    return body


def validate_wire_command_document(
    body: dict[str, Any],
    command_document: dict[str, Any],
    *,
    capability,
) -> None:
    command_name = capability.name
    command_value = command_document.get(command_name)
    if command_name == "authenticate":
        if command_value not in (True, 1):
            raise OperationFailure("wire authenticate requires the command value 1")
        mechanism = command_document.get("mechanism")
        if not isinstance(mechanism, str) or not mechanism:
            raise OperationFailure("authenticate requires mechanism")
        username = command_document.get("user")
        if not isinstance(username, str) or not username:
            raise OperationFailure("authenticate requires user")
        database = body.get("db", body.get("$db", "admin"))
        if not isinstance(database, str) or not database:
            raise OperationFailure("authenticate requires db")
        return
    if command_name == "saslStart":
        if command_value not in (True, 1):
            raise OperationFailure("wire saslStart requires the command value 1")
        mechanism = command_document.get("mechanism")
        if not isinstance(mechanism, str) or not mechanism:
            raise OperationFailure("saslStart requires mechanism")
        return
    if command_name == "saslContinue":
        if command_value not in (True, 1):
            raise OperationFailure("wire saslContinue requires the command value 1")
        conversation_id = command_document.get("conversationId")
        if not isinstance(conversation_id, int) or isinstance(conversation_id, bool):
            raise OperationFailure("saslContinue requires conversationId")
        return
    if command_name == "logout":
        if command_value not in (True, 1):
            raise OperationFailure("wire logout requires the command value 1")
        return
    if command_name == "endSessions":
        if not isinstance(command_value, list):
            raise OperationFailure("wire endSessions requires a list")
        return
    if command_name == "getMore":
        if not isinstance(command_value, int) or isinstance(command_value, bool):
            raise OperationFailure("wire getMore cursor id must be an integer")
        collection_name = command_document.get("collection")
        if not isinstance(collection_name, str) or not collection_name:
            raise OperationFailure("wire getMore requires a non-empty collection name")
        batch_size = command_document.get("batchSize")
        if batch_size is not None and (
            not isinstance(batch_size, int) or isinstance(batch_size, bool) or batch_size < 0
        ):
            raise OperationFailure("wire getMore batchSize must be a non-negative integer")
        return
    if command_name == "killCursors":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire killCursors requires a non-empty collection name")
        cursors = command_document.get("cursors")
        if not isinstance(cursors, list):
            raise OperationFailure("wire killCursors requires a cursors list")
        if any(not isinstance(cursor_id, int) or isinstance(cursor_id, bool) for cursor_id in cursors):
            raise OperationFailure("wire killCursors cursor ids must be integers")
        return
    if command_name in {"commitTransaction", "abortTransaction"}:
        if command_value not in (True, 1):
            raise OperationFailure(f"wire {command_name} requires the command value 1")
        if body.get("lsid") is None:
            raise OperationFailure("wire transaction command requires lsid")
        return
    if command_name == "explain":
        if not isinstance(command_value, dict) or not command_value:
            raise OperationFailure("wire explain requires a command document")
        verbosity = command_document.get("verbosity")
        if verbosity is not None and not isinstance(verbosity, str):
            raise OperationFailure("wire explain verbosity must be a string")
        return
    if command_name == "connectionStatus":
        if command_value not in (True, 1):
            raise OperationFailure("wire connectionStatus requires the command value 1")
        show_privileges = command_document.get("showPrivileges")
        if show_privileges is not None and not isinstance(show_privileges, bool):
            raise OperationFailure("wire connectionStatus showPrivileges must be a bool")
        return
    if command_name == "currentOp":
        if command_value not in (True, 1):
            raise OperationFailure("wire currentOp requires the command value 1")
        return
    if command_name == "killOp":
        if command_value not in (True, 1):
            raise OperationFailure("wire killOp requires the command value 1")
        opid = command_document.get("op")
        if not isinstance(opid, str) or not opid:
            raise OperationFailure("wire killOp requires a non-empty string op")
        return
    if command_name == "collStats":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire collStats requires a non-empty collection name")
        scale = command_document.get("scale")
        if scale is not None and (
            not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0
        ):
            raise OperationFailure("wire collStats scale must be a positive integer")
        return
    if command_name == "dbHash":
        if command_value not in (True, 1):
            raise OperationFailure("wire dbHash requires the command value 1")
        collections = command_document.get("collections")
        if collections is not None and (
            not isinstance(collections, list)
            or any(not isinstance(name, str) or not name for name in collections)
        ):
            raise OperationFailure("wire dbHash collections must be a list of non-empty strings")
        comment = command_document.get("comment")
        if comment is not None and not isinstance(comment, str):
            raise OperationFailure("wire dbHash comment must be a string")
        return
    if command_name == "dbStats":
        if command_value not in (True, 1):
            raise OperationFailure("wire dbStats requires the command value 1")
        scale = command_document.get("scale")
        if scale is not None and (
            not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0
        ):
            raise OperationFailure("wire dbStats scale must be a positive integer")
        return
    if command_name == "listCollections":
        if command_value not in (True, 1):
            raise OperationFailure("wire listCollections requires the command value 1")
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
        return
    if command_name == "listDatabases":
        if command_value not in (True, 1):
            raise OperationFailure("wire listDatabases requires the command value 1")
        name_only = command_document.get("nameOnly")
        if name_only is not None and not isinstance(name_only, bool):
            raise OperationFailure("wire listDatabases nameOnly must be a bool")
        filter_spec = command_document.get("filter")
        if filter_spec is not None and not isinstance(filter_spec, dict):
            raise OperationFailure("wire listDatabases filter must be a document")
        comment = command_document.get("comment")
        if comment is not None and not isinstance(comment, str):
            raise OperationFailure("wire listDatabases comment must be a string")
        return
    if command_name == "profile":
        if not isinstance(command_value, int) or isinstance(command_value, bool):
            raise OperationFailure("wire profile level must be an integer")
        slow_ms = command_document.get("slowms")
        if slow_ms is not None and (
            not isinstance(slow_ms, int) or isinstance(slow_ms, bool)
        ):
            raise OperationFailure("wire profile slowms must be an integer")
        return
    if command_name == "aggregate":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire aggregate requires a non-empty collection name")
        pipeline = command_document.get("pipeline")
        if not isinstance(pipeline, list):
            raise OperationFailure("wire aggregate requires a pipeline list")
        cursor = command_document.get("cursor")
        if cursor is not None and not isinstance(cursor, dict):
            raise OperationFailure("wire aggregate cursor must be a document")
        try:
            normalize_command_hint(command_document.get("hint"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))
        try:
            normalize_command_max_time_ms(command_document.get("maxTimeMS"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))
        if command_document.get("allowDiskUse") is not None and not isinstance(command_document.get("allowDiskUse"), bool):
            raise OperationFailure("wire aggregate allowDiskUse must be a bool")
        let = command_document.get("let")
        if let is not None and not isinstance(let, dict):
            raise OperationFailure("wire aggregate let must be a document")
        if cursor is not None:
            try:
                normalize_command_batch_size(cursor.get("batchSize"))
            except (TypeError, ValueError, OperationFailure) as exc:
                _raise_wire_shape_error(command_name, str(exc))
        return
    if command_name == "count":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire count requires a non-empty collection name")
        _validate_wire_find_like_command(command_name, command_document, filter_field="query")
        return
    if command_name == "distinct":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire distinct requires a non-empty collection name")
        key = command_document.get("key")
        if not isinstance(key, str) or not key:
            raise OperationFailure("wire distinct requires a non-empty key string")
        _validate_wire_find_like_command(command_name, command_document, filter_field="query")
        return
    if command_name == "validate":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire validate requires a non-empty collection name")
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
        return
    if command_name == "find":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire find requires a non-empty collection name")
        _validate_wire_find_like_command(command_name, command_document, filter_field="filter")
        return
    if command_name == "listIndexes":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire listIndexes requires a non-empty collection name")
        comment = command_document.get("comment")
        if comment is not None and not isinstance(comment, str):
            raise OperationFailure("wire listIndexes comment must be a string")
        return
    if command_name == "createIndexes":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire createIndexes requires a non-empty collection name")
        try:
            normalize_index_models_from_command(command_document.get("indexes"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))
        try:
            normalize_command_max_time_ms(command_document.get("maxTimeMS"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))
        return
    if command_name == "dropIndexes":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire dropIndexes requires a non-empty collection name")
        target = command_document.get("index")
        if target != "*" and not isinstance(target, (str, list, tuple, dict)):
            raise OperationFailure("wire dropIndexes index must be '*', a name, or a key specification")
        comment = command_document.get("comment")
        if comment is not None and not isinstance(comment, str):
            raise OperationFailure("wire dropIndexes comment must be a string")
        return
    if command_name == "findAndModify":
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure("wire findAndModify requires a non-empty collection name")
        try:
            normalize_filter_document(command_document.get("query"))
        except TypeError as exc:
            _raise_wire_shape_error(command_name, str(exc))
        try:
            normalize_command_sort_document(command_document.get("sort"))
        except TypeError as exc:
            _raise_wire_shape_error(command_name, str(exc))
        try:
            normalize_command_projection(command_document.get("fields"))
        except TypeError as exc:
            _raise_wire_shape_error(command_name, str(exc))
        try:
            normalize_command_hint(command_document.get("hint"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))
        try:
            normalize_command_max_time_ms(command_document.get("maxTimeMS"))
        except (TypeError, ValueError, OperationFailure) as exc:
            _raise_wire_shape_error(command_name, str(exc))
        let = command_document.get("let")
        if let is not None and not isinstance(let, dict):
            raise OperationFailure("wire findAndModify let must be a dict")
        return
    if command_name in _COLLECTION_NAME_COMMANDS:
        if not isinstance(command_value, str) or not command_value:
            raise OperationFailure(f"wire {command_name} requires a non-empty collection name")
        if command_name not in _LIST_PAYLOAD_FIELDS:
            return
    if command_name in _LIST_PAYLOAD_FIELDS:
        field_name = _LIST_PAYLOAD_FIELDS[command_name]
        payload = command_document.get(field_name)
        if not isinstance(payload, list) or not payload:
            raise OperationFailure(f"wire {command_name} requires a non-empty {field_name} list")
        return
    if command_name in _FLAG_COMMANDS:
        if command_value not in (True, 1):
            raise OperationFailure(f"wire {command_name} requires the command value 1")


def patch_connection_status_auth_info(result: dict[str, Any], *, connection) -> dict[str, Any]:
    auth_info = result.get("authInfo")
    if isinstance(auth_info, dict):
        auth_info["authenticatedUsers"] = list(connection.authenticated_users)
        auth_info["authenticatedUserRoles"] = list(connection.authenticated_roles)
        if "authenticatedUserPrivileges" in auth_info:
            auth_info["authenticatedUserPrivileges"] = []
    return result


def build_error_document(exc: Exception) -> dict[str, Any]:
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
