from __future__ import annotations

from typing import Any

from mongoeco.api import AsyncMongoClient
from mongoeco.errors import MongoEcoError, OperationFailure, PyMongoError
from mongoeco.wire._executor_validation import validate_wire_command_document
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
