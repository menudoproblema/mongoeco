"""Server BSON grammar shared by raw write command adapters."""

from mongoeco.core.bson_scalars import is_bson_numeric, unwrap_bson_numeric
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.server_errors import ErrorScope, server_error


_GENERIC_FIELDS = frozenset(
    {
        "$db",
        "$clusterTime",
        "$readPreference",
        "lsid",
        "txnNumber",
        "autocommit",
        "startTransaction",
        "comment",
        "maxTimeMS",
        "writeConcern",
        "readConcern",
        "apiVersion",
        "apiStrict",
        "apiDeprecationErrors",
        "shardVersion",
        "databaseVersion",
        "clientOperationKey",
    }
)
_COMMAND_FIELDS = {
    "update": {
        "update",
        "updates",
        "ordered",
        "bypassDocumentValidation",
        "let",
    },
    "delete": {"delete", "deletes", "ordered", "let", "bypassDocumentValidation"},
    "findAndModify": {
        "findAndModify",
        "query",
        "sort",
        "remove",
        "update",
        "new",
        "fields",
        "upsert",
        "bypassDocumentValidation",
        "arrayFilters",
        "hint",
        "let",
        "collation",
    },
}
_ENTRY_FIELDS = {
    "update": {
        "q",
        "u",
        "multi",
        "upsert",
        "arrayFilters",
        "collation",
        "hint",
        "sort",
        "c",
    },
    "delete": {"q", "limit", "collation", "hint"},
}


def validate_write_fields(spec, command, *, entry=False):
    allowed = (
        _ENTRY_FIELDS[command] if entry else _COMMAND_FIELDS[command] | _GENERIC_FIELDS
    )
    if spec.keys() - allowed:
        message = f"Unknown field in {command}: {next(iter(spec.keys() - allowed))}"
        raise server_error(message, code=40415, scope=ErrorScope.COMMAND)


def command_boolean(value, field, *, numeric=False):
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if numeric:
        internal = DocumentCodec.to_internal(value)
        if is_bson_numeric(internal):
            return bool(unwrap_bson_numeric(internal))
    message = f"{field} must be a boolean" + (" or number" if numeric else "")
    raise server_error(message, code=14, scope=ErrorScope.COMMAND)


def validate_command_let(value):
    if value is not None and not isinstance(value, dict):
        message = "let must be a document"
        raise server_error(message, code=14, scope=ErrorScope.COMMAND)


def validate_command_array_filters(spec, *, dialect):
    if "arrayFilters" in spec and spec["arrayFilters"] is None and dialect == "7.0":
        message = "arrayFilters must be an array"
        raise server_error(message, code=10065, scope=ErrorScope.COMMAND)
