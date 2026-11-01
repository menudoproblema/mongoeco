"""Server BSON grammar shared by raw write command adapters."""

from mongoeco.core.bson_scalars import wrap_bson_numeric
from mongoeco.core.bson_values import matches_bson_type
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.server_errors import ErrorScope, server_error
from mongoeco.types import UndefinedType


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
    "renameCollection": {"renameCollection", "to", "dropTarget", "stayTemp"},
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


def rename_collection_options(spec, database):
    """Validate the administrative entrypoint before admitting its namespaces."""
    if database != "admin":
        message = "renameCollection may only be run against the admin database"
        raise server_error(message, code=13, scope=ErrorScope.COMMAND)
    validate_write_fields(spec, "renameCollection")
    for field, value in spec.items():
        if field in {"renameCollection", "to"} and not matches_bson_type(
            value, "string"
        ):
            message = f"{field} must be a string"
            raise server_error(message, code=14, scope=ErrorScope.COMMAND)
        if field in {"dropTarget", "stayTemp"} and not isinstance(value, bool):
            message = f"{field} must be a bool"
            raise server_error(message, code=14, scope=ErrorScope.COMMAND)
    namespaces = []
    for field in ("renameCollection", "to"):
        if field not in spec:
            message = f"renameCollection requires {field}"
            raise server_error(message, code=40414, scope=ErrorScope.COMMAND)
        db_name, separator, coll_name = spec[field].partition(".")
        if not db_name or not separator or not coll_name or "\x00" in spec[field]:
            message = f"Invalid namespace in {field}"
            raise server_error(message, code=73, scope=ErrorScope.COMMAND)
        namespaces.append((db_name, coll_name))
    return *namespaces, spec.get("dropTarget", False)


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


def legacy_command_truth_value(value):
    """BSON trueValue for legacy flags; distinct from typed boolean fields."""
    if value is None or isinstance(value, UndefinedType):
        return False
    if isinstance(value, bool):
        return value
    internal = DocumentCodec.to_internal(value)
    numeric = wrap_bson_numeric(internal)
    if numeric is not None:
        return bool(numeric.value)
    return True


def command_boolean(value, field, *, numeric=False):
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if numeric:
        internal = DocumentCodec.to_internal(value)
        wrapped = wrap_bson_numeric(internal)
        if wrapped is not None:
            return bool(wrapped.value)
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
