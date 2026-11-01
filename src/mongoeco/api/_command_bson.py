"""Server BSON grammar shared by raw write command adapters."""

from mongoeco.core.bson_scalars import (
    INT64_MAX,
    INT64_MIN,
    _is_nan_numeric_value,
    bson_numeric_alias,
    compare_bson_numeric,
    truncate_bson_int64,
    wrap_bson_numeric,
)
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


def legacy_command_int64(value):
    """BSON safeNumberLong: truncate, saturate and map NaN to zero."""
    numeric = wrap_bson_numeric(value)
    if numeric is None:
        return None
    try:
        integer = int(numeric.value)
    except ValueError:
        integer = 0
    except OverflowError:
        integer = INT64_MAX if numeric.value > 0 else INT64_MIN
    return max(INT64_MIN, min(INT64_MAX, integer))


def command_number_long(value, field):
    integer = legacy_command_int64(value)
    if integer is None:
        raise command_type_error(value, field, ("long", "int", "decimal", "double"))
    return integer


def command_count_limit(value, *, dialect):
    numeric = wrap_bson_numeric(value)
    if numeric is None:
        message = "limit value is not a valid number"
        raise server_error(message, code=2, scope=ErrorScope.COMMAND)
    integer = truncate_bson_int64(value)
    if integer is None or compare_bson_numeric(value, integer) != 0:
        is_decimal = bson_numeric_alias(value) == "decimal"
        is_nan = _is_nan_numeric_value(numeric.value)
        display = "NaN" if is_decimal and is_nan else str(numeric.value)
        if not is_decimal and dialect != "9.0" and display in {"nan", "inf", "-inf"}:
            display += ".0"
        if not is_decimal and is_nan:
            message = f"Expected an integer, but found NaN in: limit: {display}"
        elif is_decimal or integer is None:
            message = f"Cannot represent as a 64-bit integer: limit: {display}"
        else:
            message = f"Expected an integer: limit: {display}"
        raise server_error(message, code=9, scope=ErrorScope.COMMAND)
    if integer == INT64_MIN:
        message = "limit value for count cannot be min long"
        raise server_error(message, code=2, scope=ErrorScope.COMMAND)
    return abs(integer)


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


def command_type_name(value):
    """Name a received BSON type with the existing predicates, only for diagnostics."""
    actual = bson_numeric_alias(value)
    if actual is None:
        actual = next(
            (
                alias
                for alias in (
                    "null",
                    "bool",
                    "string",
                    "object",
                    "array",
                    "binData",
                    "date",
                    "objectId",
                    "regex",
                    "timestamp",
                    "javascript",
                    "javascriptWithScope",
                    "minKey",
                    "maxKey",
                    "undefined",
                )
                if matches_bson_type(value, alias)
            ),
            type(value).__name__,
        )
    return actual


def command_type_error(value, field, expected):
    actual = command_type_name(value)
    expectation = (
        f"types '[{', '.join(expected)}]'"
        if isinstance(expected, tuple)
        else f"type '{expected}'"
    )
    return server_error(
        f"BSON field '{field}' is the wrong type '{actual}', expected {expectation}",
        code=14,
        scope=ErrorScope.COMMAND,
    )


def require_command_type(value, field, expected, *, required=False):
    if required and value is None:
        message = f"BSON field '{field}' is missing but a required field"
        raise server_error(message, code=40414, scope=ErrorScope.COMMAND)
    if not matches_bson_type(value, expected):
        raise command_type_error(value, field, expected)
    return value


def command_nonnegative(value, field, *, dialect, minimum=0):
    if value < minimum:
        message = (
            f"BSON field '{field}' value must be >= {minimum}, actual value '{value}'"
        )
        raise server_error(
            message, code=51024 if dialect == "7.0" else 2, scope=ErrorScope.COMMAND
        )
    return value


def validate_write_batch(value):
    # Empty BSON objects are accepted by the sequence reader as zero entries.
    if value is None:
        message = "'NoneType' object is not iterable"
        raise TypeError(message)
    if value in ({}, []):
        message = "Write batch sizes must be between 1 and 100000. Got 0 operations."
        raise server_error(message, code=16, scope=ErrorScope.COMMAND)
