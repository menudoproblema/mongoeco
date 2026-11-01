"""Client encoding admission, execution phase and state against a live server."""


# ruff: noqa: B023 - each operation closure is invoked before the next loop iteration

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from bson import Binary, Code, DBRef, Decimal128
from bson.binary import UuidRepresentation
from pymongo import InsertOne, UpdateOne


SEED = [{"_id": 0, "v": 1}]
UUID_VALUE = UUID("00112233-4455-6677-8899-aabbccddeeff")
VALUES = (
    ("raw-decimal", lambda: Decimal("1.25")),
    ("object", object),
    ("set", lambda: {1, 2}),
    ("frozenset", lambda: frozenset({1})),
    ("nested-object", lambda: {"x": [object()]}),
    ("nested-decimal", lambda: [1, {"x": Decimal("1.25")}]),
    ("nul-key", lambda: {"bad\x00key": 1}),
    ("integer-key", lambda: {1: "bad"}),
    ("overflow-int", lambda: 1 << 70),
    ("bytearray", lambda: bytearray(b"ab")),
    ("date", lambda: date(2026, 11, 1)),
    ("surrogate", lambda: "\ud800"),
    ("code-scope-decimal", lambda: Code("x", {"x": Decimal("1.25")})),
    ("dbref-object", lambda: DBRef("c", object())),
    ("uuid", lambda: UUID_VALUE),
    ("decimal128", lambda: Decimal128("1.25")),
    ("binary0", lambda: Binary(b"ab", 0)),
    ("binary5", lambda: Binary(b"ab", 5)),
    ("bytes", lambda: b"ab"),
    ("datetime", lambda: datetime(1970, 1, 1, 0, 0, 1, 1000, tzinfo=UTC)),
    ("tuple", lambda: (1, "a")),
    ("document", lambda: {"x": [True, None, "a", 1.5]}),
)


def _view(value):  # noqa: PLR0911 - closed observation of supported and rejected types
    if isinstance(value, Decimal128):
        return {"type": "decimal128", "bid": value.bid.hex()}
    if isinstance(value, Decimal):
        return {"type": "Decimal", "value": str(value)}
    if isinstance(value, Code):
        return {"type": "code", "value": str(value), "scope": _view(value.scope)}
    if isinstance(value, DBRef):
        return {"type": "dbref", "document": _view(value.as_doc())}
    if isinstance(value, UUID):
        return {"type": "uuid", "hex": value.hex}
    if isinstance(value, bytes):
        return {
            "type": "binary",
            "subtype": getattr(value, "subtype", 0),
            "hex": value.hex(),
        }
    if isinstance(value, dict):
        return {
            "type": "document",
            "fields": [[_view(k), _view(v)] for k, v in value.items()],
        }
    if isinstance(value, list | tuple):
        return {"type": type(value).__name__, "items": [_view(v) for v in value]}
    if isinstance(value, set | frozenset):
        return {"type": type(value).__name__, "items": sorted(value)}
    if isinstance(value, datetime | date):
        return {"type": type(value).__name__, "value": value.isoformat()}
    if isinstance(value, str):
        return {"type": "str", "utf8": value.encode("utf-8", "surrogatepass").hex()}
    if value is None or isinstance(value, int | float | bool):
        return {"type": type(value).__name__, "value": value}
    return {"type": type(value).__name__, "opaque": True}


def _reset(collection):
    # A permissive candidate can persist values that cannot even be read back.
    # Reset by DDL so one failed admission does not poison later witnesses.
    collection.drop()
    collection.insert_many(SEED)


def _observe(collection, call, *, cursor=False):
    phase = "invoke"
    result_cursor = None
    try:
        result = call()
        if cursor:
            result_cursor = result
            phase = "consume"
            result = list(result)
        if isinstance(result, dict) and isinstance(result.get("cursor"), dict):
            cursor_document = dict(result["cursor"])
            assert (
                cursor_document["ns"] == f"{collection.database.name}.{collection.name}"
            )
            cursor_document["ns"] = "<namespace>"
            result = {**result, "cursor": cursor_document}
        if (
            isinstance(result, dict | list | str | int | float | bytes)
            or result is None
        ):
            observed = {"ok": True, "result": _view(result)}
        else:
            observed = {"ok": True}
            for name in (
                "matched_count",
                "modified_count",
                "deleted_count",
                "inserted_id",
                "inserted_ids",
                "bulk_api_result",
            ):
                if hasattr(type(result), name):
                    observed[name] = _view(getattr(result, name))
    except Exception as error:
        observed = {
            "ok": False,
            "phase": phase,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
        }
    finally:
        if result_cursor is not None:
            result_cursor.close()
    try:
        observed["state"] = _view(list(collection.find().sort("_id")))
    except Exception as error:
        observed["state"] = {"exception": type(error).__name__}
    return observed


def admission_reads(collection):
    result = {}
    for label, factory in VALUES:
        for route in ("find", "find_one", "count", "distinct", "aggregate"):
            _reset(collection)
            value = factory()
            query = {"v": value}
            operations = {
                "find": lambda: collection.find(query),
                "find_one": lambda: collection.find_one(query),
                "count": lambda: collection.count_documents(query),
                "distinct": lambda: collection.distinct("v", query),
                "aggregate": lambda: collection.aggregate([{"$match": query}]),
            }
            result[f"{label}/{route}"] = _observe(
                collection, operations[route], cursor=route in {"find", "aggregate"}
            )
    return result


def admission_writes(collection):
    result = {}
    for label, factory in VALUES:
        for route in (
            "insert",
            "insert_many",
            "update",
            "update_many",
            "pipeline",
            "replace",
            "find_modify",
            "delete",
            "bulk_ordered",
            "bulk_unordered",
            "bulk_inserts",
            "bulk_updates",
        ):
            _reset(collection)
            value = factory()
            operations = {
                "insert": lambda: collection.insert_one({"_id": 1, "v": value}),
                "insert_many": lambda: collection.insert_many(
                    [{"_id": 1, "v": 1}, {"_id": 2, "v": value}]
                ),
                "update": lambda: collection.update_one(
                    {"_id": 0}, {"$set": {"v": value}}
                ),
                "update_many": lambda: collection.update_many(
                    {}, {"$set": {"v": value}}
                ),
                "pipeline": lambda: collection.update_one(
                    {}, [{"$set": {"v": {"$literal": value}}}]
                ),
                "replace": lambda: collection.replace_one(
                    {"_id": 0}, {"_id": 0, "v": value}
                ),
                "find_modify": lambda: collection.find_one_and_update(
                    {"_id": 0}, {"$set": {"v": value}}
                ),
                "delete": lambda: collection.delete_one({"v": value}),
                "bulk_inserts": lambda: collection.bulk_write(
                    [InsertOne({"_id": 1}), InsertOne({"_id": 2, "v": value})]
                ),
                "bulk_updates": lambda: collection.bulk_write(
                    [
                        UpdateOne({"_id": 0}, {"$set": {"first": True}}),
                        UpdateOne({"_id": 0}, {"$set": {"v": value}}),
                    ]
                ),
                "bulk_ordered": lambda: collection.bulk_write(
                    [
                        InsertOne({"_id": 1}),
                        UpdateOne({"_id": 0}, {"$set": {"v": value}}),
                    ]
                ),
                "bulk_unordered": lambda: collection.bulk_write(
                    [
                        InsertOne({"_id": 1}),
                        UpdateOne({"_id": 0}, {"$set": {"v": value}}),
                    ],
                    ordered=False,
                ),
            }
            result[f"{label}/{route}"] = _observe(collection, operations[route])
    return result


def admission_options(collection):
    result = {}
    for label, factory in VALUES:
        for route in (
            "projection",
            "find_let",
            "aggregate_let",
            "array_filters",
            "comment",
            "validator",
            "partial_index",
        ):
            _reset(collection)
            value = factory()
            options_collection = collection.database[
                collection.name + "_admission_options"
            ]
            options_collection.drop()
            operations = {
                "projection": lambda: collection.find({}, {"v": value}),
                "find_let": lambda: collection.find({}, let={"x": value}),
                "aggregate_let": lambda: collection.aggregate([], let={"x": value}),
                "array_filters": lambda: collection.update_one(
                    {}, {"$set": {"a.$[x]": 1}}, array_filters=[{"x": value}]
                ),
                "comment": lambda: collection.find({}, comment=value),
                "validator": lambda: collection.database.create_collection(
                    options_collection.name, validator={"v": value}
                ),
                "partial_index": lambda: options_collection.create_index(
                    "v", partialFilterExpression={"v": value}
                ),
            }
            result[f"{label}/{route}"] = _observe(
                collection,
                operations[route],
                cursor=route in {"projection", "find_let", "aggregate_let", "comment"},
            )
            result[f"{label}/{route}"]["aux_exists"] = bool(
                list(
                    collection.database.list_collections(
                        filter={"name": options_collection.name}
                    )
                )
            )
            options_collection.drop()
    return result


def admission_commands(collection):
    result = {}
    for label, factory in VALUES:
        for route in ("find", "insert", "update", "aggregate"):
            _reset(collection)
            value = factory()
            commands = {
                "find": {"find": collection.name, "filter": {"v": value}},
                "insert": {
                    "insert": collection.name,
                    "documents": [{"_id": 1}, {"_id": 2, "v": value}],
                },
                "update": {
                    "update": collection.name,
                    "updates": [{"q": {"_id": 0}, "u": {"$set": {"v": value}}}],
                },
                "aggregate": {
                    "aggregate": collection.name,
                    "pipeline": [{"$match": {"v": value}}],
                    "cursor": {},
                },
            }
            result[f"{label}/{route}"] = _observe(
                collection, lambda: collection.database.command(commands[route])
            )
    return result


def admission_uuid_modes(collection):
    result = {}
    for mode in (
        "default",
        "unspecified",
        "standard",
        "pythonLegacy",
        "javaLegacy",
        "csharpLegacy",
    ):
        selected = collection
        if mode != "default":
            options = collection.codec_options
            local = isinstance(options.type_registry, tuple)
            modes = {
                "unspecified": UuidRepresentation.UNSPECIFIED,
                "standard": UuidRepresentation.STANDARD,
                "pythonLegacy": UuidRepresentation.PYTHON_LEGACY,
                "javaLegacy": UuidRepresentation.JAVA_LEGACY,
                "csharpLegacy": UuidRepresentation.CSHARP_LEGACY,
            }
            selected = collection.with_options(
                codec_options=type(options)(
                    uuid_representation=mode if local else modes[mode]
                )
            )
        for route in ("insert", "filter", "update", "pipeline", "command"):
            _reset(collection)
            operations = {
                "insert": lambda: selected.insert_one({"_id": 1, "v": UUID_VALUE}),
                "filter": lambda: selected.find({"v": UUID_VALUE}),
                "update": lambda: selected.update_one({}, {"$set": {"v": UUID_VALUE}}),
                "pipeline": lambda: selected.aggregate(
                    [{"$project": {"v": {"$literal": UUID_VALUE}}}]
                ),
                "command": lambda: selected.database.command(
                    {"find": selected.name, "filter": {"v": UUID_VALUE}}
                ),
            }
            result[f"{mode}/{route}"] = _observe(
                selected, operations[route], cursor=route in {"filter", "pipeline"}
            )
    return result


def admission_cursor_lifecycle(collection):
    result = {}
    selected_values = {"raw-decimal", "set", "overflow-int", "uuid", "decimal128"}
    for label, factory in VALUES:
        if label not in selected_values:
            continue
        for route in (
            "close",
            "clone",
            "rewind",
            "explain",
            "invalid_to_valid",
            "valid_to_invalid",
        ):
            _reset(collection)
            value = factory()

            def invoke():
                query = {"v": 1 if route == "valid_to_invalid" else value}
                cursor = collection.find(query)
                if route == "close":
                    return cursor.close()
                if route == "clone":
                    cloned = cursor.clone()
                    cursor.close()
                    return cloned
                if route == "rewind":
                    return cursor.rewind()
                if route == "explain":
                    try:
                        cursor.explain()
                        return {"explain_encoded": True}
                    finally:
                        cursor.close()
                if route == "invalid_to_valid":
                    query["v"] = 1
                elif route == "valid_to_invalid":
                    query["v"] = value
                return cursor

            result[f"{label}/{route}"] = _observe(
                collection, invoke, cursor=route not in {"close", "explain"}
            )
    return result


SEMANTIC_BSON_ADMISSION_CASES = (
    ("semantic_admission_reads", admission_reads),
    ("semantic_admission_writes", admission_writes),
    ("semantic_admission_options", admission_options),
    ("semantic_admission_commands", admission_commands),
    ("semantic_admission_uuid_modes", admission_uuid_modes),
    ("semantic_admission_cursor_lifecycle", admission_cursor_lifecycle),
)
