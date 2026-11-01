"""UUID request ownership, physical bytes and independent response policies."""

# ruff: noqa: B023 - each witness is observed before advancing the matrix

from bson import Binary

from tests.differential.semantic_bson_admission_cases import UUID_VALUE, _observe, _view
from tests.differential.semantic_bson_dispatch_cases import _options


SEED = [{"_id": 0, "v": 1}]
MODES = (
    "default",
    "unspecified",
    "standard",
    "pythonLegacy",
    "javaLegacy",
    "csharpLegacy",
)
PAYLOAD = UUID_VALUE.bytes


def _selected(collection, mode):
    return (
        collection if mode == "default" else _options(collection, representation=mode)
    )


def _reset(collection):
    collection.drop()
    collection.insert_many(SEED)


def uuid_request_owners(collection):
    output = {}
    for mode in MODES:
        selected = _selected(collection, mode)
        database = collection.database.with_options(
            codec_options=selected.codec_options
        )
        for route in (
            "insert-id",
            "insert-nested",
            "filter",
            "update",
            "pipeline",
            "command-default",
            "command-explicit",
        ):
            _reset(collection)
            command = {
                "insert": collection.name,
                "documents": [{"_id": 2, "v": UUID_VALUE}],
            }
            operations = {
                "insert-id": lambda: selected.insert_one({"_id": UUID_VALUE}),
                "insert-nested": lambda: selected.insert_one(
                    {"_id": 1, "v": {"u": [UUID_VALUE]}}
                ),
                "filter": lambda: selected.find({"v": UUID_VALUE}),
                "update": lambda: selected.update_one(
                    {"_id": 0}, {"$set": {"v": UUID_VALUE}}
                ),
                "pipeline": lambda: selected.aggregate(
                    [{"$project": {"v": {"$literal": UUID_VALUE}}}]
                ),
                "command-default": lambda: database.command(command),
                "command-explicit": lambda: database.command(
                    command, codec_options=selected.codec_options
                ),
            }
            output[f"{mode}/{route}"] = _observe(
                collection, operations[route], cursor=route in {"filter", "pipeline"}
            )
    return output


def uuid_write_read_matrix(collection):
    output = {}
    for writer_mode in MODES:
        _reset(collection)
        writer = _selected(collection, writer_mode)
        rows = [
            {"_id": 1, "u": Binary(PAYLOAD, 3), "nested": [Binary(PAYLOAD, 3)]},
            {"_id": 2, "u": Binary(PAYLOAD, 4), "nested": [Binary(PAYLOAD, 4)]},
        ]
        if writer_mode not in {"default", "unspecified"}:
            rows.append({"_id": 3, "u": UUID_VALUE, "nested": [UUID_VALUE]})
        writer.insert_many(rows)
        physical = _view(
            list(_options(collection, representation="unspecified").find().sort("_id"))
        )
        for reader_mode in MODES:
            reader = _selected(collection, reader_mode)
            database = collection.database.with_options(
                codec_options=reader.codec_options
            )
            command = {"find": collection.name, "sort": {"_id": 1}}
            for route in ("find", "aggregate", "command-default", "command-explicit"):
                operations = {
                    "find": lambda: list(reader.find().sort("_id")),
                    "aggregate": lambda: list(
                        reader.aggregate([{"$sort": {"_id": 1}}])
                    ),
                    # Observe rows; complete response DTOs have their own unit.
                    "command-default": lambda: database.command(command)["cursor"][
                        "firstBatch"
                    ],
                    "command-explicit": lambda: database.command(
                        command, codec_options=reader.codec_options
                    )["cursor"]["firstBatch"],
                }
                observed = _observe(collection, operations[route])
                observed["physical"] = physical
                output[f"{writer_mode}/{reader_mode}/{route}"] = observed
    return output


def _ordered_distinct(collection, key):
    # Distinct promises values, not their iteration order. Preserve BSON types
    # and complete values; only stabilize the outer result array for replay.
    return sorted(collection.distinct(key), key=lambda value: repr(_view(value)))


def uuid_distinct_presence(collection):
    output = {}
    for label, key, rows in (
        ("root-missing", "u", [{"_id": 1}, {"_id": 2}]),
        ("root-null", "u", [{"_id": 1}, {"_id": 2, "u": None}]),
        (
            "root-arrays",
            "u",
            [{"_id": 1}, {"_id": 2, "u": []}, {"_id": 3, "u": [None, 1]}],
        ),
        (
            "dotted-missing",
            "o.u",
            [{"_id": 1}, {"_id": 2, "o": {}}, {"_id": 3, "o": None}],
        ),
        ("dotted-null", "o.u", [{"_id": 1}, {"_id": 2, "o": {"u": None}}]),
        ("array-missing", "o.u", [{"_id": 1, "o": [{}, {"other": 1}]}]),
        ("array-null", "o.u", [{"_id": 1, "o": [{}, {"u": None}, {"u": 1}]}]),
    ):
        collection.drop()
        collection.insert_many(rows)
        output[label] = _observe(collection, lambda: _ordered_distinct(collection, key))
        output[label + "/command"] = _observe(
            collection,
            lambda: sorted(
                collection.database.command({"distinct": collection.name, "key": key})[
                    "values"
                ],
                key=lambda value: repr(_view(value)),
            ),
        )
    return output


def uuid_filter_identity(collection):
    output = {}
    for mode in ("standard", "pythonLegacy", "javaLegacy", "csharpLegacy"):
        _reset(collection)
        selected = _selected(collection, mode)
        selected.insert_many(
            [
                {"_id": UUID_VALUE, "u": UUID_VALUE},
                {"_id": 1, "u": Binary(PAYLOAD, 4)},
                {"_id": 2, "u": Binary(PAYLOAD, 3)},
            ]
        )
        output[f"{mode}/id"] = _observe(
            collection, lambda: list(selected.find({"_id": UUID_VALUE}))
        )
        output[f"{mode}/field"] = _observe(
            collection, lambda: list(selected.find({"u": UUID_VALUE}).sort("_id"))
        )
        output[f"{mode}/in"] = _observe(
            collection,
            lambda: list(selected.find({"u": {"$in": [UUID_VALUE]}}).sort("_id")),
        )
        output[f"{mode}/distinct"] = _observe(
            collection, lambda: _ordered_distinct(selected, "u")
        )
    return output


SEMANTIC_BSON_UUID_POLICY_CASES = (
    ("semantic_uuid_request_owners", uuid_request_owners),
    ("semantic_uuid_write_read_matrix", uuid_write_read_matrix),
    ("semantic_uuid_filter_identity", uuid_filter_identity),
    ("semantic_uuid_distinct_presence", uuid_distinct_presence),
)
