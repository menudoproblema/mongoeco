"""Independent P2 raw-command witnesses; isolated state per retained shape."""

from bson import Decimal128


SEED = [{"_id": 0}]
COMMANDS = [
    {"createIndexes": "events", "indexes": ()},
    {"distinct": "events", "key": 1},
    {"dropIndexes": "events", "index": 1.5},
    {"listDatabases": 1, "nameOnly": 1},
    {"listCollections": 1, "nameOnly": 1},
    {"listCollections": 1, "authorizedCollections": 1},
    {"count": "events", "skip": -1},
    {"count": "events", "limit": -1},
    {"dbStats": 1, "scale": 0},
    {"collStats": "events", "scale": -1},
    {"insert": "events", "documents": {}},
    {"update": "events", "updates": {}},
    {"delete": "events", "deletes": {}},
    {"find": "events", "batchSize": -1},
    {"aggregate": "events", "pipeline": [], "cursor": 1},
    {"aggregate": "events", "pipeline": [], "allowDiskUse": 1},
    {"aggregate": "events", "pipeline": [], "let": 1},
    {"explain": 1},
    {"explain": {"find": "events"}, "verbosity": 1},
    {"connectionStatus": 1, "showPrivileges": 1},
    {"find": "events", "projection": 1},
    {"find": "events", "sort": 1},
    {"count": "events", "query": 1},
    {"distinct": "events", "key": ""},
    {"listCollections": 1, "comment": 1},
    {"listIndexes": "events", "comment": 1},
    {"validate": "events", "comment": 1},
    {"validate": "events", "background": "yes"},
    {
        "findAndModify": "events",
        "query": {},
        "update": {"$set": {"done": True}},
        "let": 1,
    },
    {"findAndModify": "events"},
    {"explain": {"distinct": "events", "key": ""}},
    {"explain": {"update": "events", "updates": []}},
]


CONTROLS = [
    (
        "find-valid",
        {
            "find": "events",
            "projection": {"_id": 1},
            "sort": {"_id": 1},
            "batchSize": 1,
        },
    ),
    ("distinct-valid", {"distinct": "events", "key": "v"}),
    ("drop-valid", {"dropIndexes": "events", "index": "*"}),
    (
        "list-valid",
        {"listCollections": 1, "nameOnly": True, "authorizedCollections": False},
    ),
    ("databases-admin-bool", {"listDatabases": 1, "nameOnly": True}),
    ("databases-admin-number", {"listDatabases": 1, "nameOnly": 1}),
    ("count-valid", {"count": "events", "query": {}, "skip": 0, "limit": 1}),
    ("dbstats-valid", {"dbStats": 1, "scale": 1}),
    ("collstats-valid", {"collStats": "events", "scale": 1}),
    ("insert-valid", {"insert": "events", "documents": [{"_id": 2, "v": 2}]}),
    (
        "update-valid",
        {"update": "events", "updates": [{"q": {"_id": 1}, "u": {"$set": {"v": 2}}}]},
    ),
    ("delete-valid", {"delete": "events", "deletes": [{"q": {}, "limit": 1}]}),
    (
        "aggregate-valid",
        {
            "aggregate": "events",
            "pipeline": [],
            "cursor": {},
            "let": {},
            "allowDiskUse": False,
        },
    ),
    ("explain-valid", {"explain": {"find": "events"}, "verbosity": "queryPlanner"}),
    ("privileges-valid", {"connectionStatus": 1, "showPrivileges": False}),
    ("find-multiple", {"find": "events", "projection": 1, "sort": 1, "batchSize": -1}),
    (
        "aggregate-multiple",
        {"aggregate": "events", "pipeline": [], "cursor": 1, "allowDiskUse": 1},
    ),
    (
        "modify-valid",
        {
            "findAndModify": "events",
            "query": {"_id": 1},
            "update": {"$set": {"v": 2}},
            "let": {},
        },
    ),
    ("projection-null", {"find": "events", "projection": None}),
    ("sort-null", {"find": "events", "sort": None}),
    ("query-null", {"count": "events", "query": None}),
    ("key-null", {"distinct": "events", "key": None}),
    ("nameOnly-null", {"listCollections": 1, "nameOnly": None}),
    ("privileges-null", {"connectionStatus": 1, "showPrivileges": None}),
    ("dbscale-null", {"dbStats": 1, "scale": None}),
    ("collscale-null", {"collStats": "events", "scale": None}),
    ("batch-null", {"find": "events", "batchSize": None}),
    ("cursor-null", {"aggregate": "events", "pipeline": [], "cursor": None}),
    (
        "disk-null",
        {"aggregate": "events", "pipeline": [], "cursor": {}, "allowDiskUse": None},
    ),
    ("let-null", {"aggregate": "events", "pipeline": [], "cursor": {}, "let": None}),
    ("explain-null", {"explain": None}),
    ("explain-empty", {"explain": {}}),
    ("verbosity-null", {"explain": {"find": "events"}, "verbosity": None}),
    ("insert-null", {"insert": "events", "documents": None}),
    ("update-null", {"update": "events", "updates": None}),
    ("delete-null", {"delete": "events", "deletes": None}),
    (
        "modify-null",
        {"findAndModify": "events", "update": {"$set": {"v": 2}}, "let": None},
    ),
]


_BATCH_TYPES = (
    ("string", "invalid"),
    ("bool", True),
    ("double", 1.5),
    ("negative-fraction", -0.5),
    ("negative-double", -1.5),
    ("nan", float("nan")),
    ("infinity", float("inf")),
    ("negative-infinity", float("-inf")),
    ("overflow", float(2**63)),
    ("decimal", Decimal128("1.5")),
    ("decimal-nan", Decimal128("NaN")),
    ("decimal-snan", Decimal128("sNaN")),
    ("decimal-infinity", Decimal128("Infinity")),
    ("decimal-negative-infinity", Decimal128("-Infinity")),
    ("decimal-overflow", Decimal128("1E100")),
)
CONTROLS += [
    (f"{command}-batch-{label}", spec)
    for label, value in _BATCH_TYPES
    for command, spec in (
        ("find", {"find": "events", "batchSize": value}),
        (
            "cursor",
            {"aggregate": "events", "pipeline": [], "cursor": {"batchSize": value}},
        ),
    )
]


_BOUND_TYPES = (
    *_BATCH_TYPES,
    ("null", None),
    ("min-long", -(2**63)),
    ("integral-double", 1.0),
    ("integral-decimal", Decimal128("1.0")),
)
CONTROLS += [
    (f"{command}-{field}-{label}", {command: "events", field: value})
    for command in ("find", "count")
    for field in ("skip", "limit")
    for label, value in _BOUND_TYPES
]
CONTROLS += [
    ("aggregate-cursor-required", {"aggregate": "events", "pipeline": []}),
    (
        "explain-aggregate-no-cursor",
        {"explain": {"aggregate": "events", "pipeline": []}},
    ),
]


def command_shape_contract(collection):
    database = collection.database
    events = database.get_collection("events")
    output = {}
    for state in ("absent", "empty", "populated"):
        cases = [(str(i), spec) for i, spec in enumerate(COMMANDS)] + CONTROLS
        for index, spec in cases:
            database.drop_collection("events")
            if state != "absent":
                database.create_collection("events")
            if state == "populated":
                events.insert_one({"_id": 1, "v": 1})
            phase = "invoke"
            try:
                if index.startswith("databases-admin"):
                    # SDK and local sync databases retain the owning client;
                    # the existing async replay view exposes it as .client.
                    try:
                        client = database._client
                    except AttributeError:
                        client = database.client
                    target = client["admin"]
                else:
                    target = database
                target.command(spec)
                result = {"ok": True}
            except Exception as error:
                details = getattr(error, "details", None) or {}
                message = details.get("errmsg", str(error))
                # Native IDL printers differ in closing punctuation for this list.
                # Retain every expected type; only repair the bracket/quote order.
                if "dropIndexes.index" in message and message.endswith("']"):
                    message = message[:-2] + "]'"
                if (
                    any(
                        f".{field}'" in message
                        for field in ("batchSize", "skip", "limit")
                    )
                    and "expected types '[" in message
                ):
                    prefix, aliases = message.rsplit("expected types '[", 1)
                    # IDL type-set order changes with native hash-table seeds.
                    # Retain all aliases, including their multiplicities.
                    aliases = aliases[:-2].split(", ")
                    message = (
                        prefix + "expected types '[" + ", ".join(sorted(aliases)) + "]'"
                    )
                result = {
                    "ok": False,
                    "family": type(error).__name__,
                    "code": getattr(error, "code", None),
                    "code_name": details.get(
                        "codeName", getattr(error, "code_name", None)
                    ),
                    "message": message.replace(
                        database.name + ".events", "<db>.events"
                    ),
                    "phase": phase,
                }
            exists = "events" in database.list_collection_names()
            result["state"] = {
                "exists": exists,
                "rows": list(
                    events.find({}, {"_id": 1, "v": 1, "done": 1}).sort("_id")
                ),
                "indexes": sorted(item["name"] for item in events.list_indexes())
                if exists
                else [],
            }
            result["recovery"] = database.command({"ping": 1})["ok"]
            output[f"{state}/{index}"] = result
    database.drop_collection("events")
    return output


SEMANTIC_COMMAND_SHAPE_CASES = (
    ("semantic_command_shape_contract", command_shape_contract),
)
