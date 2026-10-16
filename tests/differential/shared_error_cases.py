"""Anonymous negative cases spanning command and per-item error boundaries."""

from bson import Binary, Code, Int64, MaxKey, MinKey
from pymongo import InsertOne, UpdateOne

from tests.differential.shared_error_mirror_cases import MIRROR_ERROR_CASES
from tests.differential.write_cases import WriteCase, WriteRequest


def recovery():
    return WriteRequest("update_one", [{"_id": "a"}, {"$set": {"recovered": True}}])


def specimens():  # noqa: PLR0912 - explicit finite differential inventory
    cases = []
    seed = [{"_id": "a", "items": [1]}, {"_id": "b", "items": [2]}]
    updates = {
        "variable": ([{"$set": {"key": "$$missing"}}], {}),
        "divide": ([{"$set": {"key": {"$divide": [1, 0]}}}], {}),
        "mod": ([{"$set": {"key": {"$mod": [1, 0]}}}], {}),
        "filters": (
            {"$set": {"items.$[i]": 3}},
            {"array_filters": [{"i": 1}, {"i": 2}]},
        ),
    }
    for variant, (change, options) in updates.items():
        for method in ("find_one_and_update", "update_one", "update_many"):
            cases.append(  # noqa: PERF401 - keep specimens explicit
                WriteCase(
                    f"{method}_{variant}",
                    seed,
                    [WriteRequest(method, [{"_id": "b"}, change], options), recovery()],
                )
            )
        for ordered in (True, False):
            models = [
                UpdateOne({"_id": "a"}, {"$set": {"before": 1}}),
                UpdateOne({"_id": "b"}, change, **options),
                UpdateOne({"_id": "a"}, {"$set": {"after": 1}}),
            ]
            cases.append(
                WriteCase(
                    f"bulk_{variant}_{ordered}",
                    seed,
                    [
                        WriteRequest("bulk_write", [models], {"ordered": ordered}),
                        recovery(),
                    ],
                )
            )
        command_options = {"arrayFilters": options["array_filters"]} if options else {}
        cases.append(
            WriteCase(
                f"command_{variant}",
                seed,
                [
                    WriteRequest(
                        "command",
                        [
                            {
                                "update": "items",
                                "updates": [
                                    {"q": {"_id": "b"}, "u": change, **command_options}
                                ],
                            }
                        ],
                    ),
                    recovery(),
                ],
            )
        )
    for name, command in {
        "bad_update": {"update": "items", "updates": [{"q": {}, "u": 7}]},
        "bad_write_concern": {
            "insert": "items",
            "documents": [{"_id": "c"}],
            "writeConcern": {"j": "bad"},
        },
        "sort": {
            "update": "items",
            "updates": [{"q": {}, "u": {"$set": {"sorted": True}}, "sort": {"_id": 1}}],
        },
    }.items():
        cases.append(
            WriteCase(
                "command_" + name,
                seed,
                [WriteRequest("command", [command]), recovery()],
            )
        )
    cases.append(
        WriteCase(
            "aggregate_variable",
            seed,
            [WriteRequest("aggregate", [[{"$set": {"key": "$$missing"}}]]), recovery()],
        )
    )
    for ordered in (True, False):
        cases.append(  # noqa: PERF401 - keep specimens explicit
            WriteCase(
                f"insert_many_{ordered}",
                seed,
                [
                    WriteRequest(
                        "insert_many",
                        [[{"_id": "c"}, {"_id": "a"}, {"_id": "d"}]],
                        {"ordered": ordered},
                    ),
                    recovery(),
                ],
            )
        )
    for ordered in (True, False):
        cases.append(  # noqa: PERF401 - keep specimens explicit
            WriteCase(
                f"bulk_insert_{ordered}",
                seed,
                [
                    WriteRequest(
                        "bulk_write",
                        [
                            [
                                InsertOne({"_id": "c"}),
                                InsertOne({"_id": "a"}),
                                InsertOne({"_id": "d"}),
                            ]
                        ],
                        {"ordered": ordered},
                    ),
                    recovery(),
                ],
            )
        )
    values = {
        "bytes": b"ab",
        "min": MinKey(),
        "max": MaxKey(),
        "code": Code("x"),
        "code_scope": Code("x", {"a": 1}),
        "code_empty_scope": Code("x", {}),
        "code_long": Code("x" * 90),
        "empty_array": [],
        "empty_document": {},
        "string_long": "a" * 170,
        "binary_long": Binary(b"a" * 90, 128),
        "uuid": Binary(bytes(range(16)), 4),
    }
    for name, value in values.items():
        for method in ("find_one_and_update", "find_one_and_replace"):
            change = {"key": value}
            if method.endswith("update"):
                change = {"$set": change}
            cases.append(
                WriteCase(
                    f"{method}_{name}",
                    [
                        {"_id": "a", "key": "old"},
                        {"_id": "b", "key": value},
                    ],
                    [WriteRequest(method, [{"_id": "a"}, change]), recovery()],
                    indexes=[{"keys": [("key", 1)], "unique": True}],
                )
            )
    for name, value in {"code": Code("x"), "code_scope": Code("x", {"a": 1})}.items():
        cases.append(
            WriteCase(
                f"index_{name}_valid",
                [{"_id": "a", "key": "x"}],
                [
                    WriteRequest("insert_one", [{"_id": "b", "key": value}]),
                    WriteRequest("find_one", [{"key": value}]),
                    WriteRequest("count_documents", [{"key": {"$in": [value]}}]),
                    WriteRequest(
                        "update_one",
                        [{"_id": "a"}, {"$set": {"key": Code("x", {"a": 2})}}],
                    ),
                    recovery(),
                ],
                indexes=[{"keys": [("key", 1)], "unique": True}],
            )
        )
    cases.append(
        WriteCase(
            "code_scope_grouping",
            [
                {"_id": "a", "key": "x"},
                {"_id": "b", "key": Code("x")},
                {"_id": "c", "key": Code("x", {"a": 1})},
                {"_id": "d", "key": Code("x", {"a": 2})},
                {"_id": "e", "key": Code("x", {"a": 1})},
            ],
            [
                WriteRequest(
                    "aggregate",
                    [
                        [
                            {"$group": {"_id": "$key", "n": {"$sum": 1}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ],
                ),
                recovery(),
            ],
        )
    )
    cases.append(
        WriteCase(
            "code_scope_numeric_key",
            [{"_id": "a", "key": Code("x", {"a": 1})}],
            [
                WriteRequest(
                    "insert_one", [{"_id": "b", "key": Code("x", {"a": Int64(1)})}]
                ),
                WriteRequest("find_one", [{"key": Code("x", {"a": 1})}]),
                recovery(),
            ],
            indexes=[{"keys": [("key", 1)], "unique": True}],
        )
    )
    collation_options = [
        {"locale": "en", "strength": strength, **extra}
        for strength in (1, 2, 3)
        for extra in ({}, {"caseLevel": True}, {"numericOrdering": True})
    ]
    for number, options in enumerate(collation_options):
        value = "a0012" if options.get("numericOrdering") else "Taken"
        for method in ("find_one_and_update", "find_one_and_replace"):
            change = {"key": value}
            cases.append(
                WriteCase(
                    f"{method}_collation_{number}",
                    [
                        {"_id": "a", "key": "old"},
                        {"_id": "b", "key": value},
                    ],
                    [
                        WriteRequest(
                            method,
                            [
                                {"_id": "a"},
                                {"$set": change}
                                if method.endswith("update")
                                else change,
                            ],
                        ),
                        recovery(),
                    ],
                    indexes=[
                        {"keys": [("key", 1)], "unique": True, "collation": options}
                    ],
                )
            )
    for number, value in enumerate(["café", "æ", "ß", "\u03b1β", "\u0430\u0431", "中"]):
        for method in ("find_one_and_update", "find_one_and_replace"):
            change = {"key": value}
            cases.append(
                WriteCase(
                    f"{method}_unicode_collation_{number}",
                    [
                        {"_id": "a", "key": "old"},
                        {"_id": "b", "key": value},
                    ],
                    [
                        WriteRequest(
                            method,
                            [
                                {"_id": "a"},
                                {"$set": change}
                                if method.endswith("update")
                                else change,
                            ],
                        ),
                        recovery(),
                    ],
                    indexes=[
                        {
                            "keys": [("key", 1)],
                            "unique": True,
                            "collation": {"locale": "en", "strength": 2},
                        }
                    ],
                )
            )
    for method in ("find_one_and_update", "find_one_and_replace"):
        change = {"group": 1, "key": "TAKEN"}
        cases.append(
            WriteCase(
                f"{method}_compound_collation",
                [
                    {"_id": "a", "group": 1, "key": "old"},
                    {"_id": "b", "group": 1, "key": "taken"},
                ],
                [
                    WriteRequest(
                        method,
                        [
                            {"_id": "a"},
                            {"$set": change} if method.endswith("update") else change,
                        ],
                    ),
                    recovery(),
                ],
                indexes=[
                    {
                        "keys": [("group", 1), ("key", 1)],
                        "unique": True,
                        "collation": {"locale": "en", "strength": 2},
                    }
                ],
            )
        )
    for name, value in {
        "document": {"nested": "TAKEN"},
        "number": 12,
        "empty_array": [],
        "long": "a" * 170,
        "code_scope": Code("x", {"a": 1}),
    }.items():
        for method in ("find_one_and_update", "find_one_and_replace"):
            change = {"key": value}
            cases.append(
                WriteCase(
                    f"{method}_collation_value_{name}",
                    [
                        {"_id": "a", "key": "old"},
                        {"_id": "b", "key": value},
                    ],
                    [
                        WriteRequest(
                            method,
                            [
                                {"_id": "a"},
                                {"$set": change}
                                if method.endswith("update")
                                else change,
                            ],
                        ),
                        recovery(),
                    ],
                    indexes=[
                        {
                            "keys": [("key", 1)],
                            "unique": True,
                            "collation": {"locale": "en", "strength": 2},
                        }
                    ],
                )
            )
    return cases


SHARED_ERROR_CASES = specimens() + MIRROR_ERROR_CASES
