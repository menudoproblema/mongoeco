"""Anonymous command-error specimens with recovery after a unique-key collision."""

from datetime import UTC, datetime

from bson import Binary, Decimal128, Int64, ObjectId, Regex, Timestamp

from tests.differential.write_cases import WriteCase, WriteRequest


def specimens():
    cases = []
    values = {
        "integer": 123,
        "long": Int64(2**40),
        "double": 1.5,
        "document": {"nested": [1, 2]},
        "boolean": True,
        "escaped": 'a "quoted" value',
        "objectid": ObjectId("000000000000000000000001"),
        "datetime": datetime(2026, 1, 1, tzinfo=UTC),
        "decimal": Decimal128("1.25"),
        "binary": Binary(b"ab", 128),
        "regex": Regex("ab", "i"),
        "timestamp": Timestamp(2, 3),
    }
    for method in ("find_one_and_update", "find_one_and_replace"):
        for variant in (
            "secondary",
            "compound",
            "null",
            "multikey",
            "dotted",
            "upsert",
            "id_upsert",
            *values,
        ):
            first = {"_id": "a", "key": "old"}
            second = {"_id": "b", "key": "taken"}
            index = {"keys": [("key", 1)], "unique": True}
            replacement = {"key": "taken"}
            query = {"_id": "a"}
            options = {}
            if variant == "compound":
                first["group"] = second["group"] = "group"
                replacement["group"] = "group"
                index = {
                    "keys": [("group", 1), ("key", -1)],
                    "unique": True,
                    "name": "unique_group_key",
                }
            elif variant == "null":
                second["key"] = replacement["key"] = None
            elif variant == "multikey":
                replacement["key"] = ["unused", "taken"]
            elif variant == "dotted":
                first = {"_id": "a", "profile": {"key": "old"}}
                second = {"_id": "b", "profile": {"key": "taken"}}
                replacement = {"profile": {"key": "taken"}}
                index = {"keys": [("profile.key", 1)], "unique": True}
            elif variant == "upsert":
                query = {"_id": "missing"}
                options = {"upsert": True}
            elif variant == "id_upsert":
                query = {"_id": "a", "guard": "missing"}
                replacement = {"key": "free"}
                options = {"upsert": True}
            elif variant in values:
                second["key"] = replacement["key"] = values[variant]
            change = {"$set": replacement} if method.endswith("update") else replacement
            cases.append(
                WriteCase(
                    f"{method}_{variant}",
                    [first, second],
                    [
                        WriteRequest(method, [query, change], options),
                        WriteRequest("find_one", [{"_id": "b"}]),
                        WriteRequest(
                            "find_one_and_update",
                            [{"_id": "a"}, {"$set": {"recovered": True}}],
                        ),
                    ],
                    indexes=[index],
                )
            )
    return cases


FIND_MODIFY_CASES = specimens()
