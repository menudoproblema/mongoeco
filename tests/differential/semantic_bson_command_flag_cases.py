"""Numeric command boolean controls retain Decimal128 zero semantics."""

# ruff: noqa: B023 - each witness is evaluated before the next iteration

from bson import Decimal128

from tests.differential.semantic_bson_admission_cases import _observe


SEED = [{"_id": 0, "v": 1}]


def command_numeric_booleans(collection):
    output = {}
    for label, value in (
        ("zero", Decimal128("0")),
        ("negative-zero", Decimal128("-0")),
        ("one", Decimal128("1")),
        ("nan", Decimal128("NaN")),
        ("int-zero", 0),
        ("double-zero", 0.0),
    ):
        for route in ("upsert", "ordered", "new"):
            collection.drop()
            collection.insert_many(SEED)

            def invoke():
                if route == "new":
                    return collection.database.command(
                        {
                            "findAndModify": collection.name,
                            "query": {"_id": 0},
                            "update": {"$set": {"v": 2}},
                            "new": value,
                        },
                    )
                updates = [
                    {
                        "q": {"_id": 2},
                        "u": {"$set": {"v": 2}},
                        "upsert": value if route == "upsert" else True,
                    }
                ]
                if route == "ordered":
                    updates.insert(0, {"q": {"_id": 0}, "u": {"$set": {"_id": 99}}})
                command = {"update": collection.name, "updates": updates}
                if route == "ordered":
                    command["ordered"] = value
                return collection.database.command(command)

            output[f"{label}/{route}"] = _observe(collection, invoke)
    return output


SEMANTIC_BSON_COMMAND_FLAG_CASES = (
    ("semantic_command_numeric_booleans", command_numeric_booleans),
)
