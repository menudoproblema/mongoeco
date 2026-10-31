"""Public Binary witnesses for H48; observations do not use runtime comparators."""

from bson import Binary


SEED = [
    {"_id": 1, "v": b"abcdefghijklmnop"},
    {"_id": 2, "v": Binary(b"abcdefghijklmnop", 0)},
    {"_id": 3, "v": Binary(b"abcdefghijklmnop", 5)},
]


def selection(collection):
    output = {}
    for label, value in (("bytes", b"abcdefghijklmnop"), ("subtype0", Binary(b"abcdefghijklmnop", 0)), ("subtype5", Binary(b"abcdefghijklmnop", 5))):
        for operator in ("$eq", "$ne", "$in", "$nin", "$gt", "$gte", "$lt", "$lte"):
            operand = [value] if operator in {"$in", "$nin"} else value
            output[f"{label}_{operator}"] = [
                row["_id"]
                for row in collection.find({"v": {operator: operand}}, sort=[("_id", 1)])
            ]
        output[f"{label}_expr"] = [
            row["_id"]
            for row in collection.find({"$expr": {"$eq": ["$v", value]}}, sort=[("_id", 1)])
        ]
    return output


def distinct(collection):
    # Subtype zero is decoded as bytes by the native default codec. Compare
    # BSON content, not Python classes or cardinality, and ignore result order.
    return sorted(
        (getattr(value, "subtype", 0), bytes(value).hex())
        for value in collection.distinct("v")
    )


def assignments(collection):
    output = {}
    for label, update in (
        ("classic", {"$set": {"v": Binary(b"abcdefghijklmnop", 5)}}),
        ("pipeline", [{"$set": {"v": {"$literal": Binary(b"abcdefghijklmnop", 5)}}}]),
    ):
        result = collection.update_one({"_id": 3}, update)
        value = collection.find_one({"_id": 3})["v"]
        output[label] = {
            "matched": result.matched_count,
            "modified": result.modified_count,
            "subtype": getattr(value, "subtype", 0),
            "bytes": bytes(value).hex(),
        }
    return output


SEMANTIC_BINARY_CASES = (
    ("semantic_binary_selection", selection),
    ("semantic_binary_distinct", distinct),
    ("semantic_binary_assignments", assignments),
)
