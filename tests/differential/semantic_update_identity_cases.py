"""Physical modification identity, observed through public native writes."""

from copy import deepcopy
from functools import partial
from struct import unpack

from bson import BSON, Binary, Code, DBRef, Decimal128, Int64


SEED = [{"_id": "initial"}]
SCENARIOS = (
    ("scope_order", Code("x", {"a": 1, "b": 2}), Code("x", {"b": 2, "a": 1})),
    ("scope_numeric_type", Code("x", {"v": 1}), Code("x", {"v": 1.0})),
    (
        "scope_nested",
        Code("x", {"v": [{"a": 1, "b": 2}]}),
        Code("x", {"v": [{"b": 2, "a": 1}]}),
    ),
    ("scope_none_empty", Code("x"), Code("x", {})),
    ("scope_empty_none", Code("x", {}), Code("x")),
    ("scope_noop", Code("x", {"v": [1.0]}), Code("x", {"v": [1.0]})),
    ("binary_noop", Binary(b"abcdefghijklmnop", 5), Binary(b"abcdefghijklmnop", 5)),
    ("binary_zero_alias", b"abcdefghijklmnop", Binary(b"abcdefghijklmnop", 0)),
    ("numeric_type", 1, 1.0),
    ("numeric_width", 1, Int64(1)),
    ("signed_zero", 0.0, -0.0),
    ("float_noop", 1.0, 1.0),
    ("nan_payload", float("nan"), unpack("<d", bytes.fromhex("010000000000f87f"))[0]),
    ("decimal_cohort", Decimal128("1.0"), Decimal128("1.00")),
    ("decimal_signed_zero", Decimal128("0"), Decimal128("-0")),
    ("dbref_alias", DBRef("c", 1), {"$ref": "c", "$id": 1}),
    ("dbref_numeric_type", DBRef("c", 1), DBRef("c", 1.0)),
)


def assignments(collection, *, mode):
    output = {}
    for name, before, after in SCENARIOS:
        collection.delete_many({})
        collection.insert_one({"_id": name, "v": deepcopy(before)})
        if mode == "replacement":
            result = collection.replace_one(
                {"_id": name}, {"_id": name, "v": deepcopy(after)}
            )
        else:
            update = (
                {"$set": {"v": deepcopy(after)}}
                if mode == "classic"
                else [{"$set": {"v": {"$literal": deepcopy(after)}}}]
            )
            result = collection.update_one({"_id": name}, update)
        output[name] = {
            "matched": result.matched_count,
            "modified": result.modified_count,
            # Physical BSON, not SDK decoder class identity: bytes and Binary
            # subtype zero encode identically. Numeric width and scope order
            # remain observable without any production comparison helper.
            "document_bson": BSON.encode(collection.find_one({"_id": name})).hex(),
        }
    return output


SEMANTIC_UPDATE_IDENTITY_CASES = tuple(
    (f"semantic_update_identity_{mode}", partial(assignments, mode=mode))
    for mode in ("classic", "pipeline", "replacement")
)
