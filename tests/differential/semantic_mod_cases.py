"""Native H47 witnesses: prepared int64 query mod, not aggregation arithmetic."""

from functools import partial

from bson import Decimal128, Int64
from pymongo.errors import OperationFailure

from tests.differential.semantic_expression_grammar_cases import _reset
from tests.differential.write_cases import WriteCase, WriteRequest
from tests.support_write_parity import observe_write_case


VALUES = [
    0,
    1,
    -1,
    5,
    -5,
    5.5,
    -5.5,
    5.999999999999,
    9007199254740993,
    Int64(-(2**63)),
    Int64(2**63 - 1),
    float(2**63),
    float(-(2**63)),
    1e30,
    -1e30,
    float("nan"),
    float("inf"),
    float("-inf"),
    Decimal128("5.5"),
    Decimal128("-5.5"),
    Decimal128("9223372036854775807"),
    Decimal128("9223372036854775808"),
    Decimal128("-9223372036854775809"),
    Decimal128("NaN"),
    Decimal128("Infinity"),
    True,
    "5",
    [5.5],
    [[5]],
    None,
    Decimal128("9223372036854775807.9"),
    Decimal128("-9223372036854775808.9"),
    Decimal128("-Infinity"),
]
SEED = [{"_id": i, "a": value, "o": {"a": value}} for i, value in enumerate(VALUES)] + [
    {"_id": 100}
]
ARGUMENTS = {
    "positive": [2, 1],
    "negative": [2, -1],
    "zero": [2, 0],
    "fractional": [2.5, 1.9],
    "negative_fractional": [-2.5, -1.9],
    "decimal": [Decimal128("2.5"), Decimal128("1.9")],
    "min": [Int64(-(2**63)), 0],
    "minus_one": [-1, 0],
    "decimal_max": [Decimal128("9223372036854775807.9"), 0],
    "decimal_min": [Decimal128("-9223372036854775808.9"), 0],
    "large_remainder": [2, Decimal128("9223372036854775807.9")],
    "near_remainder": [2, 1.0000000000001],
    "fractional_zero": [2, -0.9],
    "impossible_remainder": [2, 5],
}
INVALID = {
    "not_array": None,
    "empty": [],
    "short": [1],
    "long": [1, 2, 3],
    "bool_divisor": [True, 0],
    "bool_remainder": [2, True],
    "text_divisor": ["2", 0],
    "null_remainder": [2, None],
    "zero": [0, 0],
    "fractional_zero": [0.5, 0],
    "nan_divisor": [float("nan"), 0],
    "nan_remainder": [2, float("nan")],
    "inf_divisor": [float("inf"), 0],
    "inf_remainder": [2, float("inf")],
    "double_bound": [float(2**63), 0],
    "remainder_bound": [2, 1e30],
    "decimal_bound": [Decimal128("9223372036854775808"), 0],
    "decimal_nan": [Decimal128("NaN"), 0],
}


def reads(collection, *, surface):
    output = {}
    for path in ("a", "o.a"):
        for label, args in ARGUMENTS.items():
            query = {path: {"$mod": args}}
            if surface == "count":
                value = collection.count_documents(query)
            elif surface == "aggregate":
                value = [
                    row["_id"]
                    for row in collection.aggregate(
                        [{"$match": query}, {"$sort": {"_id": 1}}]
                    )
                ]
            else:
                value = [
                    row["_id"] for row in collection.find(query, sort=[("_id", 1)])
                ]
            output[path + "_" + label] = value
    return output


def sql_controls(collection):
    # Untagged scalar and heterogeneous values expose SQL rather than a global
    # Decimal128/array fallback; fractional doubles must still truncate.
    output = {}
    for shape, values in {
        "integer": [0, 1, -1, 5, -5, 2**53 + 1],
        "tagged": [
            Decimal128("5.5"),
            Decimal128("-5.5"),
            Decimal128("NaN"),
            Int64(2**63 - 1),
        ],
        "mixed": [0, 5.5, -5.5, True, "5", None],
    }.items():
        _reset(
            collection,
            [{"_id": i, "a": v, "o": {"a": v}} for i, v in enumerate(values)],
        )
        output[shape] = {
            label: sorted(row["_id"] for row in collection.find({"a": {"$mod": args}}))
            for label, args in ARGUMENTS.items()
        }
    return output


def preparation(collection):
    output = {}
    for populated in (False, True):
        _reset(collection, [{"_id": 1, "a": 5}] if populated else [])
        for label, args in INVALID.items():
            for route in ("find", "aggregate", "update"):
                query = {"a": {"$mod": args}}
                try:
                    if route == "find":
                        list(collection.find(query))
                    elif route == "aggregate":
                        list(collection.aggregate([{"$match": query}]))
                    else:
                        collection.update_many(query, {"$set": {"touched": True}})
                    result = {"ok": True}
                except OperationFailure as error:
                    result = {
                        "ok": False,
                        "code": error.code,
                        "message": error.details.get("errmsg"),
                    }
                output[f"{populated}_{label}_{route}"] = result
        output[f"{populated}_state"] = list(collection.find({}, sort=[("_id", 1)]))
        output[f"{populated}_recovery"] = collection.count_documents({})
    return output


def selection_writes(collection):
    cases = {
        "selection": WriteCase(
            "mod_selection",
            SEED,
            [
                WriteRequest(
                    "update_many",
                    [{"a": {"$mod": [2.5, 1.9]}}, {"$set": {"selected": True}}],
                ),
                WriteRequest("delete_many", [{"a": {"$mod": [2, -1]}}]),
            ],
        ),
        "pull": WriteCase(
            "mod_pull",
            [
                {"_id": 1, "a": [5.5, -5.5, True, Decimal128("5.5"), 2]},
                {"_id": 2, "a": [[5.5], 6]},
            ],
            [WriteRequest("update_many", [{}, {"$pull": {"a": {"$mod": [2, 1]}}}])],
        ),
    }
    return {
        label: observe_write_case(
            collection.database,
            collection.name + "_" + label,
            case,
            error_contract=True,
        )
        for label, case in cases.items()
    }


SEMANTIC_MOD_CASES = (
    *(
        (f"semantic_mod_{surface}", partial(reads, surface=surface))
        for surface in ("find", "count", "aggregate")
    ),
    ("semantic_mod_sql_controls", sql_controls),
    ("semantic_mod_preparation", preparation),
    ("semantic_mod_selection_writes", selection_writes),
)
