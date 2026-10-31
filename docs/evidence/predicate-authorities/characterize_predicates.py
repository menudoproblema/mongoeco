import argparse
import hashlib
import sys

from pathlib import Path


sys.path[:0] = [str(Path.cwd() / "src"), str(Path.cwd())]
import pymongo

from bson import Decimal128, Int64, json_util

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90
from mongoeco.core.collation import normalize_collation
from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter
from mongoeco.types import Decimal128 as LocalDecimal128


CASES = []


def case(name, value, clause, *, schema=False, doc=None, empty=False, collation=None):
    CASES.append(
        {
            "id": name,
            "documents": []
            if empty
            else [({"_id": 0, "a": value} if doc is None else {"_id": 0, **doc})],
            "filter": {"$jsonSchema": {"properties": {"a": clause}}}
            if schema
            else {"a": clause},
            "collation": collation,
        }
    )


for name, value, clause in [
    ("string_number", "z", {"$gt": 5}),
    ("bool_number", True, {"$gt": 0}),
    ("null_number", None, {"$lt": 5}),
    ("numeric_control", 6, {"$gt": 5}),
    ("array_empty", [], {"$gt": 5}),
    ("array_numeric", [6], {"$gt": 5}),
    ("nested_array", [[6]], {"$gt": 5}),
]:
    case("query_" + name, value, clause)
    case("dotted_" + name, None, clause, doc={"o": {"a": value}})
    CASES[-1]["filter"] = {"o.a": clause}
case("expr_cross_type_control", "z", {})
CASES[-1]["filter"] = {"$expr": {"$gt": ["$a", 5]}}
for name, value, schema in [
    ("enum_bool", True, {"enum": [1]}),
    ("enum_nested_bool", {"x": True}, {"enum": [{"x": 1}]}),
    ("enum_numeric", Decimal128("1.0"), {"enum": [1]}),
    ("enum_array", [1], {"enum": [1]}),
    ("enum_order", {"b": 2, "a": 1}, {"enum": [{"a": 1, "b": 2}]}),
    ("enum_collation", "A", {"enum": ["a"]}),
    ("double_max", 0.1, {"maximum": Decimal128("0.1")}),
    ("double_min", 0.1, {"minimum": Decimal128("0.100000000000000002")}),
    ("bool_min", True, {"minimum": 0}),
    ("nan_min", float("nan"), {"minimum": 0}),
    ("decimal_nan_min", Decimal128("NaN"), {"minimum": 0}),
    ("inf_min", float("inf"), {"minimum": 0}),
    ("exclusive", 1, {"minimum": 1, "exclusiveMinimum": True}),
    ("unique", [1, 1], {"uniqueItems": True}),
    ("type_intersection", 1, {"type": "string", "bsonType": "int"}),
    ("type_int", 1, {"type": "int"}),
    ("enum_empty", 1, {"enum": []}),
    ("enum_duplicate", 1, {"enum": [1, 1]}),
    ("unknown_keyword", 1, {"minimumm": 5}),
    ("multiple_of", 3, {"multipleOf": 2}),
    ("required_duplicate", {}, {"required": ["x", "x"]}),
    ("pattern_invalid", "x", {"pattern": "["}),
    ("negative_multiple", 1, {"multipleOf": -1}),
    ("additional_schema", {"x": 3}, {"additionalProperties": {"bsonType": "string"}}),
]:
    case(
        "schema_" + name,
        value,
        schema,
        schema=True,
        collation={"locale": "en", "strength": 2} if name == "enum_collation" else None,
    )
for name, value, operand in [
    ("near_remainder", 1.0, [2, 1 + 1e-13]),
    ("fraction_divisor", 8, [4.5, 0]),
    ("fraction_remainder", 5, [2, 1.9]),
    ("fraction_value", 5.5, [2, 1]),
    ("negative", -5, [2, -1]),
    ("negative_divisor", -5, [-2, -1]),
    ("large", Int64(2**63 - 1), [3, 1]),
    ("decimal", Decimal128("5"), [2, 1]),
    ("nan_remainder", 1, [2, float("nan")]),
    ("inf_remainder", 1, [2, float("inf")]),
    ("range_divisor", 1, [1e30, 0]),
    ("range_remainder", 1, [2, 1e30]),
    ("decimal_divisor", 5, [Decimal128("2"), 1]),
    ("zero_truncated", 1, [0.5, 0]),
    ("divisor_zero", 1, [0, 0]),
    ("negative_zero", 1, [-0.5, 0]),
]:
    case("mod_" + name, value, {"$mod": operand})
# Empty inputs test preparation rather than row-dependent evaluation.
for name, spec in [
    ("unknown", {"x-unknown": True}),
    ("enum", {"enum": []}),
    ("pattern", {"pattern": "["}),
    ("exclusive", {"minimum": 1, "exclusiveMinimum": "yes"}),
]:
    case("empty_schema_" + name, None, spec, schema=True, empty=True)

case("large_numeric_candidate", 2**53 + 1, {"$gt": 2**53})
case("array_range_combined", [1, 20], {"$gt": 5, "$lt": 10})
case("array_range_and", [1, 20], {})
CASES[-1]["filter"] = {"$and": [{"a": {"$gt": 5}}, {"a": {"$lt": 10}}]}
case("missing_null", None, None, doc={})
case("dotted_array_docs", None, {}, doc={"items": [{"x": 7}]})
CASES[-1]["filter"] = {"items.x": {"$gt": 5}}
case("dotted_array_position", None, {}, doc={"a": [7]})
CASES[-1]["filter"] = {"a.0": 7}


def observed(call):
    try:
        return {"ok": True, "value": call()}
    except Exception as error:
        return {
            "ok": False,
            "type": type(error).__name__,
            "code": getattr(error, "code", None),
            "message": str(error),
        }


def adapted(value):
    if isinstance(value, Decimal128):
        return LocalDecimal128(value.to_decimal())
    if isinstance(value, dict):
        return {k: adapted(v) for k, v in value.items()}
    if isinstance(value, list):
        return [adapted(v) for v in value]
    return value


def local(case, dialect, compiled, *, adapt=True):
    if adapt:
        case = adapted(case)

    def action():
        plan = compile_filter(case["filter"], dialect=dialect)
        collation = normalize_collation(case["collation"])
        matcher = (
            CompiledQuery(plan, dialect=dialect, collation=collation)
            if compiled
            else None
        )
        return [
            d["_id"]
            for d in case["documents"]
            if (
                matcher.match(d)
                if matcher
                else QueryEngine.match_plan(
                    d, plan, dialect=dialect, collation=collation
                )
            )
        ]

    return observed(action)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    with pymongo.MongoClient(args.uri, serverSelectionTimeoutMS=3000) as client:
        report = {
            "base": "79529a2",
            "build_info": client.server_info(),
            "fcv": client.admin.command(
                {"getParameter": 1, "featureCompatibilityVersion": 1}
            )["featureCompatibilityVersion"],
            "python": sys.version,
            "pymongo": pymongo.version,
            "read_only_native": "collectionless aggregate with literal documents; no collection writes or user data",
            "cases": [],
        }
        for item in CASES:
            cmd = {
                "aggregate": 1,
                "pipeline": [
                    {"$documents": {"$literal": item["documents"]}},
                    {"$match": item["filter"]},
                ],
                "cursor": {},
            }
            if item["collation"]:
                cmd["collation"] = item["collation"]
            native = observed(
                lambda: [
                    d["_id"]
                    for d in client["mongoeco_analysis_only"].command(cmd)["cursor"][
                        "firstBatch"
                    ]
                ]
            )
            current = {
                d.key: {
                    "interpreted": local(item, d, False),
                    "compiled": local(item, d, True),
                }
                for d in [MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90]
            }
            report["cases"].append(
                {
                    **item,
                    "native": native,
                    "local": current,
                    "local_driver_bson": {
                        "interpreted": local(
                            item, MONGODB_DIALECT_90, False, adapt=False
                        ),
                        "compiled": local(item, MONGODB_DIALECT_90, True, adapt=False),
                    },
                }
            )
            values = current[MONGODB_DIALECT_90.key]
            if any(
                v["ok"] != native["ok"]
                or (v["ok"] and v["value"] != native.get("value"))
                for v in values.values()
            ):
                print(
                    item["id"], "native", str(native)[:160], "local", str(values)[:230]
                )
        report["sources_sha256"] = {
            str(p): hashlib.sha256(p.read_bytes()).hexdigest()
            for p in Path("src").rglob("*.py")
        }
        Path(args.output).write_text(
            json_util.dumps(
                report, json_options=json_util.CANONICAL_JSON_OPTIONS, indent=2
            )
            + "\n"
        )
        print("TOTAL", len(CASES))


if __name__ == "__main__":
    main()
