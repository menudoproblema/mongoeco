"""String parsing and stored nonfinite decimal controls, observed on MongoDB."""

import math

from datetime import UTC, datetime, timedelta

from bson import Code, Decimal128


SEED = [{"_id": 0}]
STRINGS = (
    "sNaN",
    "+sNaN",
    "-sNaN",
    "SNAN",
    "snan",
    "sNaN42",
    "NaN42",
    "NaN",
    "nan",
    "NAN",
    "+NaN",
    "-NaN",
    "Infinity",
    "+Inf",
    "-Infinity",
    "0",
    "-0",
    "1.250",
    "1e2",
    "1.234567890123456789012345678901234",
    " NaN",
    "1.0 ",
    "1_0",
    "",
    "1e6145",
    "1e-6177",
    "1e-6176",
    "1e6144",
    ".5",
    "1.",
    "01",
    "١٢",
    "İnf",
    "ınfinity",  # noqa: RUF001 - native parser rejects Unicode lookalikes
    "1.2345678901234567890123456789012345",
    "1.0000000000000000000000000000000000",
)
STORED = (
    ("nan-double", math.nan),
    ("negative-nan-double", math.copysign(math.nan, -1)),
    ("negative-zero-double", -0.0),
    ("subnormal-double", 1e-320),
    ("inf-double", math.inf),
    ("negative-inf-double", -math.inf),
    ("finite-double", 1.2345678901234567),
    ("tenth-double", 0.1),
    ("quiet-decimal", Decimal128("NaN")),
    ("signaling-decimal", Decimal128("sNaN")),
    ("negative-signaling", Decimal128("-sNaN")),
    ("inf-decimal", Decimal128("Infinity")),
    ("finite-decimal", Decimal128("2.50")),
    ("integer", 2**53 + 1),
    ("boolean", True),
    ("positive-date", datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=1001)),
    ("negative-date", datetime(1970, 1, 1, tzinfo=UTC) - timedelta(milliseconds=1001)),
)


def _value(value):
    if isinstance(value, Decimal128):
        return {"type": "decimal", "bid": value.bid.hex()}
    return {"type": type(value).__name__, "value": value}


def _observe(collection, expression, *, pipeline=None):
    cursor = None
    phase = "open"
    try:
        cursor = collection.aggregate(
            pipeline
            if pipeline is not None
            else [{"$project": {"_id": 0, "v": expression}}],
            batchSize=1,
        )
        phase = "iterate"
        return {"ok": True, "rows": [_value(row["v"]) for row in cursor]}
    except Exception as error:
        return {
            "ok": False,
            "phase": phase,
            "exception": type(error).__name__,
            "code": getattr(error, "code", None),
            "codeName": (getattr(error, "details", None) or {}).get("codeName"),
        }
    finally:
        if cursor is not None:
            cursor.close()


def _expressions(value):
    return {
        "shortcut": {"$toDecimal": value},
        "convert": {"$convert": {"input": value, "to": "decimal"}},
        "onError": {
            "$convert": {
                "input": value,
                "to": "decimal",
                "onError": "ERROR",
                "onNull": "NULL",
            }
        },
    }


def decimal_string_parsing(collection):
    result = {}
    for token in STRINGS:
        for populated in (False, True):
            collection.delete_many({})
            if populated:
                collection.insert_one({"_id": 0, "v": token})
            for source, value in (("literal", {"$literal": token}), ("field", "$v")):
                for name, expression in _expressions(value).items():
                    result[f"{token!r}/{populated}/{source}/{name}"] = _observe(
                        collection, expression
                    )
    return result


def decimal_stored_controls(collection):
    result = {}
    for label, value in STORED:
        collection.delete_many({})
        collection.insert_one({"_id": 0, "v": value})
        for name, expression in _expressions("$v").items():
            result[f"{label}/{name}"] = _observe(collection, expression)
        if isinstance(value, Decimal128):
            # A conversion error must not become a ban on stored signaling NaN.
            result[f"{label}/stored"] = _value(collection.find_one({"_id": 0})["v"])
    return result


def decimal_code_controls(collection):
    result = {}
    for value in (Code("1.25"), Code("sNaN"), Code("1.25", {})):
        collection.delete_many({})
        collection.insert_one({"_id": 0, "v": value})
        for name, expression in _expressions("$v").items():
            result[f"{value!r}/{name}"] = _observe(collection, expression)
    return result


def decimal_branch_controls(collection):
    result = {}
    for populated in (False, True):
        collection.delete_many({})
        if populated:
            collection.insert_one({"_id": 0, "v": "1.25"})
        bad = {"$toDecimal": "sNaN"}
        expressions = {
            "dead-cond": {"$cond": [True, "OK", bad]},
            "live-cond": {"$cond": [False, "OK", bad]},
            "field-cond": {"$cond": [{"$eq": ["$v", "1.25"]}, "OK", bad]},
            "masked": {"$convert": {"input": "sNaN", "to": "decimal", "onError": "OK"}},
            "null": {"$convert": {"input": None, "to": "decimal", "onNull": "NULL"}},
            "missing": {
                "$convert": {"input": "$absent", "to": "decimal", "onNull": "NULL"}
            },
        }
        for label, expression in expressions.items():
            result[f"{populated}/{label}"] = _observe(collection, expression)
    return result


def decimal_pipeline_updates(collection):
    result = {}
    values = (
        ("valid-string", "1.25"),
        ("bad-string", "sNaN"),
        ("finite-double", 0.1),
        ("negative-nan", math.copysign(math.nan, -1)),
        ("stored-snan", Decimal128("sNaN")),
        ("code", Code("1.25")),
    )
    for label, value in values:
        for name, expression in _expressions("$v").items():
            collection.delete_many({})
            collection.insert_one({"_id": 0, "v": value})
            for repeat in range(2):
                key = f"{label}/{name}/{repeat}"
                try:
                    update = collection.update_one(
                        {"_id": 0}, [{"$set": {"v": expression}}]
                    )
                    observed = {
                        "ok": True,
                        "matched": update.matched_count,
                        "modified": update.modified_count,
                    }
                except Exception as error:
                    observed = {
                        "ok": False,
                        "exception": type(error).__name__,
                        "code": getattr(error, "code", None),
                    }
                observed["stored"] = _value(collection.find_one({"_id": 0})["v"])
                result[key] = observed
    return result


def decimal_context_controls(collection):
    collection.delete_many({})
    collection.insert_one({"_id": 0, "v": 0.1})
    expression = {"$toDecimal": "$v"}
    project = {"$project": {"_id": 0, "v": expression}}
    return {
        "let": _observe(
            collection, {"$let": {"vars": {"x": "$v"}, "in": {"$toDecimal": "$$x"}}}
        ),
        "facet": _observe(
            collection,
            None,
            pipeline=[
                {"$facet": {"rows": [project]}},
                {"$unwind": "$rows"},
                {"$replaceWith": "$rows"},
            ],
        ),
        "lookup": _observe(
            collection,
            None,
            pipeline=[
                {
                    "$lookup": {
                        "from": collection.name,
                        "pipeline": [project],
                        "as": "rows",
                    }
                },
                {"$unwind": "$rows"},
                {"$replaceWith": "$rows"},
            ],
        ),
    }


SEMANTIC_DECIMAL_CONVERSION_CASES = (
    ("semantic_decimal_string_parsing", decimal_string_parsing),
    ("semantic_decimal_stored_controls", decimal_stored_controls),
    ("semantic_decimal_code_controls", decimal_code_controls),
    ("semantic_decimal_branch_controls", decimal_branch_controls),
    ("semantic_decimal_pipeline_updates", decimal_pipeline_updates),
    ("semantic_decimal_context_controls", decimal_context_controls),
)
