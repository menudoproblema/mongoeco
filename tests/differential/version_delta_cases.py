"""Reproducible server-version probes, independent of Mongoeco's runtime."""

from __future__ import annotations

import copy
import datetime

from typing import TYPE_CHECKING, Any

from bson import Binary, Decimal128, MaxKey, MinKey, ObjectId, Regex, Timestamp
from pymongo.errors import OperationFailure

from tests.differential.cases import RealParityCase


if TYPE_CHECKING:
    from collections.abc import Callable


def capture_outcome(action: Callable[[], Any]) -> dict[str, Any]:
    try:
        return {"ok": True, "result": action()}
    except OperationFailure as error:
        # Configuration, network and programming failures are not valid
        # semantic observations. Only server errors belong in these captures.
        if error.code is None:
            raise
        details = error.details or {}
        return {
            "ok": False,
            "error_type": type(error).__name__,
            "code": error.code,
            "code_name": details.get("codeName", getattr(error, "code_name", None)),
            "error_labels": sorted(details.get("errorLabels", [])),
            "message": details.get("errmsg", str(error)),
        }


def _aggregation_case(
    name: str,
    pipeline: list[dict[str, Any]],
    seeds: list[dict[str, Any]] | None = None,
) -> RealParityCase:
    return RealParityCase(
        name=name,
        seed_documents=[{"_id": "seed"}] if seeds is None else seeds,
        action=lambda collection: capture_outcome(
            lambda: list(collection.aggregate(copy.deepcopy(pipeline)))
        ),
    )


_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
_DATE_SEEDS = [
    {"_id": str(offset), "start": _EPOCH + datetime.timedelta(milliseconds=offset)}
    for offset in (-1500, -1000, -500, -1, 0, 500)
]
_DATE_UNITS = (
    "millisecond",
    "second",
    "minute",
    "hour",
    "day",
    "week",
    "month",
    "quarter",
    "year",
)


def _date_matrix_case(timezone: str, amount: int) -> RealParityCase:
    projection: dict[str, Any] = {"_id": 1, "start": 1}
    for operator in ("$dateAdd", "$dateSubtract"):
        for unit in _DATE_UNITS:
            projection[f"{operator[1:]}_{unit}"] = {
                operator: {
                    "startDate": "$start",
                    "unit": unit,
                    "amount": amount,
                    "timezone": timezone,
                }
            }
    return _aggregation_case(
        f"dates_{timezone.replace('/', '_')}_{amount}",
        [{"$project": projection}, {"$sort": {"_id": 1}}],
        copy.deepcopy(_DATE_SEEDS),
    )


def _indexes(collection):
    def action():
        collection.create_index("a", name="implicit_simple")
        collection.create_index(
            "b", name="explicit_simple", collation={"locale": "simple"}
        )
        collection.create_index(
            "c", name="unicode", collation={"locale": "en", "strength": 2}
        )
        return sorted(
            (dict(index) for index in collection.list_indexes()),
            key=lambda index: index["name"],
        )

    return capture_outcome(action)


def _geo_invalid_key(collection):
    return capture_outcome(
        lambda: collection.create_index([("geo", "2dsphere")], name="geo_index")
    )


def _wildcard_invalid_projection(collection):
    return capture_outcome(
        lambda: collection.create_index(
            [("tenant", 1), ("$**", 1)], wildcardProjection={"tenant": 1}
        )
    )


def _projection_order_case(route):
    def action(collection):
        pipeline = [{"$project": {"_id": 1, "a": 1, "m.a": 1, "m.z": 1, "z": 1}}]
        if route == "find":
            return capture_outcome(
                lambda: list(collection.find({}, pipeline[0]["$project"]))
            )
        if route.startswith("merge"):
            if route == "merge_matched":
                collection.database.archive.insert_one({"_id": "seed", "prior": 0})
            pipeline.append({"$merge": {"into": "archive"}})
            list(collection.aggregate(pipeline))
            return capture_outcome(lambda: list(collection.database.archive.find({})))
        return capture_outcome(lambda: list(collection.aggregate(pipeline)))

    return RealParityCase(
        f"projection_field_order_{route}",
        [{"_id": "seed", "z": 1, "a": 2, "m": {"z": 3, "a": 4}}],
        action,
    )


_GROUP_EMPTY = {"$group": {"_id": None, "": {"$sum": 1}}}
_ARRAY_CASES = [
    _aggregation_case(
        "map_index_default",
        [
            {
                "$project": {
                    "v": {
                        "$map": {
                            "input": [10, 20, 30],
                            "in": {"$add": ["$$this", "$$IDX"]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "map_index_alias",
        [
            {
                "$project": {
                    "v": {
                        "$map": {
                            "input": [10, 20, 30],
                            "arrayIndexAs": "pos",
                            "in": {"$add": ["$$this", "$$pos"]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "filter_index_default",
        [
            {
                "$project": {
                    "v": {
                        "$filter": {
                            "input": [10, 20, 30],
                            "cond": {"$eq": ["$$IDX", 1]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "filter_index_alias",
        [
            {
                "$project": {
                    "v": {
                        "$filter": {
                            "input": [10, 20, 30],
                            "arrayIndexAs": "pos",
                            "cond": {"$eq": ["$$pos", 1]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "reduce_index_default",
        [
            {
                "$project": {
                    "v": {
                        "$reduce": {
                            "input": [10, 20, 30],
                            "initialValue": 0,
                            "in": {"$add": ["$$value", "$$this", "$$IDX"]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "reduce_index_alias",
        [
            {
                "$project": {
                    "v": {
                        "$reduce": {
                            "input": [10, 20, 30],
                            "initialValue": 0,
                            "arrayIndexAs": "pos",
                            "in": {"$add": ["$$value", "$$this", "$$pos"]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "map_alias_hides_idx",
        [
            {
                "$project": {
                    "v": {"$map": {"input": [10], "arrayIndexAs": "pos", "in": "$$IDX"}}
                }
            }
        ],
    ),
    _aggregation_case(
        "map_index_nested",
        [
            {
                "$project": {
                    "v": {
                        "$map": {
                            "input": [10, 20],
                            "in": {"$map": {"input": [1, 2], "in": "$$IDX"}},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case("idx_outside_array", [{"$project": {"v": "$$IDX"}}]),
    _aggregation_case(
        "map_invalid_index_alias",
        [
            {
                "$project": {
                    "v": {"$map": {"input": [], "arrayIndexAs": 1, "in": "$$this"}}
                }
            }
        ],
        [],
    ),
]

_CONVERSION_CASES = [
    _aggregation_case(
        f"convert_base_{base}_{target}",
        [
            {
                "$project": {
                    "v": {
                        "$convert": {
                            "input": text,
                            "to": target,
                            "base": base,
                            "onError": "conversion-error",
                        }
                    }
                }
            }
        ],
    )
    for base, text in ((2, "1010"), (8, "17"), (10, "15"), (16, "ff"))
    for target in ("int", "long", "double", "decimal")
]
_CONVERSION_CASES += [
    _aggregation_case(
        f"convert_to_string_base_{base}",
        [
            {
                "$project": {
                    "v": {"$convert": {"input": -255, "to": "string", "base": base}}
                }
            }
        ],
    )
    for base in (2, 8, 10, 16)
]
_CONVERSION_CASES += [
    _aggregation_case(
        "convert_base_invalid",
        [
            {
                "$project": {
                    "v": {
                        "$convert": {
                            "input": "10",
                            "to": "int",
                            "base": 3,
                            "onError": "conversion-error",
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "convert_base_dynamic",
        [
            {
                "$project": {
                    "v": {
                        "$convert": {
                            "input": "ff",
                            "to": "int",
                            "base": {"$add": [8, 8]},
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "convert_subnormal_double",
        [
            {
                "$project": {
                    "v": {
                        "$convert": {
                            "input": "4.9406564584124654e-324",
                            "to": "double",
                            "onError": "conversion-error",
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "convert_binary_subtype",
        [
            {
                "$project": {
                    "v": {
                        "$convert": {
                            "input": Binary(b"abc", 0),
                            "to": {"type": "binData", "subtype": 1},
                            "onError": "conversion-error",
                        }
                    }
                }
            }
        ],
    ),
    _aggregation_case(
        "convert_json_array",
        [{"$project": {"v": {"$convert": {"input": "[1,2]", "to": "array"}}}}],
    ),
    _aggregation_case(
        "convert_json_object",
        [{"$project": {"v": {"$convert": {"input": '{"a":1}', "to": "object"}}}}],
    ),
    _aggregation_case(
        "to_string_extended",
        [{"$project": {"_id": 1, "v": {"$toString": "$value"}}}, {"$sort": {"_id": 1}}],
        [
            {"_id": label, "value": value}
            for label, value in (
                ("object", {"a": 1, "z": "v"}),
                ("array", [1, "a", None]),
                ("regex", Regex("^a", "im")),
                ("max", MaxKey()),
                ("min", MinKey()),
                ("timestamp", Timestamp(10, 2)),
            )
        ],
    ),
]


def _expression_case(name, expression, seeds=None):
    return _aggregation_case(name, [{"$project": {"v": expression}}], seeds)


_ARRAY_SCOPE_CASES = [
    _expression_case(
        "filter_colliding_aliases",
        {
            "$filter": {
                "input": [10, 20],
                "as": "pos",
                "arrayIndexAs": "pos",
                "cond": True,
            }
        },
    ),
    _expression_case(
        "map_colliding_aliases",
        {
            "$map": {
                "input": [10, 20],
                "as": "pos",
                "arrayIndexAs": "pos",
                "in": "$$pos",
            }
        },
    ),
    _expression_case(
        "reduce_colliding_aliases",
        {
            "$reduce": {
                "input": [10, 20],
                "as": "pos",
                "arrayIndexAs": "pos",
                "valueAs": "pos",
                "initialValue": 0,
                "in": "$$pos",
            }
        },
    ),
    _expression_case(
        "filter_bad_limit_empty_array",
        {"$filter": {"input": [], "cond": True, "limit": 0}},
    ),
    _expression_case(
        "filter_bad_limit_null_array",
        {"$filter": {"input": None, "cond": True, "limit": 0}},
    ),
    *(
        _expression_case(
            f"filter_limit_{label}",
            {"$filter": {"input": [10, 20, 30], "cond": True, "limit": value}},
        )
        for label, value in (
            ("negative", -1),
            ("boolean", True),
            ("large", 2147483648),
            ("decimal", Decimal128("1")),
            ("nan", float("nan")),
            ("infinity", float("inf")),
        )
    ),
    _expression_case(
        "map_nested_alias_preserves_outer_idx",
        {
            "$map": {
                "input": [10, 20],
                "in": {
                    "$map": {
                        "input": [1, 2],
                        "arrayIndexAs": "pos",
                        "in": {"outer": "$$IDX", "inner": "$$pos"},
                    }
                },
            }
        },
    ),
    *(
        _expression_case(
            f"map_index_alias_invalid_{label}",
            {"$map": {"input": [], "arrayIndexAs": value, "in": "$$this"}},
            [],
        )
        for label, value in (
            ("empty", ""),
            ("uppercase", "IDX"),
            ("dot", "a.b"),
            ("underscore", "_pos"),
        )
    ),
    *(
        _expression_case(
            f"filter_limit_{label}",
            {"$filter": {"input": [10, 20, 30], "cond": True, "limit": value}},
        )
        for label, value in (("one", 1), ("null", None), ("zero", 0), ("fraction", 1.5))
    ),
    _expression_case(
        "reduce_custom_aliases",
        {
            "$reduce": {
                "input": [10, 20],
                "initialValue": 0,
                "as": "item",
                "valueAs": "acc",
                "in": {"$add": ["$$item", "$$acc", "$$IDX"]},
            }
        },
    ),
    _expression_case(
        "map_unknown_parameter_empty",
        {"$map": {"input": [], "in": "$$this", "unlisted": True}},
        [],
    ),
    _expression_case("idx_outside_array_no_input", "$$IDX", []),
]

_CONVERSION_EDGE_CASES = [
    *(
        _expression_case(
            f"convert_hex_{label}_{target}",
            {
                "$convert": {
                    "input": text,
                    "to": target,
                    "base": 16,
                    "onError": "conversion-error",
                }
            },
        )
        for label, text in (
            ("prefix", "0xff"),
            ("plus", "+FF"),
            ("negative", "-FF"),
            ("fraction", "a.f"),
            ("exponent", "a.fp2"),
            ("space", " FF "),
        )
        for target in ("int", "double", "decimal")
    ),
    *(
        _expression_case(
            f"convert_base_bad_{label}",
            {
                "$convert": {
                    "input": None,
                    "to": "int",
                    "base": value,
                    "onError": "conversion-error",
                    "onNull": "null",
                }
            },
            [],
        )
        for label, value in (
            ("fraction", 16.5),
            ("string", "16"),
            ("null", None),
            ("boolean", True),
        )
    ),
    _expression_case(
        "to_string_nested_bson",
        {
            "$toString": {
                "$literal": {
                    "date": _EPOCH,
                    "decimal": Decimal128("12.5"),
                    "id": ObjectId("000000000000000000000001"),
                    "binary": Binary(b"abc", 0),
                    "timestamp": Timestamp(10, 2),
                    "values": [True, False, None, {"nested": "value"}],
                }
            }
        },
    ),
    _expression_case(
        "convert_json_extended",
        {
            "$convert": {
                "input": (
                    '{"date":{"$date":"1970-01-01T00:00:00Z"},"n":{"$numberLong":"2"}}'
                ),
                "to": "object",
            }
        },
    ),
]


VERSION_DELTA_CASES = (
    *(
        _expression_case(
            f"convert_string_base_{base}_{label}",
            {
                "$convert": {
                    "input": value,
                    "to": "string",
                    "base": base,
                    "onError": "conversion-error",
                }
            },
        )
        for base in (2, 8, 10, 16)
        for label, value in (
            ("float_int32_max", 2147483647.0),
            ("float_int32_min", -2147483648.0),
            ("float_outside_int32", 2147483648.0),
            ("decimal_outside_int32", Decimal128("2147483648")),
            ("long_max", 9223372036854775807),
        )
    ),
    _expression_case(
        "convert_string_base_outside_int32_no_fallback",
        {"$convert": {"input": 2147483648.0, "to": "string", "base": 16}},
    ),
    *(
        _expression_case(
            f"convert_decimal_to_{target}",
            {"$convert": {"input": Decimal128("12.5"), "to": target}},
        )
        for target in ("int", "double")
    ),
    _expression_case(
        "convert_base_null_non_null_input",
        {"$convert": {"input": "12", "to": "int", "base": None}},
    ),
    *(
        _projection_order_case(route)
        for route in ("find", "aggregate", "merge", "merge_matched")
    ),
    _expression_case(
        "trim_chars_limit_no_input",
        {"$trim": {"input": "abc", "chars": "a" * 4097}},
        [],
    ),
    _expression_case(
        "trim_chars_limit_null_input", {"$trim": {"input": None, "chars": "a" * 4097}}
    ),
    *(
        _expression_case(
            f"convert_string_base_{label}",
            {
                "$convert": {
                    "input": value,
                    "to": "string",
                    "base": 16,
                    "onError": "conversion-error",
                }
            },
        )
        for label, value in (
            ("float_integral", 10.0),
            ("float_fraction", 10.5),
            ("decimal_integral", Decimal128("10")),
        )
    ),
    *(
        _expression_case(
            f"convert_null_input_base_{label}",
            {
                "$convert": {
                    "input": None,
                    "to": "int",
                    "base": value,
                    "onNull": "null",
                    "onError": "conversion-error",
                }
            },
        )
        for label, value in (
            ("invalid", 3),
            ("null", None),
            ("boolean", True),
            ("string", "16"),
            ("decimal", Decimal128("16")),
        )
    ),
    *(
        _aggregation_case(
            f"densify_bounds_{label}",
            [
                {"$densify": {"field": "n", "range": {"bounds": bounds, "step": 1}}},
                {"$project": {"_id": 0, "n": 1}},
            ],
            [{"_id": str(value), "n": value} for value in values],
        )
        for label, bounds, values in (
            ("empty", [0, 3], []),
            ("equal_empty", [1, 1], []),
            ("outside", [0, 3], [-1, 1, 4]),
            ("upper_existing", [0, 3], [3]),
            ("unaligned", [0, 3], [0.5, 2.5]),
            ("full", "full", [1, 3]),
        )
    ),
    _aggregation_case("group_empty_name", [_GROUP_EMPTY]),
    _aggregation_case("group_empty_name_no_input", [_GROUP_EMPTY], []),
    _aggregation_case(
        "group_empty_name_facet", [{"$facet": {"nested": [_GROUP_EMPTY]}}], []
    ),
    *(
        _date_matrix_case(timezone, amount)
        for timezone in ("UTC", "America/New_York")
        for amount in (0, 1, -1)
    ),
    *_ARRAY_CASES,
    *_ARRAY_SCOPE_CASES,
    *_CONVERSION_CASES,
    *_CONVERSION_EDGE_CASES,
    _aggregation_case(
        "densify_partition_prefix",
        [
            {
                "$densify": {
                    "field": "x",
                    "partitionByFields": ["x"],
                    "range": {"bounds": [0, 3], "step": 1},
                }
            }
        ],
        [],
    ),
    _aggregation_case(
        "densify_partition_child",
        [
            {
                "$densify": {
                    "field": "x",
                    "partitionByFields": ["x.p"],
                    "range": {"bounds": [0, 3], "step": 1},
                }
            }
        ],
        [],
    ),
    _aggregation_case(
        "densify_partition_parent",
        [
            {
                "$densify": {
                    "field": "x.p",
                    "partitionByFields": ["x"],
                    "range": {"bounds": [0, 3], "step": 1},
                }
            }
        ],
        [],
    ),
    _aggregation_case(
        "densify_dates_pre_epoch",
        [
            {
                "$densify": {
                    "field": "date",
                    "range": {
                        "bounds": [
                            _EPOCH - datetime.timedelta(seconds=2),
                            _EPOCH + datetime.timedelta(seconds=2),
                        ],
                        "step": 1,
                        "unit": "second",
                    },
                }
            },
            {"$project": {"_id": 0, "date": 1}},
        ],
        [],
    ),
    _aggregation_case(
        "window_dates_pre_epoch",
        [
            {
                "$setWindowFields": {
                    "sortBy": {"date": 1},
                    "output": {
                        "ids": {
                            "$push": "$_id",
                            "window": {"range": [-1, 1], "unit": "second"},
                        }
                    },
                }
            },
            {"$project": {"_id": 1, "ids": 1}},
        ],
        [
            {
                "_id": str(offset),
                "date": _EPOCH + datetime.timedelta(milliseconds=offset),
            }
            for offset in (-1500, -500, 500, 1500)
        ],
    ),
    *(
        _aggregation_case(
            f"window_dates_bound_{label}",
            [
                {
                    "$setWindowFields": {
                        "sortBy": {"date": 1},
                        "output": {
                            "n": {
                                "$sum": 1,
                                "window": {
                                    "range": [0, bound],
                                    "unit": "day",
                                },
                            }
                        },
                    }
                },
                {"$project": {"_id": 1, "n": 1}},
            ],
            [
                {"_id": str(day), "date": _EPOCH + datetime.timedelta(days=day)}
                for day in range(3)
            ],
        )
        for label, bound in (
            ("float_integral", 1.0),
            ("decimal_integral", Decimal128("1")),
            ("fraction", 0.5),
            ("boolean", True),
        )
    ),
    _aggregation_case(
        "window_dates_output_array",
        [{"$setWindowFields": {"output": []}}],
        [],
    ),
    *(
        _aggregation_case(
            f"trim_chars_dollar_{operator[1:]}",
            [
                {
                    "$project": {
                        "v": {
                            "$trim": {
                                "input": "x",
                                "chars": {operator: "$" * 4097},
                            }
                        }
                    }
                }
            ],
            [],
        )
        for operator in ("$literal", "$const")
    ),
    _aggregation_case(
        "trim_chars_limit",
        [{"$project": {"v": {"$trim": {"input": "abc", "chars": "a" * 4097}}}}],
    ),
    _aggregation_case(
        "cluster_time_standalone", [{"$project": {"v": "$$CLUSTER_TIME"}}]
    ),
    RealParityCase("list_indexes_collation", [{"_id": "seed"}], _indexes),
    RealParityCase(
        "geo_invalid_key",
        [{"_id": "seed", "geo": {"type": "Point", "coordinates": [1]}}],
        _geo_invalid_key,
    ),
    RealParityCase(
        "wildcard_invalid_projection", [{"_id": "seed"}], _wildcard_invalid_projection
    ),
)
