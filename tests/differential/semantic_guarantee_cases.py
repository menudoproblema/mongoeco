"""Native probes for the semantic families hardened in Mongoeco 4.9.0.

Explicit sort establishes order without discarding document multiplicity.
Expectations are captured independently by the native runner.
"""

import datetime

from tests.differential.version_delta_cases import _aggregation_case


_PROBES = {
    "densify_missing_field": [
        [{"_id": "missing", "payload": "keep"}, {"_id": "valid", "n": 2}],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": [0, 3]}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_null_field": [
        [{"_id": "null", "n": None, "payload": "keep"}, {"_id": "valid", "n": 2}],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": [0, 3]}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_only_missing_full": [
        [{"_id": "missing"}],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": "full"}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_only_null_full": [
        [{"_id": "null", "n": None}],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": "full"}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_partition_null": [
        [{"_id": "valid", "n": 2, "p": None}],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": 1, "bounds": [0, 3]},
                    "partitionByFields": ["p"],
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_partition_document": [
        [{"_id": "valid", "n": 2, "p": {"k": 1}}],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": 1, "bounds": [0, 3]},
                    "partitionByFields": ["p"],
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_partition_array": [
        [{"_id": "valid", "n": 2, "p": [1]}],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": 1, "bounds": [0, 3]},
                    "partitionByFields": ["p"],
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_numeric_with_unit": [
        [{"_id": "valid", "n": 2}],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": 1, "unit": "day", "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_empty_numeric_with_unit": [
        [],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": 1, "unit": "day", "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_bad_bounds_empty": [
        [],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": "bad"}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_reversed_bounds": [
        [{"_id": "valid", "n": 2}],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": [3, 0]}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_nonintegral_date_step": [
        [
            {
                "_id": "valid",
                "n": datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
            }
        ],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {
                        "step": 0.5,
                        "unit": "day",
                        "bounds": [
                            datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                            datetime.datetime(2026, 1, 3, 0, 0, tzinfo=datetime.UTC),
                        ],
                    },
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_nan_step": [
        [{"_id": "valid", "n": 2}],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": float("nan"), "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_inf_step": [
        [{"_id": "valid", "n": 2}],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": float("inf"), "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "densify_non_numeric_field": [
        [{"_id": "valid", "n": "invalid"}],
        [
            {"$densify": {"field": "n", "range": {"step": 1, "bounds": [0, 3]}}},
            {"$sort": {"n": 1, "_id": 1}},
        ],
    ],
    "variable_$$1bad": [[{"_id": "valid", "n": 2}], [{"$project": {"v": "$$1bad"}}]],
    "variable_$$bad space": [
        [{"_id": "valid", "n": 2}],
        [{"$project": {"v": "$$bad space"}}],
    ],
    "variable_$$ROOT.": [[{"_id": "valid", "n": 2}], [{"$project": {"v": "$$ROOT."}}]],
    "variable_$$ROOT..n": [
        [{"_id": "valid", "n": 2}],
        [{"$project": {"v": "$$ROOT..n"}}],
    ],
    "variable_$$unknown": [
        [{"_id": "valid", "n": 2}],
        [{"$project": {"v": "$$unknown"}}],
    ],
    "variable_$$": [[{"_id": "valid", "n": 2}], [{"$project": {"v": "$$"}}]],
    "densify_zero": [
        [],
        [{"$densify": {"field": "n", "range": {"step": 0, "bounds": [0, 3]}}}],
    ],
    "densify_negative": [
        [],
        [{"$densify": {"field": "n", "range": {"step": -1, "bounds": [0, 3]}}}],
    ],
    "densify_bool": [
        [],
        [{"$densify": {"field": "n", "range": {"step": True, "bounds": [0, 3]}}}],
    ],
    "densify_str": [
        [],
        [{"$densify": {"field": "n", "range": {"step": "a", "bounds": [0, 3]}}}],
    ],
    "densify_float": [
        [],
        [{"$densify": {"field": "n", "range": {"step": 1.0, "bounds": [0, 3]}}}],
    ],
    "densify_int_float_bounds": [
        [],
        [{"$densify": {"field": "n", "range": {"step": 1, "bounds": [0.0, 3.0]}}}],
    ],
    "densify_float_float_bounds": [
        [],
        [{"$densify": {"field": "n", "range": {"step": 0.5, "bounds": [0.0, 3.0]}}}],
    ],
    "densify_inf_double": [
        [],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {"step": float("inf"), "bounds": [0.0, 3.0]},
                }
            }
        ],
    ],
    "densify_date_without_unit": [
        [],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {
                        "step": 1,
                        "bounds": [
                            datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                            datetime.datetime(2026, 1, 2, 0, 0, tzinfo=datetime.UTC),
                        ],
                    },
                }
            }
        ],
    ],
    "densify_invalid_unit": [
        [],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {
                        "step": 1,
                        "bounds": [
                            datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                            datetime.datetime(2026, 1, 2, 0, 0, tzinfo=datetime.UTC),
                        ],
                        "unit": "bad",
                    },
                }
            }
        ],
    ],
    "densify_mixed_bounds": [
        [],
        [{"$densify": {"field": "n", "range": {"step": 1, "bounds": [0, "x"]}}}],
    ],
    "densify_step_fraction_date": [
        [],
        [
            {
                "$densify": {
                    "field": "n",
                    "range": {
                        "step": 0.5,
                        "bounds": [
                            datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                            datetime.datetime(2026, 1, 2, 0, 0, tzinfo=datetime.UTC),
                        ],
                        "unit": "day",
                    },
                }
            }
        ],
    ],
    "densify_partition_missing": [
        [{"n": 2, "_id": "0"}],
        [
            {
                "$densify": {
                    "field": "n",
                    "partitionByFields": ["p"],
                    "range": {"step": 1, "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1}},
        ],
    ],
    "densify_partition_missing_null": [
        [{"n": 2, "_id": "0"}, {"n": 1, "p": None, "_id": "1"}],
        [
            {
                "$densify": {
                    "field": "n",
                    "partitionByFields": ["p"],
                    "range": {"step": 1, "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1}},
        ],
    ],
    "densify_missing_explicit": [
        [{"_id": "m"}],
        [
            {
                "$densify": {
                    "field": "n",
                    "partitionByFields": [],
                    "range": {"step": 1, "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1}},
        ],
    ],
    "densify_missing_partition_explicit": [
        [{"_id": "m", "p": "a"}],
        [
            {
                "$densify": {
                    "field": "n",
                    "partitionByFields": ["p"],
                    "range": {"step": 1, "bounds": [0, 3]},
                }
            },
            {"$sort": {"n": 1}},
        ],
    ],
    "variable_underscore": [[{"_id": "a"}], [{"$project": {"v": "$$_foo"}}]],
    "variable_upper": [[{"_id": "a"}], [{"$project": {"v": "$$FOO"}}]],
    "variable_non_ascii": [[{"_id": "a"}], [{"$project": {"v": "$$á"}}]],
    "variable_dollar": [[{"_id": "a"}], [{"$project": {"v": "$$a$b"}}]],
    "variable_pathdollar": [[{"_id": "a"}], [{"$project": {"v": "$$ROOT.$n"}}]],
    "variable_pathnull": [[{"_id": "a"}], [{"$project": {"v": "$$ROOT.a\x00b"}}]],
    "variable_unknownbadpath": [[{"_id": "a"}], [{"$project": {"v": "$$unknown."}}]],
    "variable_systeminvalid": [[{"_id": "a"}], [{"$project": {"v": "$$ROOT space"}}]],
}

_PROBES.update(
    {
        "densify_numeric_full_unit": [
            [{"n": 2, "_id": "0"}],
            [
                {
                    "$densify": {
                        "field": "n",
                        "range": {"step": 1, "bounds": "full", "unit": "day"},
                    }
                }
            ],
        ],
        "densify_date_full_without_unit": [
            [
                {
                    "n": datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                    "_id": "0",
                }
            ],
            [{"$densify": {"field": "n", "range": {"step": 1, "bounds": "full"}}}],
        ],
        "densify_date_inf_step": [
            [],
            [
                {
                    "$densify": {
                        "field": "n",
                        "range": {
                            "step": float("inf"),
                            "bounds": [
                                datetime.datetime(
                                    2026, 1, 1, 0, 0, tzinfo=datetime.UTC
                                ),
                                datetime.datetime(
                                    2026, 1, 2, 0, 0, tzinfo=datetime.UTC
                                ),
                            ],
                            "unit": "day",
                        },
                    }
                }
            ],
        ],
        "densify_date_integral_float_step": [
            [],
            [
                {
                    "$densify": {
                        "field": "n",
                        "range": {
                            "step": 1.0,
                            "bounds": [
                                datetime.datetime(
                                    2026, 1, 1, 0, 0, tzinfo=datetime.UTC
                                ),
                                datetime.datetime(
                                    2026, 1, 2, 0, 0, tzinfo=datetime.UTC
                                ),
                            ],
                            "unit": "day",
                        },
                    }
                }
            ],
        ],
        "densify_mixed_full": [
            [
                {"n": 2, "_id": "0"},
                {
                    "n": datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                    "_id": "1",
                },
            ],
            [{"$densify": {"field": "n", "range": {"step": 1, "bounds": "full"}}}],
        ],
        "densify_date_field_numeric_bounds": [
            [
                {
                    "n": datetime.datetime(2026, 1, 1, 0, 0, tzinfo=datetime.UTC),
                    "_id": "0",
                }
            ],
            [{"$densify": {"field": "n", "range": {"step": 1, "bounds": [0, 3]}}}],
        ],
        "densify_number_field_date_bounds": [
            [{"n": 2, "_id": "0"}],
            [
                {
                    "$densify": {
                        "field": "n",
                        "range": {
                            "step": 1,
                            "bounds": [
                                datetime.datetime(
                                    2026, 1, 1, 0, 0, tzinfo=datetime.UTC
                                ),
                                datetime.datetime(
                                    2026, 1, 2, 0, 0, tzinfo=datetime.UTC
                                ),
                            ],
                            "unit": "day",
                        },
                    }
                }
            ],
        ],
    }
)

_PROBES.update(
    {
        "densify_full_large_float": (
            [{"_id": "original", "n": 1e20}],
            [{"$densify": {"field": "n", "range": {"bounds": "full", "step": 1.0}}}],
        ),
        "densify_full_infinity": (
            [{"_id": "original", "n": float("inf")}],
            [{"$densify": {"field": "n", "range": {"bounds": "full", "step": 1.0}}}],
        ),
    }
)

SEMANTIC_GUARANTEE_CASES = tuple(
    _aggregation_case(name, pipeline, documents)
    for name, (documents, pipeline) in _PROBES.items()
)
