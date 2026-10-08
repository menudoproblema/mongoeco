"""Independent server probes for the critical-review corrections."""

import datetime

from pymongo import IndexModel

from tests.differential.cases import RealParityCase
from tests.differential.version_delta_cases import _aggregation_case, capture_outcome


def _densify_case(  # noqa: PLR0913 - explicit native stage probe dimensions
    name, values, *, bounds, partitions=False, step=1, unit=None, route="direct"
):
    range_spec = {"bounds": bounds, "step": step}
    if unit is not None:
        range_spec["unit"] = unit
    spec = {"field": "n", "range": range_spec}
    if partitions:
        spec["partitionByFields"] = ["p"]
    pipeline = [
        {"$densify": spec},
        {"$project": {"_id": 0, "n": 1, "p": 1}},
        {"$sort": {"p": 1, "n": 1}},
    ]
    if route == "facet":
        pipeline = [{"$facet": {"values": pipeline}}]
    return _aggregation_case(name, pipeline, values)


def _window_case(name, dates, unit, bounds):
    return _aggregation_case(
        name,
        [
            {
                "$setWindowFields": {
                    "sortBy": {"date": 1},
                    "output": {
                        "members": {
                            "$push": "$_id",
                            "window": {"unit": unit, "range": bounds},
                        }
                    },
                }
            },
            {"$project": {"_id": 1, "members": 1}},
            {"$sort": {"_id": 1}},
        ],
        [{"_id": str(index), "date": date} for index, date in enumerate(dates)],
    )


def _index_case(name, definitions, *, create_collection=False, batch=False):
    def action(collection):
        if create_collection:
            collection.database.create_collection(collection.name)
        before = capture_outcome(lambda: list(collection.list_indexes()))
        if batch:
            calls = [
                capture_outcome(
                    lambda: collection.create_indexes(
                        [IndexModel(keys, **options) for keys, options in definitions]
                    )
                )
            ]
        else:
            calls = [
                capture_outcome(
                    lambda k=keys, o=options: collection.create_index(k, **o)
                )
                for keys, options in definitions
            ]
        after = capture_outcome(lambda: list(collection.list_indexes()))
        return {"before": before, "calls": calls, "after": after}

    return RealParityCase(name, [], action)


def _index_metadata_roundtrip(collection):
    collection.create_index([("n", 1)], collation={"locale": "simple"})
    metadata = list(collection.list_indexes())
    calls = []
    for index in metadata:
        options = {
            key: value
            for key, value in index.items()
            if key in {"name", "unique", "collation"}
        }
        calls.append(
            capture_outcome(
                lambda index=index, options=options: collection.create_index(
                    list(index["key"].items()), **options
                )
            )
        )
    return {
        "metadata": metadata,
        "calls": calls,
        "after": list(collection.list_indexes()),
    }


def _find_empty_variable(collection):
    return capture_outcome(lambda: list(collection.find({"$expr": {"$eq": ["$$", 1]}})))


_HIGH_DATES = [
    datetime.datetime(9999, 12, 30, tzinfo=datetime.UTC),
    datetime.datetime(9999, 12, 31, tzinfo=datetime.UTC),
]
_LOW_DATES = [
    datetime.datetime(1, 1, 1, tzinfo=datetime.UTC),
    datetime.datetime(1, 1, 2, tzinfo=datetime.UTC),
]
_UNITS = (
    ("millisecond", 1000000),
    ("second", 100000),
    ("minute", 2000),
    ("hour", 48),
    ("day", 10000000),
    ("week", 1000),
    ("month", 1),
    ("quarter", 1),
    ("year", 1),
)

REVIEW_IMPROVEMENT_CASES = (
    _densify_case("densify_full_empty", [], bounds="full"),
    _densify_case(
        "densify_dates_partition_full",
        [
            {
                "_id": "a",
                "p": "a",
                "n": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            },
            {
                "_id": "b",
                "p": "b",
                "n": datetime.datetime(2026, 1, 3, tzinfo=datetime.UTC),
            },
        ],
        bounds="full",
        partitions=True,
        unit="day",
    ),
    _densify_case(
        "densify_duplicates",
        [{"_id": "a", "n": 1}, {"_id": "b", "n": 1}, {"_id": "c", "n": 3}],
        bounds=[0, 3],
    ),
    _densify_case("densify_partition_empty", [], bounds=[0, 3], partitions=True),
    *(
        _densify_case(
            f"densify_partition_{label}",
            [
                {"_id": "a", "p": "a", "n": 1},
                {"_id": "b", "p": "a", "n": 3},
                {"_id": "c", "p": "b", "n": 5},
            ],
            bounds=bounds,
            partitions=True,
        )
        for label, bounds in (
            ("explicit", [0, 6]),
            ("full", "full"),
            ("local", "partition"),
        )
    ),
    _densify_case(
        "densify_equal_existing_duplicates",
        [{"_id": "a", "n": 1}, {"_id": "b", "n": 1}],
        bounds=[1, 1],
    ),
    _densify_case(
        "densify_dates",
        [
            {"_id": "a", "n": datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)},
            {"_id": "b", "n": datetime.datetime(2026, 1, 4, tzinfo=datetime.UTC)},
        ],
        bounds=[
            datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
            datetime.datetime(2026, 1, 4, tzinfo=datetime.UTC),
        ],
        unit="day",
    ),
    _densify_case(
        "densify_dates_upper_python_boundary",
        [{"_id": "a", "n": _HIGH_DATES[1]}],
        bounds=_HIGH_DATES,
        step=2,
        unit="day",
    ),
    _densify_case(
        "densify_dates_large_step",
        [],
        bounds=_HIGH_DATES,
        step=1_000_000_000,
        unit="day",
    ),
    _densify_case(
        "densify_facet_outside",
        [{"_id": "a", "n": -1}, {"_id": "b", "n": 1}, {"_id": "c", "n": 4}],
        bounds=[0, 3],
        route="facet",
    ),
    *(
        _window_case(
            f"window_{side}_{unit}",
            dates,
            unit,
            [0, amount] if side == "upper" else [-amount, 0],
        )
        for side, dates in (("upper", _HIGH_DATES), ("lower", _LOW_DATES))
        for unit, amount in _UNITS
    ),
    _window_case(
        "window_calendar_exact_membership",
        [
            datetime.datetime(2024, 1, 31, tzinfo=datetime.UTC),
            datetime.datetime(2024, 2, 28, tzinfo=datetime.UTC),
            datetime.datetime(2024, 2, 29, tzinfo=datetime.UTC),
            datetime.datetime(2024, 3, 1, tzinfo=datetime.UTC),
        ],
        "month",
        [0, 1],
    ),
    _window_case(
        "window_current_unbounded_extremes",
        [*_LOW_DATES, *_HIGH_DATES],
        "year",
        ["current", "unbounded"],
    ),
    _window_case(
        "window_unbounded_current_extremes",
        [*_LOW_DATES, *_HIGH_DATES],
        "year",
        ["unbounded", "current"],
    ),
    *(
        _window_case(f"window_invalid_offset_{label}", _HIGH_DATES, "day", [0, offset])
        for label, offset in (
            ("above_int32", 2147483648),
            ("below_int32", -2147483649),
            ("int64", 9223372036854775807),
        )
    ),
    *(
        _window_case(
            f"window_int32_extreme_{unit}_{side}",
            [_HIGH_DATES[1]],
            unit,
            [offset, 0] if offset < 0 else [0, offset],
        )
        for unit in ("day", "week", "month", "quarter", "year")
        for side, offset in (("lower", -2147483648), ("upper", 2147483647))
    ),
    *(
        _aggregation_case(f"variable_{label}", [{"$project": {"v": expression}}], seeds)
        for label, expression in (
            ("empty", "$$"),
            ("empty_dotted", "$$.value"),
            ("empty_dot", "$$."),
            ("unknown", "$$unknown"),
            ("literal_empty", {"$literal": "$$"}),
            ("const_empty", {"$const": "$$"}),
            ("let_empty_reference", {"$let": {"vars": {"a": "$$"}, "in": "$$a"}}),
        )
        for seeds in ([{"_id": "seed"}],)
    ),
    _aggregation_case("variable_empty_no_input", [{"$project": {"v": "$$"}}], []),
    RealParityCase("variable_empty_find_expr", [{"_id": "seed"}], _find_empty_variable),
    *(
        _index_case(
            f"index_id_{label}_{state}",
            [([("_id", 1)], options)],
            create_collection=created,
        )
        for state, created in (("fresh", False), ("existing", True))
        for label, options in (
            ("omitted", {}),
            ("named", {"name": "_id_"}),
            ("unique_false", {"unique": False}),
            ("unique_true", {"unique": True}),
            ("simple", {"collation": {"locale": "simple"}}),
            ("named_simple", {"name": "_id_", "collation": {"locale": "simple"}}),
        )
    ),
    _index_case(
        "index_simple_recreate",
        [
            ([("n", 1)], {"name": "n_1"}),
            ([("n", 1)], {"name": "n_1", "collation": {"locale": "simple"}}),
        ],
    ),
    _index_case(
        "index_simple_explicit_then_implicit",
        [
            ([("n", 1)], {"name": "n_1", "collation": {"locale": "simple"}}),
            ([("n", 1)], {"name": "n_1"}),
        ],
    ),
    _index_case(
        "index_simple_batch",
        [
            ([("n", 1)], {"name": "n_1"}),
            ([("p", 1)], {"name": "p_1", "collation": {"locale": "simple"}}),
        ],
        batch=True,
    ),
    _index_case("index_id_batch", [([("_id", 1)], {}), ([("n", 1)], {})], batch=True),
    RealParityCase("index_metadata_roundtrip", [], _index_metadata_roundtrip),
)
