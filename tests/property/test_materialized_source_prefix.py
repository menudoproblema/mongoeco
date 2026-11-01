"""The actual materialized-prefix route preserves its source find window."""

import pytest

from hypothesis import assume, given, settings, strategies as st

from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.compat import resolve_mongodb_dialect
from mongoeco.core.aggregation.planning import split_pushdown_pipeline


STAGES = (
    {"$match": {"x": 1}},
    {"$sort": {"_id": 1}},
    {"$sort": {"x": -1}},
    {"$skip": 1},
    {"$limit": 2},
    {"$project": {"_id": 1, "x": 1}},
    {"$project": {"_id": 1, "x": {"$literal": 5}}},
    {"$set": {"x": "$x"}},
    {"$group": {"_id": "$x", "n": {"$sum": 1}}},
    {"$facet": {"all": [{"$project": {"_id": 1}}]}},
    {"$unwind": {"path": "$items", "preserveNullAndEmptyArrays": True}},
    {"$replaceWith": {"x": "$x"}},
    {"$count": "n"},
    {"$bucket": {"groupBy": "$x", "boundaries": [0, 3, 10], "default": "other"}},
    {"$redact": "$$KEEP"},
    {"$unset": "x"},
    {"$sortByCount": "$x"},
)


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@settings(max_examples=500)
@given(pipeline=st.lists(st.sampled_from(STAGES), min_size=2, max_size=8))
def test_materialized_prefix_keeps_full_pipeline_source(target, pipeline):
    dialect = resolve_mongodb_dialect(target)
    full = split_pushdown_pipeline(pipeline, dialect=dialect)
    cursor = AsyncAggregationCursor
    # Restrict to the caller's route with spilling disabled. Arbitrary slices
    # need not preserve pushdown; this is not an assertion for every prefix.
    assume(
        cursor._split_streamable_pipeline(full.remaining_pipeline, dialect=dialect)
        is None
    )
    assume(
        cursor._split_incremental_group_pipeline(
            full.remaining_pipeline, dialect=dialect
        )
        is None
    )
    split = cursor._split_materialized_suffix(pipeline, dialect=dialect)
    assume(split is not None)
    prefix, _suffix = split
    part = split_pushdown_pipeline(prefix, dialect=dialect)
    assert (part.filter_spec, part.projection, part.sort, part.skip, part.limit) == (
        full.filter_spec,
        full.projection,
        full.sort,
        full.skip,
        full.limit,
    )
