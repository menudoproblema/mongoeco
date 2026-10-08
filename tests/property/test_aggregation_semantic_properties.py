"""Cross-cutting invariants backed by versioned native semantic witnesses."""

import copy
import datetime

import pytest

from hypothesis import example, given, seed, strategies as st

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90
from mongoeco.core.aggregation.extensions import (
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.core.aggregation.preparation import prepare_pipeline
from mongoeco.core.aggregation.runtime import evaluate_expression
from mongoeco.core.aggregation.stages import apply_pipeline
from mongoeco.errors import OperationFailure

from tests.property._config import PROPERTY_SEED


DIALECTS = st.sampled_from([MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90])


@seed(PROPERTY_SEED)
@example(values=[-1, 1, 1, 4], lower=0, width=3, step=1, dialect=MONGODB_DIALECT_90)
@given(
    values=st.lists(st.one_of(st.none(), st.integers(-20, 20)), max_size=20),
    lower=st.integers(-10, 10),
    width=st.integers(1, 20),
    step=st.integers(1, 5),
    dialect=DIALECTS,
)
def test_densify_preserves_originals_multiplicity_content_and_grid(
    values, lower, width, step, dialect
):
    documents = [
        {"_id": i, "n": value, "payload": {"keep": [i, value]}}
        for i, value in enumerate(values)
    ]
    before = copy.deepcopy(documents)
    pipeline = [
        {
            "$densify": {
                "field": "n",
                "range": {"bounds": [lower, lower + width], "step": step},
            }
        }
    ]
    result = apply_pipeline(documents, pipeline, dialect=dialect)
    originals = [doc for doc in result if "_id" in doc]
    assert sorted(originals, key=lambda doc: doc["_id"]) == before
    assert documents == before
    synthetic = [doc for doc in result if "_id" not in doc]
    assert all(set(doc) == {"n"} for doc in synthetic)
    assert all(
        lower <= doc["n"] < lower + width and (doc["n"] - lower) % step == 0
        for doc in synthetic
    )
    grid = set(range(lower, lower + width, step))
    present = {value for value in values if value is not None}
    assert {doc["n"] for doc in synthetic} == grid - present
    assert len(synthetic) == len(grid - present)


@seed(PROPERTY_SEED)
@given(
    values=st.lists(st.tuples(st.integers(0, 2), st.integers(-8, 8)), max_size=20),
    step=st.integers(1, 4),
    dialect=DIALECTS,
)
def test_densify_partitions_never_exchange_content_or_duplicate_synthetic_points(
    values, step, dialect
):
    documents = [
        {"_id": i, "p": {"partition": part}, "n": value, "keep": i}
        for i, (part, value) in enumerate(values)
    ]
    result = apply_pipeline(
        documents,
        [
            {
                "$densify": {
                    "field": "n",
                    "partitionByFields": ["p"],
                    "range": {"bounds": [-5, 6], "step": step},
                }
            }
        ],
        dialect=dialect,
    )
    assert (
        sorted((doc for doc in result if "_id" in doc), key=lambda doc: doc["_id"])
        == documents
    )
    synthetic = [doc for doc in result if "_id" not in doc]
    if not values:
        assert all("p" not in doc for doc in synthetic)
        return
    partitions = {part for part, _ in values}
    assert all(doc["p"]["partition"] in partitions for doc in synthetic)
    for part in partitions:
        expected = set(range(-5, 6, step)) - {v for p, v in values if p == part}
        actual = [doc["n"] for doc in synthetic if doc["p"]["partition"] == part]
        assert set(actual) == expected
        assert len(actual) == len(expected)


@seed(PROPERTY_SEED)
@example(days=[1, 30, 31], offset=10000000)
@given(
    days=st.lists(st.integers(1, 31), min_size=1, max_size=15),
    offset=st.integers(1, 10000000),
)
def test_valid_date_windows_exceeding_python_range_keep_inclusive_membership(
    days, offset
):
    dates = sorted(
        datetime.datetime(9999, 12, day, tzinfo=datetime.UTC) for day in days
    )
    documents = [{"_id": i, "date": value} for i, value in enumerate(dates)]
    result = apply_pipeline(
        documents,
        [
            {
                "$setWindowFields": {
                    "sortBy": {"date": 1},
                    "output": {
                        "members": {
                            "$push": "$_id",
                            "window": {"unit": "day", "range": ["unbounded", offset]},
                        }
                    },
                }
            }
        ],
        dialect=MONGODB_DIALECT_90,
    )
    for doc in result:
        expected = [
            other["_id"]
            for other in documents
            if (other["date"] - doc["date"]).days <= offset
        ]
        assert doc["members"] == expected


@seed(PROPERTY_SEED)
@given(
    value=st.integers(-100, 100),
    extra=st.lists(st.integers(-100, 100), max_size=10),
    namespace=st.sampled_from(["records", "foreign"]),
    dialect=st.sampled_from([MONGODB_DIALECT_70, MONGODB_DIALECT_90]),
)
def test_prepared_and_direct_bindings_reuse_and_invalidation_preserve_context(
    value, extra, namespace, dialect
):
    pipeline = [
        {"$match": {"$expr": {"$eq": ["$n", "$$wanted"]}}},
        {
            "$project": {
                "n": 1,
                "copy": {"$let": {"vars": {"local": "$n"}, "in": "$$local"}},
            }
        },
    ]
    documents = [{"_id": i, "n": n} for i, n in enumerate([value, *extra])]
    prepared = prepare_pipeline(
        pipeline,
        dialect=dialect,
        collection=namespace,
        variables={"wanted"},
        path=(2, "$lookup.pipeline"),
        scope="$lookup",
    )
    assert prepare_pipeline(prepared, dialect=dialect) is prepared
    assert apply_pipeline(
        documents, prepared, dialect=dialect, variables={"wanted": value}
    ) == apply_pipeline(
        documents, pipeline, dialect=dialect, variables={"wanted": value}
    )
    changed = prepare_pipeline(
        prepared, dialect=MONGODB_DIALECT_80, collection=namespace + "2"
    )
    assert changed is not prepared
    assert changed.context.collection == namespace + "2"
    assert changed.context.variables == prepared.context.variables
    assert changed.context.scopes == prepared.context.scopes
    assert changed.addresses == prepared.addresses
    register_aggregation_stage("$property490", lambda docs, spec, context: docs)
    try:
        rebound = prepare_pipeline(prepared, dialect=dialect)
        assert rebound is not prepared
        assert rebound.requests == prepared.requests
        assert rebound.addresses == prepared.addresses
        assert rebound.context.variables == prepared.context.variables
        assert rebound.context.scopes == prepared.context.scopes
        assert rebound.context.collection == namespace
    finally:
        unregister_aggregation_stage("$property490")


@seed(PROPERTY_SEED)
@given(
    suffix=st.text(alphabet="abcxyz", min_size=1, max_size=12),
    kind=st.sampled_from(
        ["empty", "initial", "character", "unknown", "trailing", "component"]
    ),
)
def test_preparation_and_direct_errors_keep_native_classification_and_precedence(
    suffix, kind
):

    expression, code = {
        "empty": ("$$", 9),
        "initial": ("$$1" + suffix, 9),
        "character": ("$$a$" + suffix, 9),
        "unknown": ("$$missing_" + suffix + ".", 17276),
        "trailing": ("$$ROOT." + suffix + ".", 40353),
        "component": ("$$ROOT." + suffix + "..value", 15998),
    }[kind]
    expected_name = (
        "FailedToParse"
        if kind in {"empty", "initial", "character"}
        else f"Location{code}"
    )
    for action in (
        lambda: prepare_pipeline(
            [{"$project": {"v": expression}}], dialect=MONGODB_DIALECT_90
        ),
        lambda: evaluate_expression({}, expression, dialect=MONGODB_DIALECT_90),
    ):
        with pytest.raises(OperationFailure) as caught:
            action()
        assert caught.value.code == code
        assert caught.value.details["codeName"] == expected_name
