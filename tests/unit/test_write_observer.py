"""The native oracle must detect type, order, identity and error differences."""

import pytest

from bson import Int64, ObjectId
from pymongo.errors import (
    BulkWriteError,
    ExecutionTimeout,
    OperationFailure,
    WriteError,
)
from pymongo.results import UpdateResult

from tests.support_write_parity import (
    BSONObservation,
    public_error,
    public_result,
    raw_result,
)


def test_observer_distinguishes_null_upsert_from_a_match_with_the_same_counts():
    observer = BSONObservation([])
    counts = {"n": 1, "nModified": 0}
    upsert = public_result(
        UpdateResult({**counts, "upserted": None}, acknowledged=True), observer
    )
    match = public_result(UpdateResult(counts, acknowledged=True), observer)
    assert upsert["matched_count"] == match["matched_count"] == 1
    assert upsert["did_upsert"] is True
    assert match["did_upsert"] is False
    assert upsert != match
    assert raw_result(
        {"lastErrorObject": {"n": 1, "upserted": None}}, observer
    ) != raw_result({"lastErrorObject": {"n": 1}}, observer)


def test_bulk_error_observer_detects_missing_wrong_and_mutated_operations():
    observer = BSONObservation([])

    def observed(**fields):
        return public_error(
            BulkWriteError(
                {
                    "writeErrors": [{"index": 3, "code": 11000, **fields}],
                    "nInserted": 1,
                }
            ),
            observer,
        )

    reference = observed(op={"_id": "a", "nested": {"value": Int64(2)}})
    assert reference != observed()
    assert observed() != observed(op=None)
    assert reference != observed(op="InsertOne")
    assert reference != observed(op={"_id": "a", "nested": {"value": 2}})
    assert reference != observed(op={"_id": "b", "nested": {"value": Int64(2)}})


def test_bson_observer_retains_numeric_width_missing_null_and_embedded_order():
    observer = BSONObservation([])
    assert observer.value(1) != observer.value(Int64(1))
    assert observer.value(1) != observer.value(1.0)
    assert observer.value({}) != observer.value({"field": None})
    assert observer.value({"a": 1, "b": 2}) != observer.value({"b": 2, "a": 1})
    assert observer.value([1, 2]) != observer.value([2, 1])
    assert observer.value({"a": 1, "b": 2}, root=True) == observer.value(
        {"b": 2, "a": 1}, root=True
    )


def test_generated_id_bijection_preserves_aliases_and_explicit_ids():
    explicit = ObjectId("000000000000000000000001")
    first, second = ObjectId(), ObjectId()
    observer = BSONObservation([explicit])
    assert observer.value(first) == observer.value(first)
    assert observer.value(first) != observer.value(second)
    assert observer.value(explicit) == {"$oid": str(explicit)}
    assert observer.value(explicit) != observer.value(first)


def test_errors_retain_public_family_and_exact_code():
    observer = BSONObservation([])
    assert public_error(WriteError("x", code=14), observer) != public_error(
        WriteError("x", code=28), observer
    )
    assert public_error(WriteError("x", code=14), observer) != public_error(
        OperationFailure("x", code=14), observer
    )
    assert public_error(TypeError("x"), observer)["code"] is None


def test_timeout_is_an_infrastructure_failure_in_this_unbounded_write_corpus():
    with pytest.raises(ExecutionTimeout):
        public_error(ExecutionTimeout("server timed out", code=50), BSONObservation([]))


def test_count_observer_detects_wrong_values_and_numeric_width():
    observer = BSONObservation([])
    assert public_result(0, observer) != public_result(1, observer)
    assert public_result(1, observer) != public_result(1.0, observer)
    boolean = True
    assert public_result(1, observer) != public_result(boolean, observer)
