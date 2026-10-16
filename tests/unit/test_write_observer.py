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


def test_command_error_observer_rejects_partial_or_malformed_details():
    observer = BSONObservation([])
    details = {
        "ok": 0.0,
        "code": 11000,
        "codeName": "DuplicateKey",
        "errmsg": "Plan executor error during findAndModify :: caused by :: "
        "E11000 duplicate key error collection: db.items "
        'index: key_1 dup key: { key: "taken" }',
        "keyPattern": {"key": 1},
        "keyValue": {"key": "taken"},
    }

    def observed(value):
        return public_error(
            OperationFailure("failed", code=11000, details=value),
            observer,
            command_namespace="db.items",
        )

    reference = observed(details)
    assert observed(None) != observed({})
    for field in details:
        assert (
            observed({key: value for key, value in details.items() if key != field})
            != reference
        )
    for message in (
        None,
        1,
        "",
        "duplicate key",
        details["errmsg"].replace("key_1", "wrong"),
        details["errmsg"].replace("db.items", "db.other"),
    ):
        assert observed({**details, "errmsg": message}) != reference
    assert observed({**details, "keyValue": {"key": Int64(2)}}) != reference
    assert observed({**details, "code": 2}) != reference


def test_strict_command_observer_detects_partial_envelopes_and_embedded_exceptions():
    observer = BSONObservation([], error_contract=True)
    details = {
        "ok": 0.0,
        "code": 17276,
        "codeName": "Location17276",
        "errmsg": "Use of undefined variable: missing",
    }

    def observe(value):
        return public_error(
            OperationFailure("original", code=17276, details=value), observer
        )

    reference = observe(details)
    assert observe(None) != observe({})
    for field in details:
        assert observe({k: v for k, v in details.items() if k != field}) != reference
    for value in [None, 1, "", "diagnostic, full error: {}"]:
        assert observe({**details, "errmsg": value}) != reference


def test_bulk_observer_detects_missing_write_concern_errors_and_message_corruption():
    observer = BSONObservation([], error_contract=True)
    details = {
        "writeErrors": [{"index": 0, "code": 2, "errmsg": "can't $divide by zero"}],
        "writeConcernErrors": [],
    }

    def observe(value):
        return public_error(BulkWriteError(value), observer)

    reference = observe(details)
    assert observe({"writeErrors": details["writeErrors"]}) != reference
    with pytest.raises(KeyError):
        observe({**details, "writeErrors": [{"code": 2, "errmsg": "invalid"}]})
    for field in ["code", "errmsg"]:
        assert (
            observe(
                {
                    **details,
                    "writeErrors": [
                        {
                            k: v
                            for k, v in details["writeErrors"][0].items()
                            if k != field
                        }
                    ],
                }
            )
            != reference
        )
    assert (
        observe(
            {
                **details,
                "writeErrors": [
                    {
                        **details["writeErrors"][0],
                        "errmsg": "diagnostic, full error: {}",
                    }
                ],
            }
        )
        != reference
    )


@pytest.mark.parametrize("kind", ["item", "bulk", "raw"])
def test_duplicate_item_observer_retains_diagnostic_content(kind):
    observer = BSONObservation([], namespace="db.items", error_contract=True)

    def observed(message, *, code=11000):
        entry = {
            "index": 0,
            "code": code,
            "errmsg": message,
            "keyPattern": {"key": 1},
            "keyValue": {"key": "taken"},
        }
        if kind == "raw":
            return raw_result({"ok": 1.0, "writeErrors": [entry]}, observer)
        error = (
            BulkWriteError({"writeErrors": [entry], "writeConcernErrors": []})
            if kind == "bulk"
            else WriteError(message, code=code, details=entry)
        )
        return public_error(error, observer)

    message = (
        'E11000 duplicate key error collection: db.items index: key_1 '
        'dup key: { key: "taken" }'
    )
    reference = observed(message)
    for mutation in (
        "Duplicate key for unique index 'key_1'",
        message.replace("taken", "other"),
        message.replace("db.items", "other.items"),
        message.replace("key_1", "other_1"),
        "Plan executor error during update :: caused by :: " + message,
    ):
        assert observed(mutation) != reference
    # Omitting the mandatory code is an observable defect, including raw replies.
    assert observed(message, code=None) != reference


def test_raw_observer_preserves_missing_code_as_a_difference_without_crashing():
    observer = BSONObservation([], error_contract=True)
    missing = raw_result({"writeErrors": [{"index": 0, "errmsg": "bad"}]}, observer)
    complete = raw_result(
        {"writeErrors": [{"index": 0, "code": 9, "errmsg": "bad"}]}, observer
    )
    assert missing != complete
    assert missing["write_errors"] == [[0, None]]
