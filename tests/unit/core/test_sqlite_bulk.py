"""Preparation budgets, error precedence and cooperative cancellation."""

import contextvars

import pytest

from mongoeco.engines._sqlite_bulk import (
    SQLiteBulkPreparation,
    _prepared_document_bytes,
)


def prepare(document):
    return str(document["_id"]), str(document), []


@pytest.mark.parametrize(["row_limit", "byte_target"], [(0, 1), (1, 0), (-1, 1)])
def test_preparation_requires_positive_limits(row_limit, byte_target):
    with pytest.raises(ValueError, match="limits must be positive"):
        SQLiteBulkPreparation(
            [], prepare, document_limit=row_limit, byte_target=byte_target
        )


def test_preparation_steps_preserve_order_and_bound_rows():
    documents = [{"_id": index} for index in range(5)]
    preparation = SQLiteBulkPreparation(documents, prepare, document_limit=2)
    assert not preparation.step()
    assert preparation.rows == [prepare(doc) for doc in documents[:2]]
    assert not preparation.step()
    assert preparation.step()
    assert preparation.rows == [prepare(doc) for doc in documents]
    assert preparation.retained_bytes == sum(
        _prepared_document_bytes(row) for row in preparation.rows
    )
    assert preparation.step()
    assert len(preparation.rows) == len(documents)


def test_oversized_document_finishes_its_own_step_without_truncation():
    documents = [{"_id": 0, "payload": "x" * 10000}, {"_id": 1}]
    preparation = SQLiteBulkPreparation(documents, prepare, byte_target=100)
    assert not preparation.step()
    assert preparation.position == 1
    assert preparation.rows == [prepare(documents[0])]
    assert preparation.step()
    assert preparation.rows == [prepare(doc) for doc in documents]


def test_prepared_size_includes_multikey_rows():
    empty = ("id", "payload", [])
    indexed = ("id", "payload", [("index", "string", 3, "x" * 10000)])
    assert _prepared_document_bytes(indexed) > _prepared_document_bytes(empty) + 10000


def test_validation_error_wins_over_prior_encoding_error():
    error = ValueError("encoding")
    calls = []

    def fail_encoding(document):
        calls.append(document["_id"])
        raise error

    def validate(document):
        invalid_id = 2
        if document["_id"] == invalid_id:
            message = "validation"
            raise TypeError(message)

    preparation = SQLiteBulkPreparation(
        [{"_id": index} for index in range(3)],
        fail_encoding,
        validate,
        document_limit=1,
    )
    assert not preparation.step()
    assert not preparation.step()
    with pytest.raises(TypeError, match="validation"):
        preparation.step()
    assert preparation.preparation_error is error
    assert calls == [0]
    assert not preparation.rows


def test_encoding_error_is_preserved_after_all_validation_succeeds():
    error = ValueError("encoding")
    validated = []

    def fail_encoding(_document):
        raise error

    preparation = SQLiteBulkPreparation(
        [{"_id": index} for index in range(3)],
        fail_encoding,
        lambda document: validated.append(document["_id"]),
    )
    assert preparation.step()
    assert preparation.preparation_error is error
    assert validated == [0, 1, 2]


def test_encoding_error_without_validator_stops_immediately():
    def fail_encoding(_document):
        message = "encoding"
        raise ValueError(message)

    preparation = SQLiteBulkPreparation([{"_id": 0}, {"_id": 1}], fail_encoding)
    with pytest.raises(ValueError, match="encoding"):
        preparation.step()
    assert preparation.position == 1


def test_context_is_isolated_between_documents_and_phases():
    marker = contextvars.ContextVar("bulk_context", default="caller")
    observations = []

    def validate(document):
        observations.append(("validate", document["_id"], marker.get()))
        marker.set("validator mutation")

    def encode(document):
        observations.append(("encode", document["_id"], marker.get()))
        marker.set("encoder mutation")
        return prepare(document)

    preparation = SQLiteBulkPreparation([{"_id": 0}, {"_id": 1}], encode, validate)
    assert preparation.step()
    assert observations == [
        ("validate", 0, "caller"),
        ("encode", 0, "caller"),
        ("validate", 1, "caller"),
        ("encode", 1, "caller"),
    ]
    assert marker.get() == "caller"


def test_stop_after_validation_does_not_encode_or_process_following_documents():
    preparation = SQLiteBulkPreparation([{"_id": 0}, {"_id": 1}], prepare)
    preparation.validate_document = lambda _document: preparation.stop_event.set()
    assert not preparation.step()
    assert preparation.position == 1
    assert not preparation.rows
    assert not preparation.step()
    assert preparation.position == 1


def test_empty_preparation_is_complete():
    preparation = SQLiteBulkPreparation([], prepare)
    assert preparation.step()
    assert not preparation.rows
