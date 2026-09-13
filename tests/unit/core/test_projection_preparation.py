"""Execution-owned projection preparation, error timing and operand isolation."""

from copy import deepcopy
from unittest.mock import patch

import pytest

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MongoDialect
from mongoeco.core import projections
from mongoeco.core.search import TEXT_SCORE_FIELD, VECTOR_SEARCH_SCORE_FIELD
from mongoeco.engines.semantic_core import (
    compile_find_semantics,
    stream_finalize_documents,
)
from mongoeco.errors import OperationFailure


@pytest.fixture
def document():
    return {
        "_id": 1,
        "name": "one",
        "nested": {"value": 2, "private": [3]},
        "items": [{"v": 1, "extra": 9}, {"v": 2, "extra": 8}],
        "numbers": [1, 2, 3],
        TEXT_SCORE_FIELD: 4.0,
        VECTOR_SEARCH_SCORE_FIELD: 0.5,
    }


@pytest.mark.parametrize("dialect", [MONGODB_DIALECT_70, MONGODB_DIALECT_80])
@pytest.mark.parametrize(
    "spec",
    [
        {},
        {"_id": 1},
        {"_id": 0},
        {"name": 1, "nested.value": 1},
        {"name": 1, "_id": 0},
        {"nested.private": 0},
        {"items.v": 1},
        {"numbers": {"$slice": 2}},
        {"numbers": {"$slice": -2}},
        {"numbers": {"$slice": [1, 1]}, "name": 1},
        {"items": {"$elemMatch": {"v": {"$gte": 2}}}, "name": 1},
        {"items.$": 1, "name": 1},
        {"score": {"$meta": "textScore"}},
        {"score": {"$meta": "vectorSearchScore"}},
    ],
)
def test_prepared_projection_matches_single_document_contract(document, spec, dialect):
    selector = {"items.v": 2}
    expected = projections.apply_projection(
        document, spec, selector_filter=selector, dialect=dialect
    )
    original = deepcopy(document)
    with patch.object(
        projections, "_parse_projection_spec", wraps=projections._parse_projection_spec
    ) as parse:
        program = projections._ProjectionExecutor(
            spec, selector_filter=selector, dialect=dialect
        )
        results = [program(document) for _ in range(5)]
        assert parse.call_count == 1
    assert results == [expected] * 5
    assert [list(result) for result in results] == [list(expected)] * 5
    assert document == original
    # Every application owns a new mutable result even when the source repeats.
    results[0].clear()
    assert results[1:] == [expected] * 4
    assert document == original


def test_prepared_program_owns_projection_and_positional_selector(document):
    spec = {"items.$": 1, "numbers": {"$slice": [0, 2]}}
    selector = {"items.v": {"$gte": 2}}
    program = projections._ProjectionExecutor(spec, selector_filter=selector)
    first = program(document)
    spec["numbers"]["$slice"][1] = 1
    selector["items.v"]["$gte"] = 1
    spec["name"] = 0
    second = program(document)
    assert (
        first
        == second
        == {
            "_id": 1,
            "items": [{"v": 2, "extra": 8}],
            "numbers": [1, 2],
        }
    )
    first["items"][0]["v"] = "mutated"
    first["numbers"].append(99)
    assert program(document) == second
    assert document["items"][1] == {"v": 2, "extra": 8}


def test_non_positional_projection_does_not_copy_or_retain_unused_selector(document):
    class UncopyableFilter(dict):
        def __deepcopy__(self, memo):
            message = "unused selector must not be copied"
            raise AssertionError(message)

    program = projections._ProjectionExecutor(
        {"name": 1}, selector_filter=UncopyableFilter(unused=True)
    )
    assert program(document) == {"_id": 1, "name": "one"}
    assert program._selector is None


def test_no_projection_does_not_parse_copy_or_construct_a_program(document):
    with patch.object(projections, "_ProjectionExecutor") as prepare:
        assert projections.apply_projection(document, None) is document
        prepare.assert_not_called()
    program = projections._ProjectionExecutor(None)
    with patch.object(projections, "_parse_projection_spec") as parse:
        assert program(document) is document
        parse.assert_not_called()


@pytest.mark.parametrize("max_time_ms", [None, 60_000])
def test_stream_compiles_once_after_skip_and_retains_prefix_before_source_error(
    max_time_ms,
):
    def source():
        yield {"_id": "skipped", "nested": {"value": 0}}
        yield {"_id": 1, "nested": {"value": 1}}
        yield {"_id": 2, "nested": {"value": 2}}
        message = "source failed after valid prefix"
        raise RuntimeError(message)

    semantics = compile_find_semantics(
        {}, projection={"nested.value": 1}, skip=1, max_time_ms=max_time_ms
    )
    with patch.object(
        projections, "_parse_projection_spec", wraps=projections._parse_projection_spec
    ) as parse:
        stream = iter(stream_finalize_documents(source(), semantics))
        assert parse.call_count == 0
        assert next(stream) == {"_id": 1, "nested": {"value": 1}}
        assert next(stream) == {"_id": 2, "nested": {"value": 2}}
        with pytest.raises(RuntimeError, match="source failed after valid prefix"):
            next(stream)
        assert parse.call_count == 1


@pytest.mark.parametrize("max_time_ms", [None, 60_000])
def test_stream_does_not_validate_skipped_or_empty_projection(max_time_ms):
    semantics = compile_find_semantics(
        {}, projection={"name": 1, "nested": 0}, skip=1, max_time_ms=max_time_ms
    )
    with patch.object(
        projections, "_parse_projection_spec", wraps=projections._parse_projection_spec
    ) as parse:
        assert list(stream_finalize_documents([], semantics)) == []
        assert list(stream_finalize_documents([{"name": "skipped"}], semantics)) == []
        assert parse.call_count == 0
        with pytest.raises(OperationFailure):
            list(stream_finalize_documents([{}, {}], semantics))
        assert parse.call_count == 1


def test_projection_preparation_uses_its_own_dialect():
    class NoBooleanProjectionDialect(MongoDialect):
        def projection_flag(self, value):
            if isinstance(value, bool):
                return None
            return super().projection_flag(value)

    strict = projections._ProjectionExecutor(
        {"name": True},
        dialect=NoBooleanProjectionDialect(
            key="no-bool", server_version="test", label="No boolean flags"
        ),
    )
    default = projections._ProjectionExecutor({"name": True})
    assert default({"name": "one"}) == {"name": "one"}
    with pytest.raises(OperationFailure):
        strict({"name": "one"})
