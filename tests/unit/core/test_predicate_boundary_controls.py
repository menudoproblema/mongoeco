"""Independent controls for preparation and conservative Search boundaries."""

import asyncio

import pytest

from mongoeco.compat import MONGODB_DIALECT_90
from mongoeco.core.bson_ordering import Code
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.schema_validation import CompiledJsonSchema
from mongoeco.core.search import coerce_search_vector, compile_search_stage
from mongoeco.core.search_execution import SearchRequest
from mongoeco.core.search_filter_prefilter import (
    filter_value_key,
    scalar_filter_candidates,
    value_key_matches_range,
)
from mongoeco.core.search_models import SearchExecutionMode, SearchExplainVerbosity
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


@pytest.mark.parametrize(
    "selector",
    [{"$and": [7]}, {1: 7}, {"$and": []}, {"$where": "true"}],
)
def test_array_selector_grammar_is_prepared_without_a_document(selector):
    with pytest.raises(OperationFailure) as error:
        UpdateEngine.compile_update_plan(
            {"$set": {"items.$[item]": 1}}, array_filters=[selector]
        )
    expected_code = 2
    assert error.value.code == expected_code


def test_pipeline_validator_rejects_a_document_instead_of_a_stage_sequence():
    with pytest.raises(OperationFailure) as error:
        UpdateEngine.validate_update_pipeline({"$set": {"a": 1}})
    expected_code = 14
    assert error.value.code == expected_code


def test_number_schema_accepts_bson_numbers_without_accepting_booleans():
    schema = CompiledJsonSchema({"properties": {"a": {"bsonType": "number"}}})
    assert schema.validate({"a": 1}).valid
    assert schema.validate({"a": 0.5}).valid
    assert not schema.validate({"a": True}).valid


def test_scalar_postings_decline_unrepresentable_or_malformed_domains():
    assert filter_value_key(Code("return 1")) is None
    assert not value_key_matches_range(("number", 9), {"$lte": 8})
    for clause in [{"$exists": 1}, {"$in": "not an array"}]:
        candidates, _operator, exact = scalar_filter_candidates(
            "a",
            clause,
            all_candidates={1, 2},
            exists_index={"a": {1}},
            value_index={},
            unrepresented_paths=frozenset(),
        )
        assert candidates is None
        assert not exact


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_missing_search_index_explain_is_an_error_then_provider_recovers(
    engine_factory,
):
    async def exercise():
        engine = engine_factory()
        await engine.connect()
        try:
            specification = {
                "index": "absent",
                "text": {"query": "ada", "path": "title"},
            }
            request = SearchRequest(
                operator="$search",
                specification=specification,
                query=compile_search_stage("$search", specification),
                mode=SearchExecutionMode.HITS,
                operation_context=OperationContext.create(dialect=MONGODB_DIALECT_90),
            )
            with pytest.raises(OperationFailure, match="search index not found"):
                await engine.explain_search(
                    "db", "records", request, SearchExplainVerbosity.EXECUTION_STATS
                )
            await engine.insert_document("db", "records", {"_id": 1, "title": "Ada"})
            assert await engine.get_document("db", "records", 1) == {
                "_id": 1,
                "title": "Ada",
            }
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_unrepresentable_vector_coordinate_is_outside_local_search_domain():
    assert coerce_search_vector([10**1000, 0]) is None
    assert coerce_search_vector([1, 0]) == (1.0, 0.0)


def test_schema_preparation_propagates_invalid_group_regex():
    with pytest.raises(
        OperationFailure, match="Regular expression is invalid"
    ) as error:
        CompiledJsonSchema({"properties": {"a": {"pattern": "("}}})
    expected_code = 51091
    assert error.value.code == expected_code
