"""Native materialized contexts exercise the prepared-prefix route itself."""

import asyncio

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.core.aggregation.preparation import PreparedPipeline
from mongoeco.engines import MemoryEngine, SQLiteEngine

from tests.differential._real_parity_base import _normalize
from tests.differential.semantic_materialized_context_cases import (
    SEMANTIC_MATERIALIZED_CONTEXT_CASES,
)
from tests.integration.api.test_semantic_group_regressions import _AsyncView, _capture


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("threshold", [None, 1])
@pytest.mark.parametrize(["name", "action"], SEMANTIC_MATERIALIZED_CONTEXT_CASES)
def test_native_contexts_keep_prepared_prefix_and_source(
    factory, target, threshold, name, action
):
    prefixes = []
    original = AsyncAggregationCursor._materialize_pipeline

    async def materialize(cursor, pipeline=None, **kwargs):
        if pipeline is not None:
            effective = cursor._effective_pipeline()
            assert isinstance(pipeline, PreparedPipeline)
            assert pipeline.context is effective.context
            assert pipeline.dialect is effective.dialect
            assert pipeline.addresses == effective.addresses[: len(pipeline)]
            assert pipeline.requests == effective.requests
            prefixes.append(pipeline)
        return await original(cursor, pipeline, **kwargs)

    async def run():
        async with AsyncMongoClient(
            factory(aggregation_spill_threshold=threshold), mongodb_dialect=target
        ) as client:
            return await asyncio.to_thread(
                action, _AsyncView(client.prefix.rows, asyncio.get_running_loop())
            )

    with patch.object(AsyncAggregationCursor, "_materialize_pipeline", materialize):
        got = asyncio.run(run())
    assert prefixes
    assert _normalize(got) == _normalize(
        _capture(target, "materialized_context")["cases"][name]
    )
