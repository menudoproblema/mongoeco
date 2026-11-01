"""The test oracle must be selected before the first real Search execution."""

import asyncio

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.support_search_reference import aggregate_for_search_oracle


MODEL = SearchIndexModel(
    {"mappings": {"dynamic": False, "fields": {"title": {"type": "string"}}}},
    name="text",
)
PIPELINE = [
    {"$search": {"index": "text", "text": {"query": "ada", "path": "title"}}},
    {"$limit": 1},
]


@pytest.mark.parametrize("engine", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_oracle_mode_reaches_the_executor_before_first_response(engine, surface):
    modes = []
    execute = AsyncAggregationCursor._execute_search

    async def observe(cursor, *args, **kwargs):
        plan = kwargs["pipeline_plan"]
        modes.append((plan.mode.value, plan.strategy.value))
        return await execute(cursor, *args, **kwargs)

    with patch.object(AsyncAggregationCursor, "_execute_search", observe):
        if surface == "sync":
            with MongoClient(engine()) as client:
                c = client.oracle.rows
                c.insert_one({"_id": 1, "title": "Ada"})
                c.create_search_index(MODEL)
                for reference in (False, True):
                    cursor = aggregate_for_search_oracle(
                        c, PIPELINE, reference=reference
                    )
                    try:
                        assert cursor.to_list() == [{"_id": 1, "title": "Ada"}]
                    finally:
                        cursor.close()
        else:

            async def run():
                async with AsyncMongoClient(engine()) as client:
                    c = client.oracle.rows
                    await c.insert_one({"_id": 1, "title": "Ada"})
                    await c.create_search_index(MODEL)
                    for reference in (False, True):
                        cursor = aggregate_for_search_oracle(
                            c, PIPELINE, reference=reference
                        )
                        try:
                            assert await cursor.to_list() == [
                                {"_id": 1, "title": "Ada"}
                            ]
                        finally:
                            await cursor.close()

            asyncio.run(run())
    assert modes[0][0] == "optimized"
    assert modes[0][1] != "full"
    assert modes[1:] == [("reference", "full")]
    assert AsyncAggregationCursor._force_full_search_execution is False


def test_oracle_scope_restores_its_setting_when_sync_opening_fails():
    with MongoClient() as client:
        with pytest.raises(OperationFailure, match="search index not found"):
            aggregate_for_search_oracle(client.oracle.rows, PIPELINE)
        assert AsyncAggregationCursor._force_full_search_execution is False


@pytest.mark.parametrize("engine", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("verbosity", [None, "queryPlanner", "executionStats"])
def test_aggregate_explain_command_forwards_the_requested_phase(
    engine, surface, verbosity
):
    command = {"explain": {"aggregate": "rows", "pipeline": PIPELINE, "cursor": {}}}
    if verbosity is not None:
        command["verbosity"] = verbosity
    if surface == "sync":
        with MongoClient(engine(simulate_search_index_latency=60)) as client:
            c = client.oracle.rows
            c.insert_one({"_id": 1, "title": "Ada"})
            c.create_search_index(MODEL)
            if verbosity == "queryPlanner":
                assert (
                    c.database.command(command)["engine_plan"]["details"]["status"]
                    == "PENDING"
                )
            else:
                with pytest.raises(OperationFailure, match="not ready"):
                    c.database.command(command)
    else:

        async def run():
            async with AsyncMongoClient(
                engine(simulate_search_index_latency=60)
            ) as client:
                c = client.oracle.rows
                await c.insert_one({"_id": 1, "title": "Ada"})
                await c.create_search_index(MODEL)
                if verbosity == "queryPlanner":
                    result = await c.database.command(command)
                    assert result["engine_plan"]["details"]["status"] == "PENDING"
                else:
                    with pytest.raises(OperationFailure, match="not ready"):
                        await c.database.command(command)

        asyncio.run(run())
