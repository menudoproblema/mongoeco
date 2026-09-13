"""Exact vector selection must bound ranking/output work without losing ties."""

import asyncio

import numpy as np
import pytest

from mongoeco import AsyncMongoClient, SearchIndexModel
from mongoeco.engines import _memory_search_runtime as runtime
from mongoeco.engines.memory import MemoryEngine


_TOP_K = 2
_MATRIX_DIMENSIONS = 2
_RESIDUAL_MATCH_COUNT = 3


def index_model(similarity="dotProduct"):
    return SearchIndexModel(
        {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": 2,
                    "similarity": similarity,
                }
            ]
        },
        name="vectors",
        type="vectorSearch",
    )


def pipeline(vector, *, filter_spec=None, limit=2):
    spec = {
        "index": "vectors",
        "path": "embedding",
        "queryVector": vector,
        "numCandidates": 20,
        "limit": limit,
    }
    if filter_spec is not None:
        spec["filter"] = filter_spec
    return [{"$vectorSearch": spec}]


@pytest.mark.parametrize(
    "filter_spec", [None, {"group": 1}, {"name": {"$regex": "^yes"}}]
)
def test_only_selected_vector_hits_are_materialized(monkeypatch, filter_spec):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            coll = client.db.records
            await coll.insert_many(
                [
                    {
                        "_id": number,
                        "embedding": [0.0, float(number)],
                        "group": 1,
                        "name": "yes",
                        "payload": ["x" * 1024],
                    }
                    for number in range(100)
                ]
            )
            await coll.create_search_index(index_model())
            materialized = []
            original = runtime.attach_vector_search_score

            def observe(document, score):
                materialized.append(document["_id"])
                return original(document, score)

            with monkeypatch.context() as patch:
                patch.setattr(runtime, "attach_vector_search_score", observe)
                result = await coll.aggregate(
                    pipeline([0.0, 1.0], filter_spec=filter_spec)
                ).to_list()
            assert [row["_id"] for row in result] == [99, 98]
            assert materialized == [99, 98]

    asyncio.run(exercise())


def test_cold_topk_does_not_sort_the_complete_score_array(monkeypatch):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            coll = client.db.records
            await coll.insert_many(
                [
                    {"_id": number, "embedding": [0.0, float(number)]}
                    for number in range(1000)
                ]
            )
            await coll.create_search_index(index_model())
            sizes = []
            original = np.argsort

            def observe(values, *args, **kwargs):
                sizes.append(len(values))
                return original(values, *args, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(np, "argsort", observe)
                result = await coll.aggregate(pipeline([0.0, 1.0])).to_list()
            assert [row["_id"] for row in result] == [999, 998]
            assert max(sizes, default=0) <= _TOP_K

    asyncio.run(exercise())


def test_cosine_norms_are_reused_across_distinct_queries(monkeypatch):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            coll = client.db.records
            await coll.insert_many(
                [
                    {"_id": number, "embedding": [1.0, float(number)]}
                    for number in range(100)
                ]
            )
            await coll.create_search_index(index_model("cosine"))
            await coll.aggregate(pipeline([1.0, 0.0])).to_list()
            calls = []
            original = np.linalg.norm

            def observe(values, *args, **kwargs):
                if values.ndim == _MATRIX_DIMENSIONS:
                    calls.append(values.shape)
                return original(values, *args, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(np.linalg, "norm", observe)
                for number in range(1, 4):
                    result = await coll.aggregate(
                        pipeline([1.0, float(number)])
                    ).to_list()
                    assert result[0]["_id"] == number
            assert calls == []

    asyncio.run(exercise())


def test_partial_prefilter_keeps_its_residual_before_topk():
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            coll = client.db.records
            await coll.insert_many(
                [
                    {
                        "_id": number,
                        "embedding": [0.0, float(number)],
                        "group": 1,
                        "name": "yes" if number < _RESIDUAL_MATCH_COUNT else "no",
                    }
                    for number in range(10)
                ]
            )
            await coll.create_search_index(index_model())
            result = await coll.aggregate(
                pipeline(
                    [0.0, 1.0], filter_spec={"group": 1, "name": {"$regex": "^yes"}}
                )
            ).to_list()
            assert [row["_id"] for row in result] == [2, 1]

    asyncio.run(exercise())


@pytest.mark.parametrize("downstream", [False, True])
def test_partial_prefilter_explain_matches_execution(downstream):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.db.records
            await coll.insert_many(
                [
                    {
                        "_id": number,
                        "embedding": [0.0, float(number)],
                        "group": 1,
                        "name": "yes" if number < _RESIDUAL_MATCH_COUNT else "no",
                    }
                    for number in range(10)
                ]
            )
            await coll.create_search_index(index_model())
            residual = {"$and": [{"group": 1}, {"name": {"$regex": "^yes"}}]}
            spec = pipeline([0.0, 1.0], filter_spec=None if downstream else residual)[
                0
            ]["$vectorSearch"]
            kwargs = {"downstream_filter_spec": residual} if downstream else {}
            explanation = await runtime.explain_search_documents(
                engine, "db", "records", "$vectorSearch", spec, **kwargs
            )
            assert (
                explanation.details["documentsMatchedBeforeLimit"]
                == _RESIDUAL_MATCH_COUNT
            )
            assert (
                explanation.details["documentsFiltered"] == 10 - _RESIDUAL_MATCH_COUNT
            )
            results = await runtime.execute_search_documents(
                engine, "db", "records", "$vectorSearch", spec, **kwargs
            )
            assert [row["_id"] for row in results] == [2, 1]

    asyncio.run(exercise())


@pytest.mark.parametrize("filter_spec", [None, {"name": {"$regex": "^yes"}}])
def test_zero_result_hint_remains_empty(filter_spec):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            coll = client.db.records
            await coll.insert_one({"_id": 1, "embedding": [1.0, 0.0], "name": "yes"})
            await coll.create_search_index(index_model())
            spec = pipeline([1.0, 0.0], filter_spec=filter_spec)[0]["$vectorSearch"]
            assert (
                await runtime.execute_search_documents(
                    engine, "db", "records", "$vectorSearch", spec, result_limit_hint=0
                )
                == []
            )

    asyncio.run(exercise())


@pytest.mark.parametrize("limit", [1, 2, 5, 12])
def test_topk_preserves_public_tie_order_not_insertion_order(limit):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            coll = client.db.records
            identifiers = ["z", "f", "h", "c", "e", "b", "a", "d", "g", "i"]
            await coll.insert_many(
                [
                    {"_id": identifier, "embedding": [1.0, 0.0]}
                    for identifier in identifiers
                ]
            )
            await coll.create_search_index(index_model())
            for _ in range(2):
                result = await coll.aggregate(
                    pipeline([1.0, 0.0], limit=limit)
                ).to_list()
                assert [row["_id"] for row in result] == sorted(identifiers)[:limit]

    asyncio.run(exercise())
