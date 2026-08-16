from __future__ import annotations

import asyncio

from copy import deepcopy

from hypothesis import given, seed, settings, strategies as st

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine

from tests.property._config import PROPERTY_SEED


ENGINE_FACTORIES = (MemoryEngine, SQLiteEngine)
UNBOUNDED_EVENT_MESSAGE = "generated writeback emitted an unbounded event stream"
UNEXPECTED_SUCCESS_MESSAGE = "invalid Search pipeline unexpectedly succeeded"
DOCUMENTS = (
    {
        "_id": 1,
        "title": "Ada one",
        "kind": "keep",
        "tags": ["a", "b"],
        "embedding": [1.0, 0.0],
    },
    {
        "_id": 2,
        "title": "Ada two",
        "kind": "drop",
        "tags": ["b"],
        "embedding": [0.9, 0.1],
    },
    {
        "_id": 3,
        "title": "Ada three",
        "kind": "keep",
        "tags": [],
        "embedding": [0.0, 1.0],
    },
)
FOREIGN_DOCUMENTS = ({"_id": "foreign-keep", "kind": "keep", "title": "Foreign"},)
TEXT_INDEX = SearchIndexModel(
    {
        "mappings": {
            "dynamic": False,
            "fields": {
                "title": {"type": "string"},
                "kind": {"type": "token"},
            },
        },
    },
    name="by_text",
)
VECTOR_INDEX = SearchIndexModel(
    {
        "fields": [
            {
                "type": "vector",
                "path": "embedding",
                "numDimensions": 2,
                "similarity": "cosine",
            },
        ],
    },
    name="by_vector",
    type="vectorSearch",
)


@st.composite
def _runtime_case(draw: st.DrawFn) -> tuple[list[dict[str, object]], bool]:
    operator = draw(st.sampled_from(("search", "search-meta", "vector")))
    writeback = operator == "search" and draw(st.booleans())
    if operator == "search-meta":
        pipeline = [
            {
                "$searchMeta": {
                    "index": "by_text",
                    "text": {"query": "ada", "path": "title"},
                    "count": {"type": "total"},
                    "facet": {"path": "kind", "type": "token"},
                },
            },
        ]
        tail = draw(
            st.sampled_from(
                (
                    [],
                    [{"$project": {"count": 1, "facet": 1}}],
                    [{"$set": {"observed": True}}],
                ),
            ),
        )
    elif operator == "vector":
        pipeline = [
            {
                "$vectorSearch": {
                    "index": "by_vector",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0],
                    "numCandidates": 3,
                    "limit": 3,
                },
            },
        ]
        tail = draw(
            st.sampled_from(
                (
                    [],
                    [{"$match": {"kind": "keep"}}],
                    [{"$skip": 1}, {"$limit": 1}],
                    [{"$project": {"_id": 1, "kind": 1}}],
                    [
                        {
                            "$project": {
                                "_id": 1,
                                "matches": {"$meta": "searchHighlights"},
                            },
                        },
                    ],
                ),
            ),
        )
    else:
        highlight = draw(st.booleans())
        specification: dict[str, object] = {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
        }
        if highlight:
            specification["highlight"] = {"path": "title"}
        pipeline = [{"$search": specification}]
        tail = draw(
            st.sampled_from(
                (
                    [],
                    [{"$match": {"kind": "keep"}}, {"$limit": 2}],
                    [{"$sort": {"_id": -1}}, {"$skip": 1}, {"$limit": 1}],
                    [{"$project": {"_id": 1, "kind": 1}}],
                    [{"$set": {"ordinary": True}}, {"$limit": 2}],
                    [{"$replaceWith": "$$ROOT"}],
                    [{"$unwind": "$tags"}],
                    [{"$group": {"_id": "$kind", "count": {"$sum": 1}}}],
                    [{"$facet": {"kept": [{"$match": {"kind": "keep"}}]}}],
                    [
                        {
                            "$lookup": {
                                "from": "foreign",
                                "localField": "kind",
                                "foreignField": "kind",
                                "as": "joined",
                            },
                        },
                    ],
                    [{"$unionWith": "foreign"}],
                ),
            ),
        )
    pipeline.extend(deepcopy(tail))
    if writeback:
        if tail and "$facet" in tail[-1]:
            pipeline.append({"$set": {"_id": "facet-result"}})
        pipeline.append({"$merge": {"into": "generated_target"}})
    return pipeline, writeback


def _setup_sync(client: MongoClient):
    collection = client.generated.documents
    collection.insert_many(DOCUMENTS)
    client.generated.foreign.insert_many(FOREIGN_DOCUMENTS)
    collection.create_search_index(TEXT_INDEX)
    collection.create_search_index(VECTOR_INDEX)
    return collection


def _event_signature(event: dict[str, object]) -> dict[str, object]:
    return {
        key: deepcopy(event[key])
        for key in ("operationType", "ns", "documentKey", "fullDocument")
        if key in event
    }


def _drain_sync_events(stream: object) -> list[dict[str, object]]:
    events = []
    for _ in range(20):
        event = stream.try_next()
        if event is None:
            return events
        events.append(_event_signature(event))
    raise AssertionError(UNBOUNDED_EVENT_MESSAGE)


async def _drain_async_events(stream: object) -> list[dict[str, object]]:
    events = []
    for _ in range(20):
        event = await stream.try_next()
        if event is None:
            return events
        events.append(_event_signature(event))
    raise AssertionError(UNBOUNDED_EVENT_MESSAGE)


def _run_sync(
    factory: type, pipeline: list[dict[str, object]]
) -> tuple[object, object, object]:
    with MongoClient(factory()) as client:
        collection = _setup_sync(client)
        optimized_target = client.generated.generated_target
        optimized_stream = optimized_target.watch(max_await_time_ms=5)
        optimized = collection.aggregate(pipeline).to_list()
        optimized_written = optimized_target.find().sort("_id").to_list()
        optimized_events = _drain_sync_events(optimized_stream)

    reference_pipeline = deepcopy(pipeline)
    with MongoClient(factory()) as client:
        collection = _setup_sync(client)
        reference_target = client.generated.generated_target
        reference_stream = reference_target.watch(max_await_time_ms=5)
        cursor = collection.aggregate(reference_pipeline)
        cursor._async_aggregation_cursor._force_full_search_execution = True
        reference = cursor.to_list()
        reference_written = reference_target.find().sort("_id").to_list()
        reference_events = _drain_sync_events(reference_stream)
        explanation = collection.aggregate(
            reference_pipeline[:-1] if reference_written else reference_pipeline
        ).explain(
            "queryPlanner",
        )
    search_plan = explanation["pushdown"]["searchPlan"]
    engine_plan = explanation["engine_plan"]["details"]["pipelinePlan"]
    message = (
        f"seed={PROPERTY_SEED} engine={factory.__name__} pipeline={pipeline!r} "
        f"plan={search_plan!r} optimized={optimized!r} reference={reference!r} "
        f"optimizedWritten={optimized_written!r} "
        f"referenceWritten={reference_written!r} "
        f"optimizedEvents={optimized_events!r} referenceEvents={reference_events!r}"
    )
    assert optimized == reference, message
    assert optimized_written == reference_written, message
    assert optimized_events == reference_events, message
    assert search_plan == engine_plan, message
    return optimized, optimized_written, optimized_events


async def _setup_async(client: AsyncMongoClient):
    collection = client.generated.documents
    await collection.insert_many(DOCUMENTS)
    await client.generated.foreign.insert_many(FOREIGN_DOCUMENTS)
    await collection.create_search_index(TEXT_INDEX)
    await collection.create_search_index(VECTOR_INDEX)
    return collection


async def _run_async(
    factory: type,
    pipeline: list[dict[str, object]],
) -> tuple[object, object, object]:
    async with AsyncMongoClient(factory()) as client:
        collection = await _setup_async(client)
        optimized_target = client.generated.generated_target
        optimized_stream = optimized_target.watch(max_await_time_ms=5)
        optimized = await collection.aggregate(pipeline).to_list()
        optimized_written = await optimized_target.find().sort("_id").to_list()
        optimized_events = await _drain_async_events(optimized_stream)

    reference_pipeline = deepcopy(pipeline)
    async with AsyncMongoClient(factory()) as client:
        collection = await _setup_async(client)
        reference_target = client.generated.generated_target
        reference_stream = reference_target.watch(max_await_time_ms=5)
        cursor = collection.aggregate(reference_pipeline)
        cursor._force_full_search_execution = True
        reference = await cursor.to_list()
        reference_written = await reference_target.find().sort("_id").to_list()
        reference_events = await _drain_async_events(reference_stream)
        explain_pipeline = (
            reference_pipeline[:-1] if reference_written else reference_pipeline
        )
        explanation = await collection.aggregate(explain_pipeline).explain(
            "queryPlanner"
        )
    search_plan = explanation["pushdown"]["searchPlan"]
    engine_plan = explanation["engine_plan"]["details"]["pipelinePlan"]
    message = (
        f"seed={PROPERTY_SEED} engine={factory.__name__} pipeline={pipeline!r} "
        f"plan={search_plan!r} optimized={optimized!r} reference={reference!r} "
        f"optimizedWritten={optimized_written!r} "
        f"referenceWritten={reference_written!r} "
        f"optimizedEvents={optimized_events!r} referenceEvents={reference_events!r}"
    )
    assert optimized == reference, message
    assert optimized_written == reference_written, message
    assert optimized_events == reference_events, message
    assert search_plan == engine_plan, message
    return optimized, optimized_written, optimized_events


def _invalid_pipeline(operator: str) -> list[dict[str, object]]:
    if operator == "search":
        return [
            {
                "$search": {
                    "index": "missing",
                    "text": {"query": "ada", "path": "title"},
                },
            },
        ]
    if operator == "search-meta":
        return [
            {
                "$searchMeta": {
                    "index": "missing",
                    "text": {"query": "ada", "path": "title"},
                    "count": {"type": "total"},
                },
            },
        ]
    return [
        {
            "$vectorSearch": {
                "index": "missing",
                "path": "embedding",
                "queryVector": [1.0, 0.0],
                "numCandidates": 3,
                "limit": 2,
            },
        },
    ]


def _sync_error_signature(
    factory: type,
    pipeline: list[dict[str, object]],
    *,
    reference: bool,
) -> tuple[type[Exception], str]:
    with MongoClient(factory()) as client:
        collection = _setup_sync(client)
        cursor = collection.aggregate(pipeline)
        cursor._async_aggregation_cursor._force_full_search_execution = reference
        try:
            cursor.to_list()
        except Exception as error:
            return type(error), str(error)
    raise AssertionError(UNEXPECTED_SUCCESS_MESSAGE)


async def _async_error_signature(
    factory: type,
    pipeline: list[dict[str, object]],
    *,
    reference: bool,
) -> tuple[type[Exception], str]:
    async with AsyncMongoClient(factory()) as client:
        collection = await _setup_async(client)
        cursor = collection.aggregate(pipeline)
        cursor._force_full_search_execution = reference
        try:
            await cursor.to_list()
        except Exception as error:
            return type(error), str(error)
    raise AssertionError(UNEXPECTED_SUCCESS_MESSAGE)


@seed(PROPERTY_SEED)
@given(case=_runtime_case())
@settings(max_examples=min(settings().max_examples, 60))
def test_generated_search_runtime_matches_reference_engines_and_surfaces(
    case: tuple[list[dict[str, object]], bool],
) -> None:
    pipeline, _writeback = case
    original = deepcopy(pipeline)
    for factory in ENGINE_FACTORIES:
        try:
            sync_result = _run_sync(factory, deepcopy(pipeline))
            async_result = asyncio.run(_run_async(factory, deepcopy(pipeline)))
        except Exception as error:
            message = (
                f"seed={PROPERTY_SEED} engine={factory.__name__} "
                f"pipeline={pipeline!r} error={error!r}"
            )
            raise AssertionError(message) from error
        assert sync_result == async_result, (
            f"seed={PROPERTY_SEED} engine={factory.__name__} pipeline={pipeline!r} "
            f"sync={sync_result!r} async={async_result!r}"
        )
    assert pipeline == original


@seed(PROPERTY_SEED)
@given(operator=st.sampled_from(("search", "search-meta", "vector")))
def test_generated_search_errors_match_reference_engines_and_surfaces(
    operator: str,
) -> None:
    pipeline = _invalid_pipeline(operator)
    original = deepcopy(pipeline)
    signatures = []
    for factory in ENGINE_FACTORIES:
        optimized = _sync_error_signature(factory, pipeline, reference=False)
        reference = _sync_error_signature(factory, pipeline, reference=True)
        async_optimized = asyncio.run(
            _async_error_signature(factory, pipeline, reference=False),
        )
        async_reference = asyncio.run(
            _async_error_signature(factory, pipeline, reference=True),
        )
        message = (
            f"seed={PROPERTY_SEED} engine={factory.__name__} operator={operator} "
            f"optimized={optimized!r} reference={reference!r} "
            f"asyncOptimized={async_optimized!r} asyncReference={async_reference!r}"
        )
        assert optimized == reference == async_optimized == async_reference, message
        signatures.append(optimized)
    assert signatures[0] == signatures[1], (
        f"seed={PROPERTY_SEED} operator={operator} signatures={signatures!r}"
    )
    assert pipeline == original
