from __future__ import annotations

import random
import unittest

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


ENGINE_FACTORIES = (MemoryEngine, SQLiteEngine)
ORACLE_SEED = 4600
SEARCH_INDEX = SearchIndexModel(
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


def _pipelines() -> tuple[list[dict[str, object]], ...]:
    search = {
        "$search": {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
        },
    }
    highlighted = {
        "$search": {
            "index": "by_text",
            "text": {"query": "ada", "path": "title"},
            "highlight": {"path": "title"},
        },
    }
    return (
        [search, {"$project": {"_id": 1}}, {"$skip": 1}, {"$limit": 2}],
        [search, {"$match": {"kind": "keep"}}, {"$limit": 2}],
        [
            search,
            {"$match": {"kind": {"$in": ["keep", "other"]}}},
            {"$skip": 1},
            {"$limit": 1},
        ],
        [
            highlighted,
            {"$project": {"_id": 1, "matches": {"$meta": "searchHighlights"}}},
            {"$limit": 2},
        ],
    )


def _documents() -> list[dict[str, object]]:
    documents = [
        {"_id": 1, "title": "Ada one", "kind": "keep"},
        {"_id": 2, "title": "Ada two", "kind": "drop"},
        {"_id": 3, "title": "Ada three", "kind": "keep"},
        {"_id": 4, "title": "Ada four", "kind": "other"},
    ]
    random.Random(ORACLE_SEED).shuffle(documents)  # noqa: S311
    return documents


def _search_meta_pipeline() -> list[dict[str, object]]:
    return [
        {
            "$searchMeta": {
                "index": "by_text",
                "text": {"query": "ada", "path": "title"},
                "count": {"type": "total"},
                "facet": {"path": "kind", "type": "token"},
            },
        },
    ]


def _writeback_pipeline(target: str) -> list[dict[str, object]]:
    return [
        {
            "$search": {
                "index": "by_text",
                "text": {"query": "ada", "path": "title"},
                "highlight": {"path": "title"},
            },
        },
        {
            "$project": {
                "_id": 1,
                "title": 1,
                "copiedHighlights": {"$meta": "searchHighlights"},
            },
        },
        {"$limit": 2},
        {"$merge": {"into": target}},
    ]


def _contains_runtime_namespace(value: object) -> bool:
    if isinstance(value, list):
        return any(_contains_runtime_namespace(item) for item in value)
    if isinstance(value, dict):
        return any(
            key.startswith("\x00mongoeco") or _contains_runtime_namespace(item)
            for key, item in value.items()
        )
    return False


def _metric_by_name(explanation: dict[str, object], name: str) -> dict[str, object]:
    engine_plan = explanation["engine_plan"]
    assert isinstance(engine_plan, dict)
    details = engine_plan["details"]
    assert isinstance(details, dict)
    execution_stats = details["executionStats"]
    assert isinstance(execution_stats, dict)
    metrics = execution_stats["metrics"]
    assert isinstance(metrics, list)
    return next(item for item in metrics if item["name"] == name)


def _oracle_message(factory: type, pipeline: object, difference: str) -> str:
    return (
        f"engine={factory.__name__} seed={ORACLE_SEED} "
        f"pipeline={pipeline!r} difference={difference}"
    )


class AsyncSearchPlanningEquivalenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_optimized_and_reference_modes_are_equivalent(  # noqa: PLR0915
        self,
    ) -> None:
        for factory in ENGINE_FACTORIES:
            with self.subTest(engine=factory.__name__):
                async with AsyncMongoClient(factory()) as client:
                    collection = client.oracle.documents
                    await collection.insert_many(_documents())
                    await collection.create_search_index(SEARCH_INDEX)

                    for pipeline in _pipelines():
                        optimized = await collection.aggregate(pipeline).to_list()
                        reference_cursor = collection.aggregate(pipeline)
                        reference_cursor._force_full_search_execution = True
                        reference = await reference_cursor.to_list()
                        self.assertEqual(
                            optimized,
                            reference,
                            msg=_oracle_message(factory, pipeline, "result/order"),
                        )

                    meta_pipeline = _search_meta_pipeline()
                    optimized_meta = await collection.aggregate(meta_pipeline).to_list()
                    reference_meta_cursor = collection.aggregate(meta_pipeline)
                    reference_meta_cursor._force_full_search_execution = True
                    reference_meta = await reference_meta_cursor.to_list()
                    self.assertEqual(
                        optimized_meta,
                        reference_meta,
                        msg=_oracle_message(factory, meta_pipeline, "collectors"),
                    )

                    optimized_target = collection.database.optimized_archive
                    reference_target = collection.database.reference_archive
                    optimized_stream = optimized_target.watch(max_await_time_ms=5)
                    reference_stream = reference_target.watch(max_await_time_ms=5)
                    await collection.aggregate(
                        _writeback_pipeline("optimized_archive"),
                    ).to_list()
                    reference_writeback = collection.aggregate(
                        _writeback_pipeline("reference_archive"),
                    )
                    reference_writeback._force_full_search_execution = True
                    await reference_writeback.to_list()
                    optimized_written = (
                        await optimized_target.find().sort("_id").to_list()
                    )
                    reference_written = (
                        await reference_target.find().sort("_id").to_list()
                    )
                    self.assertEqual(
                        optimized_written,
                        reference_written,
                        msg=_oracle_message(factory, "$merge", "persisted writeback"),
                    )
                    self.assertEqual(len(optimized_written), 2)
                    self.assertFalse(_contains_runtime_namespace(optimized_written))
                    optimized_events = [
                        await optimized_stream.try_next(),
                        await optimized_stream.try_next(),
                    ]
                    reference_events = [
                        await reference_stream.try_next(),
                        await reference_stream.try_next(),
                    ]
                    self.assertEqual(
                        [event["operationType"] for event in optimized_events],
                        ["insert", "insert"],
                    )
                    self.assertEqual(
                        [event["operationType"] for event in reference_events],
                        ["insert", "insert"],
                    )
                    self.assertIsNone(await optimized_stream.try_next())
                    self.assertIsNone(await reference_stream.try_next())

                    missing_pipeline = [
                        {
                            "$search": {
                                "index": "missing",
                                "text": {"query": "ada", "path": "title"},
                            },
                        },
                    ]
                    errors = []
                    for reference_mode in (False, True):
                        cursor = collection.aggregate(missing_pipeline)
                        cursor._force_full_search_execution = reference_mode
                        with self.assertRaises(Exception) as raised:
                            await cursor.to_list()
                        errors.append((type(raised.exception), str(raised.exception)))
                    self.assertEqual(
                        errors[0],
                        errors[1],
                        msg=_oracle_message(
                            factory, missing_pipeline, "error contract"
                        ),
                    )

                    optimized_explain = await collection.aggregate(
                        _pipelines()[0],
                    ).explain("executionStats")
                    reference_explain_cursor = collection.aggregate(_pipelines()[0])
                    reference_explain_cursor._force_full_search_execution = True
                    reference_explain = await reference_explain_cursor.explain(
                        "executionStats",
                    )
                    self.assertEqual(
                        _metric_by_name(
                            optimized_explain,
                            "queryMatchedCount",
                        ),
                        _metric_by_name(
                            reference_explain,
                            "queryMatchedCount",
                        ),
                        msg=_oracle_message(factory, _pipelines()[0], "query metric"),
                    )

                    explanation = await collection.aggregate(
                        _pipelines()[0],
                    ).explain("queryPlanner")
                    search_plan = explanation["pushdown"]["searchPlan"]
                    self.assertEqual(
                        search_plan,
                        explanation["engine_plan"]["details"]["pipelinePlan"],
                    )
                    self.assertEqual(
                        search_plan["appliedRules"],
                        ["search.direct-window"],
                    )
                    writeback_explanation = await collection.aggregate(
                        [
                            _pipelines()[0][0],
                            {"$merge": {"into": "archive"}},
                        ],
                    ).explain("queryPlanner")
                    writeback_plan = writeback_explanation["pushdown"]["searchPlan"]
                    self.assertTrue(
                        writeback_explanation["pushdown"]["searchWriteback"]
                    )
                    self.assertEqual(writeback_plan["strategy"], "full")
                    self.assertIn(
                        "search.writeback", writeback_plan["rejectionReasons"]
                    )
                    self.assertEqual(
                        writeback_plan,
                        writeback_explanation["engine_plan"]["details"]["pipelinePlan"],
                    )


class SyncSearchPlanningEquivalenceTests(unittest.TestCase):
    def test_optimized_and_reference_modes_are_equivalent(  # noqa: PLR0915
        self,
    ) -> None:
        for factory in ENGINE_FACTORIES:
            with (
                self.subTest(engine=factory.__name__),
                MongoClient(factory()) as client,
            ):
                collection = client.oracle_sync.documents
                collection.insert_many(_documents())
                collection.create_search_index(SEARCH_INDEX)

                for pipeline in _pipelines():
                    optimized = collection.aggregate(pipeline).to_list()
                    reference_cursor = collection.aggregate(pipeline)
                    async_cursor = reference_cursor._async_aggregation_cursor
                    async_cursor._force_full_search_execution = True
                    reference = reference_cursor.to_list()
                    self.assertEqual(
                        optimized,
                        reference,
                        msg=_oracle_message(factory, pipeline, "result/order"),
                    )

                meta_pipeline = _search_meta_pipeline()
                optimized_meta = collection.aggregate(meta_pipeline).to_list()
                reference_meta_cursor = collection.aggregate(meta_pipeline)
                reference_meta_async_cursor = (
                    reference_meta_cursor._async_aggregation_cursor
                )
                reference_meta_async_cursor._force_full_search_execution = True
                self.assertEqual(
                    optimized_meta,
                    reference_meta_cursor.to_list(),
                    msg=_oracle_message(factory, meta_pipeline, "collectors"),
                )

                optimized_target = collection.database.optimized_archive
                reference_target = collection.database.reference_archive
                optimized_stream = optimized_target.watch(max_await_time_ms=5)
                reference_stream = reference_target.watch(max_await_time_ms=5)
                collection.aggregate(
                    _writeback_pipeline("optimized_archive"),
                ).to_list()
                reference_writeback = collection.aggregate(
                    _writeback_pipeline("reference_archive"),
                )
                reference_writeback_async_cursor = (
                    reference_writeback._async_aggregation_cursor
                )
                reference_writeback_async_cursor._force_full_search_execution = True
                reference_writeback.to_list()
                optimized_written = optimized_target.find().sort("_id").to_list()
                reference_written = reference_target.find().sort("_id").to_list()
                self.assertEqual(
                    optimized_written,
                    reference_written,
                    msg=_oracle_message(factory, "$merge", "persisted writeback"),
                )
                self.assertEqual(len(optimized_written), 2)
                self.assertFalse(_contains_runtime_namespace(optimized_written))
                optimized_events = [
                    optimized_stream.try_next(),
                    optimized_stream.try_next(),
                ]
                reference_events = [
                    reference_stream.try_next(),
                    reference_stream.try_next(),
                ]
                self.assertEqual(
                    [event["operationType"] for event in optimized_events],
                    ["insert", "insert"],
                )
                self.assertEqual(
                    [event["operationType"] for event in reference_events],
                    ["insert", "insert"],
                )
                self.assertIsNone(optimized_stream.try_next())
                self.assertIsNone(reference_stream.try_next())

                missing_pipeline = [
                    {
                        "$search": {
                            "index": "missing",
                            "text": {"query": "ada", "path": "title"},
                        },
                    },
                ]
                errors = []
                for reference_mode in (False, True):
                    cursor = collection.aggregate(missing_pipeline)
                    cursor._async_aggregation_cursor._force_full_search_execution = (
                        reference_mode
                    )
                    with self.assertRaises(Exception) as raised:
                        cursor.to_list()
                    errors.append((type(raised.exception), str(raised.exception)))
                self.assertEqual(
                    errors[0],
                    errors[1],
                    msg=_oracle_message(factory, missing_pipeline, "error contract"),
                )

                optimized_explain = collection.aggregate(_pipelines()[0]).explain(
                    "executionStats",
                )
                reference_explain_cursor = collection.aggregate(_pipelines()[0])
                reference_explain_async_cursor = (
                    reference_explain_cursor._async_aggregation_cursor
                )
                reference_explain_async_cursor._force_full_search_execution = True
                reference_explain = reference_explain_cursor.explain("executionStats")
                self.assertEqual(
                    _metric_by_name(optimized_explain, "queryMatchedCount"),
                    _metric_by_name(reference_explain, "queryMatchedCount"),
                    msg=_oracle_message(factory, _pipelines()[0], "query metric"),
                )

                explanation = collection.aggregate(_pipelines()[0]).explain(
                    "queryPlanner",
                )
                search_plan = explanation["pushdown"]["searchPlan"]
                self.assertEqual(
                    search_plan,
                    explanation["engine_plan"]["details"]["pipelinePlan"],
                )
