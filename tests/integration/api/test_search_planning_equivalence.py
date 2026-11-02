from __future__ import annotations

import inspect
import random
import sqlite3
import unittest

from unittest.mock import patch

import pytest

from benchmarks.data.generator import generate_users
from benchmarks.runners.workloads import _augment_search_documents

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.core import search
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine

from tests.support_search_reference import aggregate_for_search_oracle


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
                        reference_cursor = aggregate_for_search_oracle(
                            collection, pipeline
                        )
                        reference = await reference_cursor.to_list()
                        self.assertEqual(
                            optimized,
                            reference,
                            msg=_oracle_message(factory, pipeline, "result/order"),
                        )

                    meta_pipeline = _search_meta_pipeline()
                    optimized_meta = await collection.aggregate(meta_pipeline).to_list()
                    reference_meta_cursor = aggregate_for_search_oracle(
                        collection, meta_pipeline
                    )
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
                    reference_writeback = aggregate_for_search_oracle(
                        collection,
                        _writeback_pipeline("reference_archive"),
                    )
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
                        with self.assertRaises(Exception) as raised:
                            cursor = aggregate_for_search_oracle(
                                collection, missing_pipeline, reference=reference_mode
                            )
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
                    reference_explain_cursor = aggregate_for_search_oracle(
                        collection, _pipelines()[0]
                    )
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
    def test_optimized_and_reference_modes_are_equivalent(
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
                    reference_cursor = aggregate_for_search_oracle(collection, pipeline)
                    reference = reference_cursor.to_list()
                    self.assertEqual(
                        optimized,
                        reference,
                        msg=_oracle_message(factory, pipeline, "result/order"),
                    )

                meta_pipeline = _search_meta_pipeline()
                optimized_meta = collection.aggregate(meta_pipeline).to_list()
                reference_meta_cursor = aggregate_for_search_oracle(
                    collection, meta_pipeline
                )
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
                reference_writeback = aggregate_for_search_oracle(
                    collection,
                    _writeback_pipeline("reference_archive"),
                )
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
                    with self.assertRaises(Exception) as raised:
                        cursor = aggregate_for_search_oracle(
                            collection, missing_pipeline, reference=reference_mode
                        )
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
                reference_explain_cursor = aggregate_for_search_oracle(
                    collection, _pipelines()[0]
                )
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


@pytest.mark.parametrize("dynamic", [False, True])
def test_sqlite_residual_materialization_reuses_exact_document_entries(  # noqa: PLR0915 - cache lifecycle matrix
    tmp_path, dynamic
):

    path = str(tmp_path / "materialization.sqlite")
    docs = generate_users(1000)
    _augment_search_documents(docs)
    pipeline = [
        {
            "$search": {
                "index": "by_text",
                "compound": {
                    "must": [
                        {"text": {"query": "report 0", "path": ["title", "body"]}}
                    ],
                    "should": [
                        {"exists": {"path": "title"}},
                        {"wildcard": {"query": "*vector*", "path": "body"}},
                        {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                    ],
                    "minimumShouldMatch": 1,
                },
            }
        },
        {"$limit": 10},
    ]
    model = SearchIndexModel(
        {
            "mappings": {
                "dynamic": dynamic,
                "fields": {
                    "title": {"type": "string"},
                    "body": {"type": "string"},
                    "kind": {"type": "token"},
                    "score": {"type": "number"},
                    "publishedAt": {"type": "date"},
                },
            }
        },
        name="by_text",
    )
    engine = SQLiteEngine(path)
    with MongoClient(engine) as client:
        collection = client.materialized.rows
        collection.insert_many(docs)
        collection.create_search_index(model)
        content_visits = []
        original = search.tokenize_classic_text

        def counted(text):
            if (
                inspect.currentframe().f_back.f_code.co_name
                == "materialized_search_document_from_entries"
            ):
                content_visits.append(text)
            return original(text)

        with patch.object(search, "tokenize_classic_text", counted):
            cold = list(collection.aggregate(pipeline))
            assert content_visits
            content_visits.clear()
            warm = list(collection.aggregate(pipeline))
            assert not content_visits
        assert cold == warm == list(aggregate_for_search_oracle(collection, pipeline))

        backup_path = str(tmp_path / "snapshot.sqlite")
        with sqlite3.connect(path) as source, sqlite3.connect(backup_path) as target:
            source.backup(target)

        # A different engine's version counter cannot invalidate this engine's bucket.
        with MongoClient(SQLiteEngine(path)) as writer:
            other = writer.materialized.rows
            other.update_one(
                {"_id": 0}, {"$set": {"title": "changed", "body": "unrelated"}}
            )
            other.delete_one({"_id": 1})
            other.insert_one(
                {"_id": 1001, "title": "Ada algorithms", "body": "vector report 0"}
            )
        changed = list(collection.aggregate(pipeline))
        assert changed == list(aggregate_for_search_oracle(collection, pipeline))
        assert all(row["_id"] not in (0, 1) for row in changed)
        with patch.object(search, "tokenize_classic_text", counted):
            content_visits.clear()
            assert list(collection.aggregate(pipeline)) == changed
            assert not content_visits

        for title in (None, [], {}, "", {"nested": "alg"}, ["alg", None]):
            collection.update_one({"_id": 1001}, {"$set": {"title": title}})
            assert list(collection.aggregate(pipeline)) == list(
                aggregate_for_search_oracle(collection, pipeline)
            )
        with client.start_session() as session:
            for commit in (False, True):
                session.start_transaction()
                collection.update_one(
                    {"_id": 1001},
                    {"$set": {"body": "report 0 transaction"}},
                    session=session,
                )
                with patch.object(
                    AsyncAggregationCursor, "_force_full_search_execution", new=True
                ):
                    expected = list(collection.aggregate(pipeline, session=session))
                with patch(
                    "mongoeco.engines._sqlite_search_runtime.materialize_search_document",
                    wraps=search.materialize_search_document,
                ) as fallback:
                    assert (
                        list(collection.aggregate(pipeline, session=session))
                        == expected
                    )
                    assert fallback.call_count > 0
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                assert list(collection.aggregate(pipeline)) == list(
                    aggregate_for_search_oracle(collection, pipeline)
                )
        collection.drop_search_index("by_text")
        collection.create_search_index(model)
        assert list(collection.aggregate(pipeline)) == list(
            aggregate_for_search_oracle(collection, pipeline)
        )
    with MongoClient(SQLiteEngine(path)) as reopened:
        collection = reopened.materialized.rows
        assert list(collection.aggregate(pipeline)) == list(
            aggregate_for_search_oracle(collection, pipeline)
        )

    with MongoClient(SQLiteEngine(backup_path)) as restored:
        assert list(restored.materialized.rows.aggregate(pipeline)) == cold
