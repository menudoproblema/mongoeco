import unittest
from types import SimpleNamespace
from unittest.mock import patch

from mongoeco.api._async.client import AsyncDatabase
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api.operations import compile_aggregate_operation
from mongoeco.api._sync.aggregation_cursor import AggregationCursor
from mongoeco.core.aggregation import (
    AggregationCostPolicy,
    AggregationSpillPolicy,
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.core.bson_scalars import BsonInt32
from mongoeco.core.aggregation import _CURRENT_COLLECTION_RESOLVER_KEY
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import MatchAll
from mongoeco.core.sorting import sort_documents
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import ExecutionTimeout, InvalidOperation, OperationFailure
from mongoeco.types import PlanningIssue, PlanningMode


class _FakeAsyncFindCursor:
    def __init__(self, documents):
        self._documents = documents
        self._plan = MatchAll()

    async def to_list(self):
        return list(self._documents)


class _FakeCollection:
    def __init__(self, documents):
        self._documents = documents
        self.calls = []
        self.built_operations = []
        self._engine = _FakeEngine()
        self._db_name = "db"
        self._collection_name = "coll"

    def find(
        self,
        filter_spec=None,
        projection=None,
        *,
        collation=None,
        sort=None,
        skip=0,
        limit=None,
        hint=None,
        comment=None,
        max_time_ms=None,
        batch_size=None,
        session=None,
    ):
        self.calls.append(
            {
                "filter_spec": filter_spec,
                "projection": projection,
                "sort": sort,
                "skip": skip,
                "limit": limit,
                "hint": hint,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "batch_size": batch_size,
                "session": session,
            }
        )
        if collation is not None:
            self.calls[-1]["collation"] = collation
        documents = list(self._documents)
        documents = sort_documents(documents, sort)
        if skip:
            documents = documents[skip:]
        if limit is not None:
            documents = documents[:limit]
        if projection is not None:
            documents = [apply_projection(document, projection) for document in documents]
        return _FakeAsyncFindCursor(documents)

    def _build_cursor(self, operation, *, session=None):
        self.built_operations.append((operation, session))
        return self.find(
            operation.filter_spec,
            operation.projection,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            batch_size=operation.batch_size,
            session=session,
        )


class _FakeEngine:
    def __init__(self):
        self.explain_semantics_calls = []
        self.scan_semantics_calls = []
        self.aggregation_spill_policy = AggregationSpillPolicy(threshold=1)
        self.aggregation_cost_policy = None

    async def explain_find_semantics(self, db_name, coll_name, semantics, **kwargs):
        self.explain_semantics_calls.append((db_name, coll_name, semantics, kwargs))
        return {"engine": "fake", "details": ["IXSCAN"]}

    def scan_find_semantics(self, db_name, coll_name, semantics, **kwargs):
        self.scan_semantics_calls.append((db_name, coll_name, semantics, kwargs))

        async def _iter():
            if coll_name == "coll":
                yield {"_id": "1", "name": "Ada"}
            elif coll_name == "roles":
                yield {"_id": "r1", "label": "admin"}

        return _iter()


class _SyncClientStub:
    def _run(self, awaitable):
        import asyncio

        return asyncio.run(awaitable)


class _BrokenSyncClientStub:
    def _run(self, awaitable):
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        raise RuntimeError("boom")


class _AsyncAggregationCursorStub:
    def __init__(self, documents):
        self.documents = documents
        self.to_list_calls = 0
        self.first_calls = 0
        self.close_calls = 0
        self._iterator = None

    async def to_list(self):
        self.to_list_calls += 1
        return list(self.documents)

    async def first(self):
        self.first_calls += 1
        return self.documents[0] if self.documents else None

    def __aiter__(self):
        self._iterator = iter(self.documents)
        return self

    async def __anext__(self):
        if self._iterator is None:
            self._iterator = iter(self.documents)
        try:
            return next(self._iterator)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        self.close_calls += 1


class AsyncAggregationCursorTests(unittest.IsolatedAsyncioTestCase):
    def test_cxp_projection_reports_minimal_profile_for_aggregation_search_paths(self):
        collection = _FakeCollection([])

        plain = AsyncAggregationCursor(collection, [])
        text_search = AsyncAggregationCursor(
            collection,
            [{"$search": {"text": {"query": "ada", "path": "name"}}}],
        )
        vector_search = AsyncAggregationCursor(
            collection,
            [{"$vectorSearch": {"path": "embedding", "queryVector": [0.1], "numCandidates": 5, "limit": 2}}],
        )

        self.assertEqual(plain._cxp_explain_projection()["minimalProfile"], "mongodb-core")
        self.assertEqual(
            text_search._cxp_explain_projection()["minimalProfile"],
            "mongodb-text-search",
        )
        self.assertEqual(
            vector_search._cxp_explain_projection()["minimalProfile"],
            "mongodb-search",
        )

    def test_search_result_limit_hint_is_only_exposed_for_safe_trailing_window(self):
        self.assertEqual(
            AsyncAggregationCursor._search_result_limit_hint(
                [{"$project": {"_id": 1}}, {"$skip": 2}, {"$limit": 3}]
            ),
            5,
        )
        self.assertIsNone(
            AsyncAggregationCursor._search_result_limit_hint(
                [{"$match": {"kind": "view"}}, {"$limit": 3}]
            )
        )
        self.assertEqual(
            AsyncAggregationCursor._search_prefix_output_limit(
                [{"$match": {"kind": "view"}}, {"$limit": 3}]
            ),
            3,
        )
        self.assertEqual(
            AsyncAggregationCursor._search_prefix_output_limit(
                [{"$limit": 5}, {"$skip": 2}, {"$project": {"_id": 1}}]
            ),
            3,
        )
        self.assertIsNone(
            AsyncAggregationCursor._search_prefix_output_limit(
                [{"$sort": {"rank": 1}}, {"$limit": 3}]
            )
        )
        self.assertEqual(
            AsyncAggregationCursor._next_search_prefix_fetch_limit(
                10,
                10,
                4,
                10,
            ),
            31,
        )
        self.assertEqual(
            AsyncAggregationCursor._next_search_prefix_fetch_limit(
                5,
                5,
                0,
                2,
            ),
            20,
        )
        self.assertIsNone(AsyncAggregationCursor._search_result_limit_hint(["bad"]))  # type: ignore[list-item]
        self.assertIsNone(AsyncAggregationCursor._search_prefix_output_limit(["bad"]))  # type: ignore[list-item]
        self.assertIsNone(
            AsyncAggregationCursor._leading_search_downstream_filter_spec(
                [{"$match": "bad"}]  # type: ignore[list-item]
            )
        )
        self.assertEqual(
            AsyncAggregationCursor._leading_search_downstream_filter_spec(
                [{"$match": {"kind": "note"}}, {"$match": {"active": True}}, {"$project": {"_id": 1}}]
            ),
            {"$and": [{"kind": "note"}, {"active": True}]},
        )
        self.assertIsNone(
            AsyncAggregationCursor._leading_search_downstream_filter_spec(
                [{"$match": {"kind": "note"}, "$project": {"_id": 1}}]
            )
        )

    async def test_search_helpers_cover_missing_invalid_and_engine_fallback_paths(self):
        collection = _FakeCollection([])
        cursor = AsyncAggregationCursor(collection, [])
        self.assertIsNone(cursor._leading_search_stage())

        cursor = AsyncAggregationCursor(collection, ["invalid"])  # type: ignore[list-item]
        self.assertIsNone(cursor._leading_search_stage())

        cursor = AsyncAggregationCursor(collection, [{"$search": {"text": {"query": "Ada", "path": "name"}}}])
        with self.assertRaisesRegex(OperationFailure, "not supported by this engine"):
            await cursor._search_documents()

        with self.assertRaisesRegex(OperationFailure, "search stage was not present"):
            await AsyncAggregationCursor(collection, [])._search_documents()

    async def test_search_materialization_expands_prefix_for_match_then_limit(self):
        collection = _FakeCollection([])
        calls: list[int | None] = []
        search_documents = [
            {"_id": "1", "kind": "drop"},
            {"_id": "2", "kind": "drop"},
            {"_id": "3", "kind": "keep"},
        ]

        async def _search_documents(*args, **kwargs):
            del args
            calls.append(kwargs.get("result_limit_hint"))
            limit_hint = kwargs.get("result_limit_hint")
            if isinstance(limit_hint, int) and limit_hint > 0:
                return search_documents[:limit_hint]
            return list(search_documents)

        collection._engine.search_documents = _search_documents
        cursor = AsyncAggregationCursor(
            collection,
            [
                {"$search": {"text": {"query": "keep", "path": "kind"}}},
                {"$match": {"kind": "keep"}},
                {"$limit": 1},
            ],
        )

        self.assertEqual(await cursor.to_list(), [{"_id": "3", "kind": "keep"}])
        self.assertEqual(calls, [1, 4])

    async def test_materialize_leading_search_pipeline_returns_empty_for_zero_prefix_limit(self):
        collection = _FakeCollection([])

        async def _search_documents(*args, **kwargs):
            del args, kwargs
            raise AssertionError("search_documents should not be called when output limit is zero")

        collection._engine.search_documents = _search_documents
        cursor = AsyncAggregationCursor(
            collection,
            [
                {"$search": {"text": {"query": "keep", "path": "kind"}}},
                {"$limit": 0},
            ],
        )

        with (
            patch.object(cursor, "_search_result_limit_hint", return_value=None),
            patch.object(cursor, "_search_prefix_output_limit", return_value=0),
        ):
            self.assertEqual(
                await cursor._materialize_leading_search_pipeline(
                    [{"$limit": 0}],
                    dialect=None,
                ),
                ([], []),
            )

    async def test_materialize_leading_search_pipeline_rejects_missing_stage_and_engine_without_search(self):
        collection = _FakeCollection([])
        cursor = AsyncAggregationCursor(collection, [])
        with (
            patch.object(cursor, "_search_result_limit_hint", return_value=None),
            patch.object(cursor, "_search_prefix_output_limit", return_value=1),
            patch.object(cursor, "_leading_search_stage", return_value=None),
        ):
            with self.assertRaisesRegex(OperationFailure, "search stage was not present"):
                await cursor._materialize_leading_search_pipeline([{"$match": {"kind": "note"}}], dialect=None)

        cursor = AsyncAggregationCursor(collection, [{"$search": {"text": {"query": "Ada", "path": "name"}}}])
        with (
            patch.object(cursor, "_search_result_limit_hint", return_value=None),
            patch.object(cursor, "_search_prefix_output_limit", return_value=1),
        ):
            with self.assertRaisesRegex(OperationFailure, "\\$search is not supported by this engine"):
                await cursor._materialize_leading_search_pipeline([{"$match": {"kind": "note"}}], dialect=None)

    async def test_allow_disk_use_false_disables_engine_spill_policy(self):
        collection = _FakeCollection([{"_id": "1", "rank": 2}, {"_id": "2", "rank": 1}])
        cursor = AsyncAggregationCursor(
            collection,
            compile_aggregate_operation(
                [{"$sort": {"rank": 1}}],
                allow_disk_use=False,
            ),
        )

        self.assertIsNone(cursor._spill_policy())

    async def test_aggregation_cursor_converts_internal_bson_wrappers_to_public_documents(self):
        collection = _FakeCollection([{"_id": "1", "score": BsonInt32(1)}])
        cursor = AsyncAggregationCursor(
            collection,
            [{"$project": {"score": {"$add": ["$score", 1]}, "_id": 0}}],
        )

        self.assertEqual(await cursor.to_list(), [{"score": 2}])

    async def test_collect_lookup_names_skips_invalid_stages_and_recurses_nested_pipelines(self):
        cursor = AsyncAggregationCursor(
            _FakeCollection([]),
            [],
        )

        names = cursor._collect_lookup_names(
            [
                "invalid",
                {"$lookup": []},
                {
                    "$lookup": {
                        "from": "users",
                        "pipeline": [
                            {"$lookup": {"from": "roles", "localField": "role_id", "foreignField": "_id", "as": "roles"}}
                        ],
                        "as": "users",
                    }
                },
                {"$unionWith": "archive"},
                {"$unionWith": {"coll": "audits", "pipeline": [{"$lookup": {"from": "teams", "localField": "team_id", "foreignField": "_id", "as": "teams"}}]}},
                {"$facet": {"nested": [{"$lookup": {"from": "teams", "localField": "team_id", "foreignField": "_id", "as": "teams"}}]}},
            ]
        )

        self.assertEqual(names, {"users", "roles", "teams", "archive", "audits"})

    async def test_collect_lookup_names_skips_invalid_union_with_payloads(self):
        cursor = AsyncAggregationCursor(
            _FakeCollection([]),
            [],
        )

        names = cursor._collect_lookup_names(
            [
                {"$unionWith": []},
                {"$unionWith": {"pipeline": []}},
                {"$unionWith": {"coll": "archive"}},
            ]
        )

        self.assertEqual(names, {"archive"})

    async def test_split_streamable_pipeline_uses_registered_stage_execution_mode(self):
        register_aggregation_stage(
            "$future",
            lambda documents, _spec, _context: documents,
            execution_mode="streamable",
        )
        try:
            stream_plan = AsyncAggregationCursor._split_streamable_pipeline(
                [{"$match": {"x": 1}}, {"$future": {}}, {"$limit": 1}]
            )
        finally:
            unregister_aggregation_stage("$future")

        self.assertEqual(stream_plan, ([{"$match": {"x": 1}}, {"$future": {}}], 0, 1))

    async def test_split_streamable_pipeline_returns_none_after_trailing_window_and_accumulates_skip_limit(self):
        self.assertIsNone(
            AsyncAggregationCursor._split_streamable_pipeline(
                [{"$skip": 1}, {"$match": {"x": 1}}],
            )
        )
        self.assertEqual(
            AsyncAggregationCursor._split_streamable_pipeline(
                [{"$match": {"x": 1}}, {"$skip": 2}, {"$limit": 5}, {"$limit": 3}],
            ),
            ([{"$match": {"x": 1}}], 2, 3),
        )

    async def test_materialize_pushes_safe_prefix_to_find(self):
        collection = _FakeCollection(
            [
                {"_id": "1", "kind": "view", "rank": 2},
                {"_id": "2", "kind": "view", "rank": 3},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [
                {"$match": {"kind": "view"}},
                {"$sort": {"rank": 1}},
                {"$skip": 1},
                {"$limit": 1},
                {"$project": {"rank": 1, "_id": 0}},
            ],
        )

        documents = await cursor.to_list()

        self.assertEqual(
            collection.calls,
            [
                {
                    "filter_spec": {"kind": "view"},
                    "projection": {"rank": 1, "_id": 0},
                    "sort": [("rank", 1)],
                    "skip": 1,
                    "limit": 1,
                    "hint": None,
                    "comment": None,
                    "max_time_ms": None,
                    "batch_size": None,
                    "session": None,
                }
            ],
        )
        self.assertEqual(documents, [{"rank": 3}])
        self.assertEqual(len(collection.built_operations), 1)
        self.assertEqual(collection.built_operations[0][0].sort, [("rank", 1)])

    async def test_load_referenced_collections_uses_compiled_find_operations(self):
        collection = _FakeCollection([])
        cursor = AsyncAggregationCursor(
            collection,
            [
                {"$lookup": {"from": "roles", "localField": "role_id", "foreignField": "_id", "as": "roles"}},
                {"$unionWith": {"pipeline": []}},
            ],
            comment="agg-trace",
            max_time_ms=15,
        )

        loaded = await cursor._load_referenced_collections()

        self.assertEqual(loaded["roles"], [{"_id": "r1", "label": "admin"}])
        self.assertEqual(loaded[_CURRENT_COLLECTION_RESOLVER_KEY], [{"_id": "1", "name": "Ada"}])
        self.assertEqual(len(collection._engine.scan_semantics_calls), 2)
        for db_name, coll_name, semantics, kwargs in collection._engine.scan_semantics_calls:
            self.assertEqual(db_name, "db")
            self.assertIn(coll_name, {"coll", "roles"})
            self.assertEqual(semantics.filter_spec, {})
            self.assertEqual(semantics.comment, "agg-trace")
            self.assertEqual(semantics.max_time_ms, 15)
            self.assertIsNone(semantics.projection)
            self.assertEqual(kwargs["context"], None)

    async def test_materialize_keeps_remaining_pipeline_when_prefix_breaks(self):
        collection = _FakeCollection(
            [
                {"kind": "view"},
                {"kind": "click"},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [
                {"$project": {"kind": 1, "_id": 0}},
                {"$sort": {"kind": 1}},
            ],
        )

        documents = await cursor.to_list()

        self.assertEqual(
            collection.calls,
            [
                {
                    "filter_spec": {},
                    "projection": {"kind": 1, "_id": 0},
                    "sort": None,
                    "skip": 0,
                    "limit": None,
                    "hint": None,
                    "comment": None,
                    "max_time_ms": None,
                    "batch_size": None,
                    "session": None,
                }
            ],
        )
        self.assertEqual(documents, [{"kind": "click"}, {"kind": "view"}])

    async def test_materialize_propagates_aggregate_cursor_options_to_find(self):
        collection = _FakeCollection([{"_id": "1", "kind": "view"}])
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}],
            hint="kind_1",
            comment="trace",
            max_time_ms=5,
            batch_size=10,
            allow_disk_use=True,
            let={"tenant": "a"},
        )

        await cursor.to_list()

        self.assertEqual(collection.calls[0]["hint"], "kind_1")
        self.assertEqual(collection.calls[0]["comment"], "trace")
        self.assertEqual(collection.calls[0]["max_time_ms"], 5)
        self.assertEqual(collection.calls[0]["batch_size"], 10)

    async def test_streaming_batch_execution_splits_find_calls_for_streamable_pipeline(self):
        collection = _FakeCollection(
            [
                {"_id": "1", "kind": "view", "rank": 1},
                {"_id": "2", "kind": "view", "rank": 2},
                {"_id": "3", "kind": "view", "rank": 3},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}],
            batch_size=2,
        )

        documents = await cursor.to_list()

        self.assertEqual(documents, collection._documents)
        self.assertEqual(
            [call["skip"] for call in collection.calls],
            [0, 2, 3],
        )
        self.assertEqual(
            [call["limit"] for call in collection.calls],
            [2, 2, 2],
        )

    async def test_streaming_batch_execution_handles_trailing_skip_and_limit_globally(self):
        collection = _FakeCollection(
            [
                {"_id": "1", "kind": "view", "rank": 1},
                {"_id": "2", "kind": "view", "rank": 2},
                {"_id": "3", "kind": "view", "rank": 3},
                {"_id": "4", "kind": "view", "rank": 4},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}, {"$skip": 1}, {"$limit": 2}],
            batch_size=2,
        )

        documents = await cursor.to_list()

        self.assertEqual(
            documents,
            [
                {"_id": "2", "kind": "view", "rank": 2},
                {"_id": "3", "kind": "view", "rank": 3},
            ],
        )

    async def test_streaming_batch_execution_skips_full_first_page_and_honors_zero_remaining_limit(self):
        collection = _FakeCollection(
            [
                {"_id": "1", "kind": "view", "rank": 1},
                {"_id": "2", "kind": "view", "rank": 2},
                {"_id": "3", "kind": "view", "rank": 3},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}, {"$skip": 2}, {"$limit": 1}],
            batch_size=2,
        )

        self.assertEqual(await cursor.to_list(), [{"_id": "3", "kind": "view", "rank": 3}])

    async def test_streaming_batch_execution_falls_back_for_global_pipeline_stages(self):
        collection = _FakeCollection(
            [
                {"_id": "1", "kind": "view", "rank": 2},
                {"_id": "2", "kind": "view", "rank": 1},
                {"_id": "3", "kind": "view", "rank": 3},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [{"$group": {"_id": "$kind", "count": {"$sum": 1}}}],
            batch_size=2,
        )

        documents = await cursor.to_list()

        self.assertEqual(documents, [{"_id": "view", "count": 3}])
        self.assertEqual(len(collection.calls), 1)
        explanation = await cursor.explain()
        self.assertFalse(explanation["streaming_batch_execution"])
        self.assertEqual(explanation["pushdown"]["mode"], "pipeline-prefix")
        self.assertEqual(explanation["pushdown"]["pushedDownStages"], 0)
        self.assertEqual(explanation["pushdown"]["remainingStages"], 1)

    async def test_async_aggregation_cursor_explain_includes_engine_plan_and_options(self):
        collection = _FakeCollection([{"_id": "1", "kind": "view"}])
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}],
            hint="kind_1",
            comment="trace",
            max_time_ms=5,
            batch_size=10,
            allow_disk_use=True,
            let={"tenant": "a"},
        )

        explanation = await cursor.explain()

        self.assertEqual(explanation["engine_plan"], {"engine": "fake", "details": ["IXSCAN"]})
        self.assertEqual(explanation["pushdown"]["mode"], "pipeline-prefix")
        self.assertEqual(explanation["pushdown"]["totalStages"], 1)
        self.assertEqual(explanation["pushdown"]["pushedDownStages"], 1)
        self.assertEqual(explanation["pushdown"]["remainingStages"], 0)
        self.assertEqual(explanation["pushdown"]["streamableStageCount"], 0)
        self.assertEqual(explanation["hint"], "kind_1")
        self.assertEqual(explanation["comment"], "trace")
        self.assertEqual(explanation["max_time_ms"], 5)
        self.assertEqual(explanation["batch_size"], 10)
        self.assertTrue(explanation["allow_disk_use"])
        self.assertEqual(explanation["let"], {"tenant": "a"})
        self.assertTrue(explanation["streaming_batch_execution"])
        self.assertEqual(explanation["cxp"]["interface"], "database/mongodb")
        self.assertEqual(explanation["cxp"]["provider"], "mongoeco")
        self.assertEqual(explanation["cxp"]["capability"], "aggregation")
        explain_semantics = collection._engine.explain_semantics_calls[0][2]
        self.assertEqual(explain_semantics.hint, "kind_1")
        self.assertEqual(explain_semantics.comment, "trace")
        self.assertEqual(explain_semantics.max_time_ms, 5)

    async def test_async_aggregation_cursor_explain_covers_search_fallback_and_first_empty_profile(self):
        collection = _FakeCollection([])
        collection._engine.aggregation_spill_policy = lambda: AggregationSpillPolicy(threshold=2)
        profiled = []

        async def _search_documents(*args, **kwargs):
            del args, kwargs
            return []

        collection._engine.search_documents = _search_documents

        async def _profile_operation(**kwargs):
            profiled.append(kwargs)

        collection._profile_operation = _profile_operation  # type: ignore[attr-defined]
        cursor = AsyncAggregationCursor(
            collection,
            [{"$search": {"text": {"query": "Ada", "path": "name"}}}],
            batch_size=1,
        )

        explanation = await cursor.explain()
        self.assertEqual(explanation["engine_plan"]["plan"], "unsupported-search-engine")
        self.assertEqual(explanation["pushdown"]["mode"], "search")
        self.assertEqual(explanation["pushdown"]["leadingSearchOperator"], "$search")
        self.assertEqual(explanation["pushdown"]["pushedDownStages"], 1)
        self.assertEqual(explanation["cxp"]["capability"], "aggregation")
        self.assertEqual(explanation["cxp"]["additionalCapabilities"], ["search"])
        self.assertEqual(cursor._spill_policy().threshold, 2)
        self.assertIsNone(await cursor.first())
        self.assertEqual(profiled[-1]["op"], "command")

    async def test_aggregation_cursor_profile_and_collstats_cover_no_scale_and_exception_paths(self):
        collection = _FakeCollection([{"_id": "1"}])
        profiled = []

        async def _profile_operation(**kwargs):
            profiled.append(kwargs)

        collection._profile_operation = _profile_operation  # type: ignore[attr-defined]
        cursor = AsyncAggregationCursor(collection, [{"$project": {"_id": 1}}])
        with patch.object(cursor, "_stream_batches", side_effect=RuntimeError("boom")):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                await cursor.to_list()
        self.assertEqual(profiled[-1]["errmsg"], "boom")

        self.assertEqual(await cursor._load_collstats_snapshots([{"$match": {"x": 1}}]), {})

    async def test_build_pushdown_cursor_uses_collection_find_when_private_builder_is_missing(self):
        collection = _FakeCollection([{"_id": "1"}])
        collection._build_cursor = None  # type: ignore[assignment]
        cursor = AsyncAggregationCursor(collection, [{"$match": {"_id": "1"}}])

        await cursor._build_pushdown_cursor(cursor._pushdown_find_operation()).to_list()

        self.assertEqual(collection.calls[0]["filter_spec"], {"_id": "1"})

    async def test_async_aggregation_cursor_explain_surfaces_deferred_reason_details(self):
        collection = _FakeCollection([{"_id": "1", "kind": "view"}])
        cursor = AsyncAggregationCursor(collection, [{"$match": {"kind": "view"}}])
        cursor._operation = cursor._operation.with_overrides(
            planning_mode=PlanningMode.RELAXED,
            planning_issues=(PlanningIssue(scope="aggregate", message="unsupported stage"),),
        )

        explanation = await cursor.explain()

        self.assertEqual(explanation["planning_mode"], "relaxed")
        self.assertEqual(explanation["planning_issues"][0]["scope"], "aggregate")
        self.assertEqual(
            explanation["engine_plan"]["details"]["reason"],
            "operation has deferred planning issues (relaxed): aggregate: unsupported stage",
        )
        self.assertEqual(explanation["cxp"]["capability"], "aggregation")

    async def test_stream_batches_use_compiled_pushdown_operations(self):
        collection = _FakeCollection(
            [
                {"_id": "1", "kind": "view", "rank": 1},
                {"_id": "2", "kind": "view", "rank": 2},
                {"_id": "3", "kind": "view", "rank": 3},
            ]
        )
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}],
            batch_size=2,
        )

        documents = await cursor.to_list()

        self.assertEqual(len(documents), 3)
        self.assertGreaterEqual(len(collection.built_operations), 2)
        self.assertEqual(collection.built_operations[0][0].limit, 2)
        self.assertEqual(collection.built_operations[1][0].skip, 2)

    async def test_materialize_enforces_max_time_deadline(self):
        collection = _FakeCollection([{"_id": "1", "kind": "view"}])
        cursor = AsyncAggregationCursor(
            collection,
            [{"$match": {"kind": "view"}}],
            max_time_ms=1,
        )

        with patch("mongoeco.api._async.aggregation_cursor.enforce_deadline", side_effect=ExecutionTimeout("operation exceeded time limit")):
            with self.assertRaises(ExecutionTimeout):
                await cursor.to_list()

    async def test_materialize_rejects_large_blocking_pipeline_without_spill_budget(self):
        collection = _FakeCollection([{"_id": "1", "kind": "a"}, {"_id": "2", "kind": "a"}])
        collection._engine.aggregation_cost_policy = AggregationCostPolicy(max_materialized_documents=1)
        cursor = AsyncAggregationCursor(
            collection,
            compile_aggregate_operation(
                [{"$group": {"_id": "$kind", "count": {"$sum": 1}}}],
                allow_disk_use=False,
            ),
        )

        with self.assertRaises(OperationFailure):
            await cursor.to_list()

    async def test_materialize_allows_large_blocking_pipeline_when_spill_is_available(self):
        collection = _FakeCollection([{"_id": "1", "kind": "a"}, {"_id": "2", "kind": "a"}])
        collection._engine.aggregation_cost_policy = AggregationCostPolicy(max_materialized_documents=1)
        cursor = AsyncAggregationCursor(
            collection,
            compile_aggregate_operation(
                [{"$group": {"_id": "$kind", "count": {"$sum": 1}}}],
                allow_disk_use=True,
            ),
        )

        self.assertEqual(await cursor.to_list(), [{"_id": "a", "count": 2}])

    async def test_aggregation_cursor_merge_helpers_cover_validation_and_target_paths(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        source = database.get_collection("source")
        cursor = AsyncAggregationCursor(source, [])

        self.assertEqual(cursor._target_database("db").name, "db")
        self.assertEqual(cursor._target_database("analytics").name, "analytics")
        self.assertEqual(cursor._split_terminal_writeback_stage([]), ([], None))
        self.assertEqual(cursor._split_terminal_writeback_stage([{"$match": {"x": 1}}]), ([{"$match": {"x": 1}}], None))
        with self.assertRaisesRegex(OperationFailure, "only supported as the final aggregation stage"):
            cursor._split_terminal_writeback_stage(
                [{"$merge": {"into": "archive"}}, {"$merge": {"into": "archive"}}]
            )

        with self.assertRaisesRegex(OperationFailure, "document specification"):
            await cursor._apply_merge_stage([], [])
        with self.assertRaisesRegex(OperationFailure, "collection name or \\{db, coll\\}"):
            await cursor._apply_merge_stage([], {"into": 1})
        with self.assertRaisesRegex(OperationFailure, "non-empty string"):
            await cursor._apply_merge_stage([], {"into": {"db": "", "coll": "archive"}})
        with self.assertRaisesRegex(OperationFailure, "omitted on or on: '_id'"):
            await cursor._apply_merge_stage([], {"into": "archive", "on": "slug"})
        with self.assertRaisesRegex(OperationFailure, "whenMatched currently supports"):
            await cursor._apply_merge_stage([], {"into": "archive", "whenMatched": "pipeline"})
        with self.assertRaisesRegex(OperationFailure, "whenNotMatched currently supports"):
            await cursor._apply_merge_stage([], {"into": "archive", "whenNotMatched": "upsert"})
        with self.assertRaisesRegex(OperationFailure, "pipelines are not supported"):
            await cursor._apply_merge_stage([], {"into": "archive", "whenMatched": []})
        with self.assertRaisesRegex(OperationFailure, "requires documents with _id"):
            await cursor._apply_merge_stage([{"value": 1}], {"into": "archive"})

        await cursor._apply_merge_stage([{"_id": "1", "value": 1}], {"into": "archive", "whenNotMatched": "discard"})
        self.assertIsNone(await database.get_collection("archive").find_one({"_id": "1"}))
        with self.assertRaisesRegex(OperationFailure, "whenNotMatched=fail"):
            await cursor._apply_merge_stage([{"_id": "1", "value": 1}], {"into": "archive", "whenNotMatched": "fail"})

        await database.get_collection("archive").insert_one({"_id": "2", "value": 10, "extra": True})
        await cursor._apply_merge_stage([{"_id": "2", "value": 20}], {"into": "archive", "whenMatched": "keepExisting"})
        self.assertEqual((await database.get_collection("archive").find_one({"_id": "2"}))["value"], 10)
        with self.assertRaisesRegex(OperationFailure, "whenMatched=fail"):
            await cursor._apply_merge_stage([{"_id": "2", "value": 20}], {"into": "archive", "whenMatched": "fail"})
        await cursor._apply_merge_stage([{"_id": "2", "value": 30}], {"into": "archive", "whenMatched": "replace"})
        self.assertEqual((await database.get_collection("archive").find_one({"_id": "2"}))["value"], 30)
        await cursor._apply_merge_stage([{"_id": "2", "value": 40, "merged": True}], {"into": "archive", "whenMatched": "merge"})
        merged = await database.get_collection("archive").find_one({"_id": "2"})
        self.assertEqual(merged["value"], 40)
        self.assertTrue(merged["merged"])

    async def test_aggregation_cursor_collstats_and_stream_batches_cover_fallback_branches(self):
        collection = _FakeCollection([{"_id": "1"}, {"_id": "2"}])
        cursor = AsyncAggregationCursor(collection, [{"$project": {"_id": 1}}], batch_size=2)

        self.assertEqual(cursor._collect_collstats_scales([{"$collStats": {"storageStats": {"scale": 4}}}]), {4})
        self.assertEqual(cursor._collect_collstats_scales([{"$collStats": {"count": {}}}, {"$collStats": []}]), {1})

        search_cursor = AsyncAggregationCursor(collection, [{"$search": {"text": {"query": "Ada", "path": "name"}}}], batch_size=2)
        with patch.object(search_cursor, "_materialize", return_value=[{"_id": "a"}]) as materialize:
            self.assertEqual([document async for document in search_cursor._stream_batches()], [{"_id": "a"}])
            materialize.assert_awaited_once()

        merge_cursor = AsyncAggregationCursor(collection, [{"$match": {"x": 1}}, {"$merge": {"into": "archive"}}], batch_size=2)
        with patch.object(merge_cursor, "_materialize", return_value=[{"_id": "m"}]) as materialize:
            self.assertEqual([document async for document in merge_cursor._stream_batches()], [{"_id": "m"}])
            materialize.assert_awaited_once()

        unbatched_cursor = AsyncAggregationCursor(collection, [{"$project": {"_id": 1}}], batch_size=None)
        with patch.object(unbatched_cursor, "_materialize", return_value=[{"_id": "u"}]) as materialize:
            self.assertEqual([document async for document in unbatched_cursor._stream_batches()], [{"_id": "u"}])
            materialize.assert_awaited_once()

        with patch.object(cursor, "_split_streamable_pipeline", return_value=None), patch.object(cursor, "_materialize", return_value=[{"_id": "p"}]) as materialize:
            self.assertEqual([document async for document in cursor._stream_batches()], [{"_id": "p"}])
            materialize.assert_awaited_once()

        stream_cursor = AsyncAggregationCursor(collection, [{"$project": {"_id": 1}}, {"$skip": 1}, {"$limit": 1}], batch_size=2)
        with (
            patch(
                "mongoeco.api._async.aggregation_cursor.split_pushdown_pipeline",
                return_value=SimpleNamespace(
                    filter_spec={},
                    projection=None,
                    sort=None,
                    remaining_pipeline=[{"$project": {"_id": 1}}, {"$skip": 1}, {"$limit": 1}],
                    skip=0,
                    limit=3,
                ),
            ),
            patch.object(stream_cursor, "_split_streamable_pipeline", return_value=([{"$project": {"_id": 1}}], 1, 1)),
            patch.object(stream_cursor, "_load_referenced_collections", return_value={}),
            patch.object(stream_cursor, "_spill_policy", return_value=None),
        ):
            batches = [
                [{"_id": "1"}, {"_id": "2"}],
                [{"_id": "3"}],
                [],
            ]

            class _BatchCursor:
                def __init__(self, docs):
                    self._docs = docs

                async def to_list(self):
                    return self._docs

            stream_cursor._build_pushdown_cursor = lambda _operation: _BatchCursor(batches.pop(0))  # type: ignore[method-assign]
            self.assertEqual([document async for document in stream_cursor._stream_batches()], [{"_id": "2"}])

    async def test_aggregation_cursor_additional_stream_and_merge_branches(self):
        database = AsyncDatabase(MemoryEngine(), "db")
        source = database.get_collection("source")
        await source.insert_many([{"_id": "1"}, {"_id": "2"}])
        cursor = AsyncAggregationCursor(source, [], batch_size=2)

        self.assertEqual(cursor._split_terminal_writeback_stage(["bad-stage"]), (["bad-stage"], None))  # type: ignore[list-item]
        with self.assertRaisesRegex(OperationFailure, "final aggregation stage"):
            cursor._split_terminal_writeback_stage(
                [
                    {"$merge": {"into": "archive"}},
                    {"$project": {"_id": 1}},
                    {"$merge": {"into": "archive"}},
                ]
            )
        with self.assertRaisesRegex(OperationFailure, "non-empty string"):
            await cursor._apply_merge_stage([{"_id": "1"}], {"into": {"db": "analytics"}})

        merge_cursor = AsyncAggregationCursor(source, [{"$merge": {"into": "archive"}}], batch_size=2)
        with patch.object(merge_cursor, "_materialize", return_value=[{"_id": "m"}]) as materialize:
            self.assertEqual([document async for document in merge_cursor._stream_batches()], [{"_id": "m"}])
            materialize.assert_awaited_once()

        with (
            patch.object(cursor, "_leading_search_stage", return_value=None),
            patch.object(cursor, "_effective_pipeline", return_value=[{"$project": {"_id": 1}}]),
            patch("mongoeco.api._async.aggregation_cursor.split_pushdown_pipeline", return_value=SimpleNamespace(remaining_pipeline=[], skip=0, limit=None)),
            patch.object(cursor, "_split_streamable_pipeline", return_value=([], 0, 0)),
        ):
            self.assertEqual([document async for document in cursor._stream_batches()], [])

    async def test_stream_batches_consumes_entire_batch_when_trailing_skip_exceeds_transformed_size(self):
        collection = _FakeCollection([{"_id": "1"}, {"_id": "2"}, {"_id": "3"}])
        cursor = AsyncAggregationCursor(collection, [{"$project": {"_id": 1}}, {"$skip": 3}], batch_size=2)
        with (
            patch(
                "mongoeco.api._async.aggregation_cursor.split_pushdown_pipeline",
                return_value=SimpleNamespace(
                    filter_spec={},
                    projection=None,
                    sort=None,
                    remaining_pipeline=[{"$project": {"_id": 1}}, {"$skip": 3}],
                    skip=0,
                    limit=None,
                ),
            ),
            patch.object(cursor, "_split_streamable_pipeline", return_value=([{"$project": {"_id": 1}}], 3, None)),
            patch.object(cursor, "_load_referenced_collections", return_value={}),
            patch.object(cursor, "_spill_policy", return_value=None),
        ):
            batches = [[{"_id": "1"}, {"_id": "2"}], [{"_id": "3"}], []]

            class _BatchCursor:
                def __init__(self, docs):
                    self._docs = docs

                async def to_list(self):
                    return self._docs

            cursor._build_pushdown_cursor = lambda _operation: _BatchCursor(batches.pop(0))  # type: ignore[method-assign]
            self.assertEqual([document async for document in cursor._stream_batches()], [])


class SyncAggregationCursorTests(unittest.TestCase):
    def test_closed_message_remains_stable(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        cursor.close()

        with self.assertRaisesRegex(InvalidOperation, "cannot use aggregation cursor after it has been closed"):
            cursor.to_list()

    def test_first_uses_direct_async_first_path_without_materializing_cache(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(cursor.first(), {"_id": "1"})
        self.assertEqual(async_cursor.first_calls, 1)
        self.assertEqual(async_cursor.to_list_calls, 0)

    def test_close_is_idempotent_and_blocks_further_use(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        cursor.close()
        cursor.close()

        with self.assertRaises(InvalidOperation):
            cursor.first()
        with self.assertRaises(InvalidOperation):
            cursor.to_list()
        with self.assertRaises(InvalidOperation):
            list(cursor)

    def test_explain_delegates_to_async_cursor(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        async def _explain():
            return {"engine": "fake", "details": ["SCAN"]}
        async_cursor.explain = _explain  # type: ignore[method-assign]
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        explanation = cursor.explain()
        self.assertEqual(explanation["engine"], "fake")
        self.assertEqual(explanation["details"], ["SCAN"])

    def test_iteration_closes_active_async_iterator_on_early_break(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        for document in cursor:
            self.assertEqual(document, {"_id": "1"})
            break

        self.assertEqual(async_cursor.close_calls, 1)

    def test_close_closes_active_async_iterator(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)
        iterator = iter(cursor)

        self.assertEqual(next(iterator), {"_id": "1"})
        cursor.close()

        self.assertEqual(async_cursor.close_calls, 1)

    def test_reiterating_after_partial_consumption_continues_from_current_position(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertEqual(list(cursor), [{"_id": "2"}])

    def test_first_uses_current_position_after_iteration_starts(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertEqual(cursor.first(), {"_id": "2"})

    def test_iterator_stops_when_closed(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)
        iterator = iter(cursor)

        self.assertIs(iter(iterator), iterator)
        self.assertEqual(next(iterator), {"_id": "1"})
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_iterator_stops_if_replaced_by_another_active_iterator(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)
        iterator = iter(cursor)
        cursor._active_async_iterable = object()

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_iterator_del_swallows_close_errors(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_BrokenSyncClientStub(), async_cursor)
        iterator = iter(cursor)

        iterator.__del__()
        self.assertTrue(iterator._closed)

    def test_first_returns_none_when_active_iterator_is_exhausted(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)
        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertIsNone(cursor.first())

    def test_cursor_stays_exhausted_after_full_iteration(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(list(cursor), [{"_id": "1"}, {"_id": "2"}])
        self.assertEqual(list(cursor), [])
        self.assertEqual(cursor.to_list(), [])
        self.assertIsNone(cursor.first())
        self.assertEqual(async_cursor.close_calls, 1)

    def test_iter_uses_cache_when_loaded(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(cursor.to_list(), [{"_id": "1"}])
        self.assertEqual(list(cursor), [{"_id": "1"}])

    def test_iterator_raises_stop_iteration_when_closed_explicitly(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)
        iterator = iter(cursor)

        iterator.close()

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_first_returns_none_for_loaded_empty_cache(self):
        async_cursor = _AsyncAggregationCursorStub([])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(cursor.to_list(), [])
        self.assertIsNone(cursor.first())

    def test_del_swallows_close_errors(self):
        async_cursor = _AsyncAggregationCursorStub([{"_id": "1"}])
        cursor = AggregationCursor(_SyncClientStub(), async_cursor)

        def broken_close() -> None:
            raise RuntimeError("boom")

        cursor.close = broken_close
        cursor.__del__()

        self.assertTrue(cursor._closed)
