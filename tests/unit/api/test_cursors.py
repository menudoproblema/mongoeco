import unittest
from unittest.mock import patch

import mongoeco.api._async.cursor as async_cursor_module
from mongoeco.api._async.cursor import AsyncCursor, _DEFAULT_LOCAL_PREFETCH_SIZE
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._async.listing_cursor import AsyncListingCursor
from mongoeco.api._async.search_index_cursor import AsyncSearchIndexCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.api._sync.listing_cursor import ListingCursor
from mongoeco.api._sync.search_index_cursor import SearchIndexCursor
from mongoeco.core.query_plan import MatchAll
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.types import PlanningIssue, PlanningMode


class _AsyncEngineStub:
    def __init__(self, documents):
        self._documents = documents
        self.explain_semantics_calls = []

    async def _scan(self, *, skip=0, limit=None):
        documents = self._documents[skip:]
        if limit is not None:
            documents = documents[:limit]
        for document in documents:
            yield document

    def scan_find_semantics(self, db_name, coll_name, semantics, **kwargs):
        return self._scan(skip=semantics.skip, limit=semantics.limit)

    async def explain_find_semantics(self, *args, **kwargs):
        self.explain_semantics_calls.append((args, kwargs))
        return {"engine": "stub", "details": ["IXSCAN"]}


class _AsyncCollectionStub:
    def __init__(self, documents):
        self._engine = _AsyncEngineStub(documents)
        self._db_name = "db"
        self._collection_name = "coll"

    def find(self, *args, **kwargs):
        return AsyncCursor(self, {}, MatchAll(), None)


class _ProfiledAsyncCollectionStub(_AsyncCollectionStub):
    def __init__(self, documents):
        super().__init__(documents)
        self.profile_calls = []

    async def _profile_operation(self, **payload):
        self.profile_calls.append(payload)


class _PlanningIssueCollectionStub(_AsyncCollectionStub):
    planning_mode = PlanningMode.RELAXED

    def _ensure_operation_executable(self, operation):
        if operation.planning_issues:
            raise OperationFailure(async_cursor_module._operation_issue_message(operation))


class _UnsupportedExplainEngineStub(_AsyncEngineStub):
    async def explain_find_semantics(self, *args, **kwargs):
        return object()


class _UnsupportedExplainCollectionStub(_AsyncCollectionStub):
    def __init__(self, documents):
        super().__init__(documents)
        self._engine = _UnsupportedExplainEngineStub(documents)


class _BatchTrackingScanStub:
    def __init__(self, documents):
        self._documents = iter(documents)
        self.pull_count = 0
        self.yield_count = 0
        self.close_calls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.pull_count += 1
        try:
            value = next(self._documents)
            self.yield_count += 1
            return value
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        self.close_calls += 1


class _BatchTrackingEngineStub:
    def __init__(self, documents):
        self._documents = documents
        self.created_scans = []

    def scan_find_semantics(self, db_name, coll_name, semantics, **kwargs):
        skip = semantics.skip
        limit = semantics.limit
        documents = self._documents[skip:]
        if limit is not None:
            documents = documents[:limit]
        scan = _BatchTrackingScanStub(documents)
        self.created_scans.append(scan)
        return scan

    async def explain_find_semantics(self, *args, **kwargs):
        return {"engine": "tracking"}


class _BatchTrackingCollectionStub:
    def __init__(self, documents):
        self._engine = _BatchTrackingEngineStub(documents)
        self._db_name = "db"
        self._collection_name = "coll"


class _BatchTrackingFindCollectionStub:
    def __init__(self, documents):
        self._collection = _BatchTrackingCollectionStub(documents)

    @property
    def last_scan(self):
        scans = self._collection._engine.created_scans
        return scans[-1] if scans else None

    def find(self, *args, **kwargs):
        return AsyncCursor(
            self._collection,
            {},
            MatchAll(),
            None,
            batch_size=kwargs.get("batch_size"),
        )


class _AsyncCursorFactoryStub:
    def __init__(self, documents):
        self._documents = documents
        self.calls = 0
        self.first_calls = 0

    def find(self, *args, **kwargs):
        self.calls += 1
        cursor = AsyncCursor(_AsyncCollectionStub(self._documents), {}, MatchAll(), None)
        original_first = cursor.first

        async def _first():
            self.first_calls += 1
            return await original_first()

        cursor.first = _first
        return cursor


class _SyncClientStub:
    def _run(self, awaitable):
        import asyncio

        return asyncio.run(awaitable)


class _CountingSyncClientStub:
    def __init__(self):
        self.run_calls = 0

    def _run(self, awaitable):
        import asyncio

        self.run_calls += 1
        return asyncio.run(awaitable)


class _BrokenSyncClientStub:
    def _run(self, awaitable):
        close = getattr(awaitable, "close", None)
        if callable(close):
            close()
        raise RuntimeError("boom")


class _StreamingAsyncCursorStub:
    def __init__(self, documents):
        self._documents = iter(documents)
        self.close_calls = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self._documents)
        except StopIteration as exc:
            raise StopAsyncIteration from exc

    async def aclose(self):
        self.close_calls += 1


class _StreamingAsyncCollectionStub:
    def __init__(self, documents):
        self._documents = documents
        self.calls = 0
        self.last_cursor = None

    def find(self, *args, **kwargs):
        self.calls += 1
        self.last_cursor = _StreamingAsyncCursorStub(self._documents)
        return self.last_cursor


class CursorUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_cursor_private_helpers_cover_issue_messages_and_planning_mode_resolution(self):
        operation = type(
            "Operation",
            (),
            {
                "planning_mode": PlanningMode.RELAXED,
                "planning_issues": (PlanningIssue(scope="find", message="blocked"),),
            },
        )()

        self.assertEqual(
            async_cursor_module._operation_issue_message(operation),
            "operation has deferred planning issues (relaxed): find: blocked",
        )
        no_scope_operation = type(
            "Operation",
            (),
            {
                "planning_mode": PlanningMode.RELAXED,
                "planning_issues": (type("Issue", (), {"message": "fallback only"})(),),
            },
        )()
        self.assertEqual(
            async_cursor_module._operation_issue_message(no_scope_operation),
            "operation has deferred planning issues (relaxed): fallback only",
        )
        no_details_operation = type(
            "Operation",
            (),
            {
                "planning_mode": PlanningMode.RELAXED,
                "planning_issues": (),
            },
        )()
        self.assertEqual(
            async_cursor_module._operation_issue_message(no_details_operation),
            "operation has deferred planning issues (relaxed)",
        )
        with self.assertRaisesRegex(OperationFailure, "blocked"):
            async_cursor_module._ensure_operation_executable(object(), operation)
        self.assertEqual(
            async_cursor_module._resolve_planning_mode(type("Collection", (), {"planning_mode": PlanningMode.RELAXED})()),
            PlanningMode.RELAXED,
        )
        self.assertEqual(
            async_cursor_module._resolve_planning_mode(type("Collection", (), {"_planning_mode": PlanningMode.RELAXED})()),
            PlanningMode.RELAXED,
        )
        self.assertEqual(
            async_cursor_module._resolve_planning_mode(object()),
            PlanningMode.STRICT,
        )

    async def test_async_cursor_private_helpers_cover_sort_normalization_and_serialization_errors(self):
        self.assertEqual(async_cursor_module._normalize_sort_spec(("_id", 1)), [("_id", 1)])
        self.assertEqual(
            async_cursor_module._normalize_sort_spec((("name", 1),)),
            [("name", 1)],
        )
        with self.assertRaises(TypeError):
            async_cursor_module._serialize_explanation(object())

    async def test_async_cursor_private_helpers_accept_to_document_and_cover_limit_exhaustion(self):
        class _ExplainStub:
            def to_document(self):
                return {"ok": 1}

        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}]), {}, MatchAll(), None, limit=1)

        self.assertEqual(async_cursor_module._serialize_explanation(_ExplainStub()), {"ok": 1})
        self.assertEqual(await cursor._fetch_batch(1, 5), [])
        self.assertIs(cursor.collection, cursor._collection)

    async def test_async_index_cursor_supports_first_and_to_list(self):
        cursor = AsyncIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "_id_", "key": {"_id": 1}},
                    {"name": "email_1", "key": {"email": 1}},
                ]
            )
        )

        self.assertEqual(await cursor.first(), {"name": "_id_", "key": {"_id": 1}})
        self.assertEqual(
            await cursor.to_list(),
            [
                {"name": "_id_", "key": {"_id": 1}},
                {"name": "email_1", "key": {"email": 1}},
            ],
        )

    async def test_async_index_cursor_supports_rewind_clone_close_and_alive(self):
        cursor = AsyncIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "_id_", "key": {"_id": 1}},
                ]
            )
        )

        self.assertTrue(cursor.alive)
        self.assertEqual(await cursor.first(), {"name": "_id_", "key": {"_id": 1}})
        self.assertTrue(cursor.alive)
        cursor.rewind()
        self.assertTrue(cursor.alive)
        clone = cursor.clone()
        self.assertEqual(await clone.to_list(), [{"name": "_id_", "key": {"_id": 1}}])
        cursor.close()
        self.assertFalse(cursor.alive)
        with self.assertRaises(InvalidOperation):
            await cursor.to_list()

    async def test_async_index_cursor_first_does_not_exhaust_cursor(self):
        cursor = AsyncIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "_id_", "key": {"_id": 1}},
                    {"name": "email_1", "key": {"email": 1}},
                ]
            )
        )

        self.assertEqual(await cursor.first(), {"name": "_id_", "key": {"_id": 1}})
        self.assertTrue(cursor.alive)
        self.assertEqual(
            await cursor.to_list(),
            [
                {"name": "_id_", "key": {"_id": 1}},
                {"name": "email_1", "key": {"email": 1}},
            ],
        )

    async def test_async_search_index_cursor_supports_first_to_list_rewind_clone_and_close(self):
        cursor = AsyncSearchIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "default", "type": "search", "queryable": True},
                    {"name": "by_text", "type": "search", "queryable": False},
                ]
            )
        )

        self.assertTrue(cursor.alive)
        self.assertEqual(
            await cursor.first(),
            {"name": "default", "type": "search", "queryable": True},
        )
        self.assertTrue(cursor.alive)
        clone = cursor.clone()
        self.assertEqual(
            await clone.to_list(),
            [
                {"name": "default", "type": "search", "queryable": True},
                {"name": "by_text", "type": "search", "queryable": False},
            ],
        )
        cursor.rewind()
        self.assertTrue(cursor.alive)
        cursor.close()
        self.assertFalse(cursor.alive)
        with self.assertRaises(InvalidOperation):
            await cursor.to_list()

    async def test_async_listing_cursor_supports_first_to_list_and_close(self):
        cursor = AsyncListingCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "events", "type": "collection"},
                    {"name": "logs", "type": "collection"},
                ]
            )
        )

        self.assertEqual(await cursor.first(), {"name": "events", "type": "collection"})
        self.assertEqual(
            await cursor.to_list(),
            [
                {"name": "events", "type": "collection"},
                {"name": "logs", "type": "collection"},
            ],
        )
        cursor.close()
        with self.assertRaises(InvalidOperation):
            await cursor.to_list()

    async def test_async_listing_cursor_supports_rewind_clone_and_alive(self):
        cursor = AsyncListingCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "events", "type": "collection"},
                ]
            )
        )

        self.assertTrue(cursor.alive)
        self.assertEqual(await cursor.first(), {"name": "events", "type": "collection"})
        self.assertTrue(cursor.alive)
        cursor.rewind()
        self.assertTrue(cursor.alive)
        clone = cursor.clone()
        self.assertEqual(await clone.to_list(), [{"name": "events", "type": "collection"}])
        cursor.close()
        self.assertFalse(cursor.alive)
        with self.assertRaises(InvalidOperation):
            await cursor.to_list()

    async def test_async_cursor_rejects_negative_skip_and_limit_and_returns_none_first(self):
        cursor = AsyncCursor(_AsyncCollectionStub([]), {}, MatchAll(), None)

        with self.assertRaises(ValueError):
            cursor.skip(-1)

        with self.assertRaises(ValueError):
            cursor.limit(-1)

        self.assertIsNone(await cursor.first())

    async def test_async_cursor_validates_sort_spec_eagerly(self):
        cursor = AsyncCursor(_AsyncCollectionStub([]), {}, MatchAll(), None)

        cursor.sort({"name": 1})  # type: ignore[arg-type]
        self.assertEqual(cursor._sort, [("name", 1)])
        with self.assertRaises(TypeError):
            cursor.sort([("name", 1), "bad"])  # type: ignore[list-item]
        with self.assertRaises(TypeError):
            cursor.sort([(1, 1)])  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            cursor.sort([("name", 0)])
        with self.assertRaises(TypeError):
            cursor.batch_size("10")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            cursor.batch_size(-1)
        with self.assertRaises(TypeError):
            cursor.max_time_ms("5")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            cursor.max_time_ms(-1)
        cursor.hint({"name": 1})  # type: ignore[arg-type]
        self.assertEqual(cursor._hint, [("name", 1)])
        with self.assertRaises(ValueError):
            cursor.hint("")

    async def test_async_cursor_first_does_not_mutate_limit(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}, {"_id": "2"}]), {}, MatchAll(), None)

        first = await cursor.first()
        documents = await cursor.to_list()

        self.assertEqual(first, {"_id": "1"})
        self.assertEqual(documents, [{"_id": "1"}, {"_id": "2"}])

    async def test_async_cursor_to_list_uses_first_fast_path_for_limit_one(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = factory.find().limit(1)

        self.assertEqual(await cursor.to_list(), [{"_id": "1"}])
        self.assertEqual(factory.calls, 1)
        self.assertEqual(factory.first_calls, 1)

    async def test_async_cursor_first_respects_zero_limit(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}]), {}, MatchAll(), None).limit(0)

        self.assertIsNone(await cursor.first())
        self.assertEqual(await cursor.to_list(), [])

    async def test_async_cursor_rejects_mutation_after_iteration_starts(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}, {"_id": "2"}]), {}, MatchAll(), None)

        iterator = cursor.__aiter__()
        first = await iterator.__anext__()

        self.assertEqual(first, {"_id": "1"})
        with self.assertRaises(InvalidOperation):
            cursor.limit(1)
        with self.assertRaises(InvalidOperation):
            cursor.skip(1)
        with self.assertRaises(InvalidOperation):
            cursor.sort([("_id", 1)])
        with self.assertRaises(InvalidOperation):
            cursor.batch_size(10)

    async def test_async_cursor_batch_size_prefetches_local_batches(self):
        collection = _BatchTrackingCollectionStub([{"_id": "1"}, {"_id": "2"}, {"_id": "3"}])
        cursor = AsyncCursor(collection, {}, MatchAll(), None, batch_size=2)

        iterator = cursor.__aiter__()
        first = await iterator.__anext__()

        self.assertEqual(first, {"_id": "1"})
        self.assertEqual(collection._engine.created_scans[0].yield_count, 2)
        self.assertEqual(await cursor.to_list(), [{"_id": "2"}, {"_id": "3"}])

    async def test_async_cursor_batch_iterator_stops_when_ownership_is_lost(self):
        collection = _BatchTrackingCollectionStub([{"_id": "1"}])
        cursor = AsyncCursor(collection, {}, MatchAll(), None, batch_size=1)
        iterator = cursor._iter()
        cursor._active_async_iterable = object()

        self.assertEqual(await iterator.pull_chunk(1), [])

    async def test_async_cursor_batch_iterator_closes_when_fetch_batch_returns_no_documents(self):
        cursor = AsyncCursor(_AsyncCollectionStub([]), {}, MatchAll(), None, batch_size=1)
        iterator = cursor._iter()

        self.assertEqual(await iterator.pull_chunk(1), [])

    async def test_async_cursor_uses_local_prefetch_when_batch_size_is_none(self):
        documents = [{"_id": str(index)} for index in range(_DEFAULT_LOCAL_PREFETCH_SIZE + 10)]
        collection = _BatchTrackingCollectionStub(documents)
        cursor = AsyncCursor(collection, {}, MatchAll(), None)

        self.assertEqual(await cursor.to_list(), documents)
        self.assertEqual(len(collection._engine.created_scans), 1)
        self.assertEqual(collection._engine.created_scans[0].yield_count, len(documents))
        self.assertIsNone(cursor._as_operation().batch_size)

    async def test_async_cursor_uses_default_prefetch_size_when_batch_size_is_zero(self):
        documents = [{"_id": str(index)} for index in range(_DEFAULT_LOCAL_PREFETCH_SIZE + 10)]
        collection = _BatchTrackingCollectionStub(documents)
        cursor = AsyncCursor(collection, {}, MatchAll(), None, batch_size=0)

        self.assertEqual(await cursor.to_list(), documents)
        self.assertEqual(len(collection._engine.created_scans), 2)
        self.assertEqual(collection._engine.created_scans[0].yield_count, _DEFAULT_LOCAL_PREFETCH_SIZE)

    async def test_async_cursor_reuses_compiled_semantics_across_local_batches(self):
        documents = [{"_id": str(index)} for index in range(_DEFAULT_LOCAL_PREFETCH_SIZE + 10)]
        collection = _BatchTrackingCollectionStub(documents)
        cursor = AsyncCursor(collection, {}, MatchAll(), None, batch_size=0)

        from mongoeco.engines import semantic_core

        original_compile = semantic_core.compile_find_semantics_from_operation
        with patch(
            "mongoeco.engines.semantic_core.compile_find_semantics_from_operation",
            wraps=original_compile,
        ) as compile_semantics:
            self.assertEqual(await cursor.to_list(), documents)

        self.assertEqual(compile_semantics.call_count, 1)

    async def test_async_cursor_stays_exhausted_until_rewind(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}, {"_id": "2"}]), {}, MatchAll(), None)

        self.assertEqual(await cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])
        self.assertEqual(await cursor.to_list(), [])
        self.assertIsNone(await cursor.first())
        cursor.rewind()
        self.assertEqual(await cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])

    async def test_async_cursor_clone_rewind_alive_and_explain(self):
        collection = _AsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = AsyncCursor(
            collection,
            {},
            MatchAll(),
            None,
            sort=[("_id", 1)],
            skip=1,
            limit=2,
            hint="name_1",
            comment="trace",
            max_time_ms=5,
            batch_size=10,
        )

        clone = cursor.clone()
        self.assertIsNot(clone, cursor)
        self.assertEqual(clone._sort, [("_id", 1)])
        self.assertEqual(clone._skip, 1)
        self.assertEqual(clone._limit, 2)
        self.assertEqual(clone._hint, "name_1")
        self.assertEqual(clone._comment, "trace")
        self.assertEqual(clone._max_time_ms, 5)
        self.assertEqual(clone._batch_size, 10)
        self.assertTrue(cursor.alive)

        self.assertEqual(await cursor.to_list(), [{"_id": "2"}])
        self.assertFalse(cursor.alive)

        cursor.rewind()
        self.assertTrue(cursor.alive)
        self.assertEqual(await cursor.first(), {"_id": "2"})

        self.assertEqual(await cursor.explain(), {"engine": "stub", "details": ["IXSCAN"]})
        semantics = collection._engine.explain_semantics_calls[0][0][2]
        self.assertEqual(semantics.sort, [("_id", 1)])
        self.assertEqual(semantics.hint, "name_1")
        self.assertEqual(semantics.comment, "trace")
        self.assertEqual(semantics.max_time_ms, 5)

    async def test_async_cursor_accepts_dict_sort_and_hint(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}]), {}, MatchAll(), None)

        cursor.sort({"_id": 1})  # type: ignore[arg-type]
        cursor.hint({"name": 1})  # type: ignore[arg-type]

        self.assertEqual(cursor._sort, [("_id", 1)])
        self.assertEqual(cursor._hint, [("name", 1)])

    async def test_async_cursor_comment_batch_size_and_max_time_ms_mutators_invalidate_caches(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}]), {}, MatchAll(), None)
        cursor._operation_cache = object()
        cursor._semantics_cache = object()

        self.assertIs(cursor.comment("trace"), cursor)
        self.assertEqual(cursor._comment, "trace")
        self.assertIsNone(cursor._operation_cache)
        self.assertIsNone(cursor._semantics_cache)

        cursor._operation_cache = object()
        cursor._semantics_cache = object()
        self.assertIs(cursor.max_time_ms(5), cursor)
        self.assertEqual(cursor._max_time_ms, 5)
        self.assertIsNone(cursor._operation_cache)
        self.assertIsNone(cursor._semantics_cache)

        cursor._operation_cache = object()
        cursor._semantics_cache = object()
        self.assertIs(cursor.batch_size(2), cursor)
        self.assertEqual(cursor._batch_size, 2)
        self.assertIsNone(cursor._operation_cache)
        self.assertIsNone(cursor._semantics_cache)

    async def test_async_cursor_iterator_stops_when_replaced_or_closed(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}]), {}, MatchAll(), None, batch_size=1)
        iterator = cursor._iter()

        cursor._active_async_iterable = object()
        with self.assertRaises(StopAsyncIteration):
            await iterator.__anext__()

        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}]), {}, MatchAll(), None, batch_size=1)
        iterator = cursor._iter()
        self.assertEqual(await iterator.pull_chunk(0), [])
        await iterator.close()
        self.assertEqual(await iterator.pull_chunk(1), [])

    async def test_async_cursor_first_profiles_active_iterator_results_and_errors(self):
        collection = _ProfiledAsyncCollectionStub([{"_id": "1"}])
        cursor = AsyncCursor(collection, {}, MatchAll(), None)
        active = cursor._iter(limit=1, enforce_ownership=False)
        cursor._active_async_iterable = active

        self.assertEqual(await cursor.first(), {"_id": "1"})
        self.assertEqual(collection.profile_calls[-1]["op"], "query")
        self.assertNotIn("errmsg", collection.profile_calls[-1])

        collection = _ProfiledAsyncCollectionStub([{"_id": "1"}])
        cursor = AsyncCursor(collection, {}, MatchAll(), None)

        class _BrokenIterator:
            async def __anext__(self):
                raise RuntimeError("boom")

        cursor._active_async_iterable = _BrokenIterator()
        with self.assertRaisesRegex(RuntimeError, "boom"):
            await cursor.first()
        self.assertEqual(collection.profile_calls[-1]["errmsg"], "boom")

    async def test_async_cursor_first_returns_none_when_active_iterator_is_exhausted(self):
        cursor = AsyncCursor(_AsyncCollectionStub([]), {}, MatchAll(), None, batch_size=1)
        active = cursor._iter()
        cursor._active_async_iterable = active

        self.assertIsNone(await cursor.first())

    async def test_async_cursor_rewind_clears_active_iterator_and_explain_handles_planning_issues(self):
        collection = _AsyncCollectionStub([{"_id": "1"}])
        cursor = AsyncCursor(collection, {}, MatchAll(), None)
        operation = cursor._as_operation().with_overrides(
            planning_mode=PlanningMode.RELAXED,
            planning_issues=(PlanningIssue(scope="find", message="unsupported"),),
        )
        cursor._operation_cache = operation
        cursor._active_async_iterable = object()

        cursor.rewind()
        self.assertIsNone(cursor._active_async_iterable)

        explanation = await cursor.explain()
        self.assertEqual(explanation["engine"], "planner")
        self.assertEqual(explanation["planning_mode"], "relaxed")
        self.assertEqual(explanation["planning_issues"][0]["message"], "unsupported")
        self.assertEqual(
            explanation["details"]["reason"],
            "operation has deferred planning issues (relaxed): find: unsupported",
        )

        self.assertIs(cursor.skip(1), cursor)
        self.assertEqual(cursor._skip, 1)

    async def test_async_cursor_explain_rejects_unsupported_engine_result_shape(self):
        cursor = AsyncCursor(_UnsupportedExplainCollectionStub([{"_id": "1"}]), {}, MatchAll(), None)

        with self.assertRaises(TypeError):
            await cursor.explain()


    def test_sync_cursor_rejects_negative_skip_and_limit_and_supports_iteration(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}]), {}, None)

        with self.assertRaises(ValueError):
            cursor.skip(-1)

        with self.assertRaises(ValueError):
            cursor.limit(-1)

        self.assertEqual(list(cursor), [{"_id": "1"}, {"_id": "2"}])

    def test_sync_cursor_explain_surfaces_deferred_reason_details(self):
        collection = _PlanningIssueCollectionStub([{"_id": "1"}])
        cursor = Cursor(_SyncClientStub(), collection, {"$where": "this.a > 1"}, None)

        explanation = cursor.explain()

        self.assertEqual(explanation["engine"], "planner")
        self.assertEqual(explanation["planning_mode"], "relaxed")
        self.assertEqual(
            explanation["details"]["reason"],
            "operation has deferred planning issues (relaxed): query: Unsupported top-level query operator: $where",
        )

    def test_sync_cursor_accepts_dict_sort_and_hint(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}]), {}, None)

        cursor.sort({"_id": 1})  # type: ignore[arg-type]
        cursor.hint({"name": 1})  # type: ignore[arg-type]

        self.assertEqual(cursor._sort, [("_id", 1)])
        self.assertEqual(cursor._hint, [("name", 1)])

    def test_sync_cursor_validates_sort_spec_eagerly(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}]), {}, None)

        cursor.sort({"name": 1})  # type: ignore[arg-type]
        self.assertEqual(cursor._sort, [("name", 1)])
        with self.assertRaises(TypeError):
            cursor.sort([("name", 1), "bad"])  # type: ignore[list-item]
        with self.assertRaises(ValueError):
            cursor.sort([("name", True)])
        with self.assertRaises(TypeError):
            cursor.batch_size("10")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            cursor.batch_size(-1)
        with self.assertRaises(TypeError):
            cursor.max_time_ms("5")  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            cursor.max_time_ms(-1)
        cursor.hint({"name": 1})  # type: ignore[arg-type]
        self.assertEqual(cursor._hint, [("name", 1)])
        with self.assertRaises(ValueError):
            cursor.hint("")

    def test_sync_cursor_first_uses_cache_when_already_loaded(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None)

        self.assertEqual(cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])
        self.assertEqual(cursor.first(), {"_id": "1"})
        self.assertEqual(factory.calls, 1)

    def test_sync_cursor_public_error_messages_remain_stable(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}]), {}, None)

        list(cursor)
        with self.assertRaisesRegex(InvalidOperation, "cannot modify cursor after iteration has started"):
            cursor.sort([("_id", 1)])

        cursor.close()
        with self.assertRaisesRegex(InvalidOperation, "cannot use cursor after it has been closed"):
            cursor.first()

    def test_sync_cursor_first_uses_direct_first_path_without_materializing_cache(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None)

        self.assertEqual(cursor.first(), {"_id": "1"})
        self.assertEqual(factory.calls, 1)
        self.assertEqual(factory.first_calls, 1)

    def test_sync_cursor_iteration_uses_first_fast_path_for_limit_one(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None).limit(1)

        self.assertEqual(list(cursor), [{"_id": "1"}])
        self.assertEqual(factory.calls, 1)
        self.assertEqual(factory.first_calls, 1)

    def test_sync_cursor_rejects_sort_change_after_materialization(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None)

        self.assertEqual(cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])
        with self.assertRaises(InvalidOperation):
            cursor.sort([("rank", 1)])

        self.assertEqual(factory.calls, 1)

    def test_sync_cursor_rejects_mutation_after_iteration_starts(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}]), {}, None)

        self.assertEqual(cursor.first(), {"_id": "1"})
        with self.assertRaises(InvalidOperation):
            cursor.limit(1)
        with self.assertRaises(InvalidOperation):
            cursor.skip(1)
        with self.assertRaises(InvalidOperation):
            cursor.sort([("_id", 1)])
        with self.assertRaises(InvalidOperation):
            cursor.batch_size(10)

    def test_sync_cursor_batch_size_prefetches_local_batches(self):
        collection = _BatchTrackingFindCollectionStub([{"_id": "1"}, {"_id": "2"}, {"_id": "3"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None, batch_size=2)

        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertIsNotNone(collection.last_scan)
        self.assertEqual(collection.last_scan.yield_count, 2)
        self.assertEqual(list(cursor), [{"_id": "2"}, {"_id": "3"}])

    def test_sync_cursor_uses_local_prefetch_when_batch_size_is_none(self):
        documents = [{"_id": str(index)} for index in range(3)]
        collection = _BatchTrackingFindCollectionStub(documents)
        cursor = Cursor(_SyncClientStub(), collection, {}, None)

        self.assertEqual(cursor.to_list(), documents)
        self.assertEqual(len(collection._collection._engine.created_scans), 1)
        self.assertEqual(collection.last_scan.yield_count, len(documents))
        self.assertIsNone(cursor._as_operation().batch_size)

    def test_sync_cursor_iteration_streams_without_forcing_prefetch_when_batch_size_is_none(self):
        documents = [{"_id": str(index)} for index in range(_DEFAULT_LOCAL_PREFETCH_SIZE + 10)]
        collection = _BatchTrackingFindCollectionStub(documents)
        cursor = Cursor(_SyncClientStub(), collection, {}, None)

        self.assertEqual(list(cursor), documents)
        self.assertEqual(len(collection._collection._engine.created_scans), 1)
        self.assertEqual(collection.last_scan.yield_count, len(documents))

    def test_sync_cursor_iteration_batches_runner_calls(self):
        documents = [{"_id": str(index)} for index in range(_DEFAULT_LOCAL_PREFETCH_SIZE + 10)]
        collection = _BatchTrackingFindCollectionStub(documents)
        client = _CountingSyncClientStub()
        cursor = Cursor(client, collection, {}, None)

        self.assertEqual(list(cursor), documents)
        self.assertLess(client.run_calls, len(documents))

    def test_sync_cursor_close_is_idempotent_and_blocks_further_use(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}]), {}, None)

        cursor.close()
        cursor.close()

        with self.assertRaises(InvalidOperation):
            cursor.first()
        with self.assertRaises(InvalidOperation):
            cursor.to_list()
        with self.assertRaises(InvalidOperation):
            list(cursor)

    def test_sync_cursor_iteration_closes_active_async_iterator_on_early_break(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)

        for document in cursor:
            self.assertEqual(document, {"_id": "1"})
            break

        self.assertEqual(collection.calls, 1)
        self.assertEqual(collection.last_cursor.close_calls, 1)

    def test_sync_cursor_close_closes_active_async_iterator(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)
        iterator = iter(cursor)

        self.assertEqual(next(iterator), {"_id": "1"})
        cursor.close()

        self.assertEqual(collection.last_cursor.close_calls, 1)

    def test_sync_cursor_reiteration_continues_from_current_position(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)

        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertEqual(list(cursor), [{"_id": "2"}])
        self.assertEqual(collection.calls, 1)

    def test_sync_cursor_first_uses_current_position_after_iteration_starts(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)

        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertEqual(cursor.first(), {"_id": "2"})
        self.assertEqual(collection.calls, 1)

    def test_sync_cursor_iterator_stops_when_closed(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)
        iterator = iter(cursor)

        self.assertIs(iter(iterator), iterator)
        self.assertEqual(next(iterator), {"_id": "1"})
        with self.assertRaises(StopIteration):
            next(iterator)

    def test_sync_cursor_iterator_stops_if_replaced_by_another_active_iterator(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)
        iterator = iter(cursor)
        cursor._active_async_iterable = object()

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_sync_cursor_iterator_del_swallows_close_errors(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}])
        cursor = Cursor(_BrokenSyncClientStub(), collection, {}, None)
        iterator = iter(cursor)

        iterator.__del__()
        self.assertTrue(iterator._closed)

    def test_sync_cursor_first_returns_none_when_active_iterator_is_exhausted(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)
        iterator = iter(cursor)
        self.assertEqual(next(iterator), {"_id": "1"})

        self.assertIsNone(cursor.first())

    def test_sync_cursor_stays_exhausted_after_full_iteration(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)

        self.assertEqual(list(cursor), [{"_id": "1"}, {"_id": "2"}])
        self.assertEqual(list(cursor), [])
        self.assertEqual(cursor.to_list(), [])
        self.assertIsNone(cursor.first())
        self.assertEqual(collection.calls, 1)

    def test_sync_cursor_iter_uses_cache_when_loaded(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None)

        self.assertEqual(cursor.to_list(), [{"_id": "1"}])
        self.assertEqual(list(cursor), [{"_id": "1"}])

    def test_sync_cursor_iterator_raises_stop_iteration_when_closed_explicitly(self):
        collection = _StreamingAsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), collection, {}, None)
        iterator = iter(cursor)

        iterator.close()

        with self.assertRaises(StopIteration):
            next(iterator)

    def test_sync_cursor_del_swallows_close_errors(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}]), {}, None)

        def broken_close() -> None:
            raise RuntimeError("boom")

        cursor.close = broken_close
        cursor.__del__()

        self.assertTrue(cursor._closed)

    def test_sync_cursor_clone_rewind_alive_and_explain(self):
        collection = _AsyncCollectionStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(
            _SyncClientStub(),
            collection,
            {},
            None,
            sort=[("_id", 1)],
            skip=1,
            limit=2,
            hint="name_1",
            comment="trace",
            max_time_ms=5,
            batch_size=10,
        )

        clone = cursor.clone()
        self.assertIsNot(clone, cursor)
        self.assertEqual(clone._sort, [("_id", 1)])
        self.assertEqual(clone._hint, "name_1")
        self.assertTrue(cursor.alive)

        self.assertEqual(cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])
        self.assertFalse(cursor.alive)

        cursor.rewind()
        self.assertTrue(cursor.alive)
        self.assertEqual(cursor.first(), {"_id": "1"})
        self.assertEqual(cursor.explain(), {"engine": "stub", "details": ["IXSCAN"]})
        self.assertEqual(collection._engine.explain_semantics_calls[0][1]["context"], None)
        semantics = collection._engine.explain_semantics_calls[0][0][2]
        self.assertEqual(semantics.hint, "name_1")
        self.assertEqual(semantics.comment, "trace")
        self.assertEqual(semantics.max_time_ms, 5)

    def test_sync_index_cursor_supports_first_to_list_and_close(self):
        async_cursor = AsyncIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "_id_", "key": {"_id": 1}},
                    {"name": "email_1", "key": {"email": 1}},
                ]
            )
        )
        cursor = IndexCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(cursor.first(), {"name": "_id_", "key": {"_id": 1}})
        self.assertEqual(
            cursor.to_list(),
            [
                {"name": "_id_", "key": {"_id": 1}},
                {"name": "email_1", "key": {"email": 1}},
            ],
        )
        cursor.close()
        with self.assertRaises(InvalidOperation):
            cursor.to_list()

    def test_sync_index_cursor_supports_rewind_clone_and_alive(self):
        async_cursor = AsyncIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "_id_", "key": {"_id": 1}},
                ]
            )
        )
        cursor = IndexCursor(_SyncClientStub(), async_cursor)

        self.assertTrue(cursor.alive)
        self.assertEqual(cursor.first(), {"name": "_id_", "key": {"_id": 1}})
        self.assertTrue(cursor.alive)
        cursor.rewind()
        self.assertTrue(cursor.alive)
        clone = cursor.clone()
        self.assertEqual(clone.to_list(), [{"name": "_id_", "key": {"_id": 1}}])
        cursor.close()
        self.assertFalse(cursor.alive)
        with self.assertRaises(InvalidOperation):
            cursor.to_list()

    def test_sync_index_cursor_first_does_not_exhaust_cursor(self):
        async_cursor = AsyncIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "_id_", "key": {"_id": 1}},
                    {"name": "email_1", "key": {"email": 1}},
                ]
            )
        )
        cursor = IndexCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(cursor.first(), {"name": "_id_", "key": {"_id": 1}})
        self.assertTrue(cursor.alive)
        self.assertEqual(
            cursor.to_list(),
            [
                {"name": "_id_", "key": {"_id": 1}},
                {"name": "email_1", "key": {"email": 1}},
            ],
        )

    def test_sync_listing_cursor_supports_first_to_list_and_close(self):
        async_cursor = AsyncListingCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "events", "type": "collection"},
                    {"name": "logs", "type": "collection"},
                ]
            )
        )
        cursor = ListingCursor(_SyncClientStub(), async_cursor)

        self.assertEqual(cursor.first(), {"name": "events", "type": "collection"})
        self.assertEqual(
            cursor.to_list(),
            [
                {"name": "events", "type": "collection"},
                {"name": "logs", "type": "collection"},
            ],
        )
        cursor.close()
        with self.assertRaises(InvalidOperation):
            cursor.to_list()

    def test_sync_listing_cursor_supports_rewind_clone_and_alive(self):
        async_cursor = AsyncListingCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "events", "type": "collection"},
                ]
            )
        )
        cursor = ListingCursor(_SyncClientStub(), async_cursor)

        self.assertTrue(cursor.alive)
        self.assertEqual(cursor.first(), {"name": "events", "type": "collection"})
        self.assertTrue(cursor.alive)
        cursor.rewind()
        self.assertTrue(cursor.alive)
        clone = cursor.clone()
        self.assertEqual(clone.to_list(), [{"name": "events", "type": "collection"}])
        cursor.close()
        self.assertFalse(cursor.alive)
        with self.assertRaises(InvalidOperation):
            cursor.to_list()

    def test_sync_search_index_cursor_supports_first_to_list_rewind_clone_and_close(self):
        async_cursor = AsyncSearchIndexCursor(
            lambda: self._load_indexes_async(
                [
                    {"name": "default", "type": "search", "queryable": True},
                    {"name": "by_text", "type": "search", "queryable": False},
                ]
            )
        )
        cursor = SearchIndexCursor(_SyncClientStub(), async_cursor)

        self.assertTrue(cursor.alive)
        self.assertEqual(
            cursor.first(),
            {"name": "default", "type": "search", "queryable": True},
        )
        self.assertEqual(
            cursor.to_list(),
            [
                {"name": "default", "type": "search", "queryable": True},
                {"name": "by_text", "type": "search", "queryable": False},
            ],
        )
        cursor.rewind()
        clone = cursor.clone()
        self.assertEqual(
            clone.to_list(),
            [
                {"name": "default", "type": "search", "queryable": True},
                {"name": "by_text", "type": "search", "queryable": False},
            ],
        )
        cursor.close()
        self.assertFalse(cursor.alive)
        with self.assertRaises(InvalidOperation):
            cursor.to_list()

    async def _load_indexes_async(self, indexes):
        return indexes
