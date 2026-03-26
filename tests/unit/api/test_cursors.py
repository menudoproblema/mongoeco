import unittest
from unittest.mock import patch

from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.api._async.index_cursor import AsyncIndexCursor
from mongoeco.api._sync.cursor import Cursor
from mongoeco.api._sync.index_cursor import IndexCursor
from mongoeco.core.query_plan import MatchAll
from mongoeco.errors import InvalidOperation


class _AsyncEngineStub:
    def __init__(self, documents):
        self._documents = documents
        self.explain_calls = []

    async def _scan(self, limit=None):
        documents = self._documents if limit is None else self._documents[:limit]
        for document in documents:
            yield document

    def scan_collection(self, *args, **kwargs):
        return self._scan(limit=kwargs.get("limit"))

    async def explain_query_plan(self, *args, **kwargs):
        self.explain_calls.append((args, kwargs))
        return {"engine": "stub", "details": ["COLLSCAN"]}


class _AsyncCollectionStub:
    def __init__(self, documents):
        self._engine = _AsyncEngineStub(documents)
        self._db_name = "db"
        self._collection_name = "coll"

    def find(self, *args, **kwargs):
        return AsyncCursor(self, {}, MatchAll(), None)


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

    async def test_async_cursor_rejects_negative_skip_and_limit_and_returns_none_first(self):
        cursor = AsyncCursor(_AsyncCollectionStub([]), {}, MatchAll(), None)

        with self.assertRaises(ValueError):
            cursor.skip(-1)

        with self.assertRaises(ValueError):
            cursor.limit(-1)

        self.assertIsNone(await cursor.first())

    async def test_async_cursor_validates_sort_spec_eagerly(self):
        cursor = AsyncCursor(_AsyncCollectionStub([]), {}, MatchAll(), None)

        with self.assertRaises(TypeError):
            cursor.sort({"name": 1})  # type: ignore[arg-type]
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
        with self.assertRaises(TypeError):
            cursor.hint({"name": 1})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            cursor.hint("")

    async def test_async_cursor_first_does_not_mutate_limit(self):
        cursor = AsyncCursor(_AsyncCollectionStub([{"_id": "1"}, {"_id": "2"}]), {}, MatchAll(), None)

        first = await cursor.first()
        documents = await cursor.to_list()

        self.assertEqual(first, {"_id": "1"})
        self.assertEqual(documents, [{"_id": "1"}, {"_id": "2"}])

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

        self.assertEqual(await cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])
        self.assertFalse(cursor.alive)

        cursor.rewind()
        self.assertTrue(cursor.alive)
        self.assertEqual(await cursor.first(), {"_id": "1"})

        self.assertEqual(await cursor.explain(), {"engine": "stub", "details": ["COLLSCAN"]})
        self.assertEqual(collection._engine.explain_calls[0][1]["sort"], [("_id", 1)])


    def test_sync_cursor_rejects_negative_skip_and_limit_and_supports_iteration(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}]), {}, None)

        with self.assertRaises(ValueError):
            cursor.skip(-1)

        with self.assertRaises(ValueError):
            cursor.limit(-1)

        self.assertEqual(list(cursor), [{"_id": "1"}, {"_id": "2"}])

    def test_sync_cursor_validates_sort_spec_eagerly(self):
        cursor = Cursor(_SyncClientStub(), _AsyncCursorFactoryStub([{"_id": "1"}]), {}, None)

        with self.assertRaises(TypeError):
            cursor.sort({"name": 1})  # type: ignore[arg-type]
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
        with self.assertRaises(TypeError):
            cursor.hint({"name": 1})  # type: ignore[arg-type]
        with self.assertRaises(ValueError):
            cursor.hint("")

    def test_sync_cursor_first_uses_cache_when_already_loaded(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None)

        self.assertEqual(cursor.to_list(), [{"_id": "1"}, {"_id": "2"}])
        self.assertEqual(cursor.first(), {"_id": "1"})
        self.assertEqual(factory.calls, 1)
        self.assertEqual(factory.first_calls, 0)

    def test_sync_cursor_first_uses_direct_first_path_without_materializing_cache(self):
        factory = _AsyncCursorFactoryStub([{"_id": "1"}, {"_id": "2"}])
        cursor = Cursor(_SyncClientStub(), factory, {}, None)

        self.assertEqual(cursor.first(), {"_id": "1"})
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
        self.assertEqual(cursor.explain(), {"engine": "stub", "details": ["COLLSCAN"]})

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

    async def _load_indexes_async(self, indexes):
        return indexes
