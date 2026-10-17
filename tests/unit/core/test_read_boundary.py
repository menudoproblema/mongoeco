"""The shared read opening preserves provider contracts and execution phases."""

import sqlite3
import threading

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine, semantic_core
from mongoeco.engines._sqlite_reader import SQLiteScanReader
from mongoeco.engines.semantic_core import compile_find_semantics


@pytest.mark.parametrize("mode", ["first", "batches"])
@pytest.mark.asyncio
async def test_collection_source_accepts_plain_async_iterables_and_typed_windows(mode):
    async with AsyncMongoClient(MemoryEngine()) as client:
        collection = client.reads.items
        opened = []
        closed = []
        lookups = []

        def provider(operation, *, session):
            opened.append((operation, session))

            async def rows():
                try:
                    values = [{"_id": i} for i in range(4)][operation.skip :]
                    if operation.limit is not None:
                        values = values[: operation.limit]
                    for value in values:
                        yield value
                finally:
                    closed.append(True)

            return rows()

        configured_limit = 2
        cursor = collection.find({}).skip(1).limit(configured_limit).batch_size(1)
        context = cursor._operation_context

        def source_factory(_collection):
            lookups.append(True)
            return provider

        with patch.object(
            type(collection), "_engine_scan_with_operation", property(source_factory)
        ):
            if mode == "first":
                assert await cursor.first() == {"_id": 1}
            else:
                assert await cursor.to_list() == [{"_id": 1}, {"_id": 2}]
        assert len(opened) == 1
        assert lookups == [True]
        operation, session = opened[0]
        assert operation.context is context
        assert session is context.session
        assert operation.skip == 1
        assert operation.limit == (1 if mode == "first" else configured_limit)
        assert cursor._limit == configured_limit
        assert cursor._semantics_cache is None
        assert closed == [True]
        await cursor.close()


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.asyncio
async def test_fallback_read_source_reuses_context_and_renews_only_execution_deadline(
    engine_type,
):
    async with AsyncMongoClient(engine_type()) as client:
        collection = client.reads.items
        await collection.insert_many([{"_id": i} for i in range(4)])
        configured_limit = 2
        cursor = (
            collection.find({})
            .skip(1)
            .limit(configured_limit)
            .batch_size(1)
            .max_time_ms(1000)
        )
        context = cursor._operation_context
        with (
            patch.object(collection, "_engine_scan_with_operation", None),
            patch.object(
                semantic_core, "operation_deadline", return_value=float("inf")
            ) as clock,
        ):
            assert clock.call_count == 0
            assert await cursor.to_list() == [{"_id": 1}, {"_id": 2}]
            assert clock.call_count == 1
            first_semantics = cursor._base_semantics()
            assert first_semantics.operation_context is context
            cursor.rewind()
            assert clock.call_count == 1
            assert await cursor.to_list() == [{"_id": 1}, {"_id": 2}]
            assert clock.call_count == 1 + 1
            assert cursor._base_semantics() is not first_semantics
            assert cursor._operation_context is context
        await cursor.close()


def test_production_reader_stop_closes_its_dedicated_connection(tmp_path):
    engine = SQLiteEngine(str(tmp_path / "reader.sqlite"))
    with MongoClient(engine) as client:
        client.reads.items.insert_many([{"_id": i} for i in range(3)])
        stopped = threading.Event()
        reader = SQLiteScanReader(
            engine,
            "reads",
            "items",
            compile_find_semantics({}, sort=[("_id", 1)]),
            stop_event=stopped,
        )
        try:
            assert reader.fetch(1, 1024).documents == [{"_id": 0}]
            connection = reader.connection
            assert reader.owns_connection
            stopped.set()
            assert reader.fetch(1, 1024).documents == []
            assert reader.closed
            assert engine._active_scan_count == 0
            assert reader not in engine._runtime_state.scan_readers
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")
        finally:
            reader.close()
