"""One cursor position across iteration, first and bounded list consumption."""

from unittest.mock import patch

import pytest

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_mixed_consumption_drains_prefetched_documents_in_order(engine_factory):
    documents = [{"_id": index} for index in range(17)]
    with MongoClient(engine_factory()) as client:
        client.test.records.insert_many(documents)
        cursor = client.test.records.find({}).sort("_id", 1).batch_size(4)
        iterator = iter(cursor)
        try:
            assert next(iterator) == documents[0]
            with pytest.raises(ValueError, match="greater than 0"):
                cursor.to_list(length=0)
            assert cursor.to_list(length=2) == documents[1:3]
            assert cursor.first() == documents[3]
            assert next(iterator) == documents[4]
            assert cursor.to_list() == documents[5:]
            assert cursor.to_list(length=1) == []
            assert cursor.first() is None
            assert list(iterator) == []
        finally:
            cursor.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_bounded_list_consumption_crosses_runner_by_batch(engine_factory):
    size = 1000
    batch_size = 64
    documents = [{"_id": index} for index in range(size)]
    with MongoClient(engine_factory()) as client:
        client.test.records.insert_many(documents)
        cursor = client.test.records.find({}).sort("_id", 1).batch_size(batch_size)
        try:
            with patch.object(client, "_run", wraps=client._run) as runner:
                assert cursor.to_list(length=size) == documents
            maximum_crossings = (size + batch_size - 1) // batch_size + 1
            assert runner.call_count <= maximum_crossings
            assert cursor.to_list(length=1) == []
        finally:
            cursor.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_known_find_eof_crosses_runner_only_for_owned_cleanup(engine_factory):
    with MongoClient(engine_factory()) as client:
        collection = client.test.records
        collection.insert_one({"_id": 1})
        cursor = collection.find({}, sort=[("_id", 1)], limit=1)
        iterator = iter(cursor)
        try:
            assert next(iterator) == {"_id": 1}
            with patch.object(client, "_run", wraps=client._run) as runner:
                assert list(iterator) == []
            assert runner.call_count == 1
            assert cursor._active_async_iterable is None
            assert cursor._async_cursor._active_async_iterable is None
        finally:
            cursor.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_known_find_eof_still_surfaces_retired_cleanup_failure(engine_factory):
    expected_closes = 2

    class Source:
        closes = 0

        async def aclose(self):
            self.closes += 1
            if self.closes == 1:
                message = "retired cleanup"
                raise ValueError(message)

    with MongoClient(engine_factory()) as client:
        collection = client.test.records
        collection.insert_one({"_id": 1})
        cursor = collection.find({})
        source = Source()
        try:
            iterator = iter(cursor)
            assert next(iterator) == {"_id": 1}
            cursor._async_cursor._retired_sources.append(source)
            with pytest.raises(ValueError, match="retired cleanup"):
                next(iterator)
        finally:
            cursor.close()
        assert source.closes == expected_closes


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("kind", ["find", "aggregate"])
def test_temporary_iteration_keeps_snapshot_and_allows_followup_writes(
    engine_factory, kind
):
    documents = [{"_id": index} for index in range(257)]
    with MongoClient(engine_factory()) as client:
        collection = client.test.records
        collection.insert_many(documents)
        cursor = (
            collection.find({}, sort=[("_id", 1)], batch_size=2)
            if kind == "find"
            else collection.aggregate([{"$sort": {"_id": 1}}], batchSize=2)
        )
        try:
            assert next(iter(cursor)) == documents[0]
            collection.insert_one({"_id": 999})
            assert cursor.to_list() == documents[1:]
            assert collection.find_one({"_id": 999}) == {"_id": 999}
        finally:
            cursor.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("started", [False, True])
def test_aggregate_close_reaches_the_real_async_cursor(engine_factory, started):
    with MongoClient(engine_factory()) as client:
        collection = client.test.records
        collection.insert_many([{"_id": index} for index in range(257)])
        cursor = collection.aggregate([], batchSize=1)
        owner = cursor._async_aggregation_cursor
        with patch.object(owner, "close", wraps=owner.close) as close:
            if started:
                assert next(iter(cursor)) == {"_id": 0}
            cursor.close()
            cursor.close()
        assert close.call_count == 1
        assert owner._closed
