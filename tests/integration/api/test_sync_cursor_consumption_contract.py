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
            assert cursor.to_list(length=0) == []
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
