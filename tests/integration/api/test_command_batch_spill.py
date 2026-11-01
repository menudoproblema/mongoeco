"""The same protocol references hold when aggregation outputs spill."""

import asyncio

from pathlib import Path
from unittest.mock import patch

import pymongo
import pytest

from mongoeco import AsyncMongoClient
from mongoeco.core.aggregation.spill import AggregationSortSpool
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire.proxy import AsyncMongoEcoProxyServer

from tests.differential._real_parity_base import _normalize
from tests.differential.semantic_command_batch_cases import (
    SEED,
    SEMANTIC_COMMAND_BATCH_CASES,
)
from tests.integration.api.test_semantic_group_regressions import _capture


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize(["name", "action"], SEMANTIC_COMMAND_BATCH_CASES)
def test_protocol_batches_with_forced_spill(dialect, factory, name, action):
    paths = []
    original = AggregationSortSpool._write_run

    def write_run(spool, documents):
        path = original(spool, documents)
        paths.append(Path(path))
        return path

    async def run():
        async with AsyncMongoEcoProxyServer(
            engine=factory(aggregation_spill_threshold=1), mongodb_dialect=dialect
        ) as proxy:

            def observe():
                with pymongo.MongoClient(
                    f"mongodb://{proxy.address.host}:{proxy.address.port}"
                ) as client:
                    collection = client.batch_spill.rows
                    collection.insert_many(SEED)
                    assert _normalize(action(collection)) == _normalize(
                        _capture(dialect, "command_batch")["cases"][name]
                    )

            await asyncio.to_thread(observe)

    with patch.object(AggregationSortSpool, "_write_run", write_run):
        asyncio.run(run())
    if name in {"semantic_command_batch_shapes", "semantic_command_batch_limits"}:
        assert paths
    assert all(not path.exists() for path in paths)


def test_close_while_opening_owns_spill_output_before_delegate_starts():
    paths = []
    original_write = AggregationSortSpool._write_run

    def write_run(spool, documents):
        path = original_write(spool, documents)
        paths.append(Path(path))
        return path

    async def run():
        async with AsyncMongoClient(
            MemoryEngine(aggregation_spill_threshold=1), mongodb_dialect="9.0"
        ) as client:
            collection = client.test.rows
            await collection.insert_many(SEED)
            cursor = await collection.aggregate(
                [{"$group": {"_id": "$_id", "v": {"$sum": "$v"}}}], batchSize=0
            )
            opened, resume = asyncio.Event(), asyncio.Event()
            original_open = cursor._open_batch_stream

            async def paused_open():
                delegate = await original_open()
                opened.set()
                await resume.wait()
                return delegate

            with patch.object(cursor, "_open_batch_stream", paused_open):
                reading = asyncio.create_task(cursor._read_response(1))
                try:
                    await opened.wait()
                    assert paths
                    assert any(path.exists() for path in paths)
                    await cursor.close()
                    assert all(not path.exists() for path in paths)
                finally:
                    resume.set()
                    assert await reading == []

    with patch.object(AggregationSortSpool, "_write_run", write_run):
        asyncio.run(run())
