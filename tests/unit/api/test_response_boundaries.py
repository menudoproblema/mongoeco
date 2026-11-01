"""Owned overflow, error precedence, and lifecycle at the response boundary."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.api._async import _response
from mongoeco.core.aggregation.extensions import (
    register_aggregation_stage,
    unregister_aggregation_stage,
)
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import CodecOptions
from mongoeco.wire._lifecycle import finish_cleanup


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_evaluated_overflow_is_retained_once_and_close_discards_it(
    monkeypatch, engine_factory
):
    monkeypatch.setattr(_response, "_RESPONSE_BYTES", 256)
    seen = []
    second_id = 2

    def observe(documents, _spec, _context):
        seen.extend(row["_id"] for row in documents)
        return documents

    async def exercise():
        async with AsyncMongoClient(engine_factory()) as client:
            collection = client.responses.rows
            await collection.insert_many(
                [{"_id": n, "p": "x" * 100} for n in range(1, 5)]
            )
            cursor = await collection.aggregate([{"$responseObserve": {}}])
            assert seen == [1, 2]
            assert (await cursor.__anext__())["_id"] == 1
            assert (await cursor.__anext__())["_id"] == second_id
            assert seen == [1, 2, 3]
            await cursor.close()
            assert cursor._response_reader._pending is None
            assert await cursor.to_list() == []
            assert seen == [1, 2, 3]

    register_aggregation_stage("$responseObserve", observe, execution_mode="streamable")
    try:
        asyncio.run(exercise())
    finally:
        unregister_aggregation_stage("$responseObserve")


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_find_rewind_retires_overflow_and_restarts_its_response(
    monkeypatch, engine_factory
):
    monkeypatch.setattr(_response, "_RESPONSE_BYTES", 256)

    async def exercise():
        async with AsyncMongoClient(engine_factory()) as client:
            collection = client.responses.rows
            await collection.insert_many(
                [{"_id": n, "p": "x" * 100} for n in range(1, 4)]
            )
            cursor = collection.find({}, sort=[("_id", 1)])
            assert [row["_id"] for row in await cursor._read_response(None)] == [1]
            cursor.rewind()
            batches = []
            while True:
                batch = await cursor._read_response(None)
                if not batch:
                    break
                batches.append([row["_id"] for row in batch])
            assert batches == [[1], [2], [3]]
            await cursor.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("mode", ["aggregate", "find", "batched_find"])
def test_evaluation_error_survives_cleanup_failure_without_reopening_source(mode):
    expected_source_pulls = 2
    expected_cleanup_attempts = 2

    class BrokenSource:
        pulls = 0
        closes = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            self.pulls += 1
            if self.pulls == 1:
                return {"_id": 1}
            message = "evaluation failed"
            raise RuntimeError(message)

        async def aclose(self):
            self.closes += 1
            if self.closes == 1:
                message = "cleanup failed"
                raise ValueError(message)

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            source = BrokenSource()
            if mode == "aggregate":
                cursor = client.responses.rows.aggregate([])
                cursor._stream_batches = lambda: source
                iterator = cursor
            else:
                cursor = client.responses.rows.find(
                    {}, batch_size=2 if mode == "batched_find" else None
                )
                cursor._open_read_source = lambda **_options: source
                iterator = cursor.__aiter__()

            async def first():
                if mode == "aggregate":
                    await cursor
                else:
                    await iterator.__anext__()

            with pytest.raises(RuntimeError, match="evaluation failed") as error:
                await first()
            assert any("cleanup failed" in note for note in error.value.__notes__)
            with pytest.raises(StopAsyncIteration):
                await iterator.__anext__()
            assert source.pulls == expected_source_pulls
            await cursor.close()
            assert source.closes == expected_cleanup_attempts

    asyncio.run(exercise())


def test_cleanup_can_complete_in_the_inline_memory_runner():
    closed = []

    async def cleanup():
        closed.append(True)

    coroutine = finish_cleanup(cleanup())
    with pytest.raises(StopIteration):
        coroutine.send(None)
    assert closed == [True]


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("builder", ["legacy", "find", "find_without_let"])
def test_legacy_internal_builder_cannot_decode_a_failed_aggregate_response(
    engine_factory,
    builder,
):
    expected_code = 2
    calls = []

    def observe(value):
        calls.append(value)
        return value

    async def exercise():
        async with AsyncMongoClient(engine_factory()) as client:
            collection = client.responses.rows.with_options(
                codec_options=CodecOptions(type_registry={int: observe})
            )
            await collection.insert_many([{"_id": 1, "v": [1]}, {"_id": 2, "v": [0]}])
            build = collection._build_cursor

            def legacy(operation, *, session=None):
                return build(operation, session=session)

            cursor = collection.aggregate(
                [{"$unwind": "$v"}, {"$set": {"x": {"$divide": [1, "$v"]}}}]
            )
            if builder == "legacy":
                collection._build_cursor = legacy
            else:

                class PublicFindCollection:
                    _build_cursor = None

                    def __getattr__(self, name):
                        return getattr(collection, name)

                    def find(self, *args, **kwargs):
                        if builder == "find_without_let" and "let" in kwargs:
                            message = "unexpected keyword argument 'let'"
                            raise TypeError(message)
                        return collection.find(*args, **kwargs)

                cursor._collection = PublicFindCollection()
            with pytest.raises(OperationFailure) as error:
                await cursor
            assert error.value.code == expected_code
            assert calls == []
            await cursor.close()

    asyncio.run(exercise())


def test_rewind_keeps_retired_source_owned_when_close_must_be_retried():
    expected_closes = 2

    class Source:
        closes = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            return {"_id": 1}

        async def aclose(self):
            self.closes += 1
            if self.closes == 1:
                message = "retired close failed"
                raise ValueError(message)

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            source = Source()
            cursor = client.responses.rows.find({}, batch_size=1)
            cursor._open_read_source = lambda **_options: source
            assert await cursor.__aiter__().__anext__() == {"_id": 1}
            cursor.rewind()
            with pytest.raises(ValueError, match="retired close failed"):
                await cursor.close()
            assert cursor._retired_sources == [source]
            await cursor.close()
            assert not cursor._retired_sources
            assert source.closes == expected_closes

    asyncio.run(exercise())


def test_repeated_cancellation_completes_aggregation_cleanup():
    async def exercise():
        fetching = asyncio.Event()
        closing = asyncio.Event()
        release = asyncio.Event()
        closed = []

        class WaitingSource:
            async def __anext__(self):
                fetching.set()
                await asyncio.Future()

            async def aclose(self):
                closed.append(True)
                closing.set()
                await release.wait()

        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.aggregate([])
            cursor._stream_batches = WaitingSource
            task = asyncio.create_task(cursor._initialize())
            await asyncio.wait_for(fetching.wait(), 1)
            task.cancel()
            await asyncio.wait_for(closing.wait(), 1)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(StopAsyncIteration):
                await cursor.__anext__()
            await cursor.close()
            assert closed == [True]

    asyncio.run(exercise())
