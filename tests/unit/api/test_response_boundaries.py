"""Owned overflow, error precedence, and lifecycle at the response boundary."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient, MongoClient
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
def test_known_limit_eof_preserves_the_final_document_pending_by_bytes(
    monkeypatch, engine_factory
):
    monkeypatch.setattr(_response, "_RESPONSE_BYTES", 256)
    final_id = 2

    async def exercise():
        async with AsyncMongoClient(engine_factory()) as client:
            collection = client.responses.rows
            await collection.insert_many(
                [{"_id": n, "p": "x" * 100} for n in range(1, 3)]
            )
            cursor = await collection.aggregate([{"$limit": 2}])
            assert cursor._response_reader._pending["_id"] == final_id
            assert not cursor._closed
            assert [row["_id"] for row in await cursor.to_list()] == [1, 2]
            assert cursor._closed
            assert cursor._response_reader._pending is None

    asyncio.run(exercise())


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_empty_client_batch_finalizer_cannot_close_a_resumable_cursor(
    monkeypatch, engine_factory
):
    cleanup = []
    calls = []

    def empty(rows):
        calls.append([row["_id"] for row in rows])
        return []

    with MongoClient(engine_factory()) as client:
        monkeypatch.setattr(
            client, "_defer_cleanup", lambda close: cleanup.append(close) or True
        )
        collection = client.responses.rows
        collection.insert_many([{"_id": n} for n in range(1, 4)])
        collection = collection.with_options(
            codec_options=CodecOptions(type_registry={list: empty})
        )
        cursor = collection.find({}, batch_size=1, sort=[("_id", 1)])
        assert list(cursor) == []
        # Drain GC work synchronously so the race cannot hide behind timing.
        while cleanup:
            cleanup.pop(0)()
        assert cursor.to_list(1) == []
        assert calls == [[1], [2]]
        cursor.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("query", [{}, {"_id": 1}])
def test_find_one_checks_response_capacity_before_any_decoder(
    monkeypatch, engine_factory, query
):
    calls = []
    expected_error_code = 10334

    def observe(row):
        calls.append(row["_id"])
        return row

    with MongoClient(engine_factory()) as client:
        collection = client.responses.rows
        collection.insert_one({"_id": 1, "p": "x" * 70})
        collection = collection.with_options(
            codec_options=CodecOptions(type_registry={dict: observe})
        )
        assert collection.find_one(query)["_id"] == 1
        assert calls == [1]
        calls.clear()
        monkeypatch.setattr(_response, "_MAX_RETURNED_DOCUMENT_BYTES", 64)
        with pytest.raises(OperationFailure) as error:
            collection.find_one(query)
        assert error.value.code == expected_error_code
        assert calls == []


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
                    if mode == "find":
                        assert await iterator.__anext__() == {"_id": 1}
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


@pytest.mark.parametrize("mode", ["aggregate", "find", "chunk"])
def test_repeated_cancellation_completes_owned_cleanup(mode):
    async def exercise():
        fetching = asyncio.Event()
        closing = asyncio.Event()
        release = asyncio.Event()
        closed = []

        class WaitingSource:
            def __aiter__(self):
                return self

            async def __anext__(self):
                fetching.set()
                await asyncio.Future()

            async def aclose(self):
                closed.append(True)
                closing.set()
                await release.wait()

        async with AsyncMongoClient(MemoryEngine()) as client:
            if mode == "aggregate":
                cursor = client.responses.rows.aggregate([])
                cursor._stream_batches = WaitingSource
                consume = cursor._initialize()
            else:
                cursor = client.responses.rows.find({})
                cursor._open_read_source = lambda **_options: WaitingSource()
                iterator = cursor.__aiter__()
                consume = (
                    iterator.pull_chunk(1) if mode == "chunk" else iterator.__anext__()
                )
            task = asyncio.create_task(consume)
            await asyncio.wait_for(fetching.wait(), 1)
            task.cancel()
            await asyncio.wait_for(closing.wait(), 1)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            with pytest.raises(StopAsyncIteration):
                await (cursor if mode == "aggregate" else iterator).__anext__()
            await cursor.close()
            assert closed == [True]

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "failed_resources", [(True, False), (False, True), (True, True)]
)
def test_aggregate_close_retries_only_resources_that_failed(failed_resources):
    """Cleanup keeps ownership even when more than one resource rejects close."""

    class Output:
        attempts = 0

        def close(self):
            self.attempts += 1
            if failed_resources[0] and self.attempts == 1:
                message = "spill cleanup"
                raise ValueError(message)

    class Source:
        attempts = 0

        async def aclose(self):
            self.attempts += 1
            if failed_resources[1] and self.attempts == 1:
                message = "source cleanup"
                raise RuntimeError(message)

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.aggregate([])
            output, source = Output(), Source()
            cursor._owned_spill_outputs = [output]
            cursor._active_async_iterator = source
            expected = (
                BaseExceptionGroup
                if all(failed_resources)
                else (ValueError if failed_resources[0] else RuntimeError)
            )
            with pytest.raises(expected) as error:
                await cursor.close()
            if all(failed_resources):
                assert [type(item) for item in error.value.exceptions] == [
                    ValueError,
                    RuntimeError,
                ]
            assert cursor._owned_spill_outputs == (
                [output] if failed_resources[0] else []
            )
            assert cursor._active_async_iterator is (
                source if failed_resources[1] else None
            )
            await cursor.close()
            await cursor.close()
            assert output.attempts == 1 + failed_resources[0]
            assert source.attempts == 1 + failed_resources[1]
            assert cursor._owned_spill_outputs == []
            assert cursor._active_async_iterator is None

    asyncio.run(exercise())


@pytest.mark.parametrize("batch_size", [None, 1])
def test_find_exhaustion_surfaces_cleanup_failure_and_retains_retry(batch_size):
    expected_attempts = 2

    class Source:
        pulls = 0
        closes = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            self.pulls += 1
            if self.pulls == 1:
                return {"_id": 1}
            raise StopAsyncIteration

        async def aclose(self):
            self.closes += 1
            if self.closes == 1:
                message = "exhausted source cleanup"
                raise ValueError(message)

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            source = Source()
            cursor = client.responses.rows.find({}, batch_size=batch_size)
            cursor._open_read_source = lambda **_options: source
            iterator = cursor.__aiter__()
            iterator._batch_size = 1
            assert await iterator.__anext__() == {"_id": 1}
            with pytest.raises(ValueError, match="exhausted source cleanup"):
                await iterator.__anext__()
            await cursor.close()
            assert source.pulls == expected_attempts
            assert source.closes == expected_attempts

    asyncio.run(exercise())


def test_immediate_owned_cleanup_keeps_context_and_finishes_without_scheduling():
    import contextvars  # noqa: PLC0415 - isolated execution context witness

    variable = contextvars.ContextVar("cleanup_context", default="caller")
    observed = []

    async def exercise():
        async def cleanup():
            observed.append(variable.get())
            variable.set("cleanup")

        asyncio.get_running_loop().call_soon(observed.append, "scheduled")
        await finish_cleanup(cleanup())
        assert observed == ["caller"]
        assert variable.get() == "caller"
        await asyncio.sleep(0)
        assert observed == ["caller", "scheduled"]

    asyncio.run(exercise())


def test_owned_cleanup_preserves_configured_task_factory():
    created = []
    closed = []

    async def exercise():
        loop = asyncio.get_running_loop()
        previous = loop.get_task_factory()

        def factory(loop, coroutine, **options):
            created.append(coroutine)
            return asyncio.Task(coroutine, loop=loop, **options)

        async def cleanup():
            closed.append(True)

        coroutine = cleanup()
        loop.set_task_factory(factory)
        try:
            await finish_cleanup(coroutine)
        finally:
            loop.set_task_factory(previous)
        assert created == [coroutine]
        assert closed == [True]

    asyncio.run(exercise())


@pytest.mark.parametrize("entrypoint", ["response", "chunk", "first"])
def test_find_owns_distinct_iterable_and_iterator_without_row_wrapper(entrypoint):
    async def exercise():
        events = []

        class Iterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                events.append("pull")
                return {"_id": 1}

            async def aclose(self):
                events.append("close iterator")

        iterator = Iterator()

        class Source:
            def __aiter__(self):
                events.append("iterate")
                return iterator

            async def aclose(self):
                events.append("close source")

        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.find({})
            original_resolve = cursor._resolve_operation_collation

            async def resolve():
                events.append("resolve")
                await original_resolve()

            def open_source(**_options):
                events.append("open")
                return Source()

            cursor._resolve_operation_collation = resolve
            cursor._open_read_source = open_source
            if entrypoint == "first":
                assert await cursor.first() == {"_id": 1}
            else:
                owner = cursor.__aiter__()
                assert events == []
                assert await owner.pull_chunk(0) == []
                assert events == []
                if entrypoint == "chunk":
                    assert await owner.pull_chunk(1) == [{"_id": 1}]
                    assert await owner.pull_chunk(1) == [{"_id": 1}]
                else:
                    assert await owner._read_response(1) == [{"_id": 1}]
                    assert await owner._read_response(1) == [{"_id": 1}]
                assert owner._source is iterator
                await cursor.close()
            assert events[:3] == ["resolve", "open", "iterate"]
            assert events.count("resolve") == 1
            assert events[-2:] == ["close iterator", "close source"]

    asyncio.run(exercise())


def test_find_close_before_consumption_does_not_resolve_or_open():
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.find({})

            async def forbidden():
                pytest.fail("unconsumed cursor resolved its context")

            cursor._resolve_operation_collation = forbidden
            cursor._open_read_source = lambda **_options: pytest.fail("opened source")
            cursor.__aiter__()
            await cursor.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["iterate", "pull", "close"])
def test_find_distinct_source_retains_failed_cleanup_without_reopening(failure):
    async def exercise():
        closes = []
        opens = []

        class Iterator:
            attempts = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                if failure == "pull":
                    message = "pull failed"
                    raise RuntimeError(message)
                return {"_id": 1}

            async def aclose(self):
                self.attempts += 1
                closes.append("iterator")
                if failure == "close" and self.attempts == 1:
                    message = "close failed"
                    raise RuntimeError(message)

        iterator = Iterator()

        class Source:
            def __aiter__(self):
                if failure == "iterate":
                    message = "iterate failed"
                    raise RuntimeError(message)
                return iterator

            async def aclose(self):
                closes.append("source")

        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.find({})

            def open_source(**_options):
                opens.append(True)
                return Source()

            cursor._open_read_source = open_source
            owner = cursor.__aiter__()
            if failure == "close":
                assert await owner.pull_chunk(1) == [{"_id": 1}]
                with pytest.raises(RuntimeError, match="close failed"):
                    await cursor.close()
            else:
                with pytest.raises(RuntimeError, match=f"{failure} failed"):
                    await owner.pull_chunk(1)
            await cursor.close()
            assert opens == [True]
            expected = {
                "iterate": ["source"],
                "pull": ["iterator", "source"],
                "close": ["iterator", "source", "iterator"],
            }
            assert closes == expected[failure]
            assert not cursor._retired_sources

    asyncio.run(exercise())


def test_find_lazy_open_preserves_query_counter_and_started_phase(monkeypatch):
    async def exercise():
        counters = []
        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.find({})
            monkeypatch.setattr(
                client._engine, "_record_runtime_opcounter", counters.append
            )
            assert counters == []
            owner = cursor.__aiter__()
            assert counters == ["query"]
            assert cursor._started
            assert owner._source is None
            assert await owner.pull_chunk(0) == []
            assert counters == ["query"]
            await cursor.close()
            assert counters == ["query"]

    asyncio.run(exercise())


@pytest.mark.parametrize("entrypoint", ["close", "rewind", "eof", "error"])
@pytest.mark.parametrize("failures", [(True, False), (False, True), (True, True)])
def test_find_cleanup_attempts_all_distinct_resources_and_retries_only_failed(
    entrypoint, failures
):
    async def exercise():
        attempts = [0, 0]

        class Iterator:
            pulls = 0

            def __aiter__(self):
                return self

            async def __anext__(self):
                self.pulls += 1
                if self.pulls == 1:
                    return {"_id": 1}
                if entrypoint == "error":
                    message = "read failed"
                    raise RuntimeError(message)
                raise StopAsyncIteration

            async def aclose(self):
                attempts[0] += 1
                if failures[0] and attempts[0] == 1:
                    message = "iterator cleanup failed"
                    raise ValueError(message)

        iterator = Iterator()

        class Source:
            def __aiter__(self):
                return iterator

            async def aclose(self):
                attempts[1] += 1
                if failures[1] and attempts[1] == 1:
                    message = "source cleanup failed"
                    raise TypeError(message)

        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.find({})
            cursor._open_read_source = lambda **_options: Source()
            owner = cursor.__aiter__()
            assert await owner._read_response(1) == [{"_id": 1}]
            error_type = (
                RuntimeError if entrypoint == "error"
                else (ValueError if failures[0] else TypeError)
            )
            async def finish():
                if entrypoint == "rewind":
                    cursor.rewind()
                    await cursor._close_retired_sources()
                elif entrypoint in {"eof", "error"}:
                    await owner._read_response(1)
                else:
                    await cursor.close()

            with pytest.raises(error_type) as error:
                await finish()
            assert attempts == [1, 1]
            if all(failures):
                assert any(
                    "source cleanup failed" in note
                    for note in error.value.__notes__
                )
            await cursor.close()
            assert attempts == [1 + int(failures[0]), 1 + int(failures[1])]
            assert not cursor._retired_sources

    asyncio.run(exercise())


@pytest.mark.parametrize("entrypoint", ["close", "rewind"])
@pytest.mark.parametrize("blocked_resource", [0, 1])
def test_find_distinct_resources_finish_cleanup_after_repeated_cancellation(
    entrypoint, blocked_resource
):
    async def exercise():
        closing = asyncio.Event()
        release = asyncio.Event()
        attempts = [0, 0]

        async def close_resource(index):
            attempts[index] += 1
            if index == blocked_resource:
                closing.set()
                await release.wait()

        class Iterator:
            def __aiter__(self):
                return self

            async def __anext__(self):
                return {"_id": 1}

            async def aclose(self):
                await close_resource(0)

        iterator = Iterator()

        class Source:
            def __aiter__(self):
                return iterator

            async def aclose(self):
                await close_resource(1)

        async with AsyncMongoClient(MemoryEngine()) as client:
            cursor = client.responses.rows.find({})
            cursor._open_read_source = lambda **_options: Source()
            assert await cursor.__aiter__().pull_chunk(1) == [{"_id": 1}]
            if entrypoint == "rewind":
                cursor.rewind()
            task = asyncio.create_task(cursor.close())
            await asyncio.wait_for(closing.wait(), 1)
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            release.set()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert attempts == [1, 1]
            assert not cursor._retired_sources
            await cursor.close()
            assert attempts == [1, 1]

    asyncio.run(exercise())
