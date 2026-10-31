"""Opening is one initialization; cached responses retain public read semantics."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
)
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import encode_change_stream_token

from tests.differential.semantic_watch_batch_cases import DIVIDE, _error


def _publish(hub, index, *, kind="insert", padding="", value=1):
    hub.publish(
        operation_type=kind,
        db_name="db",
        coll_name="c",
        document_key={"_id": index},
        full_document={"_id": index, "v": value, "padding": padding},
    )


def _resumed(hub, **kwargs):
    return AsyncChangeStreamCursor(
        hub,
        scope=ChangeStreamScope(db_name="db", coll_name="c"),
        resume_after={"_data": encode_change_stream_token(1)},
        **kwargs,
    )


def test_concurrent_awaiters_initialize_the_backlog_once():
    async def exercise():
        hub = ChangeStreamHub()
        _publish(hub, 1)
        _publish(hub, 2, kind="update")
        entered, release = asyncio.Event(), asyncio.Event()
        calls = []

        async def lookup(event):
            calls.append(event.token)
            entered.set()
            await release.wait()
            return {"_id": 2, "v": 1}

        stream = _resumed(
            hub, full_document="updateLookup", lookup_full_document=lookup
        )

        async def opening():
            return await stream

        first, second = asyncio.create_task(opening()), asyncio.create_task(opening())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert calls == [2]
            release.set()
            results = await asyncio.gather(first, second)
            assert all(result is stream for result in results)
            assert calls == [2]
            assert (await stream.next())["documentKey"] == {"_id": 2}
            assert calls == [2]
        finally:
            first.cancel()
            second.cancel()
            await stream.close()
            hub.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("action", ["cancel", "close"])
def test_interrupted_opening_discards_response_and_releases_watcher(action):
    async def exercise():
        hub = ChangeStreamHub()
        _publish(hub, 1)
        _publish(hub, 2, kind="update")
        entered, release = asyncio.Event(), asyncio.Event()
        decoded = []

        async def lookup(_event):
            entered.set()
            await release.wait()
            return {"_id": 2, "v": 1}

        stream = _resumed(
            hub,
            full_document="updateLookup",
            lookup_full_document=lookup,
            materialize_document=lambda document: decoded.append(document) or document,
        )
        before = stream.resume_token

        async def opening():
            return await stream

        task = asyncio.create_task(opening())
        try:
            await asyncio.wait_for(entered.wait(), 1)
            assert hub.watcher_count == 1
            if action == "cancel":
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
            else:
                await stream.close()
                release.set()
                assert await task is stream
            assert not stream.alive
            assert hub.watcher_count == 0
            assert not stream._buffer
            assert stream._pending is None
            assert stream._resume_anchor is None
            assert stream.resume_token == before
            assert not decoded
            assert await stream.try_next() is None
        finally:
            release.set()
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await stream.close()
            hub.close()

    asyncio.run(exercise())


def test_close_before_initialization_releases_the_retained_resume_anchor():
    async def exercise():
        hub = ChangeStreamHub()
        _publish(hub, 1)
        stream = _resumed(hub)
        try:
            assert stream._resume_anchor is not None
            await stream.close()
            assert stream._resume_anchor is None
            assert hub.watcher_count == 0
            assert await stream is stream
            assert await stream.try_next() is None
        finally:
            await stream.close()
            hub.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["next", "try_next"])
def test_admitted_rows_precede_a_gap_at_the_end_of_history(method):
    async def exercise():
        hub = ChangeStreamHub()
        stream = AsyncChangeStreamCursor(
            hub, scope=ChangeStreamScope(), max_await_time_ms=0
        )
        _publish(hub, 1)
        hub.mark_gap()
        try:
            assert (await getattr(stream, method)())["documentKey"] == {"_id": 1}
            before = stream.resume_token
            with pytest.raises(OperationFailure) as error:
                await getattr(stream, method)()
            assert error.value.code == 286  # noqa: PLR2004 - local history lost
            assert stream.resume_token == before
            assert not stream.alive
            assert hub.watcher_count == 0
        finally:
            await stream.close()
            hub.close()

    asyncio.run(exercise())


@pytest.mark.parametrize(["maximum", "code"], [(-1, 2), (True, 14), (False, 14)])
def test_invalid_getmore_option_does_not_discard_the_initial_response(maximum, code):
    async def exercise():
        hub = ChangeStreamHub()
        for index in (1, 2, 3):
            _publish(hub, index)
        stream = _resumed(hub, max_await_time_ms=maximum)
        try:
            assert await stream is stream
            assert (await stream.next())["documentKey"] == {"_id": 2}
            assert (await stream.next())["documentKey"] == {"_id": 3}
            with pytest.raises(OperationFailure) as error:
                await stream.try_next()
            assert error.value.code == code
            assert not stream.alive
            assert hub.watcher_count == 0
        finally:
            await stream.close()
            hub.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("failure", ["expression", "option", "codec"])
def test_failed_opening_clears_rows_and_releases_the_watcher(failure):
    async def exercise():
        hub = ChangeStreamHub()
        _publish(hub, 1)
        _publish(hub, 2, value=0)

        def decode(_document):
            message = "codec failed"
            raise ValueError(message)

        kwargs = {
            "expression": {"pipeline": DIVIDE},
            "option": {"max_await_time_ms": 1.5},
            "codec": {"materialize_document": decode},
        }[failure]
        stream = _resumed(hub, **kwargs)
        before = stream.resume_token
        try:
            expected = {
                "expression": OperationFailure,
                "option": TypeError,
                "codec": ValueError,
            }[failure]
            with pytest.raises(expected):
                await stream
            assert not stream.alive
            assert hub.watcher_count == 0
            assert not stream._buffer
            assert stream._pending is None
            assert stream.resume_token == before
        finally:
            await stream.close()
            hub.close()

    asyncio.run(exercise())


def test_close_discards_buffered_and_prepared_overflow_documents():
    async def exercise():
        hub = ChangeStreamHub()
        _publish(hub, 1)
        for index in (2, 3, 4):
            _publish(hub, index, padding="x" * (6 * 1024 * 1024))
        decoded = []

        def decode(document):
            decoded.append(document["documentKey"]["_id"])
            return document

        stream = _resumed(hub, materialize_document=decode)
        try:
            await stream
            assert decoded == [2, 3]
            assert stream._pending is not None
            await stream.close()
            assert not stream._buffer
            assert stream._pending is None
            assert hub.watcher_count == 0
            assert await stream.try_next() is None
        finally:
            await stream.close()
            hub.close()

    asyncio.run(exercise())


async def async_opening(collection, client):
    """Also run unchanged against PyMongo AsyncMongoClient in native probes."""
    output = {}
    for scope, owner in (
        ("collection", collection),
        ("database", collection.database),
        ("client", client),
    ):
        await collection.delete_many({})
        keeper = await owner.watch(max_await_time_ms=20)
        stream = None
        try:
            await collection.insert_one({"_id": "anchor", "v": 1})
            token = (await keeper.next())["_id"]
            await collection.insert_one({"_id": 1, "v": 0})
            try:
                stream = await owner.watch(DIVIDE, resume_after=token)
                output[scope] = {"unexpected_open": True}
            except Exception as error:
                output[scope] = _error(error, "open", None)
        finally:
            if stream is not None:
                await stream.close()
            await keeper.close()
    return output


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_async_opening_evaluates_backlog_before_returning(factory, dialect):
    async def exercise():
        async with AsyncMongoClient(factory(), mongodb_dialect=dialect) as client:
            actual = await async_opening(client.opening.c, client)
            assert set(actual) == {"collection", "database", "client"}
            for row in actual.values():
                assert row == {
                    "phase": "open",
                    "error": "OperationFailure",
                    "code": 2,
                    "code_name": "BadValue",
                    "alive": None,
                }
            assert client._change_hub.watcher_count == 0

    asyncio.run(exercise())
