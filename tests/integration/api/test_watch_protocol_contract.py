"""Async cancellation and lifecycle cleanup beyond the synchronous witness bridge."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
)
from mongoeco.engines import MemoryEngine, SQLiteEngine


async def async_lifecycle(collection, client):
    """Unchanged async scenario also executed against native PyMongo replica sets."""
    output = {}
    for scope, owner in (
        ("collection", collection),
        ("database", collection.database),
        ("client", client),
    ):
        stream = await owner.watch(max_await_time_ms=20)
        task = asyncio.create_task(stream.next())
        try:
            await asyncio.sleep(0.02)
            pending = not task.done()
            task.cancel()
            try:
                await task
                cancelled = False
            except asyncio.CancelledError:
                cancelled = True
            result = {
                "pending": pending,
                "cancelled": cancelled,
                "alive": stream.alive,
                "try_next": await stream.try_next(),
            }
            try:
                await stream.__anext__()
                result["next"] = "unexpected event"
            except StopAsyncIteration:
                result["next"] = "StopAsyncIteration"
        finally:
            task.cancel()
            await stream.close()
        async with await owner.watch(max_await_time_ms=20) as stream:
            result["context_identity"] = stream.__aiter__() is stream
            result["alive_inside"] = stream.alive
        result["alive_after_context"] = stream.alive
        output[scope] = result
    return output


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_public_async_cancellation_and_context_lifecycle(factory, dialect):
    async def exercise():
        async with AsyncMongoClient(factory(), mongodb_dialect=dialect) as client:
            await client.protocol.c.insert_one({"_id": 1})
            actual = await async_lifecycle(client.protocol.c, client)
            for result in actual.values():
                assert result == {
                    "pending": True,
                    "cancelled": True,
                    "alive": False,
                    "try_next": None,
                    "next": "StopAsyncIteration",
                    "context_identity": True,
                    "alive_inside": True,
                    "alive_after_context": False,
                }
            assert client._change_hub.watcher_count == 0
            assert not client._change_hub._async_waiters

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["next", "try_next"])
def test_close_while_lookup_is_waiting_never_delivers_an_event(method):
    async def exercise():
        hub = ChangeStreamHub()
        entered, release = asyncio.Event(), asyncio.Event()

        async def lookup(_event):
            entered.set()
            await release.wait()
            return {"_id": 1}

        stream = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            full_document="updateLookup",
            lookup_full_document=lookup,
            max_await_time_ms=1,
        )
        hub.publish(
            operation_type="update",
            db_name="db",
            coll_name="c",
            document_key={"_id": 1},
        )
        task = asyncio.create_task(getattr(stream, method)())
        await asyncio.wait_for(entered.wait(), 1)
        completion = stream.close()
        assert not stream.alive  # Closing still takes effect without awaiting.
        assert hub.watcher_count == 0
        await completion
        await completion  # An already completed close is safely re-awaitable.
        release.set()
        if method == "next":
            with pytest.raises(StopAsyncIteration):
                await task
        else:
            assert await task is None
        assert not hub._async_waiters
        hub.close()

    asyncio.run(exercise())


def test_default_try_next_has_a_bounded_wait_without_an_explicit_option():
    async def exercise():
        hub = ChangeStreamHub()
        stream = AsyncChangeStreamCursor(hub, scope=ChangeStreamScope())
        try:
            assert await asyncio.wait_for(stream.try_next(), 2) is None
            assert stream.alive
            assert not hub._async_waiters
        finally:
            await stream.close()
            hub.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("method", ["next", "try_next"])
@pytest.mark.parametrize("stage", ["lookup", "materialize"])
def test_callback_failure_closes_the_stream_without_swallowing_the_error(method, stage):
    async def exercise():
        hub = ChangeStreamHub()

        async def lookup(_event):
            message = "lookup callback failed"
            raise ValueError(message)

        def materialize(_document):
            message = "materializer callback failed"
            raise ValueError(message)

        kwargs = (
            {"full_document": "updateLookup", "lookup_full_document": lookup}
            if stage == "lookup"
            else {"materialize_document": materialize}
        )
        stream = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            max_await_time_ms=1,
            **kwargs,
        )
        hub.publish(
            operation_type="update" if stage == "lookup" else "insert",
            db_name="db",
            coll_name="c",
            document_key={"_id": 1},
        )
        with pytest.raises(ValueError, match="callback failed"):
            await getattr(stream, method)()
        assert not stream.alive
        assert hub.watcher_count == 0
        assert not hub._async_waiters
        assert await stream.try_next() is None
        hub.close()

    asyncio.run(exercise())


def test_cancellation_during_lookup_releases_the_watcher_and_propagates():
    async def exercise():
        hub = ChangeStreamHub()
        entered = asyncio.Event()

        async def lookup(_event):
            entered.set()
            await asyncio.Future()

        stream = AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            full_document="updateLookup",
            lookup_full_document=lookup,
        )
        hub.publish(
            operation_type="update",
            db_name="db",
            coll_name="c",
            document_key={"_id": 1},
        )
        task = asyncio.create_task(stream.next())
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not stream.alive
        assert hub.watcher_count == 0
        assert not hub._async_waiters
        hub.close()

    asyncio.run(exercise())
