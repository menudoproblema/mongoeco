"""Local persistence and read-resource guarantees beyond native image captures."""

import asyncio

from datetime import UTC, datetime

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine

from tests.integration.api.test_semantic_group_regressions import _AsyncView


_LATEST_VALUE = 2


def _journal_write(client):
    subject = client["images"].create_collection(
        "records", changeStreamPreAndPostImages={"enabled": True}
    )
    subject.insert_one({"_id": 1, "v": 0, "pad": "x" * 1000})
    stream = subject.watch(full_document="required", max_await_time_ms=1)
    try:
        subject.update_one({}, {"$set": {"v": 1}})
        token = stream.try_next()["_id"]
        subject.update_one({}, {"$set": {"v": _LATEST_VALUE}})
        client["images"].command(
            {"collMod": "records", "changeStreamPreAndPostImages": {"enabled": False}}
        )
        return token
    finally:
        stream.close()


def _journal_read(client, token):
    stream = client["images"]["records"].watch(
        [{"$set": {"n": {"$type": "$fullDocument.v"}}}],
        full_document="required",
        resume_after=token,
        max_await_time_ms=1,
    )
    try:
        event = stream.try_next()
        assert event["fullDocument"]["v"] == _LATEST_VALUE
        assert event["n"] == "int"
        client["images"]["records"].insert_one({"_id": "after_restart"})
        next_event = stream.try_next()
        assert next_event["documentKey"] == {"_id": "after_restart"}
        assert next_event["_id"] != event["_id"]
    finally:
        stream.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_available_post_image_survives_journal_restart_and_changed_options(
    tmp_path, backend, surface
):
    path = tmp_path / "journal.json"

    def engine():
        return (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "db.sqlite"))
        )

    options = {
        "change_stream_journal_path": str(path),
        "change_stream_journal_max_bytes": 1,
    }
    if surface == "sync":
        with MongoClient(engine(), **options) as client:
            token = _journal_write(client)
        with MongoClient(engine(), **options) as client:
            _journal_read(client, token)
        return

    async def exercise():
        async with AsyncMongoClient(engine(), **options) as client:
            view = _AsyncView(client, asyncio.get_running_loop(), client)
            token = await asyncio.to_thread(_journal_write, view)
        async with AsyncMongoClient(engine(), **options) as client:
            view = _AsyncView(client, asyncio.get_running_loop(), client)
            await asyncio.to_thread(_journal_read, view, token)

    asyncio.run(exercise())


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_update_lookup_uses_current_read_clock_but_keeps_stream_expression_clock(
    engine_factory,
):
    clock = [datetime(2001, 1, 1, tzinfo=UTC)]
    with MongoClient(engine_factory(), now_factory=lambda: clock[0]) as client:
        subject = client.images.records
        subject.insert_one(
            {"_id": 1, "v": 0, "expires": datetime(2005, 1, 1, tzinfo=UTC)}
        )
        subject.create_index("expires", expire_after_seconds=0)
        stream = subject.watch(
            [{"$set": {"streamClock": "$$NOW"}}],
            full_document="updateLookup",
            max_await_time_ms=1,
        )
        try:
            subject.update_one({}, {"$set": {"v": 1}})
            clock[0] = datetime(2010, 1, 1, tzinfo=UTC)
            event = stream.try_next()
            assert event["operationType"] == "update"
            assert event["fullDocument"] is None
            assert event["streamClock"] == datetime(2001, 1, 1, tzinfo=UTC).replace(
                tzinfo=None
            )
            # Read-side TTL maintenance must retain/deliver its delete event.
            deleted = stream.try_next()
            assert deleted["operationType"] == "delete"
            assert deleted["documentKey"] == {"_id": 1}
        finally:
            stream.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("commit", [False, True])
def test_post_image_is_published_only_with_the_committed_write(engine_factory, commit):
    async def exercise():
        async with AsyncMongoClient(engine_factory()) as client:
            subject = await client.images.create_collection(
                "records", changeStreamPreAndPostImages={"enabled": True}
            )
            await subject.insert_one({"_id": 1, "v": 0, "pad": "x" * 1000})
            stream = subject.watch(full_document="required", max_await_time_ms=1)
            session = client.start_session()
            session.start_transaction()
            try:
                await subject.update_one({}, {"$set": {"v": 1}}, session=session)
                assert await stream.try_next() is None
                if commit:
                    session.commit_transaction()
                    event = await stream.try_next()
                    assert event["operationType"] == "update"
                    assert event["fullDocument"]["v"] == 1
                else:
                    session.abort_transaction()
                await subject.insert_one({"_id": "barrier"})
                event = await stream.try_next()
                assert event["operationType"] == "insert"
                assert event["documentKey"] == {"_id": "barrier"}
                assert (await subject.find_one({"_id": 1}))["v"] == int(commit)
            finally:
                session.close()
                stream.close()

    asyncio.run(exercise())
