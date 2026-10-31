"""Watch owns its clock, dialect and pipeline input independently of writers."""

import asyncio

from copy import deepcopy
from datetime import UTC, datetime, timedelta, timezone

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.change_streams import (
    AsyncChangeStreamCursor,
    ChangeStreamHub,
    ChangeStreamScope,
)
from mongoeco.compat.base import MongoDialect90
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import CodecOptions

from tests.integration.api.test_semantic_group_regressions import _AsyncView


class _FalseyDialect(MongoDialect90):
    def expression_truthy(self, value):
        del value
        return False


def _exercise(collection, owner, clock, calls):
    initial = clock[0]
    derived = collection.database[collection.name]
    rows = list(derived.aggregate([{"$project": {"seen": "$$NOW"}}]))
    assert rows[0]["seen"] == initial
    pipeline = [{"$set": {"streamNow": "$$NOW", "owned": {"$literal": {"b": [1]}}}}]
    saved = deepcopy(pipeline)
    before = len(calls)
    stream = owner.watch(pipeline, max_await_time_ms=1)
    assert len(calls) == before + 1
    pipeline[0]["$set"]["streamNow"] = "mutated"
    pipeline[0]["$set"]["owned"]["$literal"]["b"].append(2)
    resumed = None
    try:
        first = None
        for value in (2, 3):
            clock[0] += timedelta(days=1000)
            collection.insert_one({"_id": value})
            event = stream.try_next()
            assert event["streamNow"] == initial
            assert event["streamNow"].utcoffset() == timedelta(hours=3)
            assert event["owned"] == {"b": [1]}
            if first is None:
                first = event
        opened = clock[0]
        before = len(calls)
        resumed = owner.watch(saved, resume_after=first["_id"], max_await_time_ms=1)
        assert len(calls) == before + 1
        event = resumed.try_next()
        assert event["documentKey"] == {"_id": 3}
        assert event["streamNow"] == opened
        assert event["streamNow"] != initial
    finally:
        if resumed is not None:
            resumed.close()
        stream.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("scope", ["collection", "database", "client"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_watch_captures_clock_and_owned_input_once_and_preserves_codec(
    engine_factory, scope, surface
):
    clock = [datetime(2001, 1, 1, tzinfo=UTC)]
    calls = []

    def now():
        calls.append(clock[0])
        return clock[0]

    options = {
        "now_factory": now,
        "codec_options": CodecOptions(
            tz_aware=True, tzinfo=timezone(timedelta(hours=3))
        ),
    }
    if surface == "sync":
        with MongoClient(engine_factory(), **options) as client:
            collection = client.context.records
            collection.insert_one({"_id": 1})
            owner = {
                "collection": collection,
                "database": collection.database,
                "client": client,
            }[scope]
            _exercise(collection, owner, clock, calls)
        return

    async def exercise():
        async with AsyncMongoClient(engine_factory(), **options) as client:
            await client.context.records.insert_one({"_id": 1})
            collection = _AsyncView(
                client.context.records, asyncio.get_running_loop(), client
            )
            owner = {
                "collection": collection,
                "database": collection.database,
                "client": collection.database.client,
            }[scope]
            await asyncio.to_thread(_exercise, collection, owner, clock, calls)

    asyncio.run(exercise())


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("scope", ["collection", "database", "client"])
def test_watch_respects_explicit_semantic_overrides(engine_factory, scope):
    with MongoClient(engine_factory(), mongodb_dialect=_FalseyDialect()) as client:
        collection = client.override.records
        collection.insert_one({"_id": 1})
        owner = {
            "collection": collection,
            "database": collection.database,
            "client": client,
        }[scope]
        stream = owner.watch(
            [{"$set": {"value": {"$cond": [True, "default", "configured"]}}}],
            max_await_time_ms=1,
        )
        try:
            collection.insert_one({"_id": 2})
            assert stream.try_next()["value"] == "configured"
        finally:
            stream.close()


def test_failed_preparation_never_registers_a_watcher():
    hub = ChangeStreamHub()
    with pytest.raises(OperationFailure) as error:
        AsyncChangeStreamCursor(
            hub,
            scope=ChangeStreamScope(),
            pipeline=[{"$project": {"x": {"$unknown": 1}}}],
        )
    assert error.value.code == 31325  # noqa: PLR2004 - native expression parse code
    assert hub.watcher_count == 0
