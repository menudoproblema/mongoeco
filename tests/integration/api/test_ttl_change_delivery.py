"""TTL maintenance shares atomic change delivery with request writes."""

# ruff: noqa: PLR2004 - exact fixture document/event counts

import asyncio

from contextlib import suppress
from datetime import UTC, datetime, timedelta

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine, sqlite as sqlite_module
from mongoeco.errors import OperationFailure


BACKENDS = ("memory", "sqlite-memory", "sqlite-file")


def _engine(backend, tmp_path):
    if backend == "memory":
        return MemoryEngine()
    return SQLiteEngine(
        str(tmp_path / "ttl.db") if backend == "sqlite-file" else ":memory:"
    )


async def _seed(collection, expiry):
    await collection.insert_many([{"_id": i, "expires": expiry} for i in (1, 2)])
    await collection.create_index("expires", expire_after_seconds=0)


async def _assert_deletes(stream):
    events = [await stream.try_next(), await stream.try_next()]
    assert all(
        event is not None and event["operationType"] == "delete" for event in events
    )
    assert sorted(event["documentKey"]["_id"] for event in events) == [1, 2]
    assert await stream.try_next() is None


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize(
    "trigger",
    ["find", "find_one", "count", "aggregate", "distinct", "index", "lookup", "union"],
)
def test_read_and_ddl_purges_deliver_exactly_once(backend, trigger, tmp_path):
    async def exercise():
        now = [datetime(1970, 1, 1, tzinfo=UTC)]
        expiry = now[0] + timedelta(milliseconds=1001)
        async with AsyncMongoClient(
            _engine(backend, tmp_path), now_factory=lambda: now[0]
        ) as client:
            coll = client.test.records
            await _seed(coll, expiry)
            driver = client.test.driver
            if trigger in {"lookup", "union"}:
                await driver.insert_one({"_id": "driver"})
            stream = coll.watch(max_await_time_ms=1)
            try:
                now[0] = expiry
                if trigger == "find":
                    assert await coll.find({}).to_list() == []
                elif trigger == "find_one":
                    assert await coll.find_one({"_id": 1}) is None
                elif trigger == "count":
                    assert await coll.count_documents({}) == 0
                elif trigger == "aggregate":
                    assert (
                        await (await coll.aggregate([{"$match": {}}])).to_list() == []
                    )
                elif trigger == "distinct":
                    assert await coll.distinct("expires") == []
                elif trigger == "lookup":
                    result = await (
                        await driver.aggregate(
                            [
                                {
                                    "$lookup": {
                                        "from": "records",
                                        "pipeline": [{"$match": {}}],
                                        "as": "matches",
                                    }
                                }
                            ]
                        )
                    ).to_list()
                    assert result == [{"_id": "driver", "matches": []}]
                elif trigger == "union":
                    result = await (
                        await driver.aggregate([{"$unionWith": "records"}])
                    ).to_list()
                    assert result == [{"_id": "driver"}]
                else:
                    await coll.create_index("other")
                await _assert_deletes(stream)
                assert await coll.count_documents({}) == 0
                assert await stream.try_next() is None
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", BACKENDS)
def test_failed_ttl_change_recording_rolls_back_documents_and_events(
    backend, tmp_path, monkeypatch
):
    async def exercise():
        now = [datetime(1970, 1, 1, tzinfo=UTC)]
        expiry = now[0] + timedelta(milliseconds=1001)
        engine = _engine(backend, tmp_path)
        async with AsyncMongoClient(engine, now_factory=lambda: now[0]) as client:
            coll = client.test.records
            await _seed(coll, expiry)
            stream = coll.watch(max_await_time_ms=1)
            try:
                now[0] = expiry
                calls = 0
                owner = engine if backend == "memory" else sqlite_module
                name = (
                    "_record_committed_payload"
                    if backend == "memory"
                    else "_sqlite_append_change"
                )
                original = getattr(owner, name)

                def fail_after_second_record(*args, **kwargs):
                    nonlocal calls
                    calls += 1
                    result = original(*args, **kwargs)
                    if calls == 2:
                        message = "injected TTL outbox failure"
                        raise RuntimeError(message)
                    return result

                with monkeypatch.context() as patch:
                    patch.setattr(owner, name, fail_after_second_record)
                    with pytest.raises(
                        RuntimeError, match="injected TTL outbox failure"
                    ):
                        await coll.count_documents({})
                now[0] -= timedelta(milliseconds=1)
                assert await coll.count_documents({}) == 2
                assert await stream.try_next() is None
                now[0] = expiry
                assert await coll.count_documents({}) == 0
                await _assert_deletes(stream)
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", BACKENDS)
def test_committed_ttl_deletes_survive_a_later_read_error(backend, tmp_path):
    async def exercise():
        now = [datetime(1970, 1, 1, tzinfo=UTC)]
        expiry = now[0] + timedelta(milliseconds=1001)
        async with AsyncMongoClient(
            _engine(backend, tmp_path), now_factory=lambda: now[0]
        ) as client:
            coll = client.test.records
            await _seed(coll, expiry)
            await coll.insert_one({"_id": 3, "zero": 0})
            stream = coll.watch(max_await_time_ms=1)
            try:
                now[0] = expiry
                with pytest.raises(OperationFailure):
                    await coll.find({"$expr": {"$divide": [1, "$zero"]}}).to_list()
                await _assert_deletes(stream)
            finally:
                stream.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("commit", [False, True])
def test_local_transaction_owns_ttl_effects_until_commit(backend, commit, tmp_path):
    async def exercise():
        now = [datetime(1970, 1, 1, tzinfo=UTC)]
        expiry = now[0] + timedelta(milliseconds=1001)
        async with AsyncMongoClient(
            _engine(backend, tmp_path), now_factory=lambda: now[0]
        ) as client:
            coll = client.test.records
            await _seed(coll, expiry)
            stream = coll.watch(max_await_time_ms=1)
            session = client.start_session()
            try:
                session.start_transaction()
                now[0] = expiry
                assert await coll.count_documents({}, session=session) == 0
                assert await stream.try_next() is None
                if commit:
                    session.commit_transaction()
                    await _assert_deletes(stream)
                else:
                    session.abort_transaction()
                    now[0] -= timedelta(milliseconds=1)
                    assert await coll.count_documents({}) == 2
                    assert await stream.try_next() is None
            finally:
                stream.close()
                session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("backend", BACKENDS)
def test_waiting_watcher_receives_purge_while_read_cursor_stays_open(backend, tmp_path):
    async def exercise():
        now = [datetime(1970, 1, 1, tzinfo=UTC)]
        expiry = now[0] + timedelta(milliseconds=1001)
        async with AsyncMongoClient(
            _engine(backend, tmp_path), now_factory=lambda: now[0]
        ) as client:
            coll = client.test.records
            await _seed(coll, expiry)
            await coll.insert_many([{"_id": 3}, {"_id": 4}])
            stream = coll.watch(max_await_time_ms=1)
            cursor = None
            pending = asyncio.create_task(stream.next())
            try:
                await asyncio.sleep(0)
                now[0] = expiry
                cursor = coll.find({}).batch_size(1)
                assert (await cursor.to_list(length=1))[0]["_id"] == 3
                first = await asyncio.wait_for(pending, timeout=1)
                second = await stream.try_next()
                assert sorted(
                    [first["documentKey"]["_id"], second["documentKey"]["_id"]]
                ) == [1, 2]
                assert await stream.try_next() is None
                assert await cursor.to_list() == [{"_id": 4}]
            finally:
                pending.cancel()
                with suppress(asyncio.CancelledError):
                    await pending
                if cursor is not None:
                    await cursor.close()
                stream.close()

    asyncio.run(exercise())
