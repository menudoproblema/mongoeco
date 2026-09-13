"""Physical SQLite reads release execution capacity between consumer requests."""

import asyncio
import sqlite3
import threading

from contextlib import AsyncExitStack

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine


@pytest.mark.parametrize("workers", [1, 4])
def test_paused_readers_release_executor_capacity(tmp_path, workers):
    async def exercise():
        engine = SQLiteEngine(
            str(tmp_path / "readers.sqlite"), executor_workers=workers
        )
        await engine.connect()
        try:
            await engine.insert_documents(
                "test", "records", [{"_id": index} for index in range(500)]
            )
            async with AsyncExitStack() as cleanup:
                for _ in range(workers):
                    source = engine.scan_find_semantics(
                        "test", "records", compile_find_semantics({}, sort=[("_id", 1)])
                    ).__aiter__()
                    cleanup.push_async_callback(source.aclose)
                    assert await anext(source) == {"_id": 0}

                # A completion barrier in the SAME pool. It needs no consumer
                # advancement and must run after every first fetch returned.
                barrier = asyncio.create_task(engine._run_blocking(lambda: "available"))
                try:
                    assert await asyncio.wait_for(barrier, 1) == "available"
                except TimeoutError:
                    pytest.fail("paused consumers still occupy executor workers")
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_empty_fallback_scan_yields_executor_after_examined_quota(
    tmp_path, monkeypatch
):
    async def exercise():
        engine = SQLiteEngine(
            str(tmp_path / "examined-quota.sqlite"), executor_workers=1
        )
        await engine.connect()
        entered, release = threading.Event(), threading.Event()
        row_count = 20

        class NoMatches:
            def __init__(self):
                self.remaining = row_count

            def __iter__(self):
                return self

            def __next__(self):
                while True:
                    matched, document = self.next_examined()
                    if matched:
                        return document

            def next_examined(self):
                if self.remaining == row_count:
                    entered.set()
                    release.wait(5)
                if self.remaining == 0:
                    raise StopIteration
                self.remaining -= 1
                return False, None

        engine._scan_examined_limit = 2
        monkeypatch.setattr(
            engine,
            "_open_scan_documents_sync",
            lambda _reader: NoMatches(),
        )
        source = engine.scan_find_semantics(
            "test", "records", compile_find_semantics({})
        ).__aiter__()
        reading = asyncio.create_task(anext(source))
        try:
            assert await asyncio.to_thread(entered.wait, 2)
            barrier = asyncio.create_task(
                engine._run_blocking(lambda: "independent job progressed")
            )
            for _ in range(100):
                admission = engine._runtime_diagnostics_info()["readResources"][
                    "executorAdmission"
                ]
                if admission["waiting"] == 1:
                    break
                await asyncio.sleep(0)
            else:
                pytest.fail("independent job never entered executor admission")
            release.set()
            assert await asyncio.wait_for(barrier, 1) == "independent job progressed"
            with pytest.raises(StopAsyncIteration):
                await asyncio.wait_for(reading, 1)
        finally:
            release.set()
            await source.aclose()
            await engine.disconnect()

    asyncio.run(exercise())


def test_sqlite_read_handoff_does_not_poll_a_timer(tmp_path, monkeypatch):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "handoff.sqlite"), executor_workers=1)
        await engine.connect()
        try:
            await engine.insert_document("test", "records", {"_id": 1})
            waits = []
            sleep = asyncio.sleep

            async def observe_sleep(delay, *args, **kwargs):
                waits.append(delay)
                return await sleep(delay, *args, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(asyncio, "sleep", observe_sleep)
                assert [
                    document
                    async for document in engine.scan_find_semantics(
                        "test", "records", compile_find_semantics({})
                    )
                ] == [{"_id": 1}]
            polling_interval = 0.01
            assert polling_interval not in waits
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_disconnect_closes_paused_physical_readers(tmp_path):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "disconnect.sqlite"), executor_workers=1)
        await engine.connect()
        source = None
        try:
            await engine.insert_documents(
                "test", "records", [{"_id": index} for index in range(500)]
            )
            source = engine.scan_find_semantics(
                "test", "records", compile_find_semantics({})
            ).__aiter__()
            await anext(source)
            await asyncio.wait_for(engine.disconnect(), 2)
            assert engine._active_scan_count == 0
            assert not engine._runtime_state.scan_stop_events
            assert engine._connection is None
        finally:
            if source is not None:
                await source.aclose()
            await engine.disconnect()

    asyncio.run(exercise())


@pytest.mark.parametrize("file_backed", [False, True])
@pytest.mark.parametrize("transaction", [False, True])
@pytest.mark.parametrize("python_fallback", [False, True])
def test_unsorted_batches_keep_the_view_before_later_writes(
    tmp_path, file_backed, transaction, python_fallback
):
    async def exercise():
        path = str(tmp_path / "stable.sqlite") if file_backed else ":memory:"
        async with AsyncMongoClient(SQLiteEngine(path)) as client:
            collection = client.test.records
            documents = [
                {"_id": str(index).zfill(4), "value": index, "label": "keep"}
                for index in range(200)
            ]
            await collection.insert_many(documents)
            session = client.start_session() if transaction else None
            if session is not None:
                session.start_transaction()
            selector = {"label": {"$regex": "^keep"}} if python_fallback else {}
            cursor = collection.find(selector, session=session).batch_size(1)
            try:
                iterator = cursor.__aiter__()
                first = await anext(iterator)
                await collection.update_one(
                    {"_id": "0199"}, {"$set": {"value": -1}}, session=session
                )
                seen = [first] + [document async for document in iterator]
                assert seen == documents
            finally:
                await cursor.close()
                if session is not None:
                    session.abort_transaction()
                    session.close()

    asyncio.run(exercise())


def test_paused_readers_do_not_starve_another_engine_in_the_shared_pool(tmp_path):
    async def exercise():
        first = SQLiteEngine(str(tmp_path / "first.sqlite"))
        second = SQLiteEngine(str(tmp_path / "second.sqlite"))
        await first.connect()
        await second.connect()
        try:
            assert first._ensure_executor() is second._ensure_executor()
            await first.insert_documents(
                "test", "records", [{"_id": index} for index in range(500)]
            )
            async with AsyncExitStack() as cleanup:
                for _ in range(first._executor_workers):
                    source = first.scan_find_semantics(
                        "test", "records", compile_find_semantics({})
                    ).__aiter__()
                    cleanup.push_async_callback(source.aclose)
                    await anext(source)
                assert (
                    await asyncio.wait_for(
                        second._run_blocking(lambda: "other engine progressed"), 1
                    )
                    == "other engine progressed"
                )
        finally:
            await second.disconnect()
            await first.disconnect()

    asyncio.run(exercise())


def test_repeated_cancellation_keeps_close_owned_until_fetch_finishes(
    tmp_path, monkeypatch
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "cancel.sqlite"), executor_workers=2)
        await engine.connect()
        entered, release = threading.Event(), threading.Event()
        observed = []
        original = engine._open_scan_documents_sync

        def blocked_source(reader):
            documents = original(reader)
            observed.append((reader, reader.connection))
            entered.set()
            release.wait(5)
            yield from documents

        try:
            await engine.insert_documents(
                "test", "records", [{"_id": index} for index in range(100)]
            )
            monkeypatch.setattr(engine, "_open_scan_documents_sync", blocked_source)
            source = engine.scan_find_semantics(
                "test", "records", compile_find_semantics({})
            ).__aiter__()
            next_task = asyncio.create_task(anext(source))
            assert await asyncio.to_thread(entered.wait, 2)
            reader, connection = observed[0]
            next_task.cancel()
            assert await asyncio.to_thread(reader.stop_event.wait, 2)
            assert not reader.close_completed.is_set()
            assert reader in engine._runtime_state.scan_readers
            next_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await next_task
            release.set()
            assert await asyncio.to_thread(reader.close_completed.wait, 2)
            assert reader.close_error is None
            assert engine._active_scan_count == 0
            with pytest.raises(sqlite3.ProgrammingError):
                connection.execute("SELECT 1")
            await source.aclose()
        finally:
            release.set()
            await engine.disconnect()

    asyncio.run(exercise())
