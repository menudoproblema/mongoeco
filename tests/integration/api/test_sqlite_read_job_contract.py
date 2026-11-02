"""Physical SQLite reads release execution capacity between consumer requests."""

import asyncio
import sqlite3
import threading

from contextlib import AsyncExitStack

import pytest

from mongoeco import AsyncMongoClient, SearchIndexModel
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import CollectionInvalid, InvalidOperation
from mongoeco.session import ClientSession


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


def test_unchanged_empty_options_do_not_submit_another_executor_job(monkeypatch):
    async def exercise():
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_collection("test", "records")
            jobs = []
            run = engine._run_blocking

            async def observe(function, *args, **kwargs):
                jobs.append(function.__name__)
                return await run(function, *args, **kwargs)

            monkeypatch.setattr(engine, "_run_blocking", observe)
            assert await engine.collection_options("test", "records") == {}
            jobs.clear()
            sql = []
            engine._connection.set_trace_callback(sql.append)
            try:
                options = await engine.collection_options("test", "records")
                options["validator"] = {"required": ["poison"]}
                assert await engine.collection_options("test", "records") == {}
            finally:
                engine._connection.set_trace_callback(None)
            assert jobs == []
            assert sql == []
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


@pytest.mark.parametrize("operation", ["find", "aggregate", "vector"])
def test_read_context_reuses_only_unchanged_empty_metadata(monkeypatch, operation):
    async def exercise():
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            document = {"_id": 1, "v": "A"}
            if operation == "vector":
                document["embedding"] = [1.0, 0.0]
            await collection.insert_one(document)
            if operation == "vector":
                await collection.create_search_index(
                    SearchIndexModel(
                        {"fields": [{
                            "type": "vector", "path": "embedding",
                            "numDimensions": 2, "similarity": "cosine",
                        }]},
                        name="vectors", type="vectorSearch",
                    )
                )

            async def read():
                if operation == "vector":
                    return await collection.aggregate([{"$vectorSearch": {
                        "index": "vectors", "path": "embedding",
                        "queryVector": [1.0, 0.0], "numCandidates": 2, "limit": 1,
                    }}]).to_list()
                cursor = (
                    collection.find({"v": "A"}) if operation == "find"
                    else collection.aggregate([{"$match": {"v": "A"}}])
                )
                return await cursor.to_list()

            assert await read() == [document]
            assert await engine.collection_options("test", "records") == {}
            jobs = []
            run = engine._run_blocking

            async def observe(function, *args, **kwargs):
                jobs.append(function.__name__)
                return await run(function, *args, **kwargs)

            monkeypatch.setattr(engine, "_run_blocking", observe)
            assert await read() == [document]
            assert await read() == [document]
            assert "_collection_options_sync" not in jobs

    asyncio.run(exercise())


@pytest.mark.parametrize("mutation", ["write", "collMod", "drop", "rename", "drop_db"])
def test_empty_options_observation_is_invalid_after_each_storage_change(mutation):
    async def exercise():
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine) as client:
            await client.test.create_collection("records")
            await client.test.create_collection(
                "replacement", collation={"locale": "en", "strength": 2}
            )
            assert await engine.collection_options("test", "records") == {}
            if mutation == "write":
                await client.test.records.insert_one({"_id": 1})
                # A data write invalidates conservatively, even without DDL.
                calls = []
                run = engine._run_blocking

                async def observe(function, *args, **kwargs):
                    calls.append(function.__name__)
                    return await run(function, *args, **kwargs)

                engine._run_blocking = observe
                assert await engine.collection_options("test", "records") == {}
                assert calls == ["_collection_options_sync"]
                return
            if mutation == "collMod":
                await client.test.command(
                    {"collMod": "records", "validator": {"required": ["v"]}}
                )
                options = await engine.collection_options("test", "records")
                assert options["validator"] == {"required": ["v"]}
                return
            if mutation == "rename":
                await client.test.replacement.rename("records", dropTarget=True)
            else:
                if mutation == "drop_db":
                    await client.drop_database("test")
                else:
                    await client.test.records.drop()
                with pytest.raises(CollectionInvalid):
                    await engine.collection_options("test", "records")
                await client.test.create_collection(
                    "records", collation={"locale": "en", "strength": 2}
                )
            options = await engine.collection_options("test", "records")
            expected_strength = 2
            assert options["collation"]["strength"] == expected_strength
            await client.test.records.insert_many(
                [{"_id": 1, "v": "A"}, {"_id": 2, "v": "a"}]
            )
            assert await client.test.records.find(
                {"v": "A"}, {"_id": 1}, sort=[("_id", 1)]
            ).to_list() == [{"_id": 1}, {"_id": 2}]

    asyncio.run(exercise())


@pytest.mark.parametrize("commit", [False, True])
def test_empty_options_observation_never_crosses_a_transaction(commit):
    async def exercise():
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine) as client:
            await client.test.create_collection("records")
            assert await engine.collection_options("test", "records") == {}
            session = client.start_session()
            session.start_transaction()
            try:
                await engine.drop_collection("test", "records", context=session)
                await engine.create_collection(
                    "test", "records", options={"capped": True}, context=session
                )
                assert await engine.collection_options(
                    "test", "records", context=session
                ) == {"capped": True}
                with pytest.raises(InvalidOperation, match="another session"):
                    await engine.collection_options("test", "records")
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                assert await engine.collection_options("test", "records") == (
                    {"capped": True} if commit else {}
                )
            finally:
                if session.in_transaction:
                    session.abort_transaction()
                session.close()

    asyncio.run(exercise())


def test_hot_empty_options_still_reject_a_foreign_session():
    async def exercise():
        engine = SQLiteEngine()
        await engine.connect()
        try:
            await engine.create_collection("test", "records")
            assert await engine.collection_options("test", "records") == {}
            with pytest.raises(InvalidOperation, match="not created"):
                await engine.collection_options(
                    "test", "records", context=ClientSession()
                )
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


@pytest.mark.parametrize("file_backed", [False, True])
def test_nonempty_and_file_options_retain_the_executor_path(
    tmp_path, monkeypatch, file_backed
):
    async def exercise():
        engine = SQLiteEngine(
            str(tmp_path / "options.sqlite") if file_backed else ":memory:"
        )
        await engine.connect()
        try:
            await engine.create_collection(
                "test", "records", options={} if file_backed else {"capped": True}
            )
            await engine.collection_options("test", "records")
            calls = []
            run = engine._run_blocking

            async def observe(function, *args, **kwargs):
                calls.append(function.__name__)
                return await run(function, *args, **kwargs)

            monkeypatch.setattr(engine, "_run_blocking", observe)
            await engine.collection_options("test", "records")
            await engine.collection_options("test", "records")
            assert calls == ["_collection_options_sync", "_collection_options_sync"]
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_file_options_observe_a_second_engine_without_local_changes(tmp_path):
    async def exercise():
        first = SQLiteEngine(str(tmp_path / "shared.sqlite"))
        second = SQLiteEngine(str(tmp_path / "shared.sqlite"))
        await first.connect()
        await second.connect()
        try:
            await first.create_collection("test", "records")
            assert await first.collection_options("test", "records") == {}
            changes = first._connection.total_changes
            await second._modify_collection_options(
                "test", "records", options={"validator": {"required": ["v"]}}
            )
            assert first._connection.total_changes == changes
            options = await first.collection_options("test", "records")
            assert options["validator"] == {"required": ["v"]}
        finally:
            await second.disconnect()
            await first.disconnect()

    asyncio.run(exercise())


def test_empty_options_observation_is_released_with_its_connection():
    async def exercise():
        engine = SQLiteEngine()
        await engine.connect()
        await engine.create_collection("test", "records")
        assert await engine.collection_options("test", "records") == {}
        connection = engine._connection
        assert connection.empty_options_observation is not None
        await engine.disconnect()
        assert connection.empty_options_observation is None
        await engine.connect()
        try:
            with pytest.raises(CollectionInvalid):
                await engine.collection_options("test", "records")
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_empty_options_are_not_reused_inside_an_active_transaction(monkeypatch):
    async def exercise():
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine) as client:
            await client.test.create_collection("records")
            assert await engine.collection_options("test", "records") == {}
            session = client.start_session()
            session.start_transaction()
            calls = []
            run = engine._run_blocking

            async def observe(function, *args, **kwargs):
                calls.append(function.__name__)
                return await run(function, *args, **kwargs)

            monkeypatch.setattr(engine, "_run_blocking", observe)
            try:
                for _ in range(2):
                    assert await engine.collection_options(
                        "test", "records", context=session
                    ) == {}
                assert calls == ["_collection_options_sync", "_collection_options_sync"]
                with pytest.raises(InvalidOperation, match="another session"):
                    await engine.collection_options("test", "records")
            finally:
                session.abort_transaction()
                session.close()

    asyncio.run(exercise())


def test_virtual_profile_options_never_reuse_a_storage_observation():
    async def exercise():
        engine = SQLiteEngine()
        await engine.connect()
        try:
            engine._profiler.set_level("test", 1)
            assert await engine.collection_options("test", "system.profile") == {}
            changes = engine._connection.total_changes
            engine._profiler.set_level("test", 0)
            engine._profiler.clear("test")
            assert engine._connection.total_changes == changes
            with pytest.raises(CollectionInvalid):
                await engine.collection_options("test", "system.profile")
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_hot_options_with_a_contended_lock_yield_to_the_executor():
    async def exercise():
        engine = SQLiteEngine()
        await engine.connect()
        entered, release = threading.Event(), threading.Event()
        thread = None
        try:
            await engine.create_collection("test", "records")
            assert await engine.collection_options("test", "records") == {}

            def hold_lock():
                with engine._lock:
                    entered.set()
                    release.wait(5)

            thread = threading.Thread(target=hold_lock)
            thread.start()
            assert await asyncio.to_thread(entered.wait, 2)
            observed = []
            task = asyncio.create_task(engine.collection_options("test", "records"))

            def release_from_loop():
                observed.append(not task.done())
                release.set()

            asyncio.get_running_loop().call_soon(release_from_loop)
            assert await asyncio.wait_for(task, 2) == {}
            assert observed == [True]
        finally:
            release.set()
            if thread is not None:
                thread.join(2)
                assert not thread.is_alive()
            await engine.disconnect()

    asyncio.run(exercise())
