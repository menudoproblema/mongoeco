"""Batch preparation bounds executor work without changing write semantics."""

import asyncio
import math
import threading

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import BulkWriteError, DocumentValidationFailure
from mongoeco.types import InsertOne


@pytest.mark.parametrize("size", [100, 1000, 5000])
@pytest.mark.parametrize("workers", [1, 4])
def test_bulk_submissions_scale_with_blocks_not_documents(
    tmp_path, monkeypatch, size, workers
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "bulk.sqlite"), executor_workers=workers)
        await engine.connect()
        executor = engine._ensure_executor()
        submit = executor.submit
        submitted = 0
        pending = 0
        peak_pending = 0
        guard = threading.Lock()

        def finished(_future):
            nonlocal pending
            with guard:
                pending -= 1

        def observe(*args, **kwargs):
            nonlocal submitted, pending, peak_pending
            with guard:
                submitted += 1
                pending += 1
                peak_pending = max(peak_pending, pending)
            future = submit(*args, **kwargs)
            future.add_done_callback(finished)
            return future

        try:
            with monkeypatch.context() as patch:
                patch.setattr(executor, "submit", observe)
                result = await engine.insert_documents(
                    "db", "records", [{"_id": index} for index in range(size)]
                )
            assert [item.applied for item in result] == [True] * size
            assert submitted <= math.ceil(size / 64) + 2
            completion_handoff_bound = 2
            assert peak_pending <= completion_handoff_bound
            assert await engine.get_document("db", "records", size - 1) == {
                "_id": size - 1
            }
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


def test_independent_read_progresses_between_bulk_preparation_blocks(
    tmp_path, monkeypatch
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "fairness.sqlite"), executor_workers=1)
        await engine.connect()
        await engine.insert_document("db", "seed", {"_id": "read"})
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = threading.Event()
        prepared = []
        read_at = []
        encode = engine._prepare_bulk_document_with_indexes_sync
        get = engine._get_document_sync

        def pause_first(document, indexes):
            if not prepared:
                loop.call_soon_threadsafe(entered.set)
                assert release.wait(5), "test did not release first preparation"
            prepared.append(document["_id"])
            return encode(document, indexes)

        def observe_read(*args, **kwargs):
            read_at.append(len(prepared))
            return get(*args, **kwargs)

        bulk = None
        try:
            with monkeypatch.context() as patch:
                patch.setattr(
                    engine, "_prepare_bulk_document_with_indexes_sync", pause_first
                )
                patch.setattr(engine, "_get_document_sync", observe_read)
                size = 1000
                bulk = asyncio.create_task(
                    engine.insert_documents(
                        "db", "records", [{"_id": index} for index in range(size)]
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
                read = asyncio.create_task(engine.get_document("db", "seed", "read"))
                await asyncio.sleep(0)  # Admit the read behind the blocked first job.
                release.set()
                assert await asyncio.wait_for(read, 2) == {"_id": "read"}
                assert read_at
                assert 0 < read_at[0] < size
                assert [item.applied for item in await bulk] == [True] * size
        finally:
            release.set()
            if bulk is not None:
                await asyncio.gather(bulk, return_exceptions=True)
            await engine.disconnect()

    asyncio.run(exercise())


def test_cancelled_preparation_stops_after_running_document_without_publication(
    tmp_path, monkeypatch
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "cancel.sqlite"), executor_workers=1)
        await engine.connect()
        loop = asyncio.get_running_loop()
        entered = asyncio.Event()
        release = threading.Event()
        prepared = []
        encode = engine._prepare_bulk_document_with_indexes_sync

        def pause_first(document, indexes):
            prepared.append(document["_id"])
            if document["_id"] == 0:
                loop.call_soon_threadsafe(entered.set)
                assert release.wait(5), "test did not release first preparation"
            return encode(document, indexes)

        bulk = None
        try:
            with monkeypatch.context() as patch:
                patch.setattr(
                    engine, "_prepare_bulk_document_with_indexes_sync", pause_first
                )
                bulk = asyncio.create_task(
                    engine.insert_documents(
                        "db", "records", [{"_id": index} for index in range(1000)]
                    )
                )
                await asyncio.wait_for(entered.wait(), 2)
                bulk.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await bulk
                release.set()
                # Same-pool completion barrier proves the physical job ended.
                await asyncio.wait_for(engine._run_blocking(lambda: None), 2)
                assert prepared == [0]
                assert await engine.get_document("db", "records", 0) is None
        finally:
            release.set()
            if bulk is not None:
                await asyncio.gather(bulk, return_exceptions=True)
            await engine.disconnect()

    asyncio.run(exercise())


@pytest.mark.parametrize("later_validation_failure", [False, True])
def test_bulk_preserves_validation_precedence_without_publishing_prefix(
    tmp_path, monkeypatch, later_validation_failure
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "validation.sqlite"))
        await engine.connect()
        await engine.create_collection(
            "db", "records", options={"validator": {"ok": True}}
        )
        encode = engine._prepare_bulk_document_with_indexes_sync

        def fail_encoding(document, indexes):
            if document["_id"] == 1:
                message = "encoding failure"
                raise ValueError(message)
            return encode(document, indexes)

        documents = [{"_id": index, "ok": True} for index in range(260)]
        if later_validation_failure:
            documents[-1]["ok"] = False
        expected_error = (
            DocumentValidationFailure if later_validation_failure else ValueError
        )
        try:
            with monkeypatch.context() as patch:
                patch.setattr(
                    engine, "_prepare_bulk_document_with_indexes_sync", fail_encoding
                )
                with pytest.raises(expected_error):
                    await engine.insert_documents("db", "records", documents)
            assert await engine.get_document("db", "records", 0) is None
            assert await engine.get_document("db", "records", 259) is None
        finally:
            await engine.disconnect()

    asyncio.run(exercise())


@pytest.mark.parametrize("ordered", [False, True])
def test_bulk_duplicate_after_block_boundary_keeps_error_index_and_partial_results(
    tmp_path, ordered
):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "duplicates.sqlite"))
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            size = 260
            duplicate_index = 150
            requests = [InsertOne({"_id": index}) for index in range(size)]
            requests[duplicate_index] = InsertOne({"_id": 0})
            with pytest.raises(BulkWriteError) as failure:
                await collection.bulk_write(requests, ordered=ordered)
            assert failure.value.details["writeErrors"][0]["index"] == duplicate_index
            expected = (
                list(range(duplicate_index))
                if ordered
                else [index for index in range(size) if index != duplicate_index]
            )
            assert failure.value.details["nInserted"] == len(expected)
            actual = await collection.find({}, sort=[("_id", 1)]).to_list()
            assert actual == [{"_id": index} for index in expected]

    asyncio.run(exercise())


@pytest.mark.parametrize("commit", [False, True])
def test_preparation_blocks_do_not_commit_a_user_transaction(tmp_path, commit):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "transaction.sqlite"))
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            documents = [{"_id": index} for index in range(260)]
            session = client.start_session()
            try:
                session.start_transaction()
                await collection.insert_many(documents, session=session)
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                assert await collection.find({}, sort=[("_id", 1)]).to_list() == (
                    documents if commit else []
                )
            finally:
                session.close()

    asyncio.run(exercise())
