"""Resource ownership and failure paths of finite SQLite read jobs."""

import asyncio
import contextvars
import json
import sqlite3
import subprocess
import sys
import threading

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from mongoeco.core.filtering import QueryEngine
from mongoeco.engines._sqlite_reader import (
    SQLiteScanReader,
    _owned_document_size,
    _serialized_document_size_estimate,
)
from mongoeco.engines.semantic_core import FiniteDocumentScan, compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine, _observe_scan_completion
from mongoeco.engines.sqlite_planner import SQLiteReadExecutionPlan


@pytest.fixture
def engine(tmp_path):
    instance = SQLiteEngine(str(tmp_path / "reader.sqlite"), executor_workers=2)

    async def prepare():
        await instance.connect()
        await instance.put_documents_bulk(
            "test", "records", [{"_id": str(index)} for index in range(5)]
        )

    asyncio.run(prepare())
    try:
        yield instance
    finally:
        asyncio.run(instance.disconnect())


def make_reader(engine):
    return SQLiteScanReader(engine, "test", "records", compile_find_semantics({}))


def test_estimated_container_cost_handles_aliases_and_cycles():
    shared = ["payload" * 1000]
    document = {"first": shared, "second": shared}
    shared.append(document)
    assert _owned_document_size(document) > len(shared[0])


@pytest.mark.parametrize(
    "document",
    [
        {},
        {"items": [{} for _ in range(100)]},
        {"nested": [[[[[0]]]]]},
        {"values": [None, True, False, 0, "x"] * 20},
    ],
)
def test_serialized_size_hint_conservatively_bounds_builtin_json_trees(document):
    payload = json.dumps(document, separators=(",", ":"))

    assert _serialized_document_size_estimate(payload) >= _owned_document_size(
        document
    )


@pytest.mark.parametrize(["max_documents", "max_bytes"], [(0, 1), (1, 0), (-1, 1)])
def test_batch_limits_must_be_positive_without_opening(
    engine, max_documents, max_bytes
):
    reader = make_reader(engine)
    with pytest.raises(ValueError, match="must be positive"):
        reader.fetch(max_documents, max_bytes)
    assert reader.connection is None
    reader.close()


def test_row_and_byte_targets_release_the_job_not_the_snapshot(engine):
    reader = make_reader(engine)
    try:
        first = reader.fetch(64, 1)
        assert first.documents == [{"_id": "0"}]
        assert first.estimated_bytes > 1
        assert not first.exhausted
        second = reader.fetch(2, 1024 * 1024)
        assert second.documents == [{"_id": "1"}, {"_id": "2"}]
        assert engine._active_scan_count == 1
        rest = reader.fetch(64, 1024 * 1024)
        assert rest.documents == [{"_id": "3"}, {"_id": "4"}]
        assert rest.exhausted
        assert reader.close_completed.is_set()
        assert reader.fetch(1, 1).documents == []
    finally:
        reader.close()


def test_default_sql_scan_reuses_payload_size_without_walking_document(engine):
    reader = make_reader(engine)
    try:
        with patch(
            "mongoeco.engines._sqlite_reader._owned_document_size",
            side_effect=AssertionError("unexpected structural size walk"),
        ):
            batch = reader.fetch(64, 1024 * 1024)
    finally:
        reader.close()

    assert batch.documents == [{"_id": str(index)} for index in range(5)]
    assert batch.estimated_bytes > 0
    assert batch.exhausted


def test_fallback_scan_keeps_structural_size_estimation(engine, monkeypatch):
    semantics = compile_find_semantics({})
    monkeypatch.setattr(
        engine,
        "_open_scan_documents_sync",
        lambda _reader: iter(({"_id": "fallback"},)),
    )
    reader = SQLiteScanReader(engine, "test", "records", semantics)
    try:
        with patch(
            "mongoeco.engines._sqlite_reader._owned_document_size",
            wraps=_owned_document_size,
        ) as estimate:
            batch = reader.fetch(64, 1024 * 1024)
    finally:
        reader.close()

    assert batch.documents == [{"_id": "fallback"}]
    estimate.assert_called_once_with(batch.documents[0])


def test_empty_fallback_batches_stop_at_examined_row_quota(engine, monkeypatch):
    engine._scan_examined_limit = 3
    semantics = compile_find_semantics({"keep": True})
    rows = ({"_id": str(index), "keep": False} for index in range(7))

    monkeypatch.setattr(
        engine,
        "_open_scan_documents_sync",
        lambda _reader: FiniteDocumentScan(rows, semantics),
    )
    reader = SQLiteScanReader(engine, "test", "records", semantics)
    try:
        first = reader.fetch(64, 1024 * 1024)
        second = reader.fetch(64, 1024 * 1024)
        last = reader.fetch(64, 1024 * 1024)
    finally:
        reader.close()

    assert first.documents == second.documents == last.documents == []
    assert (first.examined_documents, second.examined_documents) == (3, 3)
    assert not first.exhausted
    assert not second.exhausted
    assert last.examined_documents == 1
    assert last.exhausted


def test_fallback_batch_preserves_matches_across_examined_row_quota(
    engine, monkeypatch
):
    examined_limit = 2
    engine._scan_examined_limit = examined_limit
    semantics = compile_find_semantics({"keep": True})
    rows = iter(
        [
            {"_id": "0", "keep": False},
            {"_id": "1", "keep": True},
            {"_id": "2", "keep": False},
            {"_id": "3", "keep": True},
        ]
    )
    monkeypatch.setattr(
        engine,
        "_open_scan_documents_sync",
        lambda _reader: FiniteDocumentScan(rows, semantics),
    )
    reader = SQLiteScanReader(engine, "test", "records", semantics)
    try:
        first = reader.fetch(64, 1024 * 1024)
        second = reader.fetch(64, 1024 * 1024)
        last = reader.fetch(64, 1024 * 1024)
    finally:
        reader.close()

    assert first.documents == [{"_id": "1", "keep": True}]
    assert second.documents == [{"_id": "3", "keep": True}]
    assert first.examined_documents == second.examined_documents == examined_limit
    assert not first.exhausted
    assert not second.exhausted
    assert last.documents == []
    assert last.examined_documents == 0
    assert last.exhausted


def test_sql_prefilter_executes_only_declared_residual_plan(engine, monkeypatch):
    asyncio.run(
        engine.put_documents_bulk(
            "test",
            "residual",
            [
                {"_id": "keep", "prefilter": True, "kind": "note"},
                {"_id": "reject", "prefilter": True, "kind": "task"},
                {"_id": "outside", "prefilter": False, "kind": "note"},
            ],
        )
    )
    semantics = compile_find_semantics({"prefilter": True, "kind": "note"})
    residual_plan = compile_find_semantics({"kind": "note"}).query_plan
    plan = SQLiteReadExecutionPlan(
        semantics=semantics,
        strategy="hybrid",
        execution_lineage=(),
        use_sql=True,
        sql=(
            "SELECT document FROM documents "
            "WHERE db_name = ? AND coll_name = ? "
            "AND json_extract(document, '$.prefilter') = 1"
        ),
        params=("test", "residual"),
        apply_python_residual=True,
        residual_query_plan=residual_plan,
    )
    observed_plans = []
    original_match = QueryEngine.match_plan

    def observe_match(document, query_plan, **kwargs):
        observed_plans.append(query_plan)
        return original_match(document, query_plan, **kwargs)

    monkeypatch.setattr(engine, "_compile_read_execution_plan", lambda *_a, **_k: plan)
    monkeypatch.setattr(QueryEngine, "match_plan", staticmethod(observe_match))
    reader = SQLiteScanReader(engine, "test", "residual", semantics)
    try:
        batch = reader.fetch(64, 1024 * 1024)
    finally:
        reader.close()

    assert batch.documents == [{"_id": "keep", "prefilter": True, "kind": "note"}]
    assert observed_plans
    assert all(observed is residual_plan for observed in observed_plans)


def test_source_error_preserves_prefix_and_releases_reader(engine, monkeypatch):
    def broken_source(_reader):
        yield {"_id": "prefix"}
        message = "source failed"
        raise ValueError(message)

    monkeypatch.setattr(engine, "_open_scan_documents_sync", broken_source)
    reader = make_reader(engine)
    batch = reader.fetch(64, 1024 * 1024)
    assert batch.documents == [{"_id": "prefix"}]
    assert isinstance(batch.error, ValueError)
    assert batch.exhausted
    assert reader.close_completed.is_set()
    assert engine._active_scan_count == 0


def test_fetch_reports_automatic_cleanup_failure(engine, monkeypatch):
    class BrokenResource:
        def close(self):
            message = "automatic cleanup failed"
            raise ValueError(message)

    monkeypatch.setattr(engine, "_open_scan_documents_sync", lambda _reader: iter(()))
    reader = make_reader(engine)
    reader.own(BrokenResource())

    batch = reader.fetch(1, 1024)

    assert isinstance(batch.error, ValueError)
    assert str(batch.error) == "automatic cleanup failed"
    assert batch.exhausted


def test_owned_connection_close_failure_is_recorded(engine):
    class BrokenConnection:
        def close(self):
            message = "connection close failed"
            raise ValueError(message)

    reader = SQLiteScanReader(
        engine,
        "test",
        "records",
        compile_find_semantics({}),
        tracked=False,
    )
    reader.connection = BrokenConnection()
    reader.owns_connection = True

    with pytest.raises(ValueError, match="connection close failed"):
        reader.close()

    assert isinstance(reader.close_error, ValueError)
    assert reader.close_completed.is_set()


def test_matched_prefilter_row_must_contain_a_document(engine, monkeypatch):
    class InvalidPrefilterSource:
        def __iter__(self):
            return self

        def __next__(self):
            raise StopIteration

        @staticmethod
        def next_examined():
            return True, None

        @staticmethod
        def close():
            return None

    monkeypatch.setattr(
        engine,
        "_open_scan_documents_sync",
        lambda _reader: InvalidPrefilterSource(),
    )
    reader = make_reader(engine)

    batch = reader.fetch(1, 1024)

    assert isinstance(batch.error, RuntimeError)
    assert "has no document" in str(batch.error)
    assert batch.exhausted


def test_cleanup_attempts_every_resource_and_records_failure(engine):
    closed = []

    class SourceResource:
        def __init__(self, name, *, fail=False):
            self.name = name
            self.fail = fail

        def close(self):
            closed.append(self.name)
            if self.fail:
                message = "cleanup failed"
                raise ValueError(message)

    reader = make_reader(engine)
    reader.fetch(1, 1024 * 1024)
    connection = reader.connection
    reader.own(SourceResource("good"))
    reader.own(SourceResource("bad", fail=True))
    reader.own(object())
    with pytest.raises(ValueError, match="cleanup failed"):
        reader.close()
    assert closed == ["bad", "good"]
    assert reader.close_error is not None
    assert reader.close_completed.is_set()
    assert engine._active_scan_count == 0
    assert engine._runtime_diagnostics_info()["readResources"]["closeFailures"] == 1
    with pytest.raises(sqlite3.ProgrammingError):
        connection.execute("SELECT 1")
    with pytest.raises(ValueError, match="cleanup failed"):
        reader.close()
    assert closed == ["bad", "good"]


def test_disconnect_finishes_other_readers_after_cleanup_error(engine):
    class BrokenResource:
        def close(self):
            message = "injected close failure"
            raise ValueError(message)

    first, second = make_reader(engine), make_reader(engine)
    first.fetch(1, 1024 * 1024)
    second.fetch(1, 1024 * 1024)
    first.own(BrokenResource())
    with pytest.raises(ValueError, match="injected close failure"):
        asyncio.run(engine.disconnect())
    assert first.close_completed.is_set()
    assert second.close_completed.is_set()
    assert engine._connection is None
    assert engine._executor is None


def test_unconnected_reader_does_not_register_or_open_resources():
    instance = SQLiteEngine()
    reader = make_reader(instance)
    batch = reader.fetch(1, 1024)
    assert isinstance(batch.error, RuntimeError)
    assert reader.close_completed.is_set()
    assert not instance._runtime_state.scan_readers


def test_connection_binding_is_restored_on_distinct_worker_threads(engine):
    reader = make_reader(engine)
    reader.fetch(1, 1024 * 1024)
    holding, release = threading.Event(), threading.Event()

    def fetch_on_thread(*, hold=False):
        sentinel = object()
        with engine._bind_connection(sentinel):
            batch = reader.fetch(1, 1024 * 1024)
            assert engine._thread_local.connection is sentinel
        assert engine._thread_local.connection is None
        if hold:
            holding.set()
            release.wait(5)
        return threading.get_ident(), batch.documents

    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            first = workers.submit(fetch_on_thread, hold=True)
            try:
                assert holding.wait(2)
                second_id, second_rows = workers.submit(fetch_on_thread).result(2)
            finally:
                release.set()
            first_id, first_rows = first.result(2)
        assert first_id != second_id
        assert first_rows + second_rows == [{"_id": "1"}, {"_id": "2"}]
    finally:
        reader.close()


def test_all_jobs_reuse_the_read_execution_context(engine, monkeypatch):
    marker = contextvars.ContextVar("sqlite_reader_test_marker", default="unset")
    observed = []
    deserialize = engine._deserialize_document

    def decode(payload):
        observed.append(marker.get())
        return deserialize(payload)

    monkeypatch.setattr(engine, "_deserialize_document", decode)
    monkeypatch.setattr("mongoeco.engines.sqlite._ASYNC_SCAN_QUEUE_BATCH_SIZE", 1)

    async def exercise():
        marker.set("read context")
        source = engine.scan_find_semantics(
            "test", "records", compile_find_semantics({})
        ).__aiter__()
        await anext(source)
        marker.set("unrelated caller context")
        await anext(source)
        await source.aclose()

    asyncio.run(exercise())
    assert observed == ["read context", "read context"]


def test_close_uses_cleanup_executor_if_owner_pool_already_stopped(engine):
    async def exercise():
        source = engine.scan_find_semantics(
            "test", "records", compile_find_semantics({})
        ).__aiter__()
        # More than one physical batch is unnecessary: use a non-exhausted
        # one-document batch so this source retains a real reader.
        await anext(source)
        engine._executor.shutdown(wait=True)
        try:
            await source.aclose()
        finally:
            engine._executor = None
        assert engine._active_scan_count == 0

    with patch("mongoeco.engines.sqlite._ASYNC_SCAN_QUEUE_BATCH_SIZE", 1):
        asyncio.run(exercise())


def test_observer_accepts_cancelled_jobs():
    async def exercise():
        future = asyncio.get_running_loop().create_future()
        future.cancel()
        _observe_scan_completion(future)

    asyncio.run(exercise())


def test_shared_fallback_capture_preserves_prefix_before_source_error(monkeypatch):
    instance = SQLiteEngine()
    asyncio.run(instance.connect())

    def rows(_db, _coll):
        yield "one", {"_id": "one", "name": "Ada"}
        message = "fallback source failed"
        raise ValueError(message)

    monkeypatch.setattr(instance, "_load_documents", rows)
    reader = SQLiteScanReader(
        instance, "test", "records", compile_find_semantics({"name": {"$regex": "^A"}})
    )
    try:
        batch = reader.fetch(64, 1024 * 1024)
        assert batch.documents == [{"_id": "one", "name": "Ada"}]
        assert isinstance(batch.error, ValueError)
        assert str(batch.error) == "fallback source failed"
        assert reader.close_completed.is_set()
    finally:
        reader.close()
        asyncio.run(instance.disconnect())


def test_shared_capture_cooperates_with_stop_before_delivering_rows(monkeypatch):
    instance = SQLiteEngine()
    asyncio.run(instance.connect())
    reader = SQLiteScanReader(
        instance, "test", "records", compile_find_semantics({"name": {"$regex": "^A"}})
    )
    visited = []

    def rows(_db, _coll):
        visited.append("one")
        yield "one", {"_id": "one", "name": "Ada"}
        reader.stop_event.set()
        visited.append("two")
        yield "two", {"_id": "two", "name": "Ana"}
        visited.append("three")

    monkeypatch.setattr(instance, "_load_documents", rows)
    try:
        batch = reader.fetch(64, 1024 * 1024)
        assert batch.documents == []
        assert batch.exhausted
        assert visited == ["one", "two"]
        assert reader.close_completed.is_set()
    finally:
        reader.close()
        asyncio.run(instance.disconnect())


def test_resource_close_can_reenter_without_self_deadlock():
    # An isolated process makes a lock regression fail with a watchdog instead
    # of wedging pytest or its own fixture teardown.
    code = """
from mongoeco.engines._sqlite_reader import SQLiteScanReader
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine
reader = SQLiteScanReader(SQLiteEngine(), 'test', 'records', compile_find_semantics({}))
closed = []
class Resource:
    def close(self):
        closed.append(True)
        reader.close()
reader.own(Resource())
reader.close()
assert closed == [True]
assert reader.close_completed.is_set()
assert reader.close_error is None
"""
    subprocess.run(  # noqa: S603 - fixed interpreter and literal regression fixture
        [sys.executable, "-c", code],
        check=True,
        timeout=5,
    )


def test_disconnect_does_not_take_a_worker_connection_lock_on_the_loop():
    code = """
import asyncio
import threading
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.engines.sqlite import SQLiteEngine
async def main():
    engine = SQLiteEngine()
    await engine.connect()
    entered, release = threading.Event(), threading.Event()
    def source_rows(reader):
        entered.set()
        release.wait()
        yield {'_id': 1}
    engine._open_scan_documents_sync = source_rows
    source = engine.scan_find_semantics('test', 'records', compile_find_semantics({}))
    reading = asyncio.create_task(anext(source))
    assert await asyncio.to_thread(entered.wait, 2)
    disconnect = asyncio.create_task(engine.disconnect())
    # FIFO: disconnect executes before this callback. Taking the worker-held
    # connection lock on the loop would prevent the only release callback.
    asyncio.get_running_loop().call_soon(release.set)
    await disconnect
    try:
        await reading
    except StopAsyncIteration:
        pass
    await source.aclose()
    assert engine._connection is None
asyncio.run(main())
"""
    subprocess.run(  # noqa: S603 - fixed interpreter and literal regression fixture
        [sys.executable, "-c", code],
        check=True,
        timeout=5,
    )
