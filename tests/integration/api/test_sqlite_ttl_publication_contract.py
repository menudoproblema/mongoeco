"""TTL candidates cannot authorize stale deletions or own an enclosing write."""

import asyncio
import sqlite3

from contextlib import closing
from datetime import UTC, datetime, timedelta

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines.sqlite import SQLiteEngine


_TTL_DOCUMENT_COUNT = 2
_FUTURE_DOCUMENT_COUNT = 100
_TARGET_DOCUMENT_ID = 42


def _ordered_storage(conn):
    return tuple(
        conn.execute(query).fetchall()
        for query in (
            "SELECT * FROM documents ORDER BY rowid",
            "SELECT * FROM multikey_entries ORDER BY rowid",
            "SELECT * FROM scalar_index_entries ORDER BY rowid",
            "SELECT * FROM ttl_index_entries ORDER BY expires_at_epoch_ms",
        )
    )


def test_ttl_without_due_candidates_visits_zero_documents(monkeypatch):
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine, now_factory=lambda: initial) as client:
            collection = client.test.records
            await collection.insert_many(
                [
                    {"_id": value, "expires": initial + timedelta(days=1)}
                    for value in range(_FUTURE_DOCUMENT_COUNT)
                ]
            )
            await collection.create_index("expires", expire_after_seconds=0)
            loaded = []
            original = engine._load_existing_document_for_storage_key

            def track(*args):
                loaded.append(args[-1])
                return original(*args)

            monkeypatch.setattr(
                engine,
                "_load_existing_document_for_storage_key",
                track,
            )
            assert (await collection.find_one({"_id": _TARGET_DOCUMENT_ID}))[
                "_id"
            ] == _TARGET_DOCUMENT_ID
            assert loaded == []
            conn = engine._connection
            assert (
                conn.execute("SELECT COUNT(*) FROM ttl_index_entries").fetchone()[0]
                == _FUTURE_DOCUMENT_COUNT
            )
            plan = conn.execute(
                """
                EXPLAIN QUERY PLAN
                SELECT storage_key FROM ttl_index_entries
                WHERE collection_id = ? AND expires_at_epoch_ms <= ?
                ORDER BY expires_at_epoch_ms, index_name, storage_key
                """,
                (engine._lookup_collection_id(conn, "test", "records"), 0),
            ).fetchall()
            assert any("idx_ttl_index_entries_expiration" in str(row) for row in plan)

    asyncio.run(exercise())


def test_ttl_schema_migration_backfills_existing_database(tmp_path):
    async def exercise():
        path = str(tmp_path / "legacy-ttl.db")
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        first = SQLiteEngine(path)
        async with AsyncMongoClient(first, now_factory=lambda: initial) as client:
            await client.test.records.create_index("expires", expire_after_seconds=0)
            await client.test.records.insert_one(
                {"_id": 1, "expires": initial + timedelta(days=1)}
            )

        with closing(sqlite3.connect(path)) as conn:
            conn.execute(
                "DELETE FROM mongoeco_schema_migrations WHERE component = ?",
                ("ttl_index_entries",),
            )
            conn.execute("DROP TABLE ttl_index_entries")

        now = [initial]
        second = SQLiteEngine(path)
        async with AsyncMongoClient(second, now_factory=lambda: now[0]) as client:
            assert (
                second._connection.execute(
                    "SELECT COUNT(*) FROM ttl_index_entries"
                ).fetchone()[0]
                == 1
            )
            now[0] += timedelta(days=2)
            assert await client.test.records.find_one({"_id": 1}) is None

    asyncio.run(exercise())


def test_ttl_ddl_removes_persisted_schedule_rows():
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine, now_factory=lambda: initial) as client:
            collection = client.test.records
            await collection.create_index("expires", expire_after_seconds=0)
            await collection.insert_one(
                {"_id": 1, "expires": initial + timedelta(days=1)}
            )
            assert (
                engine._connection.execute(
                    "SELECT COUNT(*) FROM ttl_index_entries"
                ).fetchone()[0]
                == 1
            )
            await collection.drop_index("expires_1")
            assert (
                engine._connection.execute(
                    "SELECT COUNT(*) FROM ttl_index_entries"
                ).fetchone()[0]
                == 0
            )

    asyncio.run(exercise())


def test_ttl_schedule_tracks_arrays_partial_updates_and_removed_dates():
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        engine = SQLiteEngine()
        async with AsyncMongoClient(engine, now_factory=lambda: initial) as client:
            collection = client.test.records
            await collection.create_index(
                "expires",
                expire_after_seconds=0,
                partial_filter_expression={"active": True},
            )
            await collection.insert_many(
                [
                    {
                        "_id": "array",
                        "active": True,
                        "expires": [
                            initial + timedelta(days=1),
                            initial - timedelta(days=1),
                        ],
                    },
                    {
                        "_id": "partial",
                        "active": False,
                        "expires": initial - timedelta(days=1),
                    },
                    {
                        "_id": "removed",
                        "active": True,
                        "expires": initial + timedelta(days=1),
                    },
                ]
            )
            assert await collection.find_one({"_id": "array"}) is None
            assert await collection.find_one({"_id": "partial"}) is not None
            await collection.update_one(
                {"_id": "removed"},
                {"$unset": {"expires": ""}},
            )
            await collection.update_one(
                {"_id": "partial"},
                {"$set": {"active": True}},
            )
            assert await collection.find_one({"_id": "partial"}) is None
            assert await collection.find_one({"_id": "removed"}) is not None
            rows = engine._connection.execute(
                "SELECT storage_key FROM ttl_index_entries"
            ).fetchall()
            assert rows == []

    asyncio.run(exercise())


async def _populate_ttl_records(collection, initial):
    await collection.create_index("expires", expire_after_seconds=0)
    await collection.create_index("tags")
    await collection.create_index("group")
    await collection.create_search_index(
        {"mappings": {"dynamic": False, "fields": {"text": {"type": "string"}}}}
    )
    await collection.insert_many(
        [
            {
                "_id": value,
                "expires": initial + timedelta(days=10),
                "tags": ["old", str(value)],
                "group": value,
                "text": "expiring",
            }
            for value in range(1, _TTL_DOCUMENT_COUNT + 1)
        ]
    )


async def _assert_restored_then_expired(collection, now, initial):
    now[0] = initial
    assert await collection.count_documents({"tags": "old"}) == _TTL_DOCUMENT_COUNT
    result = await collection.find_one({"group": _TTL_DOCUMENT_COUNT})
    assert result["_id"] == _TTL_DOCUMENT_COUNT
    search = [{"$search": {"text": {"query": "expiring", "path": "text"}}}]
    assert len(await collection.aggregate(search).to_list()) == _TTL_DOCUMENT_COUNT
    now[0] += timedelta(days=20)
    assert await collection.count_documents({}) == 0
    assert await collection.aggregate(search).to_list() == []


@pytest.mark.parametrize("action", ["extend", "drop_index", "exclude_partial"])
def test_ttl_revalidates_candidate_after_external_change(tmp_path, monkeypatch, action):
    async def exercise():
        path = str(tmp_path / "interleaving.db")
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        now = [initial]
        engine = SQLiteEngine(path)
        async with AsyncMongoClient(engine, now_factory=lambda: now[0]) as first:
            collection = first.test.records
            await collection.create_index(
                "expires",
                expire_after_seconds=0,
                partial_filter_expression={"active": True},
            )
            await collection.insert_one(
                {"_id": 1, "expires": initial + timedelta(days=10), "active": True}
            )
            with MongoClient(SQLiteEngine(path), now_factory=lambda: initial) as second:
                original = engine._begin_write
                intervened = False

                def interleave(conn, context):
                    nonlocal intervened
                    if not intervened:
                        intervened = True
                        if action == "drop_index":
                            second.test.records.drop_index("expires_1")
                        else:
                            change = (
                                {"expires": initial + timedelta(days=30)}
                                if action == "extend"
                                else {"active": False}
                            )
                            result = second.test.records.update_one(
                                {"_id": 1}, {"$set": change}
                            )
                            assert result.modified_count == 1
                    return original(conn, context)

                monkeypatch.setattr(engine, "_begin_write", interleave)
                now[0] += timedelta(days=20)
                result = await collection.find_one({"_id": 1})
                assert intervened
                assert result is not None
                assert result["_id"] == 1
                if action == "exclude_partial":
                    assert result["active"] is False

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "mode", [(False, False), (False, True), (True, False), (True, True)]
)
@pytest.mark.parametrize("operation", ["read", "write"])
@pytest.mark.parametrize("entry_kind", ["multikey", "scalar", "search"])
def test_failed_ttl_batch_restores_documents_and_all_index_entries(
    tmp_path, monkeypatch, mode, operation, entry_kind
):
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        now = [initial]
        file_backed, transactional = mode
        path = str(tmp_path / "rollback.db") if file_backed else ":memory:"
        engine = SQLiteEngine(path)
        async with AsyncMongoClient(engine, now_factory=lambda: now[0]) as client:
            collection = client.test.records
            await _populate_ttl_records(collection, initial)
            conn = engine._connection
            before = _ordered_storage(conn)
            session = client.start_session() if transactional else None
            if session is not None:
                session.start_transaction()
            method = f"_delete_{entry_kind}_entries_for_storage_key"
            original = getattr(engine, method)
            calls = []

            def fail_second(*args):
                original(*args)
                calls.append(args[-1])
                if len(calls) == _TTL_DOCUMENT_COUNT:
                    message = "TTL index cleanup failed"
                    raise RuntimeError(message)

            monkeypatch.setattr(engine, method, fail_second)
            try:
                now[0] += timedelta(days=20)
                invoke = (
                    collection.find_one
                    if operation == "read"
                    else collection.insert_one
                )
                with pytest.raises(RuntimeError, match="TTL index cleanup failed"):
                    await invoke({"_id": 3}, session=session)
                assert len(calls) == _TTL_DOCUMENT_COUNT
                assert _ordered_storage(conn) == before
                if session is not None:
                    assert session.in_transaction
                    session.commit_transaction()
                assert not conn.in_transaction
                assert engine._session_runtime._write_states == {}
                monkeypatch.setattr(engine, method, original)
                await _assert_restored_then_expired(collection, now, initial)
            finally:
                if session is not None:
                    if session.in_transaction:
                        session.abort_transaction()
                    session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("file_backed", [False, True])
@pytest.mark.parametrize("finish", ["autocommit", "commit", "abort"])
def test_write_with_expired_documents_preserves_transaction_owner(
    tmp_path, file_backed, finish
):
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        now = [initial]
        engine = SQLiteEngine(str(tmp_path / "write.db") if file_backed else ":memory:")
        async with AsyncMongoClient(engine, now_factory=lambda: now[0]) as client:
            collection = client.test.records
            await collection.create_index("expires", expire_after_seconds=0)
            await collection.insert_one(
                {"_id": 1, "expires": initial + timedelta(days=10)}
            )
            session = client.start_session() if finish != "autocommit" else None
            if session is not None:
                session.start_transaction()
            try:
                now[0] += timedelta(days=20)
                await collection.insert_one({"_id": 2}, session=session)
                if session is not None:
                    assert session.in_transaction
                    if finish == "commit":
                        session.commit_transaction()
                    else:
                        session.abort_transaction()
                now[0] = initial
                assert (await collection.find_one({"_id": 1}) is not None) == (
                    finish == "abort"
                )
                assert (await collection.find_one({"_id": 2}) is not None) == (
                    finish != "abort"
                )
            finally:
                if session is not None:
                    if session.in_transaction:
                        session.abort_transaction()
                    session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("file_backed", [False, True])
@pytest.mark.parametrize("finish", ["autocommit", "commit", "abort"])
def test_count_purges_using_bound_clock_and_preserves_session(
    tmp_path, file_backed, finish
):
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        now = [initial]
        path = str(tmp_path / "count.db") if file_backed else ":memory:"
        async with AsyncMongoClient(
            SQLiteEngine(path), now_factory=lambda: now[0]
        ) as client:
            collection = client.test.records
            await collection.create_index("expires", expire_after_seconds=0)
            await collection.insert_one(
                {"_id": 1, "expires": initial + timedelta(days=10)}
            )
            session = client.start_session() if finish != "autocommit" else None
            if session is not None:
                session.start_transaction()
            try:
                now[0] += timedelta(days=20)
                assert await collection.count_documents({}, session=session) == 0
                if session is not None:
                    assert session.in_transaction
                    if finish == "commit":
                        session.commit_transaction()
                    else:
                        session.abort_transaction()
                now[0] = initial
                assert await collection.count_documents({}) == int(finish == "abort")
            finally:
                if session is not None:
                    if session.in_transaction:
                        session.abort_transaction()
                    session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("file_backed", [False, True])
def test_sync_count_purges_without_an_explicit_transaction(tmp_path, file_backed):
    initial = datetime(2100, 1, 1, tzinfo=UTC)
    now = [initial]
    path = str(tmp_path / "sync-count.db") if file_backed else ":memory:"
    with MongoClient(SQLiteEngine(path), now_factory=lambda: now[0]) as client:
        collection = client.test.records
        collection.create_index("expires", expire_after_seconds=0)
        collection.insert_one({"_id": 1, "expires": initial + timedelta(days=10)})
        assert collection.count_documents({}) == 1
        now[0] += timedelta(days=20)
        assert collection.count_documents({}) == 0
