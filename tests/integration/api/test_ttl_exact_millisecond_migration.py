"""Old rounded TTL auxiliaries cannot suppress exact due candidates."""

# ruff: noqa: DTZ001, PLR2004 - BSON UTC and exact migration fixture values

import sqlite3

from contextlib import closing
from datetime import datetime, timedelta

import pytest

from mongoeco import MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("milliseconds", [-1001, 1001])
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_exact_ttl_boundary(engine_factory, milliseconds, offset):
    expiry = datetime(1970, 1, 1) + timedelta(milliseconds=milliseconds)
    now = [expiry - timedelta(milliseconds=2)]
    with MongoClient(engine_factory(), now_factory=lambda: now[0]) as client:
        client.test.c.insert_one({"_id": 1, "expires": expiry})
        client.test.c.create_index("expires", expire_after_seconds=0)
        now[0] = expiry + timedelta(milliseconds=offset)
        assert (client.test.c.find_one({"_id": 1}) is None) == (offset >= 0)


@pytest.mark.parametrize("index_first", [False, True])
def test_legacy_ttl_auxiliaries_rebuild_before_exact_cutoff(tmp_path, index_first):
    path = str(tmp_path / "legacy.sqlite")
    expiry = datetime(1970, 1, 1) - timedelta(milliseconds=1001)
    now = [expiry - timedelta(milliseconds=2)]
    with MongoClient(SQLiteEngine(path), now_factory=lambda: now[0]) as client:
        coll = client.test.c
        if index_first:
            coll.create_index("expires", expire_after_seconds=0)
        coll.insert_one({"_id": 1, "expires": expiry})
        if not index_first:
            coll.create_index("expires", expire_after_seconds=0)
    with closing(sqlite3.connect(path)) as conn, conn:
        key = conn.execute("SELECT storage_key FROM documents").fetchone()[0]
        conn.execute(
            "UPDATE mongoeco_schema_migrations SET version = 1 "
            "WHERE component = 'ttl_index_entries'"
        )
        conn.execute("UPDATE ttl_index_entries SET expires_at_epoch_ms = -1000")
    now[0] = expiry - timedelta(milliseconds=1)
    with MongoClient(SQLiteEngine(path), now_factory=lambda: now[0]) as client:
        assert client.test.c.find_one({"_id": 1}) is not None
        with closing(sqlite3.connect(path)) as conn:
            assert (
                conn.execute("SELECT storage_key FROM documents").fetchone()[0] == key
            )
            assert (
                conn.execute(
                    "SELECT expires_at_epoch_ms FROM ttl_index_entries"
                ).fetchone()[0]
                == -1001
            )
            assert (
                conn.execute(
                    "SELECT version FROM mongoeco_schema_migrations "
                    "WHERE component = 'ttl_index_entries'"
                ).fetchone()[0]
                == 2
            )
        now[0] = expiry
        assert client.test.c.find_one({"_id": 1}) is None
        now[0] = expiry + timedelta(milliseconds=1)
        assert client.test.c.find_one({"_id": 1}) is None


def test_future_ttl_auxiliary_version_is_rejected_without_rebuild(tmp_path):
    path = str(tmp_path / "future.sqlite")
    with MongoClient(SQLiteEngine(path)) as client:
        client.test.c.insert_one({"_id": 1})
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "UPDATE mongoeco_schema_migrations SET version = 99 "
            "WHERE component = 'ttl_index_entries'"
        )
    with (
        pytest.raises(
            OperationFailure, match="unsupported TTL index schema version 99"
        ),
        MongoClient(SQLiteEngine(path)),
    ):
        pass
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("SELECT count(*) FROM documents").fetchone()[0] == 1
        assert (
            conn.execute(
                "SELECT version FROM mongoeco_schema_migrations "
                "WHERE component = 'ttl_index_entries'"
            ).fetchone()[0]
            == 99
        )


def test_failed_ttl_rebuild_rolls_back_auxiliaries_and_version(tmp_path, monkeypatch):
    path = str(tmp_path / "rollback.sqlite")
    now = datetime(1970, 1, 1)
    with MongoClient(SQLiteEngine(path), now_factory=lambda: now) as client:
        client.test.c.insert_one({"_id": 1, "expires": now + timedelta(days=1)})
        client.test.c.create_index("expires", expire_after_seconds=0)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute(
            "UPDATE mongoeco_schema_migrations SET version = 1 "
            "WHERE component = 'ttl_index_entries'"
        )
        before_entries = conn.execute("SELECT * FROM ttl_index_entries").fetchall()
        before_docs = conn.execute("SELECT * FROM documents").fetchall()

    def fail_rebuild(*_args):
        message = "injected rebuild failure"
        raise RuntimeError(message)

    with monkeypatch.context() as patch:
        patch.setattr(SQLiteEngine, "_replace_ttl_entries_for_document", fail_rebuild)
        with (
            pytest.raises(RuntimeError, match="injected rebuild failure"),
            MongoClient(SQLiteEngine(path)),
        ):
            pass
    with closing(sqlite3.connect(path)) as conn:
        assert (
            conn.execute("SELECT * FROM ttl_index_entries").fetchall() == before_entries
        )
        assert conn.execute("SELECT * FROM documents").fetchall() == before_docs
        assert (
            conn.execute(
                "SELECT version FROM mongoeco_schema_migrations "
                "WHERE component = 'ttl_index_entries'"
            ).fetchone()[0]
            == 1
        )
    with MongoClient(SQLiteEngine(path), now_factory=lambda: now) as client:
        assert client.test.c.find_one({"_id": 1}) is not None
