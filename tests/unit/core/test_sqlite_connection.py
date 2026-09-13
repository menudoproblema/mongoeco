"""Connection-local cache epochs and transaction ownership are independent."""

import sqlite3

import pytest

from mongoeco.engines._sqlite_connection import SQLiteConnection, sqlite_read_scope


@pytest.fixture
def connection():
    conn = sqlite3.connect(":memory:", factory=SQLiteConnection)
    try:
        conn.execute("CREATE TABLE records (value INTEGER)")
        yield conn
    finally:
        conn.close()


def test_hot_cache_keeps_identity_until_external_epoch_changes(connection):
    connection.refresh_catalog_validity()
    records = []
    connection.index_catalogs["test", "records"] = (0, records)
    connection.refresh_catalog_validity()
    assert connection.index_catalogs["test", "records"][1] is records
    connection.execute("INSERT INTO records VALUES (1)")
    connection.commit()
    connection.refresh_catalog_validity()
    # Local catalog DDL has explicit generation/invalidation in the engine.
    assert connection.index_catalogs["test", "records"][1] is records


def test_close_reclaims_catalog_even_if_connection_object_is_retained(connection):
    connection.index_catalogs["test", "records"] = (0, [])
    connection.close()
    assert connection.index_catalogs == {}
    connection.close()
    assert connection.index_catalogs == {}


def test_read_scope_owns_only_its_short_transaction(connection):
    with sqlite_read_scope(connection):
        assert connection.in_transaction
        with sqlite_read_scope(connection):
            assert connection.in_transaction
        assert connection.in_transaction
    assert not connection.in_transaction


@pytest.mark.parametrize("fail", [False, True])
def test_read_scope_does_not_finish_user_transaction(connection, fail):
    connection.execute("BEGIN")
    connection.execute("INSERT INTO records VALUES (1)")
    try:
        with sqlite_read_scope(connection):
            if fail:
                message = "source failed"
                raise ValueError(message)
    except ValueError:
        assert fail
    assert connection.in_transaction
    assert connection.execute("SELECT value FROM records").fetchall() == [(1,)]
    connection.rollback()
    assert connection.execute("SELECT value FROM records").fetchall() == []


def test_scope_failure_rolls_back_own_work_and_invalidates_catalog(connection):
    def fail_read():
        with sqlite_read_scope(connection):
            connection.execute("INSERT INTO records VALUES (1)")
            connection.index_catalogs["test", "records"] = (0, [])
            message = "source failed"
            raise ValueError(message)

    with pytest.raises(ValueError, match="source failed"):
        fail_read()
    assert not connection.in_transaction
    assert connection.index_catalogs == {}
    assert connection.execute("SELECT value FROM records").fetchall() == []


def test_cleanup_failure_preserves_original_read_error(connection):
    def fail_read():
        with sqlite_read_scope(connection):
            connection.close()
            message = "source failed"
            raise ValueError(message)

    with pytest.raises(ValueError, match="source failed") as failure:
        fail_read()
    assert "read scope cleanup failed" in failure.value.__notes__[0]


def test_absent_connection_needs_no_scope():
    with sqlite_read_scope(None):
        pass
