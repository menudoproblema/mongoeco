"""Write ownership follows the connection, including failed nested cleanup."""

import asyncio
import sqlite3

from contextlib import closing

import pytest

from mongoeco.engines._sqlite_write_scope import sqlite_write_scope
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession


class FaultConnection(sqlite3.Connection):
    fail_prefix = None
    fail_rollback = False

    def rollback(self):
        if self.fail_rollback:
            self.fail_rollback = False
            message = "injected full rollback failure"
            raise sqlite3.OperationalError(message)
        return super().rollback()

    def execute(self, sql, parameters=()):
        if self.fail_prefix is not None and sql.startswith(self.fail_prefix):
            self.fail_prefix = None
            message = "injected cleanup failure"
            raise sqlite3.OperationalError(message)
        return super().execute(sql, parameters)


def connection():
    conn = sqlite3.connect(":memory:", factory=FaultConnection)
    conn.execute("CREATE TABLE records (value INTEGER)")
    return conn


def scope(runtime, conn, session=None):
    return sqlite_write_scope(
        conn,
        begin_write=lambda current: runtime.begin_write(current, session),
        commit_write=lambda current: runtime.commit_write(current, session),
        rollback_write=lambda current: runtime.rollback_write(current, session),
    )


def rows(conn):
    return conn.execute("SELECT value FROM records ORDER BY value").fetchall()


def test_nested_scope_on_another_connection_does_not_release_foreign_savepoint():
    runtime = SQLiteEngine()._session_runtime
    with closing(connection()) as first, closing(connection()) as second:
        with scope(runtime, first):
            first.execute("INSERT INTO records VALUES (1)")
            with scope(runtime, first):
                first.execute("INSERT INTO records VALUES (2)")
                with scope(runtime, second):
                    second.execute("INSERT INTO records VALUES (3)")
                assert not second.in_transaction
                assert first.in_transaction
        assert rows(first) == [(1,), (2,)]
        assert rows(second) == [(3,)]


@pytest.mark.parametrize("failure_prefix", ["ROLLBACK TO", "RELEASE SAVEPOINT"])
def test_failed_inner_cleanup_cannot_be_committed_by_outer_scope(failure_prefix):
    runtime = SQLiteEngine()._session_runtime
    with closing(connection()) as conn:

        def write():
            with scope(runtime, conn):
                conn.execute("INSERT INTO records VALUES (1)")

                def fail_inner():
                    with scope(runtime, conn):
                        conn.execute("INSERT INTO records VALUES (2)")
                        conn.fail_prefix = failure_prefix
                        message = "operation failed"
                        raise ValueError(message)

                with pytest.raises(ValueError, match="operation failed") as failure:
                    fail_inner()
                assert "cleanup failure" in failure.value.__notes__[0]

        with pytest.raises(InvalidOperation, match="rollback"):
            write()
        assert not conn.in_transaction
        assert rows(conn) == []
        with scope(runtime, conn):
            conn.execute("INSERT INTO records VALUES (3)")
        assert rows(conn) == [(3,)]


def test_original_error_survives_inner_cleanup_failure_and_outer_rollback():
    runtime = SQLiteEngine()._session_runtime
    with closing(connection()) as conn:

        def write():
            with scope(runtime, conn):
                conn.execute("INSERT INTO records VALUES (1)")
                with scope(runtime, conn):
                    conn.execute("INSERT INTO records VALUES (2)")
                    conn.fail_prefix = "ROLLBACK TO"
                    message = "operation failed"
                    raise ValueError(message)

        with pytest.raises(ValueError, match="operation failed") as failure:
            write()
        assert "cleanup failure" in failure.value.__notes__[0]
        assert not conn.in_transaction
        assert rows(conn) == []


def test_failed_operation_cleanup_requires_session_abort_before_commit():
    engine = SQLiteEngine()
    runtime = engine._session_runtime
    with closing(connection()) as conn:
        engine._connection = conn
        session = ClientSession()
        runtime.create_session_state(session)
        session.start_transaction()

        def write():
            with scope(runtime, conn, session):
                conn.execute("INSERT INTO records VALUES (1)")
                conn.fail_prefix = "ROLLBACK TO"
                message = "operation failed"
                raise ValueError(message)

        with pytest.raises(ValueError, match="operation failed"):
            write()
        assert session.in_transaction
        with pytest.raises(InvalidOperation, match="rollback"):
            session.commit_transaction()
        with pytest.raises(InvalidOperation, match="rollback"):
            runtime.require_connection(session)
        session.abort_transaction()
        assert not session.in_transaction
        assert rows(conn) == []
        assert runtime.require_connection(session) is conn
        session.close()


@pytest.mark.parametrize("failure", [ValueError("commit failed"), KeyboardInterrupt()])
def test_commit_failure_always_rolls_back_and_preserves_original_error(failure):
    calls = []

    def fail_commit(_conn):
        calls.append("commit")
        raise failure

    def fail_rollback(_conn):
        calls.append("rollback")
        message = "cleanup failed"
        raise RuntimeError(message)

    with (
        pytest.raises(type(failure)) as caught,
        sqlite_write_scope(
            None,
            begin_write=lambda _conn: calls.append("begin"),
            commit_write=fail_commit,
            rollback_write=fail_rollback,
        ),
    ):
        pass
    assert caught.value is failure
    assert calls == ["begin", "commit", "rollback"]
    assert "cleanup failed" in caught.value.__notes__[0]


def test_disconnect_does_not_commit_an_unfinished_write_for_control_maintenance():
    engine = SQLiteEngine()
    asyncio.run(engine.connect())
    conn = engine._connection
    statements = []
    conn.set_trace_callback(statements.append)
    runtime = engine._session_runtime
    runtime.begin_write(conn, None)
    conn.execute("CREATE TABLE unfinished (value INTEGER)")
    runtime.begin_write(conn, None)
    try:
        asyncio.run(engine.disconnect())
        assert "COMMIT" not in statements
        assert runtime._write_states == {}
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            conn.execute("SELECT 1")
    finally:
        conn.close()


def test_failed_full_rollback_is_quarantined_until_successful_retry():
    engine = SQLiteEngine()
    runtime = engine._session_runtime
    with closing(connection()) as conn, closing(connection()) as other:
        engine._connection = conn

        def write():
            with scope(runtime, conn):
                conn.execute("INSERT INTO records VALUES (1)")
                conn.fail_rollback = True
                message = "operation failed"
                raise ValueError(message)

        with pytest.raises(ValueError, match="operation failed") as failure:
            write()
        assert "full rollback failure" in failure.value.__notes__[0]
        assert conn.in_transaction
        with pytest.raises(InvalidOperation, match="rollback"):
            runtime.begin_write(conn, None)
        with pytest.raises(InvalidOperation, match="rollback"):
            runtime.require_connection()
        with scope(runtime, other):
            other.execute("INSERT INTO records VALUES (2)")
        assert rows(other) == [(2,)]
        runtime.rollback_write(conn, None)
        assert rows(conn) == []
        assert runtime._write_states == {}
        assert runtime.require_connection() is conn


def test_session_commit_cannot_finish_an_active_operation_scope():
    engine = SQLiteEngine()
    runtime = engine._session_runtime
    with closing(connection()) as conn:
        engine._connection = conn
        session = ClientSession()
        runtime.create_session_state(session)
        session.start_transaction()
        with scope(runtime, conn, session):
            conn.execute("INSERT INTO records VALUES (1)")
            with pytest.raises(InvalidOperation, match="active SQLite write scope"):
                session.commit_transaction()
            assert session.in_transaction
        session.commit_transaction()
        assert rows(conn) == [(1,)]
        assert runtime._write_states == {}
        session.close()


def test_failed_begin_does_not_claim_a_write_frame():
    runtime = SQLiteEngine()._session_runtime
    with closing(connection()) as conn:
        conn.fail_prefix = "BEGIN"
        with pytest.raises(sqlite3.OperationalError, match="injected"):
            runtime.begin_write(conn, None)
        assert runtime._write_states == {}
        with scope(runtime, conn):
            conn.execute("INSERT INTO records VALUES (1)")
        assert rows(conn) == [(1,)]


@pytest.mark.parametrize("action", ["commit", "rollback"])
def test_finished_scope_is_idempotent(action):
    runtime = SQLiteEngine()._session_runtime
    with closing(connection()) as conn:
        with scope(runtime, conn) as write:
            conn.execute("INSERT INTO records VALUES (1)")
            getattr(write, action)()
            write.commit()
            write.rollback()
        assert rows(conn) == ([(1,)] if action == "commit" else [])
        assert runtime._write_states == {}
