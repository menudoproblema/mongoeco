from __future__ import annotations

from contextlib import contextmanager
import sqlite3

from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession, EngineTransactionContext


class SQLiteSessionRuntime:
    def __init__(self, engine):
        self._engine = engine

    def create_session_state(self, session: ClientSession) -> None:
        engine_key = self._engine._engine_key()
        session.bind_engine_context(
            EngineTransactionContext(
                engine_key=engine_key,
                connected=self._engine._connection is not None,
                supports_transactions=True,
                transaction_active=False,
                metadata={"path": self._engine._path, "snapshot_version": self._engine._mvcc_version},
            )
        )
        session.register_transaction_hooks(
            engine_key,
            start=self.start_session_transaction,
            commit=self.commit_session_transaction,
            abort=self.abort_session_transaction,
        )

    def sync_session_state(
        self,
        session: ClientSession,
        *,
        transaction_active: bool | None = None,
    ) -> None:
        state = session.get_engine_context(self._engine._engine_key())
        if state is None:
            return
        state.connected = self._engine._connection is not None
        if transaction_active is not None:
            state.transaction_active = transaction_active
        state.metadata["snapshot_version"] = self._engine._mvcc_version

    def start_session_transaction(self, session: ClientSession) -> None:
        with self._engine._lock:
            if self._engine._connection is None:
                raise InvalidOperation("SQLiteEngine must be connected before starting a transaction")
            if self._engine._transaction_owner_session_id is not None:
                raise InvalidOperation("SQLiteEngine already has an active transaction bound to another session")
            conn = self._engine._connection
            conn.execute("BEGIN")
            self._engine._transaction_owner_session_id = session.session_id
            self._engine._mvcc_version += 1
            self.sync_session_state(session, transaction_active=True)

    def commit_session_transaction(self, session: ClientSession) -> None:
        with self._engine._lock:
            if self._engine._connection is None:
                raise InvalidOperation("SQLiteEngine is not connected")
            if self._engine._transaction_owner_session_id != session.session_id:
                raise InvalidOperation("This session does not own the active SQLite transaction")
            try:
                self._engine._connection.commit()
            finally:
                self._engine._transaction_owner_session_id = None
                self._engine._mvcc_version += 1
                self.sync_session_state(session, transaction_active=False)

    def abort_session_transaction(self, session: ClientSession) -> None:
        with self._engine._lock:
            if self._engine._connection is None:
                return
            if self._engine._transaction_owner_session_id != session.session_id:
                return
            try:
                self._engine._connection.rollback()
            finally:
                self._engine._transaction_owner_session_id = None
                self.sync_session_state(session, transaction_active=False)

    @contextmanager
    def bind_connection(self, conn: sqlite3.Connection):
        previous = getattr(self._engine._thread_local, "connection", None)
        self._engine._thread_local.connection = conn
        try:
            yield
        finally:
            self._engine._thread_local.connection = previous

    def session_owns_transaction(self, context: ClientSession | None) -> bool:
        return (
            context is not None
            and context.in_transaction
            and self._engine._transaction_owner_session_id == context.session_id
        )

    def require_connection(self, context: ClientSession | None = None) -> sqlite3.Connection:
        thread_bound = getattr(self._engine._thread_local, "connection", None)
        if thread_bound is not None:
            return thread_bound
        if self._engine._connection is None:
            raise RuntimeError("SQLiteEngine is not connected")
        if self._engine._transaction_owner_session_id is not None and not self.session_owns_transaction(context):
            raise InvalidOperation("SQLiteEngine has an active transaction bound to another session")
        return self._engine._connection

    def begin_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self.session_owns_transaction(context):
            conn.execute("BEGIN")

    def commit_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self.session_owns_transaction(context):
            conn.commit()

    def rollback_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self.session_owns_transaction(context):
            conn.rollback()
