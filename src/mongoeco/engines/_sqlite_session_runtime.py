from __future__ import annotations

from contextlib import contextmanager, suppress
import sqlite3

from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession, EngineTransactionContext


class SQLiteSessionRuntime:
    def __init__(self, engine):
        self._engine = engine
        self._write_savepoint_counter = 0

    def _next_write_savepoint_name(self) -> str:
        self._write_savepoint_counter += 1
        return f"mongoeco_write_{self._write_savepoint_counter}"

    def _write_savepoint_stack(self) -> list[str]:
        stack = getattr(self._engine._thread_local, "write_savepoint_stack", None)
        if stack is None:
            stack = []
            self._engine._thread_local.write_savepoint_stack = stack
        return stack

    def _clear_write_savepoint_stack(self) -> None:
        stack = getattr(self._engine._thread_local, "write_savepoint_stack", None)
        if stack is not None:
            stack.clear()

    def _clear_rollback_sensitive_caches(self) -> None:
        self._engine._invalidate_index_cache()
        self._engine._invalidate_collection_id_cache()
        self._engine._invalidate_collection_features_cache()
        self._engine._ensured_multikey_physical_indexes.clear()
        self._engine._ensured_search_backends.clear()
        self._engine._vector_search_backends.clear()
        self._engine._search_backend_versions.clear()
        self._engine._materialized_search_entry_cache.clear()
        self._engine._clear_compound_search_caches()

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
            self._clear_write_savepoint_stack()
            self._engine._transaction_owner_session_id = session.session_id
            self._engine._mvcc_version += 1
            self.sync_session_state(session, transaction_active=True)

    def commit_session_transaction(self, session: ClientSession) -> None:
        with self._engine._lock:
            if self._engine._connection is None:
                raise InvalidOperation("SQLiteEngine is not connected")
            if self._engine._transaction_owner_session_id != session.session_id:
                raise InvalidOperation("This session does not own the active SQLite transaction")
            self._engine._connection.commit()
            self._clear_write_savepoint_stack()
            self._engine._transaction_owner_session_id = None
            self._engine._mvcc_version += 1
            self.sync_session_state(session, transaction_active=False)

    def abort_session_transaction(self, session: ClientSession) -> None:
        with self._engine._lock:
            if self._engine._connection is None:
                return
            if self._engine._transaction_owner_session_id != session.session_id:
                return
            self._engine._connection.rollback()
            self._clear_rollback_sensitive_caches()
            self._clear_write_savepoint_stack()
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

    def ensure_session_can_use_engine(self, context: ClientSession | None) -> None:
        if context is None:
            return
        if context.get_engine_context(self._engine._engine_key()) is None:
            raise InvalidOperation("This session was not created by this SQLiteEngine")

    def _ensure_transaction_session_can_use_engine(self, context: ClientSession | None) -> None:
        self.ensure_session_can_use_engine(context)
        if context is not None and context.in_transaction and not self.session_owns_transaction(context):
            raise InvalidOperation("This session does not own the active SQLite transaction")

    def require_connection(self, context: ClientSession | None = None) -> sqlite3.Connection:
        thread_bound = getattr(self._engine._thread_local, "connection", None)
        if thread_bound is not None:
            self._ensure_transaction_session_can_use_engine(context)
            return thread_bound
        if self._engine._connection is None:
            raise RuntimeError("SQLiteEngine is not connected")
        self._ensure_transaction_session_can_use_engine(context)
        if self._engine._transaction_owner_session_id is not None and not self.session_owns_transaction(context):
            raise InvalidOperation("SQLiteEngine has an active transaction bound to another session")
        return self._engine._connection

    def begin_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        self._ensure_transaction_session_can_use_engine(context)
        if self.session_owns_transaction(context):
            savepoint_name = self._next_write_savepoint_name()
            conn.execute(f"SAVEPOINT {savepoint_name}")
            self._write_savepoint_stack().append(savepoint_name)
        else:
            conn.execute("BEGIN")

    def commit_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        self._ensure_transaction_session_can_use_engine(context)
        if self.session_owns_transaction(context):
            stack = self._write_savepoint_stack()
            if not stack:
                return
            savepoint_name = stack[-1]
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            stack.pop()
        else:
            conn.commit()

    def rollback_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        self._ensure_transaction_session_can_use_engine(context)
        if self.session_owns_transaction(context):
            stack = self._write_savepoint_stack()
            if not stack:
                return
            savepoint_name = stack[-1]
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
            except Exception:
                self._clear_rollback_sensitive_caches()
                raise
            with suppress(Exception):
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            stack.pop()
            self._clear_rollback_sensitive_caches()
        else:
            try:
                conn.rollback()
            finally:
                self._clear_rollback_sensitive_caches()
