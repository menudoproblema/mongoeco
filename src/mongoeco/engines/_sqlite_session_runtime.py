from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession, EngineTransactionContext


if TYPE_CHECKING:
    import sqlite3


@dataclass(slots=True)
class _WriteState:
    # None represents an owned outer transaction, names represent savepoints.
    frames: list[str | None] = field(default_factory=list)
    rollback_required: bool = False


class SQLiteSessionRuntime:
    def __init__(self, engine):
        self._engine = engine
        self._write_savepoint_counter = 0
        self._write_states: dict[sqlite3.Connection, _WriteState] = {}

    def _next_write_savepoint_name(self) -> str:
        self._write_savepoint_counter += 1
        return f"mongoeco_write_{self._write_savepoint_counter}"

    def _ensure_write_state_usable(self, conn: sqlite3.Connection) -> None:
        state = self._write_states.get(conn)
        if state is not None and state.rollback_required:
            msg = "SQLite connection requires rollback after failed write cleanup"
            raise InvalidOperation(msg)

    def _discard_finished_write_state(self, conn: sqlite3.Connection) -> None:
        state = self._write_states.get(conn)
        if state is not None and not state.frames and not state.rollback_required:
            del self._write_states[conn]

    def clear_write_states(self) -> None:
        """Release write-frame references when the engine detaches its storage."""
        self._write_states.clear()

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
                metadata={
                    "path": self._engine._path,
                    "snapshot_version": self._engine._mvcc_version,
                },
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
                msg = "SQLiteEngine must be connected before starting a transaction"
                raise InvalidOperation(msg)
            if self._engine._transaction_owner_session_id is not None:
                msg = (
                    "SQLiteEngine already has an active transaction "
                    "bound to another session"
                )
                raise InvalidOperation(msg)
            conn = self._engine._connection
            self._ensure_write_state_usable(conn)
            conn.execute("BEGIN")
            self._engine._transaction_owner_session_id = session.session_id
            self._engine._mvcc_version += 1
            self.sync_session_state(session, transaction_active=True)

    def commit_session_transaction(self, session: ClientSession) -> None:
        with self._engine._lock:
            if self._engine._connection is None:
                msg = "SQLiteEngine is not connected"
                raise InvalidOperation(msg)
            if self._engine._transaction_owner_session_id != session.session_id:
                msg = "This session does not own the active SQLite transaction"
                raise InvalidOperation(msg)
            conn = self._engine._connection
            self._ensure_write_state_usable(conn)
            state = self._write_states.get(conn)
            if state is not None and state.frames:
                msg = "Cannot commit during an active SQLite write scope"
                raise InvalidOperation(msg)
            conn.commit()
            self._write_states.pop(conn, None)
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
            self._write_states.pop(self._engine._connection, None)
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
            msg = "This session was not created by this SQLiteEngine"
            raise InvalidOperation(msg)

    def _ensure_transaction_session_can_use_engine(
        self, context: ClientSession | None
    ) -> None:
        self.ensure_session_can_use_engine(context)
        if (
            context is not None
            and context.in_transaction
            and not self.session_owns_transaction(context)
        ):
            msg = "This session does not own the active SQLite transaction"
            raise InvalidOperation(msg)

    def require_connection(
        self, context: ClientSession | None = None
    ) -> sqlite3.Connection:
        thread_bound = getattr(self._engine._thread_local, "connection", None)
        if thread_bound is not None:
            self._ensure_transaction_session_can_use_engine(context)
            self._ensure_write_state_usable(thread_bound)
            return thread_bound
        if self._engine._connection is None:
            msg = "SQLiteEngine is not connected"
            raise RuntimeError(msg)
        self._ensure_transaction_session_can_use_engine(context)
        if (
            self._engine._transaction_owner_session_id is not None
            and not self.session_owns_transaction(context)
        ):
            msg = "SQLiteEngine has an active transaction bound to another session"
            raise InvalidOperation(msg)
        self._ensure_write_state_usable(self._engine._connection)
        return self._engine._connection

    def begin_write(
        self, conn: sqlite3.Connection, context: ClientSession | None
    ) -> None:
        self._ensure_transaction_session_can_use_engine(context)
        self._ensure_write_state_usable(conn)
        savepoint_name = None
        if self.session_owns_transaction(context) or conn.in_transaction is True:
            savepoint_name = self._next_write_savepoint_name()
            conn.execute(f"SAVEPOINT {savepoint_name}")
        else:
            conn.execute("BEGIN IMMEDIATE")
        self._write_states.setdefault(conn, _WriteState()).frames.append(savepoint_name)

    def commit_write(
        self, conn: sqlite3.Connection, context: ClientSession | None
    ) -> None:
        self._ensure_transaction_session_can_use_engine(context)
        self._ensure_write_state_usable(conn)
        state = self._write_states.get(conn)
        savepoint_name = (
            state.frames[-1] if state is not None and state.frames else None
        )
        if savepoint_name is not None:
            conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
        elif not self.session_owns_transaction(context):
            conn.commit()
        if state is not None and state.frames:
            state.frames.pop()
        self._discard_finished_write_state(conn)

    def rollback_write(
        self, conn: sqlite3.Connection, context: ClientSession | None
    ) -> None:
        self._ensure_transaction_session_can_use_engine(context)
        state = self._write_states.setdefault(conn, _WriteState())
        # Consume only this scope's frame even if its cleanup fails. An outer
        # rollback must target its own boundary, never retry the failed child.
        savepoint_name = state.frames.pop() if state.frames else None
        try:
            if savepoint_name is not None:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint_name}")
                conn.execute(f"RELEASE SAVEPOINT {savepoint_name}")
            elif not self.session_owns_transaction(context):
                conn.rollback()
                state.rollback_required = False
        except BaseException:
            state.rollback_required = True
            raise
        finally:
            self._clear_rollback_sensitive_caches()
            self._discard_finished_write_state(conn)
