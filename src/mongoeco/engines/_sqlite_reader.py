"""A SQLite read resource, independent of the worker executing its next batch."""

from __future__ import annotations

import datetime
import sqlite3
import sys
import threading

from contextlib import nullcontext, suppress
from dataclasses import dataclass
from typing import TYPE_CHECKING

from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.engines._sqlite_connection import sqlite_read_scope


if TYPE_CHECKING:
    from collections.abc import Iterator

    from mongoeco.engines.semantic_core import EngineFindSemantics
    from mongoeco.engines.sqlite import SQLiteEngine
    from mongoeco.session import ClientSession
    from mongoeco.types import Document


_SERIALIZED_TREE_SIZE_FACTOR = 64


def _owned_document_size(document: Document) -> int:
    """Conservative Python-container cost, not RSS or a native allocation limit."""
    pending: list[object] = [document]
    visited_containers: set[int] = set()
    size = 0
    while pending:
        value = pending.pop()
        is_mapping = isinstance(value, dict)
        is_sequence = isinstance(value, (list, tuple))
        if is_mapping or is_sequence:
            identity = id(value)
            if identity in visited_containers:
                continue
            visited_containers.add(identity)
        size += sys.getsizeof(value)
        if is_mapping:
            pending.extend(value.keys())
            pending.extend(value.values())
        elif is_sequence:
            pending.extend(value)
    return size


def _serialized_document_size_estimate(payload: str | bytes) -> int:
    """Cheap conservative target for trees decoded by the built-in JSON codec."""
    return sys.getsizeof(payload) * _SERIALIZED_TREE_SIZE_FACTOR


@dataclass(slots=True)
class SQLiteReadBatch:
    documents: list[Document]
    exhausted: bool
    error: BaseException | None = None
    estimated_bytes: int = 0
    examined_documents: int = 0


class SQLiteScanReader:
    """Serial open/fetch/close jobs; no thread-local binding survives a job.

    A paused reader owns a connection/snapshot, but never an executor worker.
    Disconnect can close it directly without waiting for its consumer. Native
    calls already running finish under this reader's lock before close proceeds.
    """

    def __init__(  # noqa: PLR0913 - explicit resource ownership and operation boundary
        self,
        engine: SQLiteEngine,
        db_name: str,
        coll_name: str,
        semantics: EngineFindSemantics,
        *,
        context: ClientSession | None = None,
        stop_event: threading.Event | None = None,
        tracked: bool = True,
    ) -> None:
        self.engine = engine
        self.db_name = db_name
        self.coll_name = coll_name
        self.semantics = semantics
        self.context = context
        self.stop_event = stop_event if stop_event is not None else threading.Event()
        self.connection: sqlite3.Connection | None = None
        self.owns_connection = False
        self.closed = False
        self.close_completed = threading.Event()
        self.close_error: BaseException | None = None
        self.retained_snapshot_bytes = 0
        self._tracked = tracked
        self._registered = False
        # A source's close/finalizer can reenter close on this thread. The
        # terminal flag prevents recursive cleanup; other workers still wait
        # for the physical job to finish before touching the connection.
        self._lock = threading.RLock()
        self._documents: Iterator[Document] | None = None
        self._resources: list[object] = []
        self._document_size_hints: dict[int, int] = {}
        self._delivered = 0

    def own(self, resource: object) -> None:
        """Retain physical cursors even when iterator adapters do not close them."""
        self._resources.append(resource)

    def attach_size_hint(self, document: Document, estimated_bytes: int) -> Document:
        """Carry a one-use size estimate without changing the public document."""
        self._document_size_hints[id(document)] = estimated_bytes
        return document

    def _take_size_hint(self, document: Document) -> int | None:
        return self._document_size_hints.pop(id(document), None)

    def _binding(self):
        if self.connection is None:
            return nullcontext()
        return self.engine._bind_connection(self.connection)

    def _connection_guard(self):
        return nullcontext() if self.owns_connection else self.engine._lock

    def _open(self) -> None:
        engine = self.engine
        if self._tracked:
            with engine._lifecycle_condition:
                if engine._disconnecting or engine._connection_count == 0:
                    message = "SQLiteEngine is disconnecting"
                    raise RuntimeError(message)
                with engine._scan_condition:
                    engine._runtime_state.scan_readers.add(self)
                    engine._runtime_state.scan_stop_events.add(self.stop_event)
                    engine._active_scan_count += 1
                    self._registered = True

        engine._ensure_session_can_use_engine(self.context)
        with engine._lock:
            try:
                connection = engine._require_connection(self.context)
            except RuntimeError:
                connection = None
            if connection is not None and not engine._is_profile_namespace(
                self.coll_name
            ):
                with engine._bind_connection(connection):
                    engine._purge_expired_documents_sync(
                        connection,
                        self.db_name,
                        self.coll_name,
                        context=self.context,
                        now=self.semantics.variables.now.replace(tzinfo=datetime.UTC),
                    )
            # A one-result operation finishes under the shared connection guard
            # in this same job; it need not retain a separate read connection.
            if self.semantics.limit != 1 and engine._can_use_dedicated_reader(
                self.context
            ):
                self.connection = engine._create_sqlite_connection()
                self.owns_connection = True
                self.connection.execute("BEGIN")
            else:
                self.connection = connection

        with (
            self._connection_guard(),
            self._binding(),
            sqlite_read_scope(self.connection),
        ):
            self._documents = iter(engine._open_scan_documents_sync(self))
            if not self.owns_connection and self.semantics.limit == 1:
                # Finish even a lazy fallback before releasing the short view.
                # Longer shared reads already capture their source under it.
                first = next(self._documents, None)
                self._documents = iter(()) if first is None else iter((first,))

    def fetch(self, max_documents: int, max_bytes: int) -> SQLiteReadBatch:
        """Fetch one finite batch, preserving a valid prefix before a source error.

        The byte target may be exceeded by one indivisible document, never
        rejected or truncated by an internal batch policy. This is not a new
        public aggregation or result-size limit.
        """
        if max_documents <= 0 or max_bytes <= 0:
            message = "read batch limits must be positive"
            raise ValueError(message)
        with self._lock:
            batch = SQLiteReadBatch([], exhausted=False)
            if self.closed or self.stop_event.is_set():
                self._close_locked()
                batch.exhausted = True
                return batch
            try:
                enforce_deadline(self.semantics.deadline)
                if self._documents is None:
                    self._open()
                self._fill_batch(batch, max_documents, max_bytes)
            except StopIteration:
                batch.exhausted = True
            except BaseException as error:
                batch.error = error
                batch.exhausted = True
            if batch.exhausted or self.stop_event.is_set():
                try:
                    self._close_locked()
                except BaseException as error:
                    if batch.error is None:
                        batch.error = error
                batch.exhausted = True
            return batch

    def _fill_batch(
        self, batch: SQLiteReadBatch, max_documents: int, max_bytes: int
    ) -> None:
        with self._connection_guard(), self._binding():
            while (
                len(batch.documents) < max_documents
                and batch.examined_documents < self.engine._scan_examined_limit
            ):
                if self.stop_event.is_set():
                    batch.exhausted = True
                    break
                enforce_deadline(self.semantics.deadline)
                next_examined = getattr(self._documents, "next_examined", None)
                if callable(next_examined):
                    matched, document = next_examined()
                    batch.examined_documents += 1
                    if not matched:
                        continue
                    if document is None:
                        message = "matched SQLite scan row has no document"
                        raise RuntimeError(message)
                else:
                    document = next(self._documents)
                    batch.examined_documents += 1
                if self.stop_event.is_set():
                    batch.exhausted = True
                    break
                batch.documents.append(document)
                size_hint = self._take_size_hint(document)
                batch.estimated_bytes += (
                    size_hint
                    if size_hint is not None
                    else _owned_document_size(document)
                )
                self._delivered += 1
                if self.semantics.limit and self._delivered >= self.semantics.limit:
                    batch.exhausted = True
                    break
                if batch.estimated_bytes >= max_bytes:
                    break

    def close(self) -> None:
        self.stop_event.set()
        with self._lock:
            self._close_locked()

    def _close_locked(self) -> None:
        if self.closed:
            if self.close_error is not None:
                raise self.close_error
            return
        self.closed = True
        error: BaseException | None = None
        try:
            with self._connection_guard(), self._binding():
                resources = self._resources
                self._resources = []
                self._document_size_hints.clear()
                if self._documents is not None:
                    resources.append(self._documents)
                    self._documents = None
                for resource in reversed(resources):
                    close = getattr(resource, "close", None)
                    if callable(close):
                        try:
                            # A shared connection may have been explicitly
                            # closed by its owner after an untracked sync scan.
                            with suppress(sqlite3.ProgrammingError):
                                close()
                        except BaseException as close_error:
                            error = error or close_error
        finally:
            if self.owns_connection and self.connection is not None:
                try:
                    self.connection.close()
                except BaseException as close_error:
                    error = error or close_error
            self.connection = None
            self.close_error = error
            with self.engine._scan_condition:
                self.retained_snapshot_bytes = 0
                if error is not None and self._tracked:
                    self.engine._runtime_state.scan_close_failures += 1
                if self._registered:
                    self.engine._runtime_state.scan_readers.discard(self)
                    self.engine._runtime_state.scan_stop_events.discard(self.stop_event)
                    self.engine._active_scan_count -= 1
                    self._registered = False
                    self.engine._scan_condition.notify_all()
            self.close_completed.set()
        if error is not None:
            raise error
