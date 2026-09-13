"""Finite, connection-free preparation jobs for a single SQLite bulk write."""

from __future__ import annotations

import contextvars
import sys
import threading

from dataclasses import dataclass, field
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Callable

    from mongoeco.types import Document


type PreparedDocument = tuple[str, str, list[tuple[str, str, int, str]]]

_BULK_PREPARATION_DOCUMENT_LIMIT = 128
_BULK_PREPARATION_BYTE_TARGET = 1024 * 1024


def _prepared_document_bytes(document: PreparedDocument) -> int:
    """Conservative retained Python size; not a hard RSS/native memory limit."""
    storage_key, payload, index_rows = document
    return (
        sys.getsizeof(document)
        + sys.getsizeof(storage_key)
        + sys.getsizeof(payload)
        + sys.getsizeof(index_rows)
        + sum(
            sys.getsizeof(row) + sum(sys.getsizeof(value) for value in row)
            for row in index_rows
        )
    )


@dataclass(slots=True)
class SQLiteBulkPreparation:
    """One owner submits one step at a time and signals stop on cancellation.

    Prepared rows remain owned until the existing single publication call.
    Bounded steps limit executor occupancy/admission, not total bulk memory.
    No step owns a connection or changes database state.
    """

    documents: list[Document]
    prepare_document: Callable[[Document], PreparedDocument]
    validate_document: Callable[[Document], None] | None = None
    document_limit: int = _BULK_PREPARATION_DOCUMENT_LIMIT
    byte_target: int = _BULK_PREPARATION_BYTE_TARGET
    rows: list[PreparedDocument] = field(default_factory=list, init=False)
    retained_bytes: int = field(default=0, init=False)
    position: int = field(default=0, init=False)
    preparation_error: Exception | None = field(default=None, init=False)
    stop_event: threading.Event = field(default_factory=threading.Event, init=False)

    def __post_init__(self) -> None:
        if self.document_limit <= 0 or self.byte_target <= 0:
            message = "bulk preparation limits must be positive"
            raise ValueError(message)

    def step(self) -> bool:
        """Return completion after a row/byte-bounded, cooperatively stopped job.

        One indivisible document can exceed the byte target. Validation errors
        take precedence over preparation errors, as in the former two-phase
        validation-then-encoding path. After an encoding error only validation
        continues; nothing is published and no more encoded rows are retained.
        """
        batch_bytes = 0
        # Former per-document executor jobs each received an independent copy
        # of the caller's context. A validator/codec must not leak ContextVar
        # mutations into the next document or from validation into encoding.
        context = contextvars.copy_context()
        end = min(self.position + self.document_limit, len(self.documents))
        while self.position < end and not self.stop_event.is_set():
            document = self.documents[self.position]
            self.position += 1
            if self.validate_document is not None:
                context.copy().run(self.validate_document, document)
            if self.stop_event.is_set():
                return False
            if self.preparation_error is not None:
                continue
            try:
                prepared = context.copy().run(self.prepare_document, document)
            except Exception as error:
                self.preparation_error = error
                if self.validate_document is None:
                    raise
                continue
            self.rows.append(prepared)
            size = _prepared_document_bytes(prepared)
            self.retained_bytes += size
            batch_bytes += size
            if batch_bytes >= self.byte_target:
                break
        return self.position == len(self.documents)
