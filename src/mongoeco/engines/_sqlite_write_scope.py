from __future__ import annotations

from typing import TYPE_CHECKING, Self


if TYPE_CHECKING:
    import sqlite3

    from collections.abc import Callable
    from types import TracebackType


class SQLiteWriteScope:
    def __init__(
        self,
        conn: sqlite3.Connection,
        *,
        begin_write: Callable[[sqlite3.Connection], None],
        commit_write: Callable[[sqlite3.Connection], None],
        rollback_write: Callable[[sqlite3.Connection], None],
    ) -> None:
        self._conn = conn
        self._begin_write = begin_write
        self._commit_write = commit_write
        self._rollback_write = rollback_write
        self._active = False

    def __enter__(self) -> Self:
        self._begin_write(self._conn)
        self._active = True
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        if not self._active:
            return False
        if exc_type is not None:
            if exc is not None:
                self._rollback_preserving_error(exc)
            else:
                self.rollback()
            return False
        self.commit()
        return False

    def commit(self) -> None:
        if not self._active:
            return
        try:
            self._commit_write(self._conn)
        except BaseException as error:
            self._rollback_preserving_error(error)
            raise
        self._active = False

    def _rollback_preserving_error(self, error: BaseException) -> None:
        try:
            self.rollback()
        except BaseException as cleanup_error:
            error.add_note(f"SQLite write scope cleanup failed: {cleanup_error}")

    def rollback(self) -> None:
        if not self._active:
            return
        try:
            self._rollback_write(self._conn)
        finally:
            self._active = False


def sqlite_write_scope(
    conn: sqlite3.Connection,
    *,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
) -> SQLiteWriteScope:
    return SQLiteWriteScope(
        conn,
        begin_write=begin_write,
        commit_write=commit_write,
        rollback_write=rollback_write,
    )
