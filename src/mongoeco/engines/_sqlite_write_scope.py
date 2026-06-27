from __future__ import annotations

from collections.abc import Callable
import sqlite3
from types import TracebackType
from typing import Self


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
            self.rollback()
            return False
        self.commit()
        return False

    def commit(self) -> None:
        if not self._active:
            return
        try:
            self._commit_write(self._conn)
        except Exception:
            self.rollback()
            raise
        self._active = False

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
