"""Connection-owned metadata validity and short, composable read views."""

from __future__ import annotations

import sqlite3

from contextlib import contextmanager
from typing import TYPE_CHECKING, Any


if TYPE_CHECKING:
    from collections.abc import Iterator

    from mongoeco.types import EngineIndexRecord


class SQLiteConnection(sqlite3.Connection):
    """Metadata never crosses connection snapshots or outlives its owner."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.index_catalogs: dict[
            tuple[str, str], tuple[int, list[EngineIndexRecord]]
        ] = {}
        self._catalog_data_version: int | None = None

    def refresh_catalog_validity(self) -> None:
        if self.in_transaction:
            # BEGIN alone does not establish the SQLite read snapshot. Pin it
            # before comparing this connection's external-change observation.
            self.execute("SELECT 1 FROM sqlite_schema LIMIT 1").fetchone()
        version = self.execute("PRAGMA main.data_version").fetchone()[0]
        if version != self._catalog_data_version:
            self.index_catalogs.clear()
            self._catalog_data_version = version

    def close(self) -> None:
        try:
            super().close()
        finally:
            self.index_catalogs.clear()
            self._catalog_data_version = None


@contextmanager
def sqlite_read_scope(conn: sqlite3.Connection | None) -> Iterator[None]:
    """Keep catalog planning and data acquisition in the same SQLite view.

    Join an existing user/write transaction without committing it. A dedicated
    reader begins its own transaction and retains it until physical close.
    This scope only owns a short transaction on otherwise idle connections.
    """
    if not isinstance(conn, sqlite3.Connection) or conn.in_transaction:
        yield
        return
    conn.execute("SAVEPOINT mongoeco_catalog_read")
    try:
        yield
    except BaseException as error:
        try:
            conn.execute("ROLLBACK TO SAVEPOINT mongoeco_catalog_read")
            conn.execute("RELEASE SAVEPOINT mongoeco_catalog_read")
        except sqlite3.Error as cleanup_error:
            error.add_note(f"SQLite read scope cleanup failed: {cleanup_error}")
        if isinstance(conn, SQLiteConnection):
            conn.index_catalogs.clear()
        raise
    else:
        conn.execute("RELEASE SAVEPOINT mongoeco_catalog_read")
