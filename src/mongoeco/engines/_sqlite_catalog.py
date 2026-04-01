from __future__ import annotations

import sqlite3

from mongoeco.core.json_compat import json_loads
from mongoeco.types import SearchIndexDefinition


def load_search_index_rows(
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    *,
    name: str | None = None,
) -> list[tuple[SearchIndexDefinition, str | None, float | None]]:
    sql = """
        SELECT name, index_type, definition_json, physical_name, ready_at_epoch
        FROM search_indexes
        WHERE db_name = ? AND coll_name = ?
    """
    params: list[object] = [db_name, coll_name]
    if name is not None:
        sql += " AND name = ?"
        params.append(name)
    sql += " ORDER BY name"
    cursor = conn.execute(sql, tuple(params))
    if cursor is None:
        return []
    try:
        rows = cursor.fetchall()
        if not isinstance(rows, list):
            return []
        return [
            (
                SearchIndexDefinition(
                    json_loads(definition_json),
                    name=row_name,
                    index_type=index_type,
                ),
                physical_name,
                ready_at_epoch,
            )
            for row_name, index_type, definition_json, physical_name, ready_at_epoch in rows
        ]
    finally:
        close = getattr(cursor, "close", None)
        if callable(close):
            close()


def load_collection_options(
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
) -> dict[str, object] | None:
    row = conn.execute(
        """
        SELECT options_json
        FROM collections
        WHERE db_name = ? AND coll_name = ?
        """,
        (db_name, coll_name),
    ).fetchone()
    if row is None:
        return None
    return json_loads(row[0] or "{}")


def list_database_names(conn: sqlite3.Connection) -> list[str]:
    cursor = conn.execute(
        """
        SELECT db_name
        FROM collections
        UNION
        SELECT db_name
        FROM documents
        UNION
        SELECT db_name
        FROM indexes
        UNION
        SELECT db_name
        FROM search_indexes
        ORDER BY db_name
        """
    )
    return [row[0] for row in cursor.fetchall()]


def list_collection_names(conn: sqlite3.Connection, db_name: str) -> list[str]:
    cursor = conn.execute(
        """
        SELECT coll_name
        FROM collections
        WHERE db_name = ?
        UNION
        SELECT coll_name
        FROM documents
        WHERE db_name = ?
        UNION
        SELECT coll_name
        FROM indexes
        WHERE db_name = ?
        UNION
        SELECT coll_name
        FROM search_indexes
        WHERE db_name = ?
        ORDER BY coll_name
        """,
        (db_name, db_name, db_name, db_name),
    )
    return [row[0] for row in cursor.fetchall()]
