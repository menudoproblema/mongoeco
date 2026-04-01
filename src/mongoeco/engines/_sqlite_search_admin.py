from __future__ import annotations

import sqlite3
from collections.abc import Callable

from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.core.search import validate_search_index_definition
from mongoeco.core.json_compat import json_dumps_compact, json_loads
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, SearchIndexDefinition, SearchIndexDocument

from mongoeco.engines._shared_runtime import build_search_index_documents


def list_search_index_documents(
    rows: list[tuple[SearchIndexDefinition, str | None, float | None]],
    *,
    is_ready: Callable[[float | None], bool],
    name: str | None,
) -> list[SearchIndexDocument]:
    return build_search_index_documents(
        rows,
        get_name=lambda row: row[0].name,
        get_definition=lambda row: row[0],
        get_ready_at_epoch=lambda row: row[2],
        is_ready=is_ready,
        name=name,
    )


def create_search_index(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    definition: SearchIndexDefinition,
    deadline: float | None,
    begin_write: Callable[[sqlite3.Connection], None],
    ensure_collection_row: Callable[[sqlite3.Connection, str, str], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    ensure_search_backend: Callable[[sqlite3.Connection, str, str, SearchIndexDefinition, str | None], str | None],
    physical_search_index_name: Callable[[str, str, str], str],
    pending_ready_at: Callable[[], float | None],
) -> str:
    normalized_definition = SearchIndexDefinition(
        validate_search_index_definition(
            definition.definition,
            index_type=definition.index_type,
        ),
        name=definition.name,
        index_type=definition.index_type,
    )
    physical_name = (
        physical_search_index_name(db_name, coll_name, normalized_definition.name)
        if normalized_definition.index_type == "search"
        else None
    )
    try:
        begin_write(conn)
        ensure_collection_row(conn, db_name, coll_name)
        row = conn.execute(
            """
            SELECT index_type, definition_json
            FROM search_indexes
            WHERE db_name = ? AND coll_name = ? AND name = ?
            """,
            (db_name, coll_name, normalized_definition.name),
        ).fetchone()
        if row is not None:
            existing = SearchIndexDefinition(
                json_loads(row[1]),
                name=normalized_definition.name,
                index_type=row[0],
            )
            if existing != normalized_definition:
                raise OperationFailure(
                    f"Conflicting search index definition for '{normalized_definition.name}'"
                )
            commit_write(conn)
            return normalized_definition.name
        enforce_deadline(deadline)
        conn.execute(
            """
            INSERT INTO search_indexes (
                db_name, coll_name, name, index_type, definition_json, physical_name, ready_at_epoch
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                db_name,
                coll_name,
                normalized_definition.name,
                normalized_definition.index_type,
                json_dumps_compact(normalized_definition.definition, sort_keys=True),
                physical_name,
                pending_ready_at(),
            ),
        )
        ensure_search_backend(
            conn,
            db_name,
            coll_name,
            normalized_definition,
            physical_name,
        )
        commit_write(conn)
        return normalized_definition.name
    except Exception:
        rollback_write(conn)
        raise


def update_search_index(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    name: str,
    definition: Document,
    deadline: float | None,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    drop_search_backend: Callable[[sqlite3.Connection, str | None], None],
    ensure_search_backend: Callable[[sqlite3.Connection, str, str, SearchIndexDefinition, str | None], str | None],
    physical_search_index_name: Callable[[str, str, str], str],
    pending_ready_at: Callable[[], float | None],
) -> None:
    try:
        begin_write(conn)
        row = conn.execute(
            """
            SELECT index_type, physical_name
            FROM search_indexes
            WHERE db_name = ? AND coll_name = ? AND name = ?
            """,
            (db_name, coll_name, name),
        ).fetchone()
        if row is None:
            raise OperationFailure(f"search index not found with name [{name}]")
        normalized_definition = validate_search_index_definition(
            definition,
            index_type=row[0],
        )
        enforce_deadline(deadline)
        drop_search_backend(conn, row[1])
        physical_name = (
            physical_search_index_name(db_name, coll_name, name)
            if row[0] == "search"
            else None
        )
        conn.execute(
            """
            UPDATE search_indexes
            SET definition_json = ?, physical_name = ?, ready_at_epoch = ?
            WHERE db_name = ? AND coll_name = ? AND name = ?
            """,
            (
                json_dumps_compact(normalized_definition, sort_keys=True),
                physical_name,
                pending_ready_at(),
                db_name,
                coll_name,
                name,
            ),
        )
        ensure_search_backend(
            conn,
            db_name,
            coll_name,
            SearchIndexDefinition(
                normalized_definition,
                name=name,
                index_type=row[0],
            ),
            physical_name,
        )
        commit_write(conn)
    except Exception:
        rollback_write(conn)
        raise


def drop_search_index(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    name: str,
    deadline: float | None,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    drop_search_backend: Callable[[sqlite3.Connection, str | None], None],
) -> None:
    try:
        begin_write(conn)
        row = conn.execute(
            """
            SELECT physical_name
            FROM search_indexes
            WHERE db_name = ? AND coll_name = ? AND name = ?
            """,
            (db_name, coll_name, name),
        ).fetchone()
        if row is None:
            raise OperationFailure(f"search index not found with name [{name}]")
        enforce_deadline(deadline)
        conn.execute(
            """
            DELETE FROM search_indexes
            WHERE db_name = ? AND coll_name = ? AND name = ?
            """,
            (db_name, coll_name, name),
        )
        drop_search_backend(conn, row[0])
        commit_write(conn)
    except Exception:
        rollback_write(conn)
        raise
