from __future__ import annotations

import sqlite3
from collections.abc import Callable

from mongoeco.errors import CollectionInvalid
from mongoeco.types import SearchIndexDefinition

from mongoeco.engines._shared_namespace_admin import (
    merge_profile_collection_names,
    merge_profile_database_names,
    profile_namespace_options,
)


def collection_options(
    *,
    db_name: str,
    coll_name: str,
    profiler,
    profile_collection_name: str,
    load_collection_options: Callable[[sqlite3.Connection, str, str], dict[str, object] | None],
    collection_exists: Callable[[sqlite3.Connection, str, str], bool],
    conn: sqlite3.Connection,
) -> dict[str, object]:
    profile_options = profile_namespace_options(
        db_name=db_name,
        coll_name=coll_name,
        profiler=profiler,
        profile_collection_name=profile_collection_name,
    )
    if profile_options is not None:
        return profile_options
    options = load_collection_options(conn, db_name, coll_name)
    if options is not None:
        return options
    if collection_exists(conn, db_name, coll_name):
        return {}
    raise CollectionInvalid(f"collection '{coll_name}' does not exist")


def list_databases(
    *,
    conn: sqlite3.Connection,
    profiler,
    list_database_names: Callable[[sqlite3.Connection], list[str]],
) -> list[str]:
    return merge_profile_database_names(list_database_names(conn), profiler)


def list_collections(
    *,
    conn: sqlite3.Connection,
    db_name: str,
    profiler,
    profile_collection_name: str,
    list_collection_names: Callable[[sqlite3.Connection, str], list[str]],
) -> list[str]:
    return merge_profile_collection_names(
        list_collection_names(conn, db_name),
        db_name=db_name,
        profiler=profiler,
        profile_collection_name=profile_collection_name,
    )


def create_collection(
    *,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    options: dict[str, object] | None,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    collection_exists: Callable[[sqlite3.Connection, str, str], bool],
    ensure_collection_row: Callable[[sqlite3.Connection, str, str], None],
) -> None:
    try:
        begin_write(conn)
        if collection_exists(conn, db_name, coll_name):
            raise CollectionInvalid(f"collection '{coll_name}' already exists")
        ensure_collection_row(conn, db_name, coll_name, options=options)
        commit_write(conn)
    except Exception:
        rollback_write(conn)
        raise


def rename_collection(
    *,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    new_name: str,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    collection_exists: Callable[[sqlite3.Connection, str, str], bool],
    mark_index_metadata_changed: Callable[[str, str], None],
    invalidate_collection_id_cache: Callable[[str, str], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
) -> None:
    if coll_name == new_name:
        raise CollectionInvalid("collection names must differ")
    try:
        begin_write(conn)
        if not collection_exists(conn, db_name, coll_name):
            raise CollectionInvalid(f"collection '{coll_name}' does not exist")
        if collection_exists(conn, db_name, new_name):
            raise CollectionInvalid(f"collection '{new_name}' already exists")
        for table_name in ("collections", "documents", "indexes", "search_indexes"):
            conn.execute(
                f"""
                UPDATE {table_name}
                SET coll_name = ?
                WHERE db_name = ? AND coll_name = ?
                """,
                (new_name, db_name, coll_name),
            )
        commit_write(conn)
        mark_index_metadata_changed(db_name, coll_name)
        mark_index_metadata_changed(db_name, new_name)
        invalidate_collection_id_cache(db_name, coll_name)
        invalidate_collection_id_cache(db_name, new_name)
        invalidate_collection_features_cache(db_name, coll_name)
        invalidate_collection_features_cache(db_name, new_name)
    except Exception:
        rollback_write(conn)
        raise


def drop_collection(
    *,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    load_indexes: Callable[[str, str], list[dict[str, object]]],
    load_search_index_rows: Callable[[str, str], list[tuple[SearchIndexDefinition, str | None, float | None]]],
    lookup_collection_id: Callable[[sqlite3.Connection, str, str], int | None],
    quote_identifier: Callable[[str], str],
    drop_search_backend: Callable[[sqlite3.Connection, str | None], None],
    mark_index_metadata_changed: Callable[[str, str], None],
    invalidate_collection_id_cache: Callable[[str, str], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
) -> None:
    indexes = load_indexes(db_name, coll_name)
    search_indexes = load_search_index_rows(db_name, coll_name)
    try:
        begin_write(conn)
        collection_id = lookup_collection_id(conn, db_name, coll_name)
        for index in indexes:
            conn.execute(f"DROP INDEX IF EXISTS {quote_identifier(str(index['physical_name']))}")
            if index.get("scalar_physical_name"):
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(index['scalar_physical_name']))}"
                )
            if index.get("multikey"):
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(index['multikey_physical_name']))}"
                )
        for _definition, physical_name, _ready_at_epoch in search_indexes:
            drop_search_backend(conn, physical_name)
        conn.execute(
            """
            DELETE FROM documents
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        )
        if collection_id is not None:
            conn.execute(
                """
                DELETE FROM scalar_index_entries
                WHERE collection_id = ?
                """,
                (collection_id,),
            )
            conn.execute(
                """
                DELETE FROM multikey_entries
                WHERE collection_id = ?
                """,
                (collection_id,),
            )
        conn.execute(
            """
            DELETE FROM indexes
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        )
        conn.execute(
            """
            DELETE FROM search_indexes
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        )
        conn.execute(
            """
            DELETE FROM collections
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        )
        commit_write(conn)
        mark_index_metadata_changed(db_name, coll_name)
        invalidate_collection_id_cache(db_name, coll_name)
        invalidate_collection_features_cache(db_name, coll_name)
    except Exception:
        rollback_write(conn)
        raise


def drop_database(
    *,
    conn: sqlite3.Connection,
    db_name: str,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    quote_identifier: Callable[[str], str],
    drop_search_backend: Callable[[sqlite3.Connection, str | None], None],
    clear_index_metadata_versions: Callable[[str], None],
    invalidate_index_cache: Callable[[], None],
    invalidate_collection_id_cache: Callable[[], None],
    invalidate_collection_features_cache: Callable[[], None],
    clear_profiler: Callable[[str], None],
) -> None:
    index_rows = conn.execute(
        """
        SELECT physical_name, multikey_physical_name, scalar_physical_name
        FROM indexes
        WHERE db_name = ?
        """,
        (db_name,),
    ).fetchall()
    search_rows = conn.execute(
        """
        SELECT physical_name
        FROM search_indexes
        WHERE db_name = ?
        """,
        (db_name,),
    ).fetchall()
    try:
        begin_write(conn)
        for physical_name, multikey_physical_name, scalar_physical_name in index_rows:
            if physical_name:
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(physical_name))}"
                )
            if scalar_physical_name:
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(scalar_physical_name))}"
                )
            if multikey_physical_name:
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(multikey_physical_name))}"
                )
        for (physical_name,) in search_rows:
            drop_search_backend(conn, physical_name)
        conn.execute(
            """
            DELETE FROM scalar_index_entries
            WHERE collection_id IN (
                SELECT collection_id FROM collections WHERE db_name = ?
            )
            """,
            (db_name,),
        )
        conn.execute(
            """
            DELETE FROM multikey_entries
            WHERE collection_id IN (
                SELECT collection_id FROM collections WHERE db_name = ?
            )
            """,
            (db_name,),
        )
        conn.execute(
            """
            DELETE FROM documents
            WHERE db_name = ?
            """,
            (db_name,),
        )
        conn.execute(
            """
            DELETE FROM indexes
            WHERE db_name = ?
            """,
            (db_name,),
        )
        conn.execute(
            """
            DELETE FROM search_indexes
            WHERE db_name = ?
            """,
            (db_name,),
        )
        conn.execute(
            """
            DELETE FROM collections
            WHERE db_name = ?
            """,
            (db_name,),
        )
        commit_write(conn)
    except Exception:
        rollback_write(conn)
        raise
    clear_index_metadata_versions(db_name)
    invalidate_index_cache()
    invalidate_collection_id_cache()
    invalidate_collection_features_cache()
    clear_profiler(db_name)
