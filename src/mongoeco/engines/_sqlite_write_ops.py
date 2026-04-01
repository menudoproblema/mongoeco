from __future__ import annotations

from collections.abc import Callable, Iterable
import sqlite3

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.errors import DuplicateKeyError
from mongoeco.engines.semantic_core import enforce_collection_document_validation
from mongoeco.types import Document, EngineIndexRecord


def put_document(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    document: Document,
    overwrite: bool,
    bypass_document_validation: bool,
    storage_key: str,
    serialized_document: str,
    purge_expired_documents: Callable[[sqlite3.Connection, str, str], None],
    begin_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    collection_options_or_empty: Callable[[sqlite3.Connection, str, str], dict[str, object]],
    load_existing_document_for_storage_key: Callable[[sqlite3.Connection, str, str, str], Document | None],
    ensure_collection_row: Callable[..., None],
    validate_document_against_unique_indexes: Callable[[str, str, Document, str | None], None],
    load_indexes: Callable[[str, str], list[EngineIndexRecord]],
    rebuild_multikey_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[EngineIndexRecord]], None],
    supports_scalar_index: Callable[[EngineIndexRecord], bool],
    rebuild_scalar_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[EngineIndexRecord]], None],
    load_search_index_rows: Callable[[str, str], list[tuple[object, str | None, float | None]]],
    replace_search_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[tuple[object, str | None, float | None]]], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
) -> bool:
    purge_expired_documents(conn, db_name, coll_name)
    begin_write(conn)
    try:
        collection_options = collection_options_or_empty(conn, db_name, coll_name)
        original_document = None
        if not bypass_document_validation or not overwrite:
            original_document = load_existing_document_for_storage_key(conn, db_name, coll_name, storage_key)

        if not overwrite and original_document is not None:
            rollback_write(conn)
            return False

        if not bypass_document_validation:
            enforce_collection_document_validation(
                document,
                options=collection_options,
                original_document=original_document,
                dialect=MONGODB_DIALECT_70,
            )

        ensure_collection_row(conn, db_name, coll_name, options=collection_options)
        validate_document_against_unique_indexes(
            db_name,
            coll_name,
            document,
            storage_key if overwrite else None,
        )

        try:
            if overwrite:
                conn.execute(
                    """
                    INSERT INTO documents (db_name, coll_name, storage_key, document)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(db_name, coll_name, storage_key)
                    DO UPDATE SET document = excluded.document
                    """,
                    (db_name, coll_name, storage_key, serialized_document),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO documents (db_name, coll_name, storage_key, document)
                    VALUES (?, ?, ?, ?)
                    """,
                    (db_name, coll_name, storage_key, serialized_document),
                )
        except sqlite3.IntegrityError as exc:
            raise DuplicateKeyError(f"Duplicate key error: {exc}") from exc

        indexes = load_indexes(db_name, coll_name)
        if any(idx.get("multikey") for idx in indexes):
            rebuild_multikey_entries_for_document(conn, db_name, coll_name, storage_key, document, indexes)
        if any(supports_scalar_index(idx) for idx in indexes):
            rebuild_scalar_entries_for_document(conn, db_name, coll_name, storage_key, document, indexes)

        search_indexes = load_search_index_rows(db_name, coll_name)
        if search_indexes:
            replace_search_entries_for_document(
                conn,
                db_name,
                coll_name,
                storage_key,
                document,
                search_indexes,
            )

        commit_write(conn)
        invalidate_collection_features_cache(db_name, coll_name)
        return True
    except Exception:
        rollback_write(conn)
        raise


def put_documents_bulk(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    documents: list[Document],
    prepared_documents: list[tuple[str, str, list[tuple[str, str, int, str]]]],
    snapshot_indexes: list[EngineIndexRecord],
    bypass_document_validation: bool,
    snapshot_options: dict[str, object] | None,
    purge_expired_documents: Callable[[sqlite3.Connection, str, str], None],
    collection_options_or_empty: Callable[[sqlite3.Connection, str, str], dict[str, object]],
    load_indexes: Callable[[str, str], list[EngineIndexRecord]],
    load_search_index_rows: Callable[[str, str], list[tuple[object, str | None, float | None]]],
    begin_write: Callable[[sqlite3.Connection], None],
    ensure_collection_row: Callable[..., None],
    lookup_collection_id: Callable[[sqlite3.Connection, str, str, bool], int | None],
    validate_document_against_unique_indexes: Callable[[str, str, Document, str | None], None],
    delete_multikey_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    delete_scalar_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    build_multikey_rows_for_document: Callable[[str, Document, list[EngineIndexRecord]], list[tuple[str, str, int, str]]],
    ensure_multikey_physical_indexes: Callable[[sqlite3.Connection, list[EngineIndexRecord]], None],
    build_scalar_rows_for_document: Callable[[str, Document, list[EngineIndexRecord]], list[tuple[str, str, int, str]]],
    ensure_scalar_physical_indexes: Callable[[sqlite3.Connection, list[EngineIndexRecord]], None],
    replace_search_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, list[tuple[object, str | None, float | None]]], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
) -> list[bool]:
    purge_expired_documents(conn, db_name, coll_name)
    collection_options = collection_options_or_empty(conn, db_name, coll_name)
    if (
        not bypass_document_validation
        and snapshot_options is not None
        and collection_options != snapshot_options
    ):
        for document in documents:
            enforce_collection_document_validation(
                document,
                options=collection_options,
                original_document=None,
                dialect=MONGODB_DIALECT_70,
            )
    indexes = load_indexes(db_name, coll_name)
    search_indexes = load_search_index_rows(db_name, coll_name)
    results: list[bool] = []
    try:
        begin_write(conn)
        ensure_collection_row(conn, db_name, coll_name)
        collection_id = lookup_collection_id(conn, db_name, coll_name, True)
        for document, (storage_key, serialized_document, prepared_multikey_rows) in zip(
            documents,
            prepared_documents,
            strict=False,
        ):
            try:
                validate_document_against_unique_indexes(
                    db_name,
                    coll_name,
                    document,
                    None,
                )
                cursor = conn.execute(
                    """
                    INSERT INTO documents (db_name, coll_name, storage_key, document)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(db_name, coll_name, storage_key) DO NOTHING
                    """,
                    (db_name, coll_name, storage_key, serialized_document),
                )
                if cursor.rowcount == 0:
                    results.append(False)
                    break
                delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                effective_rows = prepared_multikey_rows
                if indexes != snapshot_indexes:
                    effective_rows = build_multikey_rows_for_document(storage_key, document, indexes)
                if collection_id is not None and effective_rows:
                    ensure_multikey_physical_indexes(conn, indexes)
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO multikey_entries (
                            collection_id, index_name, storage_key, element_type, type_score, element_key
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                collection_id,
                                index_name,
                                storage_key,
                                element_type,
                                type_score,
                                element_key,
                            )
                            for index_name, element_type, type_score, element_key in effective_rows
                        ],
                    )
                scalar_rows = build_scalar_rows_for_document(storage_key, document, indexes)
                if collection_id is not None and scalar_rows:
                    ensure_scalar_physical_indexes(conn, indexes)
                    conn.executemany(
                        """
                        INSERT OR IGNORE INTO scalar_index_entries (
                            collection_id, index_name, storage_key, element_type, type_score, element_key
                        ) VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                collection_id,
                                index_name,
                                storage_key,
                                element_type,
                                type_score,
                                element_key,
                            )
                            for index_name, element_type, type_score, element_key in scalar_rows
                        ],
                    )
                replace_search_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    search_indexes,
                )
                results.append(True)
            except (DuplicateKeyError, sqlite3.IntegrityError):
                results.append(False)
                break
        commit_write(conn)
        invalidate_collection_features_cache(db_name, coll_name)
        return results
    except Exception:
        rollback_write(conn)
        raise


def delete_document(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    storage_key: str,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    delete_multikey_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    delete_scalar_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    delete_search_entries_for_storage_key: Callable[[sqlite3.Connection, str, str, str], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
) -> bool:
    try:
        begin_write(conn)
        cursor = conn.execute(
            """
            DELETE FROM documents
            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
            """,
            (db_name, coll_name, storage_key),
        )
        delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
        delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
        delete_search_entries_for_storage_key(conn, db_name, coll_name, storage_key)
        commit_write(conn)
        invalidate_collection_features_cache(db_name, coll_name)
        return cursor.rowcount > 0
    except Exception:
        rollback_write(conn)
        raise
