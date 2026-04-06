from __future__ import annotations

import sqlite3
from typing import Protocol

from mongoeco.core.json_compat import json_loads
from mongoeco.engines.virtual_indexes import document_in_virtual_index, normalize_partial_filter_expression
from mongoeco._types.indexes import normalize_index_keys
from mongoeco.types import Document, EngineIndexRecord


class _SQLiteIndexRuntimeEngine(Protocol):
    _ensured_multikey_physical_indexes: set[str]

    def _quote_identifier(self, identifier: str) -> str: ...
    def _supports_scalar_index(self, index: EngineIndexRecord) -> bool: ...
    def _lookup_collection_id(self, conn: sqlite3.Connection, db_name: str, coll_name: str, *, create: bool = False) -> int | None: ...
    def _build_multikey_rows_for_document(self, storage_key: str, document: Document, indexes: list[EngineIndexRecord]) -> list[tuple[str, str, int, str]]: ...
    def _build_scalar_rows_for_document(self, storage_key: str, document: Document, indexes: list[EngineIndexRecord]) -> list[tuple[str, str, int, str]]: ...
    def _scalar_value_signature_for_document(self, document: Document, field: str): ...
    def _multikey_type_score(self, element_type: str) -> int: ...
    def _physical_scalar_index_name(self, db_name: str, coll_name: str, index_name: str) -> str: ...
    def _physical_index_name(self, db_name: str, coll_name: str, index_name: str) -> str: ...
    def _physical_multikey_index_name(self, db_name: str, coll_name: str, index_name: str) -> str: ...
    def _load_documents(self, db_name: str, coll_name: str) -> list[tuple[str, Document]]: ...
    def _delete_multikey_entries_for_storage_key(self, conn: sqlite3.Connection, db_name: str, coll_name: str, storage_key: str) -> None: ...
    def _delete_scalar_entries_for_storage_key(self, conn: sqlite3.Connection, db_name: str, coll_name: str, storage_key: str) -> None: ...


def ensure_multikey_physical_indexes_sync(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
    indexes: list[EngineIndexRecord],
) -> None:
    had_transaction = conn.in_transaction
    created_any = False
    for index in indexes:
        if not index.get("multikey"):
            continue
        physical_name = str(index["multikey_physical_name"])
        if physical_name in engine._ensured_multikey_physical_indexes:
            continue
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {engine._quote_identifier(physical_name)} "
            "ON multikey_entries (collection_id, index_name, type_score, element_key, storage_key)"
        )
        engine._ensured_multikey_physical_indexes.add(physical_name)
        created_any = True
    if created_any and not had_transaction and conn.in_transaction:
        conn.commit()


def ensure_scalar_physical_indexes_sync(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
    indexes: list[EngineIndexRecord],
) -> None:
    had_transaction = conn.in_transaction
    created_any = False
    for index in indexes:
        if not engine._supports_scalar_index(index):
            continue
        physical_name = index.get("scalar_physical_name")
        if not physical_name:
            continue
        conn.execute(
            f"CREATE INDEX IF NOT EXISTS {engine._quote_identifier(str(physical_name))} "
            "ON scalar_index_entries (collection_id, index_name, type_score, element_key, storage_key)"
        )
        created_any = True
    if created_any and not had_transaction and conn.in_transaction:
        conn.commit()


def rebuild_multikey_entries_for_document(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    storage_key: str,
    document: Document,
    indexes: list[EngineIndexRecord],
) -> None:
    engine._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
    collection_id = engine._lookup_collection_id(conn, db_name, coll_name, create=True)
    if collection_id is None:
        return
    rows = [
        (collection_id, index_name, storage_key, element_type, type_score, element_key)
        for index_name, element_type, type_score, element_key in engine._build_multikey_rows_for_document(
            storage_key,
            document,
            indexes,
        )
    ]
    if rows:
        ensure_multikey_physical_indexes_sync(engine, conn, indexes)
        conn.executemany(
            """
            INSERT OR IGNORE INTO multikey_entries (
                collection_id, index_name, storage_key, element_type, type_score, element_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def rebuild_scalar_entries_for_document(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    storage_key: str,
    document: Document,
    indexes: list[EngineIndexRecord],
) -> None:
    engine._delete_scalar_entries_for_storage_key(conn, db_name, coll_name, storage_key)
    collection_id = engine._lookup_collection_id(conn, db_name, coll_name, create=True)
    if collection_id is None:
        return
    rows = [
        (collection_id, index_name, storage_key, element_type, type_score, element_key)
        for index_name, element_type, type_score, element_key in engine._build_scalar_rows_for_document(
            storage_key,
            document,
            indexes,
        )
    ]
    if rows:
        ensure_scalar_physical_indexes_sync(engine, conn, indexes)
        conn.executemany(
            """
            INSERT OR IGNORE INTO scalar_index_entries (
                collection_id, index_name, storage_key, element_type, type_score, element_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def replace_multikey_entries_for_index_for_document(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    storage_key: str,
    document: Document,
    index: EngineIndexRecord,
) -> None:
    collection_id = engine._lookup_collection_id(conn, db_name, coll_name, create=True)
    if collection_id is None:
        return
    conn.execute(
        """
        DELETE FROM multikey_entries
        WHERE collection_id = ? AND storage_key = ? AND index_name = ?
        """,
        (collection_id, storage_key, index["name"]),
    )
    if not document_in_virtual_index(document, index):
        return
    if not index.get("multikey"):
        return
    rows = [
        (collection_id, index_name, storage_key, element_type, type_score, element_key)
        for index_name, element_type, type_score, element_key in engine._build_multikey_rows_for_document(
            storage_key,
            document,
            [index],
        )
    ]
    if rows:
        ensure_multikey_physical_indexes_sync(engine, conn, [index])
        conn.executemany(
            """
            INSERT OR IGNORE INTO multikey_entries (
                collection_id, index_name, storage_key, element_type, type_score, element_key
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            rows,
        )


def replace_scalar_entries_for_index_for_document(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
    db_name: str,
    coll_name: str,
    storage_key: str,
    document: Document,
    index: EngineIndexRecord,
) -> None:
    collection_id = engine._lookup_collection_id(conn, db_name, coll_name, create=True)
    if collection_id is None:
        return
    conn.execute(
        """
        DELETE FROM scalar_index_entries
        WHERE collection_id = ? AND storage_key = ? AND index_name = ?
        """,
        (collection_id, storage_key, index["name"]),
    )
    if not engine._supports_scalar_index(index):
        return
    if not document_in_virtual_index(document, index):
        return
    signature = engine._scalar_value_signature_for_document(document, index["fields"][0])
    if signature is None:
        return
    ensure_scalar_physical_indexes_sync(engine, conn, [index])
    conn.execute(
        """
        INSERT OR IGNORE INTO scalar_index_entries (
            collection_id, index_name, storage_key, element_type, type_score, element_key
        ) VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            collection_id,
            index["name"],
            storage_key,
            signature[0],
            engine._multikey_type_score(signature[0]),
            signature[1],
        ),
    )


def backfill_scalar_indexes_sync(
    engine: _SQLiteIndexRuntimeEngine,
    conn: sqlite3.Connection,
) -> None:
    indexes = conn.execute(
        """
        SELECT db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag,
               hidden_flag, collation_json, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name
        FROM indexes
        ORDER BY db_name, coll_name, name
        """
    ).fetchall()
    if not indexes:
        return
    grouped: dict[tuple[str, str], list[EngineIndexRecord]] = {}
    for (
        db_name,
        coll_name,
        name,
        physical_name,
        fields,
        keys,
        unique_flag,
        sparse_flag,
        hidden_flag,
        collation_json,
        partial_filter_json,
        expire_after_seconds,
        multikey_flag,
        multikey_physical_name,
        scalar_physical_name,
    ) in indexes:
        parsed_fields = json_loads(fields)
        parsed_keys = normalize_index_keys(json_loads(keys)) if keys is not None else [(field, 1) for field in parsed_fields]
        collation = json_loads(collation_json) if collation_json is not None else None
        partial_filter_expression = (
            normalize_partial_filter_expression(json_loads(partial_filter_json))
            if partial_filter_json is not None
            else None
        )
        resolved_scalar_name = scalar_physical_name
        if len(parsed_fields) == 1 and not resolved_scalar_name:
            resolved_scalar_name = engine._physical_scalar_index_name(db_name, coll_name, name)
            conn.execute(
                """
                UPDATE indexes
                SET scalar_physical_name = ?
                WHERE db_name = ? AND coll_name = ? AND name = ?
                """,
                (resolved_scalar_name, db_name, coll_name, name),
            )
        grouped.setdefault((db_name, coll_name), []).append(
            EngineIndexRecord(
                name=name,
                physical_name=physical_name or engine._physical_index_name(db_name, coll_name, name),
                fields=parsed_fields,
                key=parsed_keys,
                unique=bool(unique_flag),
                sparse=bool(sparse_flag),
                hidden=bool(hidden_flag),
                collation=collation,
                partial_filter_expression=partial_filter_expression,
                expire_after_seconds=int(expire_after_seconds) if expire_after_seconds is not None else None,
                multikey=bool(multikey_flag),
                multikey_physical_name=multikey_physical_name or engine._physical_multikey_index_name(db_name, coll_name, name),
                scalar_physical_name=resolved_scalar_name,
            )
        )
    for collection_indexes in grouped.values():
        ensure_scalar_physical_indexes_sync(engine, conn, collection_indexes)
    for (db_name, coll_name), collection_indexes in grouped.items():
        collection_id = engine._lookup_collection_id(conn, db_name, coll_name)
        if collection_id is None:
            continue
        conn.execute(
            """
            DELETE FROM scalar_index_entries
            WHERE collection_id = ?
            """,
            (collection_id,),
        )
        for storage_key, document in engine._load_documents(db_name, coll_name):
            rows = [
                (collection_id, index_name, storage_key, element_type, type_score, element_key)
                for index_name, element_type, type_score, element_key in engine._build_scalar_rows_for_document(
                    storage_key,
                    document,
                    collection_indexes,
                )
            ]
            if rows:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO scalar_index_entries (
                        collection_id, index_name, storage_key, element_type, type_score, element_key
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
