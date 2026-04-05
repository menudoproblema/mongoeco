from __future__ import annotations

from collections.abc import Callable, Iterable
from copy import deepcopy
import sqlite3

from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.engines.sqlite_query import index_expressions_sql
from mongoeco.engines.virtual_indexes import normalize_partial_filter_expression
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.types import (
    Document,
    EngineIndexRecord,
    Filter,
    IndexDocument,
    IndexInformation,
    IndexKeySpec,
    default_id_index_definition,
    default_id_index_information,
    default_index_name,
    index_fields,
    is_ordered_index_spec,
    normalize_index_keys,
    special_index_directions,
)
from mongoeco.core.json_compat import json_dumps_compact


def list_index_documents(indexes: Iterable[EngineIndexRecord]) -> list[IndexDocument]:
    result = [default_id_index_definition().to_list_document()]
    result.extend(index.to_definition().to_list_document() for index in indexes)
    return result


def build_index_information(indexes: Iterable[EngineIndexRecord]) -> IndexInformation:
    return {
        **default_id_index_information(),
        **{
            str(index["name"]): index.to_definition().to_information_entry()
            for index in indexes
        },
    }


def create_index(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    keys: IndexKeySpec,
    unique: bool,
    name: str | None,
    sparse: bool,
    hidden: bool,
    partial_filter_expression: Filter | None,
    expire_after_seconds: int | None,
    deadline: float | None,
    enforce_deadline_fn: Callable[[float | None], None],
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    purge_expired_documents: Callable[[sqlite3.Connection, str, str], None],
    mark_index_metadata_changed: Callable[[str, str], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
    load_indexes: Callable[[str, str], list[EngineIndexRecord]],
    field_traverses_array_in_collection: Callable[[str, str, str], bool],
    supports_multikey_index: Callable[[list[str], bool], bool],
    physical_index_name: Callable[[str, str, str], str],
    physical_multikey_index_name: Callable[[str, str, str], str],
    physical_scalar_index_name: Callable[[str, str, str], str],
    is_builtin_id_index: Callable[[IndexKeySpec], bool],
    replace_multikey_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, EngineIndexRecord], None],
    replace_scalar_entries_for_document: Callable[[sqlite3.Connection, str, str, str, Document, EngineIndexRecord], None],
    load_documents: Callable[[str, str], Iterable[tuple[str, Document]]],
    quote_identifier: Callable[[str], str],
) -> str:
    normalized_keys = normalize_index_keys(keys)
    partial_filter_expression = normalize_partial_filter_expression(partial_filter_expression)
    fields = index_fields(normalized_keys)
    index_name = name or default_index_name(normalized_keys)
    special_directions = special_index_directions(normalized_keys)
    if expire_after_seconds is not None:
        if (
            not isinstance(expire_after_seconds, int)
            or isinstance(expire_after_seconds, bool)
            or expire_after_seconds < 0
        ):
            raise TypeError("expire_after_seconds must be a non-negative int or None")
        if len(fields) != 1:
            raise OperationFailure("TTL indexes require a single-field key pattern")
        if fields[0] == "_id":
            raise OperationFailure("TTL indexes cannot be created on _id")
    if not isinstance(hidden, bool):
        raise TypeError("hidden must be a bool")
    if special_directions:
        if len(normalized_keys) != 1:
            raise OperationFailure("special index types currently require a single-field key pattern")
        if unique:
            raise OperationFailure(f"{special_directions[0]} indexes do not support unique")
    if is_builtin_id_index(normalized_keys):
        if (
            name not in (None, "_id_")
            or sparse
            or hidden
            or partial_filter_expression is not None
            or expire_after_seconds is not None
            or not unique
        ):
            raise OperationFailure("Conflicting index definition for '_id_'")
        return "_id_"
    if index_name == "_id_":
        raise OperationFailure("Conflicting index definition for '_id_'")
    ordered_index = is_ordered_index_spec(normalized_keys)
    physical_name = physical_index_name(db_name, coll_name, index_name) if ordered_index else None
    multikey = ordered_index and supports_multikey_index(fields, unique)
    multikey_physical_name = (
        physical_multikey_index_name(db_name, coll_name, index_name)
        if multikey
        else None
    )
    scalar_physical_name = (
        physical_scalar_index_name(db_name, coll_name, index_name)
        if ordered_index and len(fields) == 1
        else None
    )

    enforce_deadline_fn(deadline)
    indexes = load_indexes(db_name, coll_name)
    for index in indexes:
        enforce_deadline_fn(deadline)
        if index["name"] == index_name:
            if (
                index["key"] != normalized_keys
                or index["unique"] != unique
                or index.get("sparse") != sparse
                or index.get("hidden") != hidden
                or index.get("partial_filter_expression") != partial_filter_expression
                or index.get("expire_after_seconds") != expire_after_seconds
            ):
                raise OperationFailure(f"Conflicting index definition for '{index_name}'")
            return index_name
        if index["key"] == normalized_keys:
            if (
                index["unique"] != unique
                or index.get("sparse") != sparse
                or index.get("hidden") != hidden
                or index.get("partial_filter_expression") != partial_filter_expression
                or index.get("expire_after_seconds") != expire_after_seconds
            ):
                raise OperationFailure(
                    f"Conflicting index definition for key pattern '{normalized_keys!r}'"
                )
            continue

    if unique:
        for field in fields:
            if field_traverses_array_in_collection(db_name, coll_name, field):
                raise OperationFailure("SQLite unique indexes do not support paths that traverse arrays")

    expressions = None
    if ordered_index:
        expressions = ", ".join(
            [
                "db_name",
                "coll_name",
                *[
                    f"{expression}{' DESC' if direction == -1 else ''}"
                    for field, direction in normalized_keys
                    for expression in index_expressions_sql(field)
                ],
            ]
        )
    unique_sql = "UNIQUE " if unique and not (sparse or partial_filter_expression is not None) else ""

    try:
        begin_write(conn)
        if physical_name is not None and expressions is not None:
            conn.execute(
                f"CREATE {unique_sql}INDEX {quote_identifier(physical_name)} "
                f"ON documents ({expressions})"
            )
        if multikey:
            enforce_deadline_fn(deadline)
            conn.execute(
                f"CREATE INDEX {quote_identifier(str(multikey_physical_name))} "
                "ON multikey_entries (collection_id, index_name, type_score, element_key, storage_key)"
            )
        if scalar_physical_name is not None:
            enforce_deadline_fn(deadline)
            conn.execute(
                f"CREATE INDEX {quote_identifier(str(scalar_physical_name))} "
                "ON scalar_index_entries (collection_id, index_name, type_score, element_key, storage_key)"
            )
        conn.execute(
            """
            INSERT INTO indexes (
                db_name, coll_name, name, physical_name, fields, keys, unique_flag, sparse_flag, hidden_flag, partial_filter_json, expire_after_seconds, multikey_flag, multikey_physical_name, scalar_physical_name
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                db_name,
                coll_name,
                index_name,
                physical_name,
                json_dumps_compact(fields),
                json_dumps_compact(normalized_keys),
                1 if unique else 0,
                1 if sparse else 0,
                1 if hidden else 0,
                json_dumps_compact(partial_filter_expression) if partial_filter_expression is not None else None,
                expire_after_seconds,
                1 if multikey else 0,
                multikey_physical_name if multikey else None,
                scalar_physical_name,
            ),
        )
        if multikey:
            enforce_deadline_fn(deadline)
            index_metadata = EngineIndexRecord(
                name=index_name,
                physical_name=physical_name,
                fields=fields,
                key=normalized_keys,
                unique=unique,
                sparse=sparse,
                hidden=hidden,
                partial_filter_expression=deepcopy(partial_filter_expression),
                expire_after_seconds=expire_after_seconds,
                multikey=True,
                multikey_physical_name=multikey_physical_name,
                scalar_physical_name=scalar_physical_name,
            )
            for storage_key, document in load_documents(db_name, coll_name):
                enforce_deadline_fn(deadline)
                replace_multikey_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    index_metadata,
                )
        if scalar_physical_name is not None:
            enforce_deadline_fn(deadline)
            index_metadata = EngineIndexRecord(
                name=index_name,
                physical_name=physical_name,
                fields=fields,
                key=normalized_keys,
                unique=unique,
                sparse=sparse,
                hidden=hidden,
                partial_filter_expression=deepcopy(partial_filter_expression),
                expire_after_seconds=expire_after_seconds,
                multikey=multikey,
                multikey_physical_name=multikey_physical_name,
                scalar_physical_name=scalar_physical_name,
            )
            for storage_key, document in load_documents(db_name, coll_name):
                enforce_deadline_fn(deadline)
                replace_scalar_entries_for_document(
                    conn,
                    db_name,
                    coll_name,
                    storage_key,
                    document,
                    index_metadata,
                )
        enforce_deadline_fn(deadline)
        commit_write(conn)
        purge_expired_documents(conn, db_name, coll_name)
        mark_index_metadata_changed(db_name, coll_name)
        invalidate_collection_features_cache(db_name, coll_name)
        return index_name
    except sqlite3.IntegrityError as exc:
        rollback_write(conn)
        mark_index_metadata_changed(db_name, coll_name)
        raise DuplicateKeyError(str(exc)) from exc


def drop_index(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    index_or_name: str | IndexKeySpec,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    load_indexes: Callable[[str, str], list[EngineIndexRecord]],
    lookup_collection_id: Callable[[sqlite3.Connection, str, str], int | None],
    quote_identifier: Callable[[str], str],
    mark_index_metadata_changed: Callable[[str, str], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
    is_builtin_id_index: Callable[[IndexKeySpec], bool],
) -> None:
    normalized_keys: IndexKeySpec | None = None
    target_name: str
    if isinstance(index_or_name, str):
        if index_or_name == "_id_":
            raise OperationFailure("cannot drop _id index")
        target_name = index_or_name
    else:
        normalized_keys = normalize_index_keys(index_or_name)
        if is_builtin_id_index(normalized_keys):
            raise OperationFailure("cannot drop _id index")
        target_name = default_index_name(normalized_keys)

    indexes = load_indexes(db_name, coll_name)
    if normalized_keys is not None:
        matches = [index for index in indexes if index["key"] == normalized_keys]
        if not matches:
            raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")
        if len(matches) > 1:
            raise OperationFailure(
                f"multiple indexes found with key pattern {normalized_keys!r}; drop by name instead"
            )
        target_name = str(matches[0]["name"])
    target = next((index for index in indexes if index["name"] == target_name), None)
    if target is None:
        missing_target = (
            f"index not found with name [{index_or_name}]"
            if isinstance(index_or_name, str)
            else f"index not found with key pattern {normalized_keys!r}"
        )
        raise OperationFailure(missing_target)

    try:
        begin_write(conn)
        conn.execute(
            f"DROP INDEX IF EXISTS {quote_identifier(str(target['physical_name']))}"
        )
        collection_id = lookup_collection_id(conn, db_name, coll_name)
        if target.get("scalar_physical_name"):
            conn.execute(
                f"DROP INDEX IF EXISTS {quote_identifier(str(target['scalar_physical_name']))}"
            )
            conn.execute(
                """
                DELETE FROM scalar_index_entries
                WHERE collection_id = ? AND index_name = ?
                """,
                (collection_id, target["name"]),
            )
        if target.get("multikey"):
            conn.execute(
                f"DROP INDEX IF EXISTS {quote_identifier(str(target['multikey_physical_name']))}"
            )
            conn.execute(
                """
                DELETE FROM multikey_entries
                WHERE collection_id = ? AND index_name = ?
                """,
                (collection_id, target["name"]),
            )
        conn.execute(
            """
            DELETE FROM indexes
            WHERE db_name = ? AND coll_name = ? AND name = ?
            """,
            (db_name, coll_name, target["name"]),
        )
        commit_write(conn)
        mark_index_metadata_changed(db_name, coll_name)
        invalidate_collection_features_cache(db_name, coll_name)
    except Exception:
        rollback_write(conn)
        raise


def drop_all_indexes(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    begin_write: Callable[[sqlite3.Connection], None],
    commit_write: Callable[[sqlite3.Connection], None],
    rollback_write: Callable[[sqlite3.Connection], None],
    load_indexes: Callable[[str, str], list[EngineIndexRecord]],
    lookup_collection_id: Callable[[sqlite3.Connection, str, str], int | None],
    quote_identifier: Callable[[str], str],
    mark_index_metadata_changed: Callable[[str, str], None],
    invalidate_collection_features_cache: Callable[[str, str], None],
) -> None:
    indexes = load_indexes(db_name, coll_name)
    try:
        begin_write(conn)
        for index in indexes:
            conn.execute(
                f"DROP INDEX IF EXISTS {quote_identifier(str(index['physical_name']))}"
            )
            if index.get("scalar_physical_name"):
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(index['scalar_physical_name']))}"
                )
            if index.get("multikey"):
                conn.execute(
                    f"DROP INDEX IF EXISTS {quote_identifier(str(index['multikey_physical_name']))}"
                )
        collection_id = lookup_collection_id(conn, db_name, coll_name)
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
        commit_write(conn)
        mark_index_metadata_changed(db_name, coll_name)
        invalidate_collection_features_cache(db_name, coll_name)
    except Exception:
        rollback_write(conn)
        raise
