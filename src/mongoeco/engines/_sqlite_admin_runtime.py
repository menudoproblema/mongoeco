from __future__ import annotations

import sqlite3

from mongoeco.core.projections import apply_projection
from mongoeco.engines._sqlite_catalog import (
    list_collection_names as _sqlite_list_collection_names,
    list_database_names as _sqlite_list_database_names,
    load_collection_options as _sqlite_load_collection_options,
)
from mongoeco.engines._sqlite_namespace_admin import (
    collection_options as _sqlite_collection_options,
    drop_database as _sqlite_drop_database,
    list_collections as _sqlite_list_collections,
    list_databases as _sqlite_list_databases,
)
from mongoeco.types import Document


class SQLiteAdminRuntime:
    def __init__(self, engine):
        self._engine = engine

    def collection_exists(self, conn: sqlite3.Connection, db_name: str, coll_name: str) -> bool:
        row = conn.execute(
            """
            SELECT 1
            FROM collections
            WHERE db_name = ? AND coll_name = ?
            UNION
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
            UNION
            SELECT 1
            FROM indexes
            WHERE db_name = ? AND coll_name = ?
            UNION
            SELECT 1
            FROM search_indexes
            WHERE db_name = ? AND coll_name = ?
            LIMIT 1
            """,
            (db_name, coll_name, db_name, coll_name, db_name, coll_name, db_name, coll_name),
        ).fetchone()
        return row is not None

    def collection_options(self, db_name: str, coll_name: str, *, context=None) -> dict[str, object]:
        conn = self._engine._require_connection(context)
        return _sqlite_collection_options(
            db_name=db_name,
            coll_name=coll_name,
            profiler=self._engine._profiler,
            profile_collection_name=self._engine._PROFILE_COLLECTION_NAME,
            load_collection_options=_sqlite_load_collection_options,
            collection_exists=self.collection_exists,
            conn=conn,
        )

    def collection_options_or_empty(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
    ) -> dict[str, object]:
        options = _sqlite_load_collection_options(conn, db_name, coll_name)
        return {} if options is None else options

    def profile_namespace_documents(self, db_name: str) -> list[Document]:
        return self._engine._profiler.list_entries(db_name)

    def profile_namespace_document(
        self,
        db_name: str,
        profile_id: object,
        *,
        projection=None,
        dialect=None,
    ):
        document = self._engine._profiler.get_entry(db_name, profile_id)
        if document is None:
            return None
        if projection is None:
            return document
        return apply_projection(document, projection, dialect=dialect)

    def is_profile_namespace(self, coll_name: str) -> bool:
        return coll_name == self._engine._PROFILE_COLLECTION_NAME

    def clear_profile_namespace(self, db_name: str) -> None:
        self._engine._profiler.clear(db_name)

    def record_profile_event(
        self,
        db_name: str,
        *,
        op: str,
        command: dict[str, object],
        duration_micros: int,
        execution_lineage=(),
        fallback_reason: str | None = None,
        ok: float = 1.0,
        errmsg: str | None = None,
    ) -> None:
        self._engine._profiler.record(
            db_name,
            op=op,
            namespace=f"{db_name}.{self._engine._PROFILE_COLLECTION_NAME}",
            command=command,
            duration_micros=duration_micros,
            execution_lineage=execution_lineage,
            fallback_reason=fallback_reason,
            ok=ok,
            errmsg=errmsg,
        )

    def set_profiling_level(
        self,
        db_name: str,
        level: int,
        *,
        slow_ms: int | None = None,
    ):
        return self._engine._profiler.set_level(db_name, level, slow_ms=slow_ms)

    def clear_index_metadata_versions_for_database(self, db_name: str) -> None:
        stale_index_keys = [
            key
            for key in self._engine._index_metadata_versions
            if key[0] == db_name
        ]
        for key in stale_index_keys:
            self._engine._index_metadata_versions.pop(key, None)

    def drop_database(self, db_name: str, *, context=None) -> None:
        conn = self._engine._require_connection(context)
        _sqlite_drop_database(
            conn=conn,
            db_name=db_name,
            begin_write=lambda current: self._engine._begin_write(current, context),
            commit_write=lambda current: self._engine._commit_write(current, context),
            rollback_write=lambda current: self._engine._rollback_write(current, context),
            quote_identifier=self._engine._quote_identifier,
            drop_search_backend=self._engine._drop_search_backend_sync,
            clear_index_metadata_versions=self.clear_index_metadata_versions_for_database,
            invalidate_index_cache=self._engine._invalidate_index_cache,
            invalidate_collection_id_cache=self._engine._invalidate_collection_id_cache,
            invalidate_collection_features_cache=self._engine._invalidate_collection_features_cache,
            clear_profiler=self._engine._profiler.clear,
        )

    def list_databases(self, *, context=None) -> list[str]:
        conn = self._engine._require_connection(context)
        return _sqlite_list_databases(
            conn=conn,
            profiler=self._engine._profiler,
            list_database_names=_sqlite_list_database_names,
        )

    def list_collections(self, db_name: str, *, context=None) -> list[str]:
        conn = self._engine._require_connection(context)
        return _sqlite_list_collections(
            conn=conn,
            db_name=db_name,
            profiler=self._engine._profiler,
            profile_collection_name=self._engine._PROFILE_COLLECTION_NAME,
            list_collection_names=_sqlite_list_collection_names,
        )
