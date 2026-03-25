import asyncio
import hashlib
import json
import queue
import sqlite3
import threading
from copy import deepcopy
from typing import Any, AsyncIterable, override

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect, MongoDialect70, MongoDialect80
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.identity import canonical_document_id
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.paths import get_document_value
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import (
    AndCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    LessThanCondition,
    LessThanOrEqualCondition,
    MatchAll,
    NotCondition,
    OrCondition,
    QueryNode,
    ensure_query_plan,
)
from mongoeco.core.sorting import sort_documents
from mongoeco.engines.base import AsyncStorageEngine
from mongoeco.engines.sqlite_query import (
    index_expressions_sql,
    json_path_for_field,
    path_array_prefixes,
    type_expression_sql,
    translate_query_plan,
    translate_sort_spec,
    translate_update_spec,
)
from mongoeco.errors import DuplicateKeyError, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import (
    DeleteResult,
    Document,
    DocumentId,
    Filter,
    ObjectId,
    Projection,
    SortSpec,
    Update,
    UpdateResult,
)


class SQLiteEngine(AsyncStorageEngine):
    """Motor SQLite async-first usando la stdlib como backend persistente."""

    def __init__(self, path: str = ":memory:", codec: type[DocumentCodec] = DocumentCodec):
        self._path = path
        self._codec = codec
        self._connection: sqlite3.Connection | None = None
        self._connection_count = 0
        self._lock = threading.RLock()
        self._scan_condition = threading.Condition()
        self._active_scan_count = 0

    @override
    def create_session_state(self, session: ClientSession) -> None:
        session.bind_engine_state(
            f"sqlite:{id(self)}",
            {
                "connected": self._connection is not None,
                "path": self._path,
            },
        )

    def _require_connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("SQLiteEngine is not connected")
        return self._connection

    def _dialect_requires_python_fallback(self, dialect: MongoDialect) -> bool:
        return type(dialect) not in (MongoDialect70, MongoDialect80)

    def _storage_key(self, value: Any) -> str:
        return repr(canonical_document_id(value))

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + identifier.replace('"', '""') + '"'

    def _physical_index_name(self, db_name: str, coll_name: str, index_name: str) -> str:
        digest = hashlib.sha1(f"{db_name}:{coll_name}:{index_name}".encode("utf-8")).hexdigest()[:16]
        return f"idx_{digest}"

    def _serialize_document(self, document: Document) -> str:
        return json.dumps(self._codec.encode(document), separators=(",", ":"), sort_keys=False)

    def _deserialize_document(self, payload: str) -> Document:
        return self._codec.decode(json.loads(payload))

    def _load_documents(self, db_name: str, coll_name: str):
        conn = self._require_connection()
        cursor = conn.execute(
            """
            SELECT storage_key, document
            FROM documents
            WHERE db_name = ? AND coll_name = ?
            """,
            (db_name, coll_name),
        )
        try:
            for storage_key, document in cursor:
                yield storage_key, self._deserialize_document(document)
        finally:
            cursor.close()

    def _build_select_sql(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
        *,
        select_clause: str,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
    ) -> tuple[str, list[object]]:
        where_sql, params = translate_query_plan(plan)
        order_sql = translate_sort_spec(sort)
        sql = f"""
            SELECT {select_clause}
            FROM documents
            WHERE db_name = ? AND coll_name = ? AND ({where_sql})
            {order_sql}
        """
        sql_params: list[object] = [db_name, coll_name, *params]
        if limit is not None:
            sql += " LIMIT ?"
            sql_params.append(limit)
        elif skip:
            sql += " LIMIT -1"
        if skip:
            sql += " OFFSET ?"
            sql_params.append(skip)
        return sql, sql_params

    def _sort_requires_python(self, db_name: str, coll_name: str, plan: QueryNode, sort: SortSpec | None) -> bool:
        if not sort:
            return False

        conn = self._require_connection()
        if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
            return True
        where_sql, params = translate_query_plan(plan)
        for field, _direction in sort:
            if self._field_traverses_array_in_collection(db_name, coll_name, field):
                return True
            if self._field_contains_tagged_bytes_in_collection(db_name, coll_name, field):
                return True
            if self._field_contains_tagged_undefined_in_collection(db_name, coll_name, field):
                return True
            path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
            row = conn.execute(
                f"""
                SELECT 1
                FROM documents
                WHERE db_name = ? AND coll_name = ? AND ({where_sql})
                  AND (
                    json_type(document, {path_literal}) = 'array'
                    OR (
                        json_type(document, {path_literal}) = 'object'
                        AND {type_expression_sql(field)} = ''
                    )
                  )
                LIMIT 1
                """,
                (db_name, coll_name, *params),
            ).fetchone()
            if row is not None:
                return True
        return False

    @staticmethod
    def _plan_fields(plan: QueryNode) -> set[str]:
        if isinstance(plan, MatchAll):
            return set()
        if hasattr(plan, "field"):
            field = getattr(plan, "field")
            if isinstance(field, str):
                return {field}
        if hasattr(plan, "clause"):
            return SQLiteEngine._plan_fields(getattr(plan, "clause"))
        if hasattr(plan, "clauses"):
            fields: set[str] = set()
            for clause in getattr(plan, "clauses"):
                fields.update(SQLiteEngine._plan_fields(clause))
            return fields
        return set()

    def _field_traverses_array_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        prefixes = path_array_prefixes(field)
        if not prefixes:
            return False
        conn = self._require_connection()
        for prefix in prefixes:
            path_literal = "'" + json_path_for_field(prefix).replace("'", "''") + "'"
            row = conn.execute(
                f"""
                SELECT 1
                FROM documents
                WHERE db_name = ? AND coll_name = ?
                  AND json_type(document, {path_literal}) = 'array'
                LIMIT 1
                """,
                (db_name, coll_name),
            ).fetchone()
            if row is not None:
                return True
        return False

    def _plan_has_array_traversing_paths(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_traverses_array_in_collection(db_name, coll_name, field)
            for field in self._plan_fields(plan)
        )

    @staticmethod
    def _comparison_fields(plan: QueryNode) -> set[str]:
        if isinstance(plan, (GreaterThanCondition, GreaterThanOrEqualCondition, LessThanCondition, LessThanOrEqualCondition)):
            return {plan.field}
        if isinstance(plan, NotCondition):
            return SQLiteEngine._comparison_fields(plan.clause)
        if isinstance(plan, (AndCondition, OrCondition)):
            fields: set[str] = set()
            for clause in plan.clauses:
                fields.update(SQLiteEngine._comparison_fields(clause))
            return fields
        return set()

    def _field_contains_tagged_value_type_in_collection(
        self,
        db_name: str,
        coll_name: str,
        field: str,
        value_type: str,
    ) -> bool:
        conn = self._require_connection()
        tagged_type_path = json_path_for_field(field) + f'."{DocumentCodec._MARKER}".{DocumentCodec._TYPE}'
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND json_extract(document, ?) = ?
            LIMIT 1
            """,
            (db_name, coll_name, tagged_type_path, value_type),
        ).fetchone()
        return row is not None

    def _field_contains_tagged_bytes_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        return self._field_contains_tagged_value_type_in_collection(db_name, coll_name, field, 'bytes')

    def _field_contains_tagged_undefined_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        return self._field_contains_tagged_value_type_in_collection(db_name, coll_name, field, 'undefined')

    def _plan_requires_python_for_bytes(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_contains_tagged_bytes_in_collection(db_name, coll_name, field)
            for field in self._comparison_fields(plan)
        )

    def _plan_requires_python_for_undefined(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_contains_tagged_undefined_in_collection(db_name, coll_name, field)
            for field in self._comparison_fields(plan)
        )

    def _field_is_top_level_array_in_collection(self, db_name: str, coll_name: str, field: str) -> bool:
        conn = self._require_connection()
        path_literal = "'" + json_path_for_field(field).replace("'", "''") + "'"
        row = conn.execute(
            f"""
            SELECT 1
            FROM documents
            WHERE db_name = ? AND coll_name = ?
              AND json_type(document, {path_literal}) = 'array'
            LIMIT 1
            """,
            (db_name, coll_name),
        ).fetchone()
        return row is not None

    def _plan_requires_python_for_array_comparisons(self, db_name: str, coll_name: str, plan: QueryNode) -> bool:
        return any(
            self._field_is_top_level_array_in_collection(db_name, coll_name, field)
            for field in self._comparison_fields(plan)
        )

    @staticmethod
    def _document_traverses_array_on_field(document: Document, field: str) -> bool:
        for prefix in path_array_prefixes(field):
            found, value = get_document_value(document, prefix)
            if found and isinstance(value, list):
                return True
        return False

    def _validate_document_against_unique_indexes(
        self,
        db_name: str,
        coll_name: str,
        document: Document,
    ) -> None:
        for index in self._load_indexes(db_name, coll_name):
            if not index["unique"]:
                continue
            fields = index["fields"]
            if any(self._document_traverses_array_on_field(document, field) for field in fields):
                raise OperationFailure("SQLite unique indexes do not support paths that traverse arrays")

    def _select_first_document_for_plan(self, db_name: str, coll_name: str, plan: QueryNode) -> tuple[str, Document] | None:
        conn = self._require_connection()
        if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
            raise NotImplementedError("Array traversal requires Python fallback")
        if self._plan_requires_python_for_array_comparisons(db_name, coll_name, plan):
            raise NotImplementedError("Top-level array comparisons require Python fallback")
        if self._plan_requires_python_for_bytes(db_name, coll_name, plan):
            raise NotImplementedError("Tagged bytes require Python fallback")
        if self._plan_requires_python_for_undefined(db_name, coll_name, plan):
            raise NotImplementedError("Tagged undefined values require Python fallback")
        sql, params = self._build_select_sql(
            db_name,
            coll_name,
            plan,
            select_clause="storage_key, document",
            limit=1,
        )
        row = conn.execute(sql, tuple(params)).fetchone()
        if row is None:
            return None
        storage_key, document = row
        return storage_key, self._deserialize_document(document)

    def _explain_query_plan_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter | None = None,
        *,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
    ) -> list[str]:
        with self._lock:
            conn = self._require_connection()
            plan = ensure_query_plan(filter_spec)
            if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
                raise NotImplementedError("Array traversal requires Python fallback")
            if self._plan_requires_python_for_array_comparisons(db_name, coll_name, plan):
                raise NotImplementedError("Top-level array comparisons require Python fallback")
            if self._plan_requires_python_for_bytes(db_name, coll_name, plan):
                raise NotImplementedError("Tagged bytes require Python fallback")
            if self._plan_requires_python_for_undefined(db_name, coll_name, plan):
                raise NotImplementedError("Tagged undefined values require Python fallback")
            sql, params = self._build_select_sql(
                db_name,
                coll_name,
                plan,
                select_clause="document",
                sort=sort,
                skip=skip,
                limit=limit,
            )
            rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", tuple(params)).fetchall()
        return [str(row[3]) for row in rows]

    def _load_indexes(self, db_name: str, coll_name: str) -> list[dict[str, object]]:
        conn = self._require_connection()
        cursor = conn.execute(
            """
            SELECT name, physical_name, fields, unique_flag
            FROM indexes
            WHERE db_name = ? AND coll_name = ?
            ORDER BY name
            """,
            (db_name, coll_name),
        )
        indexes: list[dict[str, object]] = []
        for name, physical_name, fields, unique_flag in cursor.fetchall():
            try:
                parsed_fields = json.loads(fields)
            except json.JSONDecodeError as exc:
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                ) from exc
            if not isinstance(parsed_fields, list) or not all(isinstance(field, str) for field in parsed_fields):
                raise OperationFailure(
                    f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                )
            indexes.append(
                {
                    "name": name,
                    "physical_name": physical_name or self._physical_index_name(db_name, coll_name, name),
                    "fields": parsed_fields,
                    "unique": bool(unique_flag),
                }
            )
        return indexes

    def _connect_sync(self) -> None:
        with self._lock:
            if self._connection_count == 0:
                connection = sqlite3.connect(self._path, check_same_thread=False)
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS documents (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        document TEXT NOT NULL,
                        PRIMARY KEY (db_name, coll_name, storage_key)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS indexes (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        name TEXT NOT NULL,
                        physical_name TEXT,
                        fields TEXT NOT NULL,
                        unique_flag INTEGER NOT NULL,
                        PRIMARY KEY (db_name, coll_name, name)
                    )
                    """
                )
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(indexes)").fetchall()
                }
                if "physical_name" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN physical_name TEXT")
                connection.commit()
                self._connection = connection
            self._connection_count += 1

    def _disconnect_sync(self) -> None:
        connection: sqlite3.Connection | None = None
        with self._lock:
            if self._connection_count == 0:
                return
            self._connection_count -= 1
            if self._connection_count != 0:
                return
            with self._scan_condition:
                while self._active_scan_count > 0:
                    self._scan_condition.wait()
            connection = self._connection
            self._connection = None
        if connection is not None:
            connection.close()

    def _put_document_sync(self, db_name: str, coll_name: str, document: Document, overwrite: bool, context: ClientSession | None) -> bool:
        with self._lock:
            conn = self._require_connection()
            storage_key = self._storage_key(document.get("_id"))
            try:
                self._validate_document_against_unique_indexes(db_name, coll_name, document)
                if overwrite:
                    conn.execute(
                        """
                        INSERT INTO documents (db_name, coll_name, storage_key, document)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(db_name, coll_name, storage_key)
                        DO UPDATE SET document = excluded.document
                        """,
                        (db_name, coll_name, storage_key, self._serialize_document(document)),
                    )
                else:
                    cursor = conn.execute(
                        """
                        INSERT INTO documents (db_name, coll_name, storage_key, document)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(db_name, coll_name, storage_key) DO NOTHING
                        """,
                        (db_name, coll_name, storage_key, self._serialize_document(document)),
                    )
                    if cursor.rowcount == 0:
                        return False
                conn.commit()
                return True
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                raise DuplicateKeyError(str(exc)) from exc

    def _get_document_sync(self, db_name: str, coll_name: str, doc_id: DocumentId, projection: Projection | None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        effective_dialect = dialect or MONGODB_DIALECT_70
        with self._lock:
            conn = self._require_connection()
            row = conn.execute(
                """
                SELECT document
                FROM documents
                WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                """,
                (db_name, coll_name, self._storage_key(doc_id)),
            ).fetchone()
        if row is None:
            return None
        if effective_dialect is MONGODB_DIALECT_70:
            return apply_projection(self._deserialize_document(row[0]), projection)
        return apply_projection(
            self._deserialize_document(row[0]),
            projection,
            dialect=effective_dialect,
        )

    def _delete_document_sync(self, db_name: str, coll_name: str, doc_id: DocumentId, context: ClientSession | None) -> bool:
        with self._lock:
            conn = self._require_connection()
            cursor = conn.execute(
                """
                DELETE FROM documents
                WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                """,
                (db_name, coll_name, self._storage_key(doc_id)),
            )
            conn.commit()
            return cursor.rowcount > 0

    def _iter_scan_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter | None,
        plan: QueryNode | None,
        projection: Projection | None,
        sort: SortSpec | None,
        skip: int,
        limit: int | None,
        context: ClientSession | None,
        stop_event: threading.Event | None = None,
        dialect: MongoDialect | None = None,
    ):
        effective_dialect = dialect or MONGODB_DIALECT_70
        if skip < 0:
            raise ValueError("skip must be >= 0")
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")

        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        with self._lock:
            try:
                if self._dialect_requires_python_fallback(effective_dialect):
                    raise NotImplementedError("Custom dialect requires Python fallback")
                if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
                    raise NotImplementedError("Array traversal requires Python fallback")
                if self._plan_requires_python_for_array_comparisons(db_name, coll_name, plan):
                    raise NotImplementedError("Top-level array comparisons require Python fallback")
                if self._plan_requires_python_for_undefined(db_name, coll_name, plan):
                    raise NotImplementedError("Tagged undefined requires Python fallback")
                if self._sort_requires_python(db_name, coll_name, plan, sort):
                    raise NotImplementedError("Sort requires Python fallback")
                sql, sql_params = self._build_select_sql(
                    db_name,
                    coll_name,
                    plan,
                    select_clause="document",
                    sort=sort,
                    skip=skip,
                    limit=limit,
                )
                conn = self._require_connection()
                cursor = conn.execute(sql, tuple(sql_params))
                try:
                    for (payload,) in cursor:
                        if stop_event is not None and stop_event.is_set():
                            break
                        if effective_dialect is MONGODB_DIALECT_70:
                            yield apply_projection(self._deserialize_document(payload), projection)
                        else:
                            yield apply_projection(
                                self._deserialize_document(payload),
                                projection,
                                dialect=effective_dialect,
                            )
                finally:
                    cursor.close()
                return
            except (NotImplementedError, TypeError):
                documents_iter = (document for _, document in self._load_documents(db_name, coll_name))
                if not isinstance(plan, MatchAll):
                    documents_iter = (
                        document
                        for document in documents_iter
                        if QueryEngine.match_plan(document, plan, dialect=effective_dialect)
                    )
                try:
                    if sort is None:
                        remaining_skip = skip
                        remaining_limit = limit
                        for document in documents_iter:
                            if stop_event is not None and stop_event.is_set():
                                break
                            if remaining_skip:
                                remaining_skip -= 1
                                continue
                            if effective_dialect is MONGODB_DIALECT_70:
                                yield apply_projection(document, projection)
                            else:
                                yield apply_projection(
                                    document,
                                    projection,
                                    dialect=effective_dialect,
                                )
                            if remaining_limit is not None:
                                remaining_limit -= 1
                                if remaining_limit == 0:
                                    return
                        return

                    if effective_dialect is MONGODB_DIALECT_70:
                        documents = sort_documents(list(documents_iter), sort)
                    else:
                        documents = sort_documents(
                            list(documents_iter),
                            sort,
                            dialect=effective_dialect,
                        )
                finally:
                    close = getattr(documents_iter, "close", None)
                    if callable(close):
                        close()

        if skip:
            documents = documents[skip:]
        if limit is not None:
            documents = documents[:limit]
        for document in documents:
            if stop_event is not None and stop_event.is_set():
                break
            if effective_dialect is MONGODB_DIALECT_70:
                yield apply_projection(document, projection)
            else:
                yield apply_projection(
                    document,
                    projection,
                    dialect=effective_dialect,
                )

    def _update_matching_document_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool,
        upsert_seed: Document | None,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
    ) -> UpdateResult[DocumentId]:
        effective_dialect = dialect or MONGODB_DIALECT_70
        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        with self._lock:
            conn = self._require_connection()
            selected: tuple[str, Document] | None = None
            sql_selection_supported = False
            try:
                if self._dialect_requires_python_fallback(effective_dialect):
                    raise NotImplementedError("Custom dialect requires Python fallback")
                selected = self._select_first_document_for_plan(db_name, coll_name, plan)
                sql_selection_supported = True
            except (NotImplementedError, TypeError):
                pass

            if selected is None and not sql_selection_supported:
                for storage_key, document in self._load_documents(db_name, coll_name):
                    if not QueryEngine.match_plan(document, plan, dialect=effective_dialect):
                        continue
                    selected = (storage_key, document)
                    break

            if selected is not None:
                storage_key, original_document = selected
                document = deepcopy(original_document)
                modified = UpdateEngine.apply_update(
                    document,
                    update_spec,
                    dialect=effective_dialect,
                )
                if not modified:
                    return UpdateResult(matched_count=1, modified_count=0)
                self._validate_document_against_unique_indexes(db_name, coll_name, document)

                try:
                    if self._dialect_requires_python_fallback(effective_dialect):
                        raise NotImplementedError("Custom dialect requires Python fallback")
                    update_sql, update_params = translate_update_spec(update_spec, current_document=original_document)
                    conn.execute(
                        f"""
                        UPDATE documents
                        SET document = {update_sql}
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (*update_params, db_name, coll_name, storage_key),
                    )
                    conn.commit()
                    return UpdateResult(matched_count=1, modified_count=1)
                except (NotImplementedError, TypeError):
                    pass
                except sqlite3.IntegrityError as exc:
                    conn.rollback()
                    raise DuplicateKeyError(str(exc)) from exc

                try:
                    conn.execute(
                        """
                        UPDATE documents
                        SET document = ?
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (self._serialize_document(document), db_name, coll_name, storage_key),
                    )
                    conn.commit()
                    return UpdateResult(matched_count=1, modified_count=1)
                except sqlite3.IntegrityError as exc:
                    conn.rollback()
                    raise DuplicateKeyError(str(exc)) from exc

            if not upsert:
                return UpdateResult(matched_count=0, modified_count=0)

            new_doc = deepcopy(upsert_seed or {})
            UpdateEngine.apply_update(new_doc, update_spec, dialect=effective_dialect)
            if "_id" not in new_doc:
                new_doc["_id"] = ObjectId()
            self._validate_document_against_unique_indexes(db_name, coll_name, new_doc)

            storage_key = self._storage_key(new_doc["_id"])
            try:
                conn.execute(
                    """
                    INSERT INTO documents (db_name, coll_name, storage_key, document)
                    VALUES (?, ?, ?, ?)
                    """,
                    (db_name, coll_name, storage_key, self._serialize_document(new_doc)),
                )
                conn.commit()
                return UpdateResult(matched_count=0, modified_count=0, upserted_id=new_doc["_id"])
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                raise DuplicateKeyError(str(exc)) from exc

    def _delete_matching_document_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
    ) -> DeleteResult:
        effective_dialect = dialect or MONGODB_DIALECT_70
        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        with self._lock:
            conn = self._require_connection()
            try:
                if self._dialect_requires_python_fallback(effective_dialect):
                    raise NotImplementedError("Custom dialect requires Python fallback")
                if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
                    raise NotImplementedError("Array traversal requires Python fallback")
                if self._plan_requires_python_for_array_comparisons(db_name, coll_name, plan):
                    raise NotImplementedError("Top-level array comparisons require Python fallback")
                if self._plan_requires_python_for_bytes(db_name, coll_name, plan):
                    raise NotImplementedError("Tagged bytes require Python fallback")
                if self._plan_requires_python_for_undefined(db_name, coll_name, plan):
                    raise NotImplementedError("Tagged undefined requires Python fallback")
                select_sql, params = self._build_select_sql(
                    db_name,
                    coll_name,
                    plan,
                    select_clause="rowid",
                    limit=1,
                )
                cursor = conn.execute(
                    f"""
                    DELETE FROM documents
                    WHERE rowid IN ({select_sql})
                    """,
                    tuple(params),
                )
                conn.commit()
                return DeleteResult(deleted_count=1 if cursor.rowcount > 0 else 0)
            except (NotImplementedError, TypeError):
                pass

            for storage_key, document in self._load_documents(db_name, coll_name):
                if not QueryEngine.match_plan(document, plan, dialect=effective_dialect):
                    continue
                conn.execute(
                    """
                    DELETE FROM documents
                    WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                    """,
                    (db_name, coll_name, storage_key),
                )
                conn.commit()
                return DeleteResult(deleted_count=1)
            return DeleteResult(deleted_count=0)

    def _count_matching_documents_sync(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
    ) -> int:
        effective_dialect = dialect or MONGODB_DIALECT_70
        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        with self._lock:
            try:
                if self._dialect_requires_python_fallback(effective_dialect):
                    raise NotImplementedError("Custom dialect requires Python fallback")
                if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
                    raise NotImplementedError("Array traversal requires Python fallback")
                if self._plan_requires_python_for_array_comparisons(db_name, coll_name, plan):
                    raise NotImplementedError("Top-level array comparisons require Python fallback")
                if self._plan_requires_python_for_undefined(db_name, coll_name, plan):
                    raise NotImplementedError("Tagged undefined requires Python fallback")
                where_sql, params = translate_query_plan(plan)
                conn = self._require_connection()
                if self._plan_requires_python_for_bytes(db_name, coll_name, plan):
                    raise NotImplementedError("Tagged bytes require Python fallback")
                row = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM documents
                    WHERE db_name = ? AND coll_name = ? AND ({where_sql})
                    """,
                    (db_name, coll_name, *params),
                ).fetchone()
                return int(row[0])
            except (NotImplementedError, TypeError):
                return sum(
                    1
                    for _, document in self._load_documents(db_name, coll_name)
                    if QueryEngine.match_plan(document, plan, dialect=effective_dialect)
                )

    def _create_index_sync(
        self,
        db_name: str,
        coll_name: str,
        fields: list[str],
        unique: bool,
        name: str | None,
        context: ClientSession | None,
    ) -> str:
        index_name = name or "_".join(f"{field}_1" for field in fields)
        physical_name = self._physical_index_name(db_name, coll_name, index_name)
        with self._lock:
            conn = self._require_connection()
            indexes = self._load_indexes(db_name, coll_name)
            for index in indexes:
                if index["name"] == index_name:
                    if index["fields"] != fields or index["unique"] != unique:
                        raise DuplicateKeyError(f"Conflicting index definition for '{index_name}'")
                    return index_name

            if unique:
                for field in fields:
                    if self._field_traverses_array_in_collection(db_name, coll_name, field):
                        raise OperationFailure("SQLite unique indexes do not support paths that traverse arrays")
            expressions = ", ".join(
                ["db_name", "coll_name", *[expression for field in fields for expression in index_expressions_sql(field)]]
            )
            unique_sql = "UNIQUE " if unique else ""
            try:
                conn.execute(
                    f"CREATE {unique_sql}INDEX {self._quote_identifier(physical_name)} "
                    f"ON documents ({expressions})"
                )
                conn.execute(
                    """
                    INSERT INTO indexes (db_name, coll_name, name, physical_name, fields, unique_flag)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (db_name, coll_name, index_name, physical_name, json.dumps(fields), 1 if unique else 0),
                )
                conn.commit()
                return index_name
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                raise DuplicateKeyError(str(exc)) from exc

    def _list_indexes_sync(self, db_name: str, coll_name: str, context: ClientSession | None) -> list[dict[str, object]]:
        with self._lock:
            indexes = deepcopy(self._load_indexes(db_name, coll_name))
        return [
            {
                "name": index["name"],
                "fields": index["fields"],
                "unique": index["unique"],
            }
            for index in indexes
        ]

    def _list_databases_sync(self) -> list[str]:
        with self._lock:
            conn = self._require_connection()
            cursor = conn.execute(
                """
                SELECT DISTINCT db_name
                FROM documents
                ORDER BY db_name
                """
            )
            return [row[0] for row in cursor.fetchall()]

    def _list_collections_sync(self, db_name: str) -> list[str]:
        with self._lock:
            conn = self._require_connection()
            cursor = conn.execute(
                """
                SELECT DISTINCT coll_name
                FROM documents
                WHERE db_name = ?
                ORDER BY coll_name
                """,
                (db_name,),
            )
            return [row[0] for row in cursor.fetchall()]

    def _drop_collection_sync(self, db_name: str, coll_name: str) -> None:
        with self._lock:
            conn = self._require_connection()
            indexes = self._load_indexes(db_name, coll_name)
            try:
                conn.execute("BEGIN")
                for index in indexes:
                    conn.execute(f"DROP INDEX IF EXISTS {self._quote_identifier(index['physical_name'])}")
                conn.execute(
                    """
                    DELETE FROM documents
                    WHERE db_name = ? AND coll_name = ?
                    """,
                    (db_name, coll_name),
                )
                conn.execute(
                    """
                    DELETE FROM indexes
                    WHERE db_name = ? AND coll_name = ?
                    """,
                    (db_name, coll_name),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @override
    async def connect(self) -> None:
        await asyncio.to_thread(self._connect_sync)

    @override
    async def disconnect(self) -> None:
        await asyncio.to_thread(self._disconnect_sync)

    @override
    async def put_document(self, db_name: str, coll_name: str, document: Document, overwrite: bool = True, *, context: ClientSession | None = None) -> bool:
        return await asyncio.to_thread(self._put_document_sync, db_name, coll_name, document, overwrite, context)

    @override
    async def get_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, projection: Projection | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        return await asyncio.to_thread(self._get_document_sync, db_name, coll_name, doc_id, projection, dialect, context)

    @override
    async def delete_document(self, db_name: str, coll_name: str, doc_id: DocumentId, *, context: ClientSession | None = None) -> bool:
        return await asyncio.to_thread(self._delete_document_sync, db_name, coll_name, doc_id, context)

    @override
    def scan_collection(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter | None = None,
        *,
        plan: QueryNode | None = None,
        projection: Projection | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        async def _scan() -> AsyncIterable[Document]:
            items: queue.Queue[object] = queue.Queue()
            sentinel = object()
            stop_event = threading.Event()

            def _produce() -> None:
                with self._scan_condition:
                    self._active_scan_count += 1
                try:
                    for document in self._iter_scan_documents_sync(
                        db_name,
                        coll_name,
                        filter_spec,
                        plan,
                        projection,
                        sort,
                        skip,
                        limit,
                        context=context,
                        stop_event=stop_event,
                        dialect=dialect,
                    ):
                        if stop_event.is_set():
                            break
                        items.put(document)
                except Exception as exc:
                    items.put(exc)
                finally:
                    with self._scan_condition:
                        self._active_scan_count -= 1
                        self._scan_condition.notify_all()
                    items.put(sentinel)

            producer = asyncio.create_task(asyncio.to_thread(_produce))
            try:
                while True:
                    item = await asyncio.to_thread(items.get)
                    if item is sentinel:
                        break
                    if isinstance(item, Exception):
                        raise item
                    yield item
            finally:
                stop_event.set()
                await producer

        return _scan()

    @override
    async def update_matching_document(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter,
        update_spec: Update,
        upsert: bool = False,
        upsert_seed: Document | None = None,
        *,
        plan: QueryNode | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        return await asyncio.to_thread(
            self._update_matching_document_sync,
            db_name,
            coll_name,
            filter_spec,
            update_spec,
            upsert,
            upsert_seed,
            plan,
            context,
            dialect,
        )

    @override
    async def delete_matching_document(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> DeleteResult:
        return await asyncio.to_thread(
            self._delete_matching_document_sync,
            db_name,
            coll_name,
            filter_spec,
            plan,
            context,
            dialect,
        )

    @override
    async def count_matching_documents(self, db_name: str, coll_name: str, filter_spec: Filter, *, plan: QueryNode | None = None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> int:
        return await asyncio.to_thread(
            self._count_matching_documents_sync,
            db_name,
            coll_name,
            filter_spec,
            plan,
            context,
            dialect,
        )

    @override
    async def create_index(
        self,
        db_name: str,
        coll_name: str,
        fields: list[str],
        *,
        unique: bool = False,
        name: str | None = None,
        context: ClientSession | None = None,
    ) -> str:
        return await asyncio.to_thread(self._create_index_sync, db_name, coll_name, fields, unique, name, context)

    @override
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[dict[str, object]]:
        return await asyncio.to_thread(self._list_indexes_sync, db_name, coll_name, context)

    @override
    async def list_databases(self) -> list[str]:
        return await asyncio.to_thread(self._list_databases_sync)

    @override
    async def list_collections(self, db_name: str) -> list[str]:
        return await asyncio.to_thread(self._list_collections_sync, db_name)

    @override
    async def drop_collection(self, db_name: str, coll_name: str) -> None:
        await asyncio.to_thread(self._drop_collection_sync, db_name, coll_name)
