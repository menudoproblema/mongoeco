import asyncio
from contextlib import contextmanager, nullcontext
import hashlib
import json
import math
import queue
import sqlite3
import threading
import time
from copy import deepcopy
from decimal import Decimal
from typing import Any, AsyncIterable, override

from mongoeco.api.operations import FindOperation, UpdateOperation
from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect, MongoDialect70, MongoDialect80
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.identity import canonical_document_id
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.operation_limits import enforce_deadline, operation_deadline
from mongoeco.core.paths import get_document_value
from mongoeco.core.projections import apply_projection
from mongoeco.core.query_plan import (
    AndCondition,
    EqualsCondition,
    GreaterThanCondition,
    GreaterThanOrEqualCondition,
    InCondition,
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
    _translate_scalar_equals,
    index_expressions_sql,
    json_path_for_field,
    path_array_prefixes,
    type_expression_sql,
    translate_query_plan,
    translate_sort_spec,
    translate_update_spec,
)
from mongoeco.errors import CollectionInvalid, DuplicateKeyError, InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.session import EngineTransactionContext
from mongoeco.types import (
    ArrayFilters,
    DeleteResult,
    Document,
    DocumentId,
    EngineIndexRecord,
    Filter,
    IndexDocument,
    IndexInformation,
    IndexKeySpec,
    ObjectId,
    Projection,
    QueryPlanExplanation,
    SortSpec,
    Update,
    UpdateResult,
    default_id_index_definition,
    default_id_index_document,
    default_id_index_information,
    default_index_name,
    index_fields,
    IndexDefinition,
    normalize_index_keys,
)


class SQLiteEngine(AsyncStorageEngine):
    """Motor SQLite async-first usando la stdlib como backend persistente."""

    def __init__(self, path: str = ":memory:", codec: type[DocumentCodec] = DocumentCodec):
        self._path = path
        self._codec = codec
        self._connection: sqlite3.Connection | None = None
        self._connection_count = 0
        self._transaction_owner_session_id: str | None = None
        self._lock = threading.RLock()
        self._scan_condition = threading.Condition()
        self._active_scan_count = 0
        self._thread_local = threading.local()

    def _engine_key(self) -> str:
        return f"sqlite:{id(self)}"

    def _record_operation_metadata(
        self,
        context: ClientSession | None,
        *,
        operation: str,
        comment: object | None,
        max_time_ms: int | None,
        hint: str | IndexKeySpec | None,
    ) -> None:
        if context is None:
            return
        context.update_engine_state(
            self._engine_key(),
            last_operation={
                "operation": operation,
                "comment": comment,
                "max_time_ms": max_time_ms,
                "hint": hint,
                "recorded_at": time.monotonic(),
            },
        )

    @override
    def create_session_state(self, session: ClientSession) -> None:
        engine_key = self._engine_key()
        session.bind_engine_context(
            EngineTransactionContext(
                engine_key=engine_key,
                connected=self._connection is not None,
                supports_transactions=True,
                transaction_active=False,
                metadata={"path": self._path},
            )
        )
        session.register_transaction_hooks(
            engine_key,
            start=self._start_session_transaction,
            commit=self._commit_session_transaction,
            abort=self._abort_session_transaction,
        )

    def _sync_session_state(
        self,
        session: ClientSession,
        *,
        transaction_active: bool | None = None,
    ) -> None:
        state = session.get_engine_context(self._engine_key())
        if state is None:
            return
        state.connected = self._connection is not None
        if transaction_active is not None:
            state.transaction_active = transaction_active

    def _start_session_transaction(self, session: ClientSession) -> None:
        with self._lock:
            if self._connection is None:
                raise InvalidOperation("SQLiteEngine must be connected before starting a transaction")
            if self._transaction_owner_session_id is not None:
                raise InvalidOperation("SQLiteEngine already has an active transaction bound to another session")
            conn = self._connection
            conn.execute("BEGIN")
            self._transaction_owner_session_id = session.session_id
            self._sync_session_state(session, transaction_active=True)

    def _commit_session_transaction(self, session: ClientSession) -> None:
        with self._lock:
            if self._connection is None:
                raise InvalidOperation("SQLiteEngine is not connected")
            if self._transaction_owner_session_id != session.session_id:
                raise InvalidOperation("This session does not own the active SQLite transaction")
            try:
                self._connection.commit()
            finally:
                self._transaction_owner_session_id = None
                self._sync_session_state(session, transaction_active=False)

    def _abort_session_transaction(self, session: ClientSession) -> None:
        with self._lock:
            if self._connection is None:
                return
            if self._transaction_owner_session_id != session.session_id:
                return
            try:
                self._connection.rollback()
            finally:
                self._transaction_owner_session_id = None
                self._sync_session_state(session, transaction_active=False)

    @contextmanager
    def _bind_connection(self, conn: sqlite3.Connection):
        previous = getattr(self._thread_local, "connection", None)
        self._thread_local.connection = conn
        try:
            yield
        finally:
            self._thread_local.connection = previous

    def _session_owns_transaction(self, context: ClientSession | None) -> bool:
        return (
            context is not None
            and context.in_transaction
            and self._transaction_owner_session_id == context.session_id
        )

    def _require_connection(self, context: ClientSession | None = None) -> sqlite3.Connection:
        thread_bound = getattr(self._thread_local, "connection", None)
        if thread_bound is not None:
            return thread_bound
        if self._connection is None:
            raise RuntimeError("SQLiteEngine is not connected")
        if self._transaction_owner_session_id is not None and not self._session_owns_transaction(context):
            raise InvalidOperation("SQLiteEngine has an active transaction bound to another session")
        return self._connection

    def _begin_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self._session_owns_transaction(context):
            conn.execute("BEGIN")

    def _commit_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self._session_owns_transaction(context):
            conn.commit()

    def _rollback_write(self, conn: sqlite3.Connection, context: ClientSession | None) -> None:
        if not self._session_owns_transaction(context):
            conn.rollback()

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

    def _physical_multikey_index_name(self, db_name: str, coll_name: str, index_name: str) -> str:
        digest = hashlib.sha1(f"{db_name}:{coll_name}:{index_name}:multikey".encode("utf-8")).hexdigest()[:16]
        return f"mkidx_{digest}"

    @staticmethod
    def _is_builtin_id_index(keys: IndexKeySpec) -> bool:
        return keys == [("_id", 1)]

    def _builtin_id_hint_index(self) -> EngineIndexRecord:
        return EngineIndexRecord(
            name="_id_",
            physical_name=None,
            fields=["_id"],
            key=[("_id", 1)],
            unique=True,
            multikey=False,
            multikey_physical_name=None,
        )

    def _resolve_hint_index(
        self,
        db_name: str,
        coll_name: str,
        hint: str | IndexKeySpec | None,
        *,
        indexes: list[EngineIndexRecord] | None = None,
    ) -> EngineIndexRecord | None:
        if hint is None:
            return None

        if isinstance(hint, str):
            if hint == "_id_":
                return self._builtin_id_hint_index()
        else:
            normalized_hint = normalize_index_keys(hint)
            if self._is_builtin_id_index(normalized_hint):
                return self._builtin_id_hint_index()

        if indexes is None:
            indexes = self._load_indexes(db_name, coll_name)

        for index in indexes:
            if isinstance(hint, str):
                if index["name"] == hint:
                    return deepcopy(index)
            else:
                if index["key"] == normalized_hint:
                    return deepcopy(index)

        raise OperationFailure("hint does not correspond to an existing index")

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
        hint: str | IndexKeySpec | None = None,
    ) -> tuple[str, list[object]]:
        hinted_index = self._resolve_hint_index(db_name, coll_name, hint)
        where_sql, params = self._translate_query_plan_with_multikey(db_name, coll_name, plan)
        order_sql = translate_sort_spec(sort)
        from_clause = "documents"
        if hinted_index is not None and hinted_index.get("physical_name") is not None:
            from_clause = (
                "documents INDEXED BY "
                f"{self._quote_identifier(str(hinted_index['physical_name']))}"
            )
        sql = f"""
            SELECT {select_clause}
            FROM {from_clause}
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

    @staticmethod
    def _normalize_multikey_number(value: int | float) -> str:
        if isinstance(value, float):
            if math.isnan(value) or math.isinf(value):
                raise NotImplementedError("NaN and infinity are not supported in SQLite multikey indexes")
        decimal = Decimal(str(value)).normalize()
        if decimal == decimal.to_integral():
            return format(decimal.quantize(Decimal(1)), "f")
        text = format(decimal, "f")
        if "." in text:
            text = text.rstrip("0").rstrip(".")
        return text or "0"

    @staticmethod
    def _multikey_value_signature(value: object) -> tuple[str, str] | None:
        if value is None:
            return ("null", "")
        if isinstance(value, bool):
            return ("bool", "1" if value else "0")
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            return ("number", SQLiteEngine._normalize_multikey_number(value))
        if isinstance(value, str):
            return ("string", value)

        encoded = DocumentCodec.encode(value)
        if not DocumentCodec._is_tagged_value(encoded):
            return None
        payload = encoded[DocumentCodec._MARKER]
        value_type = payload[DocumentCodec._TYPE]
        raw_value = payload[DocumentCodec._VALUE]
        if value_type in {"datetime", "uuid", "objectid", "bytes"}:
            return (value_type, str(raw_value))
        if value_type == "undefined":
            return ("undefined", "1")
        return None

    @classmethod
    def _multikey_signatures_for_query_value(
        cls,
        value: object,
        *,
        null_matches_undefined: bool = False,
    ) -> tuple[tuple[str, str], ...]:
        signature = cls._multikey_value_signature(value)
        if signature is None:
            raise NotImplementedError("Unsupported multikey query value")
        if value is None and null_matches_undefined:
            return (("null", ""), ("undefined", "1"))
        return (signature,)

    @classmethod
    def _extract_multikey_entries(cls, document: Document, field: str) -> set[tuple[str, str]]:
        found, value = get_document_value(document, field)
        if not found or not isinstance(value, list):
            return set()
        entries: set[tuple[str, str]] = set()
        for item in value:
            signature = cls._multikey_value_signature(item)
            if signature is not None:
                entries.add(signature)
        return entries

    @staticmethod
    def _supports_multikey_index(fields: list[str], unique: bool) -> bool:
        return not unique and len(fields) == 1 and not path_array_prefixes(fields[0])

    def _find_multikey_index(self, db_name: str, coll_name: str, field: str) -> EngineIndexRecord | None:
        for index in self._load_indexes(db_name, coll_name):
            if not index.get("multikey"):
                continue
            if index["fields"] == [field]:
                return index
        return None

    def _translate_multikey_exists_clause(
        self,
        db_name: str,
        coll_name: str,
        index: EngineIndexRecord,
        signatures: tuple[tuple[str, str], ...],
    ) -> tuple[str, list[object]]:
        physical_name = self._quote_identifier(str(index["multikey_physical_name"]))
        clauses: list[str] = []
        params: list[object] = []
        for element_type, element_key in signatures:
            clauses.append(
                "EXISTS ("
                f"SELECT 1 FROM multikey_entries INDEXED BY {physical_name} "
                "WHERE db_name = ? AND coll_name = ? AND index_name = ? "
                "AND storage_key = documents.storage_key "
                "AND element_type = ? AND element_key = ?)"
            )
            params.extend([db_name, coll_name, index["name"], element_type, element_key])
        return "(" + " OR ".join(clauses) + ")", params

    def _translate_equals_with_multikey(
        self,
        db_name: str,
        coll_name: str,
        plan: EqualsCondition,
        index: EngineIndexRecord,
    ) -> tuple[str, list[object]]:
        scalar_sql, scalar_params = _translate_scalar_equals(
            plan.field,
            plan.value,
            null_matches_undefined=plan.null_matches_undefined,
        )
        array_signatures = self._multikey_signatures_for_query_value(
            plan.value,
            null_matches_undefined=plan.null_matches_undefined,
        )
        array_sql, array_params = self._translate_multikey_exists_clause(
            db_name,
            coll_name,
            index,
            array_signatures,
        )
        return f"(({scalar_sql}) OR {array_sql})", [*scalar_params, *array_params]

    def _translate_in_with_multikey(
        self,
        db_name: str,
        coll_name: str,
        plan: InCondition,
        index: EngineIndexRecord,
    ) -> tuple[str, list[object]]:
        clauses: list[str] = []
        params: list[object] = []
        for value in plan.values:
            eq_sql, eq_params = self._translate_equals_with_multikey(
                db_name,
                coll_name,
                EqualsCondition(
                    plan.field,
                    value,
                    null_matches_undefined=plan.null_matches_undefined,
                ),
                index,
            )
            clauses.append(eq_sql)
            params.extend(eq_params)
        return " OR ".join(clauses), params

    def _translate_query_plan_with_multikey(
        self,
        db_name: str,
        coll_name: str,
        plan: QueryNode,
    ) -> tuple[str, list[object]]:
        if isinstance(plan, MatchAll):
            return "1 = 1", []
        if isinstance(plan, EqualsCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                return translate_query_plan(plan)
            return self._translate_equals_with_multikey(db_name, coll_name, plan, index)
        if isinstance(plan, InCondition):
            index = self._find_multikey_index(db_name, coll_name, plan.field)
            if index is None:
                return translate_query_plan(plan)
            return self._translate_in_with_multikey(db_name, coll_name, plan, index)
        if isinstance(plan, AndCondition):
            fragments = [self._translate_query_plan_with_multikey(db_name, coll_name, clause) for clause in plan.clauses]
            sql = " AND ".join(f"({fragment_sql})" for fragment_sql, _ in fragments)
            params: list[object] = []
            for _, fragment_params in fragments:
                params.extend(fragment_params)
            return sql, params
        if isinstance(plan, OrCondition):
            fragments = [self._translate_query_plan_with_multikey(db_name, coll_name, clause) for clause in plan.clauses]
            sql = " OR ".join(f"({fragment_sql})" for fragment_sql, _ in fragments)
            params: list[object] = []
            for _, fragment_params in fragments:
                params.extend(fragment_params)
            return sql, params
        if isinstance(plan, NotCondition):
            clause_sql, clause_params = self._translate_query_plan_with_multikey(db_name, coll_name, plan.clause)
            return f"NOT COALESCE(({clause_sql}), 0)", clause_params
        return translate_query_plan(plan)

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

    def _select_first_document_for_plan(self, db_name: str, coll_name: str, plan: QueryNode, *, hint: str | IndexKeySpec | None = None) -> tuple[str, Document] | None:
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
            hint=hint,
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
        hint: str | IndexKeySpec | None = None,
        max_time_ms: int | None = None,
    ) -> list[str]:
        deadline = operation_deadline(max_time_ms)
        with self._lock:
            conn = self._require_connection()
            plan = ensure_query_plan(filter_spec)
            enforce_deadline(deadline)
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
                hint=hint,
            )
            rows = conn.execute(f"EXPLAIN QUERY PLAN {sql}", tuple(params)).fetchall()
        enforce_deadline(deadline)
        return [str(row[3]) for row in rows]

    def _load_indexes(self, db_name: str, coll_name: str) -> list[EngineIndexRecord]:
        conn = self._require_connection()
        cursor = conn.execute(
            """
            SELECT name, physical_name, fields, keys, unique_flag, multikey_flag, multikey_physical_name
            FROM indexes
            WHERE db_name = ? AND coll_name = ?
            ORDER BY name
            """,
            (db_name, coll_name),
        )
        indexes: list[EngineIndexRecord] = []
        for name, physical_name, fields, keys, unique_flag, multikey_flag, multikey_physical_name in cursor.fetchall():
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
            if keys is None:
                parsed_keys = [(field, 1) for field in parsed_fields]
            else:
                try:
                    parsed_keys = normalize_index_keys(json.loads(keys))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise OperationFailure(
                        f"Invalid SQLite index metadata for {db_name}.{coll_name}.{name}"
                    ) from exc
            indexes.append(
                EngineIndexRecord(
                    name=name,
                    physical_name=physical_name or self._physical_index_name(db_name, coll_name, name),
                    fields=parsed_fields,
                    key=parsed_keys,
                    unique=bool(unique_flag),
                    multikey=bool(multikey_flag),
                    multikey_physical_name=multikey_physical_name
                    or self._physical_multikey_index_name(db_name, coll_name, name),
                )
            )
        return indexes

    def _delete_multikey_entries_for_storage_key(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
    ) -> None:
        conn.execute(
            """
            DELETE FROM multikey_entries
            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
            """,
            (db_name, coll_name, storage_key),
        )

    def _rebuild_multikey_entries_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        indexes: list[EngineIndexRecord],
    ) -> None:
        self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
        rows: list[tuple[str, str, str, str, str, str]] = []
        for index in indexes:
            if not index.get("multikey"):
                continue
            field = index["fields"][0]
            for element_type, element_key in self._extract_multikey_entries(document, field):
                rows.append((db_name, coll_name, index["name"], storage_key, element_type, element_key))
        if rows:
            conn.executemany(
                """
                INSERT OR IGNORE INTO multikey_entries (
                    db_name, coll_name, index_name, storage_key, element_type, element_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _replace_multikey_entries_for_index_for_document(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        storage_key: str,
        document: Document,
        index: EngineIndexRecord,
    ) -> None:
        conn.execute(
            """
            DELETE FROM multikey_entries
            WHERE db_name = ? AND coll_name = ? AND storage_key = ? AND index_name = ?
            """,
            (db_name, coll_name, storage_key, index["name"]),
        )
        if not index.get("multikey"):
            return
        field = index["fields"][0]
        rows = [
            (db_name, coll_name, index["name"], storage_key, element_type, element_key)
            for element_type, element_key in self._extract_multikey_entries(document, field)
        ]
        if rows:
            conn.executemany(
                """
                INSERT OR IGNORE INTO multikey_entries (
                    db_name, coll_name, index_name, storage_key, element_type, element_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

    def _connect_sync(self) -> None:
        with self._lock:
            if self._connection_count == 0:
                connection = sqlite3.connect(self._path, check_same_thread=False)
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS collections (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        options_json TEXT NOT NULL DEFAULT '{}',
                        PRIMARY KEY (db_name, coll_name)
                    )
                    """
                )
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
                        keys TEXT,
                        unique_flag INTEGER NOT NULL,
                        multikey_flag INTEGER NOT NULL DEFAULT 0,
                        multikey_physical_name TEXT,
                        PRIMARY KEY (db_name, coll_name, name)
                    )
                    """
                )
                connection.execute(
                    """
                    CREATE TABLE IF NOT EXISTS multikey_entries (
                        db_name TEXT NOT NULL,
                        coll_name TEXT NOT NULL,
                        index_name TEXT NOT NULL,
                        storage_key TEXT NOT NULL,
                        element_type TEXT NOT NULL,
                        element_key TEXT NOT NULL
                    )
                    """
                )
                collection_columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(collections)").fetchall()
                }
                if "options_json" not in collection_columns:
                    connection.execute(
                        "ALTER TABLE collections ADD COLUMN options_json TEXT NOT NULL DEFAULT '{}'"
                    )
                columns = {
                    row[1]
                    for row in connection.execute("PRAGMA table_info(indexes)").fetchall()
                }
                if "physical_name" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN physical_name TEXT")
                if "keys" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN keys TEXT")
                if "multikey_flag" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN multikey_flag INTEGER NOT NULL DEFAULT 0")
                if "multikey_physical_name" not in columns:
                    connection.execute("ALTER TABLE indexes ADD COLUMN multikey_physical_name TEXT")
                connection.execute(
                    """
                    INSERT OR IGNORE INTO collections (db_name, coll_name, options_json)
                    SELECT db_name, coll_name, '{}' FROM documents
                    UNION
                    SELECT db_name, coll_name, '{}' FROM indexes
                    """
                )
                connection.commit()
                self._connection = connection
            self._connection_count += 1

    def _ensure_collection_row(
        self,
        conn: sqlite3.Connection,
        db_name: str,
        coll_name: str,
        *,
        options: dict[str, object] | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT OR IGNORE INTO collections (db_name, coll_name, options_json)
            VALUES (?, ?, ?)
            """,
            (db_name, coll_name, json.dumps(options or {}, separators=(",", ":"), sort_keys=True)),
        )

    def _collection_exists_sync(self, conn: sqlite3.Connection, db_name: str, coll_name: str) -> bool:
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
            LIMIT 1
            """,
            (db_name, coll_name, db_name, coll_name, db_name, coll_name),
        ).fetchone()
        return row is not None

    def _collection_options_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        with self._lock:
            conn = self._require_connection(context)
            row = conn.execute(
                """
                SELECT options_json
                FROM collections
                WHERE db_name = ? AND coll_name = ?
                """,
                (db_name, coll_name),
            ).fetchone()
            if row is not None:
                return json.loads(row[0] or "{}")
            if self._collection_exists_sync(conn, db_name, coll_name):
                return {}
            raise CollectionInvalid(f"collection '{coll_name}' does not exist")

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
            self._transaction_owner_session_id = None
        if connection is not None:
            connection.close()

    def _put_document_sync(self, db_name: str, coll_name: str, document: Document, overwrite: bool, context: ClientSession | None) -> bool:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                storage_key = self._storage_key(document.get("_id"))
                indexes = self._load_indexes(db_name, coll_name)
                try:
                    self._validate_document_against_unique_indexes(db_name, coll_name, document)
                    if overwrite:
                        self._begin_write(conn, context)
                        self._ensure_collection_row(conn, db_name, coll_name)
                        conn.execute(
                            """
                            INSERT INTO documents (db_name, coll_name, storage_key, document)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(db_name, coll_name, storage_key)
                            DO UPDATE SET document = excluded.document
                            """,
                            (db_name, coll_name, storage_key, self._serialize_document(document)),
                        )
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                    else:
                        self._begin_write(conn, context)
                        self._ensure_collection_row(conn, db_name, coll_name)
                        cursor = conn.execute(
                            """
                            INSERT INTO documents (db_name, coll_name, storage_key, document)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(db_name, coll_name, storage_key) DO NOTHING
                            """,
                            (db_name, coll_name, storage_key, self._serialize_document(document)),
                        )
                        if cursor.rowcount == 0:
                            self._rollback_write(conn, context)
                            return False
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                    self._commit_write(conn, context)
                    return True
                except sqlite3.IntegrityError as exc:
                    self._rollback_write(conn, context)
                    raise DuplicateKeyError(str(exc)) from exc

    def _get_document_sync(self, db_name: str, coll_name: str, doc_id: DocumentId, projection: Projection | None, dialect: MongoDialect | None = None, context: ClientSession | None = None) -> Document | None:
        effective_dialect = dialect or MONGODB_DIALECT_70
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
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
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                storage_key = self._storage_key(doc_id)
                try:
                    self._begin_write(conn, context)
                    cursor = conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (db_name, coll_name, storage_key),
                    )
                    self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._commit_write(conn, context)
                    return cursor.rowcount > 0
                except Exception:
                    self._rollback_write(conn, context)
                    raise

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
        *,
        hint: str | IndexKeySpec | None = None,
        max_time_ms: int | None = None,
    ):
        effective_dialect = dialect or MONGODB_DIALECT_70
        deadline = operation_deadline(max_time_ms)
        if skip < 0:
            raise ValueError("skip must be >= 0")
        if limit is not None and limit < 0:
            raise ValueError("limit must be >= 0")

        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        with self._lock:
            conn: sqlite3.Connection | None = None
            if self._session_owns_transaction(context):
                conn = self._require_connection(context)
            with self._bind_connection(conn) if conn is not None else nullcontext():
                try:
                    enforce_deadline(deadline)
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
                        hint=hint,
                    )
                    if conn is None:
                        conn = self._require_connection(context)
                    cursor = conn.execute(sql, tuple(sql_params))
                    try:
                        for (payload,) in cursor:
                            enforce_deadline(deadline)
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
                                enforce_deadline(deadline)
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
            enforce_deadline(deadline)
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
        selector_filter: Filter | None,
        array_filters: ArrayFilters | None,
        plan: QueryNode | None,
        context: ClientSession | None,
        dialect: MongoDialect | None = None,
    ) -> UpdateResult[DocumentId]:
        effective_dialect = dialect or MONGODB_DIALECT_70
        plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                selected: tuple[str, Document] | None = None
                sql_selection_supported = False
                update_plan = UpdateEngine.compile_update_plan(
                    update_spec,
                    dialect=effective_dialect,
                    selector_filter=selector_filter or filter_spec,
                    array_filters=array_filters,
                )
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
                    modified = UpdateEngine.apply_compiled_update(document, update_plan)
                    if not modified:
                        return UpdateResult(matched_count=1, modified_count=0)
                    self._validate_document_against_unique_indexes(db_name, coll_name, document)
                    indexes = self._load_indexes(db_name, coll_name)

                    try:
                        if self._dialect_requires_python_fallback(effective_dialect):
                            raise NotImplementedError("Custom dialect requires Python fallback")
                        if array_filters is not None:
                            raise NotImplementedError("array_filters require Python update fallback")
                        update_sql, update_params = translate_update_spec(update_spec, current_document=original_document)
                        self._begin_write(conn, context)
                        conn.execute(
                            f"""
                            UPDATE documents
                            SET document = {update_sql}
                            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                            """,
                            (*update_params, db_name, coll_name, storage_key),
                        )
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                        self._commit_write(conn, context)
                        return UpdateResult(matched_count=1, modified_count=1)
                    except (NotImplementedError, TypeError):
                        self._rollback_write(conn, context)
                        pass
                    except sqlite3.IntegrityError as exc:
                        self._rollback_write(conn, context)
                        raise DuplicateKeyError(str(exc)) from exc

                    try:
                        self._begin_write(conn, context)
                        conn.execute(
                            """
                            UPDATE documents
                            SET document = ?
                            WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                            """,
                            (self._serialize_document(document), db_name, coll_name, storage_key),
                        )
                        self._rebuild_multikey_entries_for_document(
                            conn,
                            db_name,
                            coll_name,
                            storage_key,
                            document,
                            indexes,
                        )
                        self._commit_write(conn, context)
                        return UpdateResult(matched_count=1, modified_count=1)
                    except sqlite3.IntegrityError as exc:
                        self._rollback_write(conn, context)
                        raise DuplicateKeyError(str(exc)) from exc

                if not upsert:
                    return UpdateResult(matched_count=0, modified_count=0)

                new_doc = deepcopy(upsert_seed or {})
                upsert_plan = UpdateEngine.compile_update_plan(
                    update_spec,
                    dialect=effective_dialect,
                    selector_filter=selector_filter or filter_spec,
                    array_filters=array_filters,
                    is_upsert_insert=True,
                )
                UpdateEngine.apply_compiled_update(new_doc, upsert_plan)
                if "_id" not in new_doc:
                    new_doc["_id"] = ObjectId()
                self._validate_document_against_unique_indexes(db_name, coll_name, new_doc)

                storage_key = self._storage_key(new_doc["_id"])
                indexes = self._load_indexes(db_name, coll_name)
                try:
                    self._begin_write(conn, context)
                    self._ensure_collection_row(conn, db_name, coll_name)
                    conn.execute(
                        """
                        INSERT INTO documents (db_name, coll_name, storage_key, document)
                        VALUES (?, ?, ?, ?)
                        """,
                        (db_name, coll_name, storage_key, self._serialize_document(new_doc)),
                    )
                    self._rebuild_multikey_entries_for_document(
                        conn,
                        db_name,
                        coll_name,
                        storage_key,
                        new_doc,
                        indexes,
                    )
                    self._commit_write(conn, context)
                    return UpdateResult(matched_count=0, modified_count=0, upserted_id=new_doc["_id"])
                except sqlite3.IntegrityError as exc:
                    self._rollback_write(conn, context)
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
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                try:
                    if self._dialect_requires_python_fallback(effective_dialect):
                        raise NotImplementedError("Custom dialect requires Python fallback")
                    selected = self._select_first_document_for_plan(db_name, coll_name, plan)
                    if selected is None:
                        return DeleteResult(deleted_count=0)
                    storage_key, _document = selected
                    self._begin_write(conn, context)
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (db_name, coll_name, storage_key),
                    )
                    self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._commit_write(conn, context)
                    return DeleteResult(deleted_count=1)
                except (NotImplementedError, TypeError):
                    self._rollback_write(conn, context)
                    pass

                for storage_key, document in self._load_documents(db_name, coll_name):
                    if not QueryEngine.match_plan(document, plan, dialect=effective_dialect):
                        continue
                    self._begin_write(conn, context)
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
                        """,
                        (db_name, coll_name, storage_key),
                    )
                    self._delete_multikey_entries_for_storage_key(conn, db_name, coll_name, storage_key)
                    self._commit_write(conn, context)
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
            conn: sqlite3.Connection | None = None
            if self._session_owns_transaction(context):
                conn = self._require_connection(context)
            with self._bind_connection(conn) if conn is not None else nullcontext():
                try:
                    if self._dialect_requires_python_fallback(effective_dialect):
                        raise NotImplementedError("Custom dialect requires Python fallback")
                    if self._plan_has_array_traversing_paths(db_name, coll_name, plan):
                        raise NotImplementedError("Array traversal requires Python fallback")
                    if self._plan_requires_python_for_array_comparisons(db_name, coll_name, plan):
                        raise NotImplementedError("Top-level array comparisons require Python fallback")
                    if self._plan_requires_python_for_undefined(db_name, coll_name, plan):
                        raise NotImplementedError("Tagged undefined requires Python fallback")
                    where_sql, params = self._translate_query_plan_with_multikey(db_name, coll_name, plan)
                    if conn is None:
                        conn = self._require_connection(context)
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
        keys: IndexKeySpec,
        unique: bool,
        name: str | None,
        max_time_ms: int | None,
        context: ClientSession | None,
    ) -> str:
        normalized_keys = normalize_index_keys(keys)
        fields = index_fields(normalized_keys)
        index_name = name or default_index_name(normalized_keys)
        deadline = operation_deadline(max_time_ms)
        if self._is_builtin_id_index(normalized_keys):
            if name not in (None, "_id_"):
                raise OperationFailure("Conflicting index definition for '_id_'")
            return "_id_"
        if index_name == "_id_":
            raise OperationFailure("Conflicting index definition for '_id_'")
        physical_name = self._physical_index_name(db_name, coll_name, index_name)
        multikey = self._supports_multikey_index(fields, unique)
        multikey_physical_name = self._physical_multikey_index_name(db_name, coll_name, index_name)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                enforce_deadline(deadline)
                indexes = self._load_indexes(db_name, coll_name)
                for index in indexes:
                    enforce_deadline(deadline)
                    if index["name"] == index_name:
                        if index["key"] != normalized_keys or index["unique"] != unique:
                            raise OperationFailure(f"Conflicting index definition for '{index_name}'")
                        return index_name
                    if index["key"] == normalized_keys:
                        if index["unique"] != unique:
                            raise OperationFailure(
                                f"Conflicting index definition for key pattern '{normalized_keys!r}'"
                            )
                        return str(index["name"])

                if unique:
                    for field in fields:
                        if self._field_traverses_array_in_collection(db_name, coll_name, field):
                            raise OperationFailure("SQLite unique indexes do not support paths that traverse arrays")
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
                unique_sql = "UNIQUE " if unique else ""
                try:
                    enforce_deadline(deadline)
                    self._begin_write(conn, context)
                    conn.execute(
                        f"CREATE {unique_sql}INDEX {self._quote_identifier(physical_name)} "
                        f"ON documents ({expressions})"
                    )
                    if multikey:
                        enforce_deadline(deadline)
                        conn.execute(
                            f"CREATE INDEX {self._quote_identifier(multikey_physical_name)} "
                            "ON multikey_entries (db_name, coll_name, index_name, element_type, element_key, storage_key)"
                        )
                    conn.execute(
                        """
                        INSERT INTO indexes (
                            db_name, coll_name, name, physical_name, fields, keys, unique_flag, multikey_flag, multikey_physical_name
                        )
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            db_name,
                            coll_name,
                            index_name,
                            physical_name,
                            json.dumps(fields),
                            json.dumps(normalized_keys),
                            1 if unique else 0,
                            1 if multikey else 0,
                            multikey_physical_name if multikey else None,
                        ),
                    )
                    if multikey:
                        enforce_deadline(deadline)
                        index_metadata = {
                            "name": index_name,
                            "fields": fields,
                            "key": normalized_keys,
                            "unique": unique,
                            "multikey": True,
                            "multikey_physical_name": multikey_physical_name,
                        }
                        for storage_key, document in self._load_documents(db_name, coll_name):
                            enforce_deadline(deadline)
                            self._replace_multikey_entries_for_index_for_document(
                                conn,
                                db_name,
                                coll_name,
                                storage_key,
                                document,
                                index_metadata,
                            )
                    enforce_deadline(deadline)
                    self._commit_write(conn, context)
                    return index_name
                except sqlite3.IntegrityError as exc:
                    self._rollback_write(conn, context)
                    raise DuplicateKeyError(str(exc)) from exc

    def _list_indexes_sync(self, db_name: str, coll_name: str, context: ClientSession | None) -> list[IndexDocument]:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = deepcopy(self._load_indexes(db_name, coll_name))
        result = [default_id_index_definition().to_list_document()]
        result.extend(
            IndexDefinition(
                deepcopy(index["key"]),
                name=str(index["name"]),
                unique=bool(index["unique"]),
            ).to_list_document()
            for index in indexes
        )
        return result

    def _index_information_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> IndexInformation:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = deepcopy(self._load_indexes(db_name, coll_name))
        return {
            **default_id_index_information(),
            **{
                str(index["name"]): IndexDefinition(
                    deepcopy(index["key"]),
                    name=str(index["name"]),
                    unique=bool(index["unique"]),
                ).to_information_entry()
                for index in indexes
            },
        }

    def _drop_index_sync(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        context: ClientSession | None,
    ) -> None:
        normalized_keys: IndexKeySpec | None = None
        target_name: str
        if isinstance(index_or_name, str):
            if index_or_name == "_id_":
                raise OperationFailure("cannot drop _id index")
            target_name = index_or_name
        else:
            normalized_keys = normalize_index_keys(index_or_name)
            if self._is_builtin_id_index(normalized_keys):
                raise OperationFailure("cannot drop _id index")
            target_name = default_index_name(normalized_keys)
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = self._load_indexes(db_name, coll_name)
                target: EngineIndexRecord | None = None
                for index in indexes:
                    if index["name"] == target_name:
                        target = index
                        break
                if target is None:
                    if isinstance(index_or_name, str):
                        raise OperationFailure(f"index not found with name [{index_or_name}]")
                    raise OperationFailure(f"index not found with key pattern {normalized_keys!r}")
                try:
                    self._begin_write(conn, context)
                    conn.execute(
                        f"DROP INDEX IF EXISTS {self._quote_identifier(str(target['physical_name']))}"
                    )
                    if target.get("multikey"):
                        conn.execute(
                            f"DROP INDEX IF EXISTS {self._quote_identifier(str(target['multikey_physical_name']))}"
                        )
                        conn.execute(
                            """
                            DELETE FROM multikey_entries
                            WHERE db_name = ? AND coll_name = ? AND index_name = ?
                            """,
                            (db_name, coll_name, target["name"]),
                        )
                    conn.execute(
                        """
                        DELETE FROM indexes
                        WHERE db_name = ? AND coll_name = ? AND name = ?
                        """,
                        (db_name, coll_name, target["name"]),
                    )
                    self._commit_write(conn, context)
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _drop_indexes_sync(
        self,
        db_name: str,
        coll_name: str,
        context: ClientSession | None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = self._load_indexes(db_name, coll_name)
                try:
                    self._begin_write(conn, context)
                    for index in indexes:
                        conn.execute(
                            f"DROP INDEX IF EXISTS {self._quote_identifier(str(index['physical_name']))}"
                        )
                        if index.get("multikey"):
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(str(index['multikey_physical_name']))}"
                            )
                    conn.execute(
                        """
                        DELETE FROM multikey_entries
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
                    self._commit_write(conn, context)
                except Exception:
                    self._rollback_write(conn, context)
                    raise

    def _list_databases_sync(self, context: ClientSession | None = None) -> list[str]:
        with self._lock:
            conn = self._require_connection(context)
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
                ORDER BY db_name
                """
            )
            return [row[0] for row in cursor.fetchall()]

    def _list_collections_sync(self, db_name: str, context: ClientSession | None = None) -> list[str]:
        with self._lock:
            conn = self._require_connection(context)
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
                ORDER BY coll_name
                """,
                (db_name, db_name, db_name),
            )
            return [row[0] for row in cursor.fetchall()]

    def _create_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        options: dict[str, object] | None = None,
        context: ClientSession | None = None,
    ) -> None:
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                if self._collection_exists_sync(conn, db_name, coll_name):
                    raise CollectionInvalid(f"collection '{coll_name}' already exists")
                self._ensure_collection_row(conn, db_name, coll_name, options=options)
                self._commit_write(conn, context)
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _rename_collection_sync(
        self,
        db_name: str,
        coll_name: str,
        new_name: str,
        context: ClientSession | None = None,
    ) -> None:
        if coll_name == new_name:
            raise CollectionInvalid("collection names must differ")
        with self._lock:
            conn = self._require_connection(context)
            try:
                self._begin_write(conn, context)
                if not self._collection_exists_sync(conn, db_name, coll_name):
                    raise CollectionInvalid(f"collection '{coll_name}' does not exist")
                if self._collection_exists_sync(conn, db_name, new_name):
                    raise CollectionInvalid(f"collection '{new_name}' already exists")
                for table_name in ("collections", "documents", "indexes", "multikey_entries"):
                    conn.execute(
                        f"""
                        UPDATE {table_name}
                        SET coll_name = ?
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (new_name, db_name, coll_name),
                    )
                self._commit_write(conn, context)
            except Exception:
                self._rollback_write(conn, context)
                raise

    def _drop_collection_sync(self, db_name: str, coll_name: str, context: ClientSession | None = None) -> None:
        with self._lock:
            conn = self._require_connection(context)
            with self._bind_connection(conn):
                indexes = self._load_indexes(db_name, coll_name)
                try:
                    self._begin_write(conn, context)
                    for index in indexes:
                        conn.execute(f"DROP INDEX IF EXISTS {self._quote_identifier(index['physical_name'])}")
                        if index.get("multikey"):
                            conn.execute(
                                f"DROP INDEX IF EXISTS {self._quote_identifier(index['multikey_physical_name'])}"
                            )
                    conn.execute(
                        """
                        DELETE FROM documents
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    conn.execute(
                        """
                        DELETE FROM multikey_entries
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
                    conn.execute(
                        """
                        DELETE FROM collections
                        WHERE db_name = ? AND coll_name = ?
                        """,
                        (db_name, coll_name),
                    )
                    self._commit_write(conn, context)
                except Exception:
                    self._rollback_write(conn, context)
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
        hint: str | IndexKeySpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        async def _scan() -> AsyncIterable[Document]:
            self._record_operation_metadata(
                context,
                operation="scan_collection",
                comment=comment,
                max_time_ms=max_time_ms,
                hint=hint,
            )
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
                        context,
                        max_time_ms=max_time_ms,
                        hint=hint,
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
    def scan_find_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> AsyncIterable[Document]:
        return self.scan_collection(
            db_name,
            coll_name,
            operation.filter_spec,
            plan=operation.plan,
            projection=operation.projection,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            dialect=dialect,
            context=context,
        )

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
        selector_filter: Filter | None = None,
        array_filters: ArrayFilters | None = None,
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
            selector_filter,
            array_filters,
            plan,
            context,
            dialect,
        )

    @override
    async def update_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        update_spec: Update,
        upsert: bool = False,
        upsert_seed: Document | None = None,
        *,
        selector_filter: Filter | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> UpdateResult[DocumentId]:
        return await self.update_matching_document(
            db_name,
            coll_name,
            operation.filter_spec,
            update_spec,
            upsert=upsert,
            upsert_seed=upsert_seed,
            selector_filter=selector_filter,
            array_filters=operation.array_filters,
            plan=operation.plan,
            dialect=dialect,
            context=context,
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
    async def delete_with_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: UpdateOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> DeleteResult:
        return await self.delete_matching_document(
            db_name,
            coll_name,
            operation.filter_spec,
            plan=operation.plan,
            dialect=dialect,
            context=context,
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
    async def count_find_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> int:
        count = 0
        async for _ in self.scan_find_operation(
            db_name,
            coll_name,
            operation,
            dialect=dialect,
            context=context,
        ):
            count += 1
        return count

    @override
    async def create_index(
        self,
        db_name: str,
        coll_name: str,
        keys: IndexKeySpec,
        *,
        unique: bool = False,
        name: str | None = None,
        max_time_ms: int | None = None,
        context: ClientSession | None = None,
    ) -> str:
        return await asyncio.to_thread(
            self._create_index_sync,
            db_name,
            coll_name,
            keys,
            unique,
            name,
            max_time_ms,
            context,
        )

    @override
    async def list_indexes(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> list[IndexDocument]:
        return await asyncio.to_thread(self._list_indexes_sync, db_name, coll_name, context)

    @override
    async def index_information(self, db_name: str, coll_name: str, *, context: ClientSession | None = None) -> IndexInformation:
        return await asyncio.to_thread(self._index_information_sync, db_name, coll_name, context)

    @override
    async def drop_index(
        self,
        db_name: str,
        coll_name: str,
        index_or_name: str | IndexKeySpec,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await asyncio.to_thread(self._drop_index_sync, db_name, coll_name, index_or_name, context)

    @override
    async def drop_indexes(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await asyncio.to_thread(self._drop_indexes_sync, db_name, coll_name, context)

    @override
    async def explain_query_plan(
        self,
        db_name: str,
        coll_name: str,
        filter_spec: Filter | None = None,
        *,
        plan: QueryNode | None = None,
        sort: SortSpec | None = None,
        skip: int = 0,
        limit: int | None = None,
        hint: str | IndexKeySpec | None = None,
        comment: object | None = None,
        max_time_ms: int | None = None,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        effective_dialect = dialect or MONGODB_DIALECT_70
        query_plan = ensure_query_plan(filter_spec, plan, dialect=effective_dialect)
        self._record_operation_metadata(
            context,
            operation="explain_query_plan",
            comment=comment,
            max_time_ms=max_time_ms,
            hint=hint,
        )
        hinted_index = await asyncio.to_thread(
            self._resolve_hint_index,
            db_name,
            coll_name,
            hint,
        )
        details = await asyncio.to_thread(
            self._explain_query_plan_sync,
            db_name,
            coll_name,
            filter_spec,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            max_time_ms=max_time_ms,
        )
        return QueryPlanExplanation(
            engine="sqlite",
            strategy="sql",
            plan=repr(query_plan),
            details=details,
            sort=sort,
            skip=skip,
            limit=limit,
            hint=hint,
            hinted_index=None if hinted_index is None else hinted_index["name"],
            comment=comment,
            max_time_ms=max_time_ms,
        )

    @override
    async def explain_find_operation(
        self,
        db_name: str,
        coll_name: str,
        operation: FindOperation,
        *,
        dialect: MongoDialect | None = None,
        context: ClientSession | None = None,
    ) -> QueryPlanExplanation:
        return await self.explain_query_plan(
            db_name,
            coll_name,
            operation.filter_spec,
            plan=operation.plan,
            sort=operation.sort,
            skip=operation.skip,
            limit=operation.limit,
            hint=operation.hint,
            comment=operation.comment,
            max_time_ms=operation.max_time_ms,
            dialect=dialect,
            context=context,
        )

    @override
    async def list_databases(self, *, context: ClientSession | None = None) -> list[str]:
        return await asyncio.to_thread(self._list_databases_sync, context)

    @override
    async def list_collections(self, db_name: str, *, context: ClientSession | None = None) -> list[str]:
        return await asyncio.to_thread(self._list_collections_sync, db_name, context)

    @override
    async def collection_options(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> dict[str, object]:
        return await asyncio.to_thread(
            self._collection_options_sync,
            db_name,
            coll_name,
            context,
        )

    @override
    async def create_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        options: dict[str, object] | None = None,
        context: ClientSession | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._create_collection_sync,
            db_name,
            coll_name,
            options,
            context,
        )

    @override
    async def rename_collection(
        self,
        db_name: str,
        coll_name: str,
        new_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await asyncio.to_thread(
            self._rename_collection_sync,
            db_name,
            coll_name,
            new_name,
            context,
        )

    @override
    async def drop_collection(
        self,
        db_name: str,
        coll_name: str,
        *,
        context: ClientSession | None = None,
    ) -> None:
        await asyncio.to_thread(self._drop_collection_sync, db_name, coll_name, context)
