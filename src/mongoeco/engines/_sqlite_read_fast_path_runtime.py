from __future__ import annotations

import sqlite3
from typing import Any, Protocol

from mongoeco.core.json_compat import json_loads
from mongoeco.core.query_plan import QueryNode
from mongoeco.engines._sqlite_fast_paths import (
    select_first_document_for_plan as _sqlite_select_first_document_for_plan,
    select_first_document_for_scalar_index as _sqlite_select_first_document_for_scalar_index,
    select_first_document_for_scalar_range as _sqlite_select_first_document_for_scalar_range,
)
from mongoeco.engines._sqlite_read_execution import (
    build_scalar_indexed_top_level_equals_sql as _sqlite_build_scalar_indexed_top_level_equals_sql,
    build_scalar_indexed_top_level_range_sql as _sqlite_build_scalar_indexed_top_level_range_sql,
)
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.types import Document, EngineIndexRecord, IndexKeySpec


class _SQLiteReadFastPathEngine(Protocol):
    def _load_indexes(self, db_name: str, coll_name: str) -> list[EngineIndexRecord]: ...
    def _supports_scalar_index(self, index: EngineIndexRecord) -> bool: ...
    def _field_is_top_level_array_in_collection(self, db_name: str, coll_name: str, field: str) -> bool: ...
    def _field_has_comparison_type_mismatch_in_collection(self, db_name: str, coll_name: str, field: str, expected_type: str) -> bool: ...
    def _scalar_range_signature(self, value: object): ...
    def _storage_key(self, value: Any) -> str: ...
    def _require_connection(self) -> sqlite3.Connection: ...
    def _deserialize_document(self, document_json: str) -> Document: ...
    def _lookup_collection_id(self, conn: sqlite3.Connection, db_name: str, coll_name: str, *, create: bool = False) -> int | None: ...
    def _multikey_signatures_for_query_value(self, value: object, *, null_matches_undefined: bool = False) -> tuple[tuple[str, str], ...]: ...
    def _multikey_type_score(self, element_type: str) -> int: ...
    def _quote_identifier(self, identifier: str) -> str: ...
    def _compile_read_execution_plan(self, db_name: str, coll_name: str, semantics, *, select_clause: str = "document", hint: str | IndexKeySpec | None = None): ...


def find_scalar_fast_path_index(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    field: str,
) -> EngineIndexRecord | None:
    for index in engine._load_indexes(db_name, coll_name):
        if index["key"] != [(field, 1)]:
            continue
        if index.get("sparse") or index.get("partial_filter_expression"):
            continue
        if not index.get("scalar_physical_name"):
            continue
        return index
    return None


def can_use_scalar_range_fast_path(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    field: str,
    value: object,
) -> tuple[EngineIndexRecord, int, str] | None:
    if "." in field:
        return None
    if engine._field_is_top_level_array_in_collection(db_name, coll_name, field):
        return None
    signature = engine._scalar_range_signature(value)
    if signature is None:
        return None
    element_type, type_score, element_key = signature
    if engine._field_has_comparison_type_mismatch_in_collection(
        db_name,
        coll_name,
        field,
        element_type,
    ):
        return None
    index = engine._find_scalar_fast_path_index(db_name, coll_name, field)
    if index is None:
        return None
    return index, type_score, element_key


def build_scalar_indexed_top_level_equals_sql(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    *,
    field: str,
    value: Any,
    index_name: str,
    physical_name: str,
    limit: int | None = None,
    null_matches_undefined: bool = False,
) -> tuple[str, tuple[object, ...]] | None:
    return _sqlite_build_scalar_indexed_top_level_equals_sql(
        db_name=db_name,
        coll_name=coll_name,
        field=field,
        value=value,
        index_name=index_name,
        physical_name=physical_name,
        limit=limit,
        null_matches_undefined=null_matches_undefined,
        lookup_collection_id=lambda current_db_name, current_coll_name: engine._lookup_collection_id(
            engine._require_connection(),
            current_db_name,
            current_coll_name,
        ),
        multikey_signatures_for_query_value=lambda current_value, current_null_matches_undefined: engine._multikey_signatures_for_query_value(
            current_value,
            null_matches_undefined=current_null_matches_undefined,
        ),
        multikey_type_score=engine._multikey_type_score,
        quote_identifier=engine._quote_identifier,
    )


def build_scalar_indexed_top_level_range_sql(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    *,
    field: str,
    value: Any,
    operator: str,
    index_name: str,
    physical_name: str,
    limit: int | None = None,
) -> tuple[str, tuple[object, ...]] | None:
    return _sqlite_build_scalar_indexed_top_level_range_sql(
        db_name=db_name,
        coll_name=coll_name,
        value=value,
        operator=operator,
        index_name=index_name,
        physical_name=physical_name,
        limit=limit,
        can_use_scalar_range_fast_path=lambda current_db_name, current_coll_name, current_value: can_use_scalar_range_fast_path(
            engine,
            current_db_name,
            current_coll_name,
            field,
            current_value,
        ),
        lookup_collection_id=lambda current_db_name, current_coll_name: engine._lookup_collection_id(
            engine._require_connection(),
            current_db_name,
            current_coll_name,
        ),
        quote_identifier=engine._quote_identifier,
    )


def select_first_document_for_scalar_index(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    *,
    field: str,
    value: Any,
    null_matches_undefined: bool = False,
) -> tuple[str, Document] | None:
    conn = engine._require_connection()
    return _sqlite_select_first_document_for_scalar_index(
        db_name=db_name,
        coll_name=coll_name,
        field=field,
        value=value,
        null_matches_undefined=null_matches_undefined,
        field_is_top_level_array_in_collection=engine._field_is_top_level_array_in_collection,
        find_scalar_index=lambda current_db_name, current_coll_name, current_field: _find_scalar_index(
            engine,
            current_db_name,
            current_coll_name,
            current_field,
        ),
        lookup_collection_id=lambda current_db_name, current_coll_name: engine._lookup_collection_id(
            conn,
            current_db_name,
            current_coll_name,
        ),
        multikey_signatures_for_query_value=lambda current_value, current_null_matches_undefined: engine._multikey_signatures_for_query_value(
            current_value,
            null_matches_undefined=current_null_matches_undefined,
        ),
        multikey_type_score=engine._multikey_type_score,
        quote_identifier=engine._quote_identifier,
        fetchone=lambda sql, params: conn.execute(sql, params).fetchone(),
        deserialize_document=engine._deserialize_document,
    )


def select_first_document_for_scalar_range(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    *,
    field: str,
    value: Any,
    operator: str,
) -> tuple[str, Document] | None:
    conn = engine._require_connection()
    return _sqlite_select_first_document_for_scalar_range(
        db_name=db_name,
        coll_name=coll_name,
        field=field,
        value=value,
        operator=operator,
        can_use_scalar_range_fast_path=lambda current_db_name, current_coll_name, current_field, current_value: can_use_scalar_range_fast_path(
            engine,
            current_db_name,
            current_coll_name,
            current_field,
            current_value,
        ),
        lookup_collection_id=lambda current_db_name, current_coll_name: engine._lookup_collection_id(
            conn,
            current_db_name,
            current_coll_name,
        ),
        quote_identifier=engine._quote_identifier,
        fetchone=lambda sql, params: conn.execute(sql, params).fetchone(),
        deserialize_document=engine._deserialize_document,
    )


def select_first_document_for_plan(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    plan: QueryNode,
    *,
    hint: str | IndexKeySpec | None = None,
) -> tuple[str, Document] | None:
    conn = engine._require_connection()
    selected = _sqlite_select_first_document_for_plan(
        db_name=db_name,
        coll_name=coll_name,
        plan=plan,
        storage_key_for_id=engine._storage_key,
        fetchone=lambda sql, params: conn.execute(sql, params).fetchone(),
        deserialize_document=engine._deserialize_document,
        select_first_document_for_scalar_index_fn=lambda current_db_name, current_coll_name, field, value, null_matches_undefined: select_first_document_for_scalar_index(
            engine,
            current_db_name,
            current_coll_name,
            field=field,
            value=value,
            null_matches_undefined=null_matches_undefined,
        ),
        select_first_document_for_scalar_range_fn=lambda current_db_name, current_coll_name, field, value, operator: select_first_document_for_scalar_range(
            engine,
            current_db_name,
            current_coll_name,
            field=field,
            value=value,
            operator=operator,
        ),
    )
    if selected is not None:
        return selected
    execution_plan = engine._compile_read_execution_plan(
        db_name,
        coll_name,
        compile_find_semantics({}, plan=plan, limit=1),
        select_clause="storage_key, document",
        hint=hint,
    )
    sql, params = execution_plan.require_sql()
    row = conn.execute(sql, tuple(params)).fetchone()
    if row is None:
        return None
    storage_key, document = row
    return storage_key, engine._deserialize_document(document)


def _find_scalar_index(
    engine: _SQLiteReadFastPathEngine,
    db_name: str,
    coll_name: str,
    field: str,
) -> EngineIndexRecord | None:
    for index in engine._load_indexes(db_name, coll_name):
        if not engine._supports_scalar_index(index):
            continue
        if index["fields"] == [field]:
            return index
    return None
