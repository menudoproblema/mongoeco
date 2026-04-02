from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
import sqlite3

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.projections import apply_projection
from mongoeco.core.search import (
    SearchPhraseQuery,
    SearchTextQuery,
    SearchVectorQuery,
    compile_search_stage,
    score_vector_document,
    vector_field_paths,
)
from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.engines.sqlite_planner import SQLiteReadExecutionPlan
from mongoeco.errors import OperationFailure
from mongoeco.types import Document, DocumentId, Projection


def get_document(
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    doc_id: DocumentId,
    projection: Projection | None,
    dialect: MongoDialect | None,
    storage_key: str,
    deserialize_document: Callable[[str], Document],
) -> Document | None:
    effective_dialect = dialect or MONGODB_DIALECT_70
    row = conn.execute(
        """
        SELECT document
        FROM documents
        WHERE db_name = ? AND coll_name = ? AND storage_key = ?
        """,
        (db_name, coll_name, storage_key),
    ).fetchone()
    if row is None:
        return None
    return apply_projection(
        deserialize_document(row[0]),
        projection,
        dialect=effective_dialect,
    )


def search_documents(
    *,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    deadline: float | None,
    load_search_index_rows: Callable[[str, str, str | None], list[tuple[object, str | None, float | None]]],
    search_index_is_ready: Callable[[float | None], bool],
    load_documents: Callable[[str, str], list[tuple[str, Document]]],
    search_sql: Callable[[str, str, object, object, str | None], list[Document]],
) -> list[Document]:
    query = compile_search_stage(operator, spec)
    rows = load_search_index_rows(db_name, coll_name, query.index_name)
    if not rows:
        raise OperationFailure(f"search index not found with name [{query.index_name}]")
    definition, physical_name, ready_at_epoch = rows[0]
    if not search_index_is_ready(ready_at_epoch):
        raise OperationFailure(f"search index [{query.index_name}] is not ready yet")
    if isinstance(query, (SearchTextQuery, SearchPhraseQuery)) and definition.index_type != "search":
        raise OperationFailure(f"search index [{query.index_name}] does not support $search")
    if isinstance(query, SearchVectorQuery) and definition.index_type != "vectorSearch":
        raise OperationFailure(f"search index [{query.index_name}] does not support $vectorSearch")
    if isinstance(query, SearchVectorQuery):
        documents = [document for _, document in load_documents(db_name, coll_name)]
        enforce_deadline(deadline)
        vector_hits: list[tuple[float, Document]] = []
        for document in documents:
            if query.filter_spec is not None and not QueryEngine.match(document, query.filter_spec):
                continue
            score = score_vector_document(
                document,
                definition=definition,
                query=query,
            )
            if score is None:
                continue
            vector_hits.append((score, document))
        vector_hits.sort(key=lambda item: item[0], reverse=True)
        if query.num_candidates is not None:
            vector_hits = vector_hits[:query.num_candidates]
        return [document for _score, document in vector_hits[: query.limit]]
    return search_sql(db_name, coll_name, definition, query, physical_name)


def build_search_explain(
    *,
    operator: str,
    spec: object,
    definition: object,
    physical_name: str | None,
    fts5_match: str | None,
) -> dict[str, object]:
    query = compile_search_stage(operator, spec)
    return {
        "operator": operator,
        "index": query.index_name,
        "indexType": definition.index_type,
        "physicalName": physical_name,
        "fts5_match": fts5_match,
        "path": query.path if isinstance(query, SearchVectorQuery) else None,
        "queryVector": list(query.query_vector) if isinstance(query, SearchVectorQuery) else None,
        "limit": query.limit if isinstance(query, SearchVectorQuery) else None,
        "numCandidates": query.num_candidates if isinstance(query, SearchVectorQuery) else None,
        "filter": deepcopy(query.filter_spec) if isinstance(query, SearchVectorQuery) and query.filter_spec is not None else None,
        "similarity": query.similarity if isinstance(query, SearchVectorQuery) else None,
        "vector_paths": list(vector_field_paths(definition)) if definition.index_type == "vectorSearch" else None,
    }


def require_sql_execution_plan(plan: SQLiteReadExecutionPlan) -> tuple[str, tuple[object, ...]]:
    return plan.require_sql()
