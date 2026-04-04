from __future__ import annotations

from copy import deepcopy
import heapq
import math
from typing import Protocol

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.search import (
    SearchCompoundQuery,
    SearchNearQuery,
    SearchVectorQuery,
    build_search_index_document,
    compile_search_stage,
    is_text_search_query,
    matches_search_query,
    search_compound_ranking,
    search_near_distance,
    search_query_explain_details,
    vector_field_paths,
)
from mongoeco.core.search_filter_prefilter import matches_candidateable_filter
from mongoeco.engines._memory_vector_runtime import (
    candidate_positions_for_vector_filter,
    candidate_rows_for_vector_filter,
    vector_scores_for_rows,
    vector_scores_for_positions,
)
from mongoeco.engines._shared_search_admin import ensure_search_index_query_supported, search_index_not_found
from mongoeco.errors import OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import Document, QueryPlanExplanation, SearchIndexDefinition


class _MemorySearchRuntimeEngine(Protocol):
    async def _get_lock(self, db_name: str, coll_name: str): ...
    def _search_indexes_view(self, context: ClientSession | None): ...
    def _search_index_is_ready(self, db_name: str, coll_name: str, name: str) -> bool: ...
    def _materialized_search_documents(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        *,
        context: ClientSession | None,
    ): ...
    def _materialized_vector_index(
        self,
        db_name: str,
        coll_name: str,
        definition: SearchIndexDefinition,
        *,
        context: ClientSession | None,
    ): ...
    _search_index_ready_at: dict[tuple[str, str, str], float]


def _vector_filter_residual_description(
    filter_spec: dict[str, object] | None,
    vector_filter_description: dict[str, object] | None,
) -> dict[str, object] | None:
    if filter_spec is None:
        return None
    if vector_filter_description is None:
        return {
            "required": True,
            "reason": "filter-not-candidateable",
            "candidateable": False,
            "exact": False,
            "spec": deepcopy(filter_spec),
        }
    residual_required = not bool(vector_filter_description.get("exact"))
    return {
        "required": residual_required,
        "reason": (
            None
            if not residual_required
            else "unsupported-clauses"
            if int(vector_filter_description.get("unsupportedClauseCount", 0)) > 0
            else "non-exact-prefilter"
        ),
        "candidateable": bool(vector_filter_description.get("candidateable")),
        "exact": bool(vector_filter_description.get("exact")),
        "supportedClauseCount": int(vector_filter_description.get("supportedClauseCount", 0)),
        "unsupportedClauseCount": int(vector_filter_description.get("unsupportedClauseCount", 0)),
        "spec": deepcopy(filter_spec),
    }


async def execute_search_documents(
    engine: _MemorySearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    *,
    context: ClientSession | None = None,
    result_limit_hint: int | None = None,
    downstream_filter_spec: dict[str, object] | None = None,
) -> list[Document]:
    query = compile_search_stage(operator, spec)
    effective_limit = result_limit_hint if isinstance(result_limit_hint, int) and result_limit_hint > 0 else None
    async with engine._get_lock(db_name, coll_name):
        indexes = deepcopy(engine._search_indexes_view(context).get(db_name, {}).get(coll_name, []))
        definition = next((item for item in indexes if item.name == query.index_name), None)
        if definition is None:
            raise search_index_not_found(query.index_name)
        ensure_search_index_query_supported(
            definition,
            query,
            ready=engine._search_index_is_ready(db_name, coll_name, query.index_name),
        )
        materialized_documents = engine._materialized_search_documents(db_name, coll_name, definition, context=context)
        vector_index = (
            engine._materialized_vector_index(db_name, coll_name, definition, context=context)
            if isinstance(query, SearchVectorQuery)
            else None
        )

    if is_text_search_query(query):
        if isinstance(query, SearchNearQuery):
            distance_hits: list[tuple[float, Document]] = []
            for document, materialized in materialized_documents:
                if downstream_filter_spec is not None and not QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70):
                    continue
                if not matches_search_query(document, definition=definition, query=query, materialized=materialized):
                    continue
                distance = search_near_distance(document, query=query)
                if distance is None:
                    continue
                distance_hits.append((distance, document))
            if effective_limit is not None:
                distance_hits = heapq.nsmallest(effective_limit, distance_hits, key=lambda item: item[0])
            else:
                distance_hits.sort(key=lambda item: item[0])
            return [document for _distance, document in distance_hits]
        if isinstance(query, SearchCompoundQuery) and query.should:
            ranked_documents: list[tuple[tuple[int, float, float], int, Document]] = []
            for order, (document, materialized) in enumerate(materialized_documents):
                if downstream_filter_spec is not None:
                    candidateable_match = matches_candidateable_filter(document, downstream_filter_spec)
                    if candidateable_match is False:
                        continue
                    if candidateable_match is None and not QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70):
                        continue
                if not matches_search_query(document, definition=definition, query=query, materialized=materialized):
                    continue
                matched_should, should_score, best_near_distance = search_compound_ranking(
                    document,
                    definition=definition,
                    query=query,
                    materialized=materialized,
                )
                rank = (
                    matched_should,
                    should_score,
                    -best_near_distance if math.isfinite(best_near_distance) else float("-inf"),
                )
                if effective_limit is not None:
                    if len(ranked_documents) < effective_limit:
                        heapq.heappush(ranked_documents, (rank, -order, document))
                    elif (rank, -order) > ranked_documents[0][:2]:
                        heapq.heapreplace(ranked_documents, (rank, -order, document))
                else:
                    ranked_documents.append((rank, -order, document))
            ranked_documents.sort(key=lambda item: item[:2], reverse=True)
            return [document for _score, _order, document in ranked_documents]
        matches: list[Document] = []
        for document, materialized in materialized_documents:
            if downstream_filter_spec is not None:
                candidateable_match = matches_candidateable_filter(document, downstream_filter_spec)
                if candidateable_match is False:
                    continue
                if candidateable_match is None and not QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70):
                    continue
            if not matches_search_query(document, definition=definition, query=query, materialized=materialized):
                continue
            matches.append(document)
            if effective_limit is not None and len(matches) >= effective_limit:
                break
        return matches

    effective_vector_limit = min(query.limit, effective_limit) if effective_limit is not None else query.limit
    if vector_index is None:
        return []
    if query.path not in vector_index.vector_specs:
        return []
    path_positions = vector_index.vector_row_positions.get(query.path, ())
    query_filter_rows, _query_filter_description = candidate_rows_for_vector_filter(
        vector_index,
        query_path=query.path,
        filter_spec=query.filter_spec,
    )
    downstream_filter_rows, _downstream_filter_description = candidate_rows_for_vector_filter(
        vector_index,
        query_path=query.path,
        filter_spec=downstream_filter_spec,
    )
    candidate_rows = list(query_filter_rows if query_filter_rows is not None else range(len(path_positions)))
    if downstream_filter_rows is not None:
        allowed = set(downstream_filter_rows)
        candidate_rows = [row_index for row_index in candidate_rows if row_index in allowed]
    scored_rows = vector_scores_for_rows(
        vector_index,
        query=query,
        candidate_rows=candidate_rows,
        limit=effective_vector_limit,
    )
    query_requires_postfilter = query.filter_spec is not None and query_filter_rows is None
    downstream_requires_postfilter = downstream_filter_spec is not None and downstream_filter_rows is None
    if not query_requires_postfilter and not downstream_requires_postfilter:
        return [
            vector_index.documents[path_positions[row_index]].document
            for _score, row_index in scored_rows[:effective_vector_limit]
        ]
    vector_hits: list[tuple[float, int, Document]] = []
    for order, (score, row_index) in enumerate(scored_rows):
        prepared = vector_index.documents[path_positions[row_index]]
        if downstream_requires_postfilter:
            candidateable_match = matches_candidateable_filter(prepared.document, downstream_filter_spec)
            if candidateable_match is False:
                continue
            if candidateable_match is None and not QueryEngine.match(prepared.document, downstream_filter_spec, dialect=MONGODB_DIALECT_70):
                continue
        if query_requires_postfilter:
            candidateable_match = matches_candidateable_filter(prepared.document, query.filter_spec)
            if candidateable_match is False:
                continue
            if candidateable_match is None and not QueryEngine.match(prepared.document, query.filter_spec, dialect=MONGODB_DIALECT_70):
                continue
        vector_hits.append((score, -order, prepared.document))
    vector_hits.sort(key=lambda item: item[:2], reverse=True)
    return [document for _score, _order, document in vector_hits[:effective_vector_limit]]


async def explain_search_documents(
    engine: _MemorySearchRuntimeEngine,
    db_name: str,
    coll_name: str,
    operator: str,
    spec: object,
    *,
    max_time_ms: int | None = None,
    context: ClientSession | None = None,
    result_limit_hint: int | None = None,
    downstream_filter_spec: dict[str, object] | None = None,
) -> QueryPlanExplanation:
    query = compile_search_stage(operator, spec)
    async with engine._get_lock(db_name, coll_name):
        indexes = deepcopy(engine._search_indexes_view(context).get(db_name, {}).get(coll_name, []))
        definition = next((item for item in indexes if item.name == query.index_name), None)
        vector_index = (
            engine._materialized_vector_index(db_name, coll_name, definition, context=context)
            if definition is not None and isinstance(query, SearchVectorQuery)
            else None
        )
    definition = next((item for item in indexes if item.name == query.index_name), None)
    if definition is None:
        raise search_index_not_found(query.index_name)
    ready = engine._search_index_is_ready(db_name, coll_name, query.index_name)
    ensure_search_index_query_supported(definition, query, ready=ready, enforce_ready=False)
    vector_filter_positions, vector_filter_description = (
        candidate_positions_for_vector_filter(
            vector_index,
            filter_spec=query.filter_spec,
            candidate_positions=vector_index.vector_row_positions.get(query.path, ()),
        )
        if isinstance(query, SearchVectorQuery) and vector_index is not None
        else (None, None)
    )
    return QueryPlanExplanation(
        engine="memory",
        strategy="search",
        plan="python-vector-search" if isinstance(query, SearchVectorQuery) else "python-search-scan",
        sort=None,
        skip=0,
        limit=None,
        hint=None,
        hinted_index=query.index_name,
        comment=None,
        max_time_ms=max_time_ms,
        details={
            "operator": operator,
            "index": query.index_name,
            "backend": "python",
            "status": "READY" if ready else "PENDING",
            "backendAvailable": True,
            "backendMaterialized": False,
            "physicalName": None,
            "readyAtEpoch": engine._search_index_ready_at.get((db_name, coll_name, query.index_name)),
            "fts5Available": None,
            "definition": build_search_index_document(
                definition,
                ready=ready,
                ready_at_epoch=engine._search_index_ready_at.get((db_name, coll_name, query.index_name)),
            ),
            **search_query_explain_details(query),
            "vector_paths": list(vector_field_paths(definition)) if definition.index_type == "vectorSearch" else None,
            "filterMode": (
                "candidate-prefilter"
                if isinstance(query, SearchVectorQuery)
                and query.filter_spec is not None
                and bool(vector_filter_description and vector_filter_description.get("exact"))
                else "candidate-prefilter+post-candidate"
                if isinstance(query, SearchVectorQuery)
                and query.filter_spec is not None
                and vector_filter_description is not None
                else "post-candidate"
                if isinstance(query, SearchVectorQuery) and query.filter_spec is not None
                else None
            ),
            "vectorFilterPrefilter": vector_filter_description if isinstance(query, SearchVectorQuery) else None,
            "vectorFilterResidual": (
                _vector_filter_residual_description(query.filter_spec, vector_filter_description)
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "documentsScanned": (
                vector_index.valid_vector_counts.get(query.path, 0)
                if isinstance(query, SearchVectorQuery) and vector_index is not None
                else None
            ),
            "documentsScannedAfterPrefilter": (
                len(vector_filter_positions)
                if isinstance(query, SearchVectorQuery) and vector_filter_positions is not None
                else vector_index.valid_vector_counts.get(query.path, 0)
                if isinstance(query, SearchVectorQuery) and vector_index is not None
                else None
            ),
            "topKLimitHint": result_limit_hint,
            "downstreamFilterPrefilter": deepcopy(downstream_filter_spec) if downstream_filter_spec is not None else None,
        },
    )
