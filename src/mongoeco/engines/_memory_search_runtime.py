from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
import math
from typing import Protocol

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.bson_ordering import bson_engine_key
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.search import (
    VECTOR_SEARCH_SCORE_FIELD,
    SearchCompoundQuery,
    SearchNearQuery,
    SearchVectorQuery,
    attach_search_highlights,
    attach_vector_search_score,
    build_search_stage_option_previews,
    build_search_index_document,
    compile_search_stage,
    is_text_search_query,
    matches_search_query,
    search_compound_ranking,
    search_clause_ranking,
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


def _document_tie_break_key(document: Document) -> str:
    return repr(bson_engine_key(document.get("_id")))


def _sort_vector_scored_documents(documents: list[Document]) -> None:
    documents.sort(
        key=lambda document: (
            -float(document.get(VECTOR_SEARCH_SCORE_FIELD, float("-inf"))),
            _document_tie_break_key(document),
        )
    )


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


def _vector_filter_mode(
    filter_spec: dict[str, object] | None,
    vector_filter_description: dict[str, object] | None,
) -> str | None:
    if filter_spec is None:
        return None
    if vector_filter_description is None:
        return "post-candidate"
    if bool(vector_filter_description.get("exact")):
        return "candidate-prefilter"
    return "candidate-prefilter+post-candidate"


def _safe_ratio(numerator: int | None, denominator: int | None) -> float | None:
    if numerator is None or denominator is None or denominator <= 0:
        return None
    return float(numerator) / float(denominator)


def _prefilter_intersection_summary(
    *,
    query_filter_applied: bool,
    downstream_filter_applied: bool,
    query_prefilter_candidate_count: int | None,
    downstream_prefilter_candidate_count: int | None,
    combined_prefilter_candidate_count: int | None,
) -> dict[str, object]:
    mode = (
        "intersection"
        if query_filter_applied and downstream_filter_applied
        else "query-only"
        if query_filter_applied
        else "downstream-only"
        if downstream_filter_applied
        else "none"
    )
    query_reduction_count = (
        max(0, query_prefilter_candidate_count - combined_prefilter_candidate_count)
        if query_filter_applied
        and query_prefilter_candidate_count is not None
        and combined_prefilter_candidate_count is not None
        else None
    )
    downstream_reduction_count = (
        max(0, downstream_prefilter_candidate_count - combined_prefilter_candidate_count)
        if downstream_filter_applied
        and downstream_prefilter_candidate_count is not None
        and combined_prefilter_candidate_count is not None
        else None
    )
    intersection_gain_count = (
        max(
            0,
            min(query_prefilter_candidate_count, downstream_prefilter_candidate_count)
            - combined_prefilter_candidate_count,
        )
        if query_filter_applied
        and downstream_filter_applied
        and query_prefilter_candidate_count is not None
        and downstream_prefilter_candidate_count is not None
        and combined_prefilter_candidate_count is not None
        else None
    )
    return {
        "mode": mode,
        "queryPrefilterCandidateCount": query_prefilter_candidate_count,
        "downstreamPrefilterCandidateCount": downstream_prefilter_candidate_count,
        "combinedPrefilterCandidateCount": combined_prefilter_candidate_count,
        "queryReductionCount": query_reduction_count,
        "downstreamReductionCount": downstream_reduction_count,
        "queryReductionRatio": _safe_ratio(
            query_reduction_count,
            query_prefilter_candidate_count,
        ),
        "downstreamReductionRatio": _safe_ratio(
            downstream_reduction_count,
            downstream_prefilter_candidate_count,
        ),
        "intersectionGainCount": intersection_gain_count,
        "intersectionGainRatio": _safe_ratio(
            intersection_gain_count,
            min(query_prefilter_candidate_count, downstream_prefilter_candidate_count)
            if query_prefilter_candidate_count is not None
            and downstream_prefilter_candidate_count is not None
            else None,
        ),
    }


def _vector_pruning_summary(
    *,
    documents_scanned: int | None,
    prefilter_candidate_count: int | None,
    candidates_evaluated: int | None,
    post_candidate_filtered_count: int | None,
    min_score_filtered_count: int | None,
) -> dict[str, object] | None:
    if (
        documents_scanned is None
        and prefilter_candidate_count is None
        and candidates_evaluated is None
    ):
        return None
    prefilter_pruned_count = (
        max(0, documents_scanned - prefilter_candidate_count)
        if documents_scanned is not None and prefilter_candidate_count is not None
        else None
    )
    candidates_skipped_before_evaluation = (
        max(0, prefilter_candidate_count - candidates_evaluated)
        if prefilter_candidate_count is not None and candidates_evaluated is not None
        else None
    )
    return {
        "documentsScanned": documents_scanned,
        "prefilterCandidateCount": prefilter_candidate_count,
        "prefilterPrunedCount": prefilter_pruned_count,
        "prefilterPrunedRatio": _safe_ratio(prefilter_pruned_count, documents_scanned),
        "candidatesEvaluated": candidates_evaluated,
        "candidatesSkippedBeforeEvaluation": candidates_skipped_before_evaluation,
        "candidateEvaluationRatio": _safe_ratio(candidates_evaluated, prefilter_candidate_count),
        "postCandidateFilteredCount": post_candidate_filtered_count,
        "postCandidateFilteredRatio": _safe_ratio(post_candidate_filtered_count, candidates_evaluated),
        "minScoreFilteredCount": min_score_filtered_count,
        "minScoreFilteredRatio": _safe_ratio(min_score_filtered_count, candidates_evaluated),
    }


def _prefilter_source(
    *,
    source: str,
    filter_spec: dict[str, object] | None,
    filter_description: dict[str, object] | None,
) -> dict[str, object] | None:
    if filter_spec is None:
        return None
    return {
        "source": source,
        "candidateable": bool(filter_description and filter_description.get("candidateable")),
        "exact": bool(filter_description and filter_description.get("exact")),
        "supportedPaths": (
            list(filter_description.get("supportedPaths", []))
            if filter_description is not None
            else []
        ),
        "supportedOperators": (
            list(filter_description.get("supportedOperators", []))
            if filter_description is not None
            else []
        ),
    }


def _memory_vector_backend_document(
    *,
    query: SearchVectorQuery,
    vector_index,
) -> dict[str, object]:
    field_spec = vector_index.vector_specs.get(query.path, {})
    return {
        "backend": "memory-exact-baseline",
        "physicalName": None,
        "path": query.path,
        "similarity": str(field_spec.get("similarity", query.similarity)),
        "numDimensions": int(field_spec.get("numDimensions", len(query.query_vector))),
        "documentsScanned": vector_index.valid_vector_counts.get(query.path, 0),
        "validVectors": vector_index.valid_vector_counts.get(query.path, 0),
        "invalidVectors": max(
            0,
            len(vector_index.documents) - vector_index.valid_vector_counts.get(query.path, 0),
        ),
        "exactBaseline": True,
    }


def _matches_vector_postfilter(
    document: Document,
    *,
    filter_spec: dict[str, object] | None,
) -> bool:
    if filter_spec is None:
        return True
    candidateable_match = matches_candidateable_filter(document, filter_spec)
    if candidateable_match is False:
        return False
    if candidateable_match is None:
        return bool(QueryEngine.match(document, filter_spec, dialect=MONGODB_DIALECT_70))
    return True


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
            distance_hits: list[tuple[float, str, Document]] = []
            for document, materialized in materialized_documents:
                if downstream_filter_spec is not None and not QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70):
                    continue
                if not matches_search_query(document, definition=definition, query=query, materialized=materialized):
                    continue
                distance = search_near_distance(document, query=query)
                if distance is None:
                    continue
                distance_hits.append((distance, _document_tie_break_key(document), document))
            distance_hits.sort(key=lambda item: item[:2])
            if effective_limit is not None:
                distance_hits = distance_hits[:effective_limit]
            results = [document for _distance, _tie_break, document in distance_hits]
            return [
                attach_search_highlights(document, definition=definition, query=query)
                for document in results
            ]
        if isinstance(query, SearchCompoundQuery) and query.should:
            ranked_documents: list[tuple[tuple[int, float, float], str, Document]] = []
            for document, materialized in materialized_documents:
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
                ranked_documents.append((rank, _document_tie_break_key(document), document))
            ranked_documents.sort(
                key=lambda item: (
                    -item[0][0],
                    -item[0][1],
                    -item[0][2],
                    item[1],
                )
            )
            if effective_limit is not None:
                ranked_documents = ranked_documents[:effective_limit]
            results = [document for _score, _tie_break, document in ranked_documents]
            return [
                attach_search_highlights(document, definition=definition, query=query)
                for document in results
            ]
        ranked_matches: list[tuple[float, str, Document]] = []
        for document, materialized in materialized_documents:
            if downstream_filter_spec is not None:
                candidateable_match = matches_candidateable_filter(document, downstream_filter_spec)
                if candidateable_match is False:
                    continue
                if candidateable_match is None and not QueryEngine.match(document, downstream_filter_spec, dialect=MONGODB_DIALECT_70):
                    continue
            matched, clause_score, _near_distance = search_clause_ranking(
                document,
                definition=definition,
                query=query,
                materialized=materialized,
            )
            if not matched:
                continue
            ranked_matches.append((-clause_score, _document_tie_break_key(document), document))
        ranked_matches.sort(key=lambda item: item[:2])
        if effective_limit is not None:
            ranked_matches = ranked_matches[:effective_limit]
        return [
            attach_search_highlights(document, definition=definition, query=query)
            for _score, _tie_break, document in ranked_matches
        ]

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
        limit=None,
    )
    query_requires_postfilter = query.filter_spec is not None and query_filter_rows is None
    downstream_requires_postfilter = downstream_filter_spec is not None and downstream_filter_rows is None
    if not query_requires_postfilter and not downstream_requires_postfilter:
        vector_documents = [
            attach_vector_search_score(
                vector_index.documents[path_positions[row_index]].document,
                vector_score,
            )
            for vector_score, row_index in scored_rows
        ]
        _sort_vector_scored_documents(vector_documents)
        return vector_documents[:effective_vector_limit]
    vector_hits: list[Document] = []
    for score, row_index in scored_rows:
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
        vector_hits.append(attach_vector_search_score(prepared.document, score))
    _sort_vector_scored_documents(vector_hits)
    return vector_hits[:effective_vector_limit]


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
    resolved_similarity = (
        str(vector_index.vector_specs.get(query.path, {}).get("similarity", query.similarity))
        if isinstance(query, SearchVectorQuery) and vector_index is not None
        else query.similarity
        if isinstance(query, SearchVectorQuery)
        else None
    )
    vector_backend = (
        _memory_vector_backend_document(query=query, vector_index=vector_index)
        if isinstance(query, SearchVectorQuery) and vector_index is not None
        else None
    )
    vector_candidates_requested: int | None = None
    vector_candidates_evaluated: int | None = None
    vector_prefilter_candidate_count: int | None = None
    vector_documents_matched_before_limit: int | None = None
    vector_documents_filtered: int | None = None
    vector_documents_filtered_by_min_score: int | None = None
    vector_mode: str | None = None
    vector_filter_mode: str | None = None
    downstream_vector_filter_mode: str | None = None
    query_prefilter_candidate_count: int | None = None
    downstream_prefilter_candidate_count: int | None = None
    downstream_filter_description: dict[str, object] | None = None
    if isinstance(query, SearchVectorQuery) and vector_index is not None:
        vector_mode = "exact"
        path_positions = vector_index.vector_row_positions.get(query.path, ())
        query_filter_rows, vector_filter_description = candidate_rows_for_vector_filter(
            vector_index,
            query_path=query.path,
            filter_spec=query.filter_spec,
        )
        downstream_filter_rows, downstream_filter_description = candidate_rows_for_vector_filter(
            vector_index,
            query_path=query.path,
            filter_spec=downstream_filter_spec,
        )
        query_prefilter_candidate_count = (
            len(query_filter_rows)
            if query_filter_rows is not None
            else len(path_positions)
        )
        downstream_prefilter_candidate_count = (
            len(downstream_filter_rows)
            if downstream_filter_rows is not None
            else len(path_positions)
        )
        candidate_rows = list(
            query_filter_rows if query_filter_rows is not None else range(len(path_positions))
        )
        if downstream_filter_rows is not None:
            allowed = set(downstream_filter_rows)
            candidate_rows = [
                row_index for row_index in candidate_rows if row_index in allowed
            ]
        vector_prefilter_candidate_count = len(candidate_rows)
        vector_candidates_requested = min(query.num_candidates, len(candidate_rows))
        raw_scored_rows = vector_scores_for_rows(
            vector_index,
            query=replace(query, min_score=None),
            candidate_rows=candidate_rows,
            limit=None,
        )
        scored_rows = vector_scores_for_rows(
            vector_index,
            query=query,
            candidate_rows=candidate_rows,
            limit=None,
        )
        vector_candidates_evaluated = len(raw_scored_rows)
        if query.min_score is not None:
            vector_documents_filtered_by_min_score = max(
                0,
                len(raw_scored_rows) - len(scored_rows),
            )
        query_requires_postfilter = query.filter_spec is not None and query_filter_rows is None
        downstream_requires_postfilter = (
            downstream_filter_spec is not None and downstream_filter_rows is None
        )
        vector_filter_mode = _vector_filter_mode(
            query.filter_spec,
            vector_filter_description,
        )
        downstream_vector_filter_mode = _vector_filter_mode(
            downstream_filter_spec,
            downstream_filter_description,
        )
        passing_hits = 0
        rejected_hits = 0
        for _score, row_index in scored_rows:
            document = vector_index.documents[path_positions[row_index]].document
            if downstream_requires_postfilter and not _matches_vector_postfilter(
                document,
                filter_spec=downstream_filter_spec,
            ):
                rejected_hits += 1
                continue
            if query_requires_postfilter and not _matches_vector_postfilter(
                document,
                filter_spec=query.filter_spec,
            ):
                rejected_hits += 1
                continue
            passing_hits += 1
        vector_documents_matched_before_limit = passing_hits
        vector_documents_filtered = rejected_hits
    vector_score_breakdown = (
        {
            "similarity": resolved_similarity,
            "scoreField": "vectorSearchScore",
            "scoreDirection": "higher-is-better",
            "scoreFormula": (
                "dot(query, candidate) / (||query|| * ||candidate||)"
                if resolved_similarity == "cosine"
                else "sum(query[i] * candidate[i])"
                if resolved_similarity == "dotProduct"
                else "-sqrt(sum((query[i] - candidate[i])^2))"
            ),
            "minScore": query.min_score,
            "documentsFilteredByMinScore": vector_documents_filtered_by_min_score,
            "backend": "memory-exact-baseline",
        }
        if isinstance(query, SearchVectorQuery)
        else None
    )
    vector_candidate_plan = (
        {
            "mode": vector_mode,
            "requestedCandidates": vector_candidates_requested,
            "evaluatedCandidates": vector_candidates_evaluated,
            "prefilterCandidateCount": vector_prefilter_candidate_count,
            "documentsMatchedBeforeLimit": vector_documents_matched_before_limit,
            "documentsScanned": (
                vector_index.valid_vector_counts.get(query.path, 0)
                if vector_index is not None
                else None
            ),
            "documentsScannedAfterPrefilter": vector_prefilter_candidate_count,
            "topKLimitHint": result_limit_hint,
            "candidateExpansionStrategy": "exact-baseline",
            "queryPrefilterCandidateCount": query_prefilter_candidate_count,
            "downstreamPrefilterCandidateCount": downstream_prefilter_candidate_count,
            "combinedPrefilterCandidateCount": vector_prefilter_candidate_count,
            "prefilterRetentionRatio": _safe_ratio(
                vector_prefilter_candidate_count,
                (
                    vector_index.valid_vector_counts.get(query.path, 0)
                    if vector_index is not None
                    else None
                ),
            ),
            "evaluationRetentionRatio": _safe_ratio(
                vector_candidates_evaluated,
                vector_prefilter_candidate_count,
            ),
            "matchedBeforeLimitRatio": _safe_ratio(
                vector_documents_matched_before_limit,
                vector_candidates_evaluated,
            ),
            "prefilterIntersection": _prefilter_intersection_summary(
                query_filter_applied=query.filter_spec is not None,
                downstream_filter_applied=downstream_filter_spec is not None,
                query_prefilter_candidate_count=query_prefilter_candidate_count,
                downstream_prefilter_candidate_count=downstream_prefilter_candidate_count,
                combined_prefilter_candidate_count=vector_prefilter_candidate_count,
            ),
            "prefilterSources": [
                source
                for source in (
                    _prefilter_source(
                        source="queryFilter",
                        filter_spec=query.filter_spec,
                        filter_description=vector_filter_description,
                    ),
                    _prefilter_source(
                        source="downstreamFilter",
                        filter_spec=downstream_filter_spec,
                        filter_description=downstream_filter_description,
                    ),
                )
                if source is not None
            ],
        }
        if isinstance(query, SearchVectorQuery)
        else None
    )
    vector_hybrid_retrieval = (
        {
            "filterMode": vector_filter_mode,
            "queryFilterMode": vector_filter_mode,
            "downstreamFilterMode": downstream_vector_filter_mode,
            "queryFilter": deepcopy(query.filter_spec) if query.filter_spec is not None else None,
            "downstreamFilter": (
                deepcopy(downstream_filter_spec)
                if downstream_filter_spec is not None
                else None
            ),
            "prefilter": deepcopy(vector_filter_description),
            "residual": _vector_filter_residual_description(
                query.filter_spec,
                vector_filter_description,
            ),
            "queryFilterPrefilter": deepcopy(vector_filter_description),
            "queryFilterResidual": _vector_filter_residual_description(
                query.filter_spec,
                vector_filter_description,
            ),
            "downstreamFilterPrefilter": deepcopy(downstream_filter_description),
            "downstreamFilterResidual": _vector_filter_residual_description(
                downstream_filter_spec,
                downstream_filter_description,
            ),
            "queryPrefilterCandidateCount": query_prefilter_candidate_count,
            "downstreamPrefilterCandidateCount": downstream_prefilter_candidate_count,
            "combinedPrefilterCandidateCount": vector_prefilter_candidate_count,
            "documentsFilteredPostCandidate": vector_documents_filtered,
        }
        if isinstance(query, SearchVectorQuery)
        else None
    )
    vector_pruning_summary = (
        _vector_pruning_summary(
            documents_scanned=(
                vector_index.valid_vector_counts.get(query.path, 0)
                if vector_index is not None
                else None
            ),
            prefilter_candidate_count=vector_prefilter_candidate_count,
            candidates_evaluated=vector_candidates_evaluated,
            post_candidate_filtered_count=vector_documents_filtered,
            min_score_filtered_count=vector_documents_filtered_by_min_score,
        )
        if isinstance(query, SearchVectorQuery)
        else None
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
            **search_query_explain_details(query, definition=definition),
            "similarity": resolved_similarity,
            "vector_paths": list(vector_field_paths(definition)) if definition.index_type == "vectorSearch" else None,
            "mode": vector_mode,
            "filterMode": vector_filter_mode,
            "downstreamFilterMode": downstream_vector_filter_mode,
            "vectorFilterPrefilter": vector_filter_description if isinstance(query, SearchVectorQuery) else None,
            "vectorFilterResidual": (
                _vector_filter_residual_description(query.filter_spec, vector_filter_description)
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "downstreamFilterCandidatePrefilter": (
                deepcopy(downstream_filter_description)
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "downstreamFilterResidual": (
                _vector_filter_residual_description(downstream_filter_spec, downstream_filter_description)
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "candidatesRequested": vector_candidates_requested,
            "candidatesEvaluated": vector_candidates_evaluated,
            "prefilterCandidateCount": vector_prefilter_candidate_count,
            "queryPrefilterCandidateCount": query_prefilter_candidate_count,
            "downstreamPrefilterCandidateCount": downstream_prefilter_candidate_count,
            "documentsMatchedBeforeLimit": vector_documents_matched_before_limit,
            "documentsFiltered": vector_documents_filtered,
            "documentsFilteredByMinScore": vector_documents_filtered_by_min_score,
            "documentsScanned": (
                vector_index.valid_vector_counts.get(query.path, 0)
                if isinstance(query, SearchVectorQuery) and vector_index is not None
                else None
            ),
            "documentsScannedAfterPrefilter": (
                vector_prefilter_candidate_count
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "candidateExpansionStrategy": (
                "exact-baseline"
                if isinstance(query, SearchVectorQuery)
                else None
            ),
            "vectorBackend": vector_backend,
            "scoreBreakdown": vector_score_breakdown,
            "candidatePlan": vector_candidate_plan,
            "hybridRetrieval": vector_hybrid_retrieval,
            "pruningSummary": vector_pruning_summary,
            "topKLimitHint": result_limit_hint,
            "downstreamFilterPrefilter": deepcopy(downstream_filter_spec) if downstream_filter_spec is not None else None,
            **(
                build_search_stage_option_previews(
                    await execute_search_documents(
                        engine,
                        db_name,
                        coll_name,
                        operator,
                        spec,
                        context=context,
                        result_limit_hint=None,
                        downstream_filter_spec=downstream_filter_spec,
                    ),
                    definition=definition,
                    query=query,
                )
                if is_text_search_query(query)
                and any(
                    value is not None
                    for value in (
                        getattr(query, "stage_options", None).count if hasattr(query, "stage_options") else None,
                        getattr(query, "stage_options", None).highlight if hasattr(query, "stage_options") else None,
                        getattr(query, "stage_options", None).facet if hasattr(query, "stage_options") else None,
                    )
                )
                else {}
            ),
        },
    )
