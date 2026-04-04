from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from dataclasses import dataclass
import sqlite3
from typing import Protocol

from mongoeco.core.search import (
    SearchAutocompleteQuery,
    SearchCompoundQuery,
    SearchEqualsQuery,
    SearchExistsQuery,
    SearchInQuery,
    SearchNearQuery,
    SearchPhraseQuery,
    SearchQuery,
    SearchRegexQuery,
    SearchRangeQuery,
    SearchTextQuery,
    SearchWildcardQuery,
    materialize_search_document,
    matches_search_query,
    search_query_operator_name,
)
from mongoeco.core.search_filter_prefilter import flatten_candidate_filter_clauses
from mongoeco.types import Document, SearchIndexDefinition


class _SQLiteCompoundEngine(Protocol):
    def _sqlite_table_exists(self, conn: sqlite3.Connection, table_name: str) -> bool: ...
    def _quote_identifier(self, identifier: str) -> str: ...


CandidateResolver = Callable[[SearchQuery], tuple[list[str] | None, str | None, bool]]


@dataclass(frozen=True, slots=True)
class DownstreamFilterRefinement:
    applied: bool
    path: str
    value_count: int
    backend: str
    exact: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "applied": self.applied,
            "path": self.path,
            "valueCount": self.value_count,
            "backend": self.backend,
            "exact": self.exact,
        }


@dataclass(frozen=True, slots=True)
class CompoundClausePlan:
    operator: str
    candidateable: bool
    exact: bool
    backend: str | None
    downstream_refinement: DownstreamFilterRefinement | None = None

    @property
    def clause_class(self) -> str:
        if not self.candidateable:
            return "post-match-only"
        if self.exact:
            return "candidateable-exact"
        return "candidateable-ranking"

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "operator": self.operator,
            "candidateable": self.candidateable,
            "exact": self.exact if self.candidateable else False,
            "backend": self.backend,
        }
        if self.downstream_refinement is not None:
            payload["downstreamRefinement"] = self.downstream_refinement.to_dict()
        return payload


@dataclass(frozen=True, slots=True)
class CompoundPartialRankingPlan:
    supported: bool
    strategy: str | None

    def to_dict(self) -> dict[str, object]:
        return {
            "supported": self.supported,
            "strategy": self.strategy,
        }


@dataclass(frozen=True, slots=True)
class CompoundCandidatePlan:
    candidates: tuple[str, ...] | None
    backend: str | None
    exact: bool
    should_candidates: tuple[tuple[str, ...], ...]
    non_candidateable_should: int

    def to_legacy_tuple(self) -> tuple[list[str] | None, str | None, bool, list[list[str]], int]:
        return (
            list(self.candidates) if self.candidates is not None else None,
            self.backend,
            self.exact,
            [list(values) for values in self.should_candidates],
            self.non_candidateable_should,
        )


def sqlite_compound_candidate_plan(
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
    *,
    candidate_resolver: CandidateResolver,
    must_not_resolver: CandidateResolver | None = None,
    downstream_filter_storage_keys: list[str] | None = None,
    downstream_filter_spec: dict[str, object] | None = None,
    downstream_filter_exact: bool = False,
) -> CompoundCandidatePlan:
    candidate_intersections: list[list[str]] = []
    should_candidates: list[list[str]] = []
    non_candidateable_should = 0
    exact = True

    for clause in (*query.must, *query.filter):
        storage_keys, _backend, clause_exact, _refinement = compound_clause_candidate_state(
            definition=definition,
            clause=clause,
            candidate_resolver=candidate_resolver,
            downstream_filter_storage_keys=downstream_filter_storage_keys,
            downstream_filter_spec=downstream_filter_spec,
            downstream_filter_exact=downstream_filter_exact,
        )
        if storage_keys is not None:
            candidate_intersections.append(storage_keys)
            exact = exact and clause_exact
        else:
            exact = False

    for clause in query.should:
        storage_keys, _backend, clause_exact, _refinement = compound_clause_candidate_state(
            definition=definition,
            clause=clause,
            candidate_resolver=candidate_resolver,
            downstream_filter_storage_keys=downstream_filter_storage_keys,
            downstream_filter_spec=downstream_filter_spec,
            downstream_filter_exact=downstream_filter_exact,
        )
        if storage_keys is None:
            non_candidateable_should += 1
            exact = False
            continue
        should_candidates.append(storage_keys)
        exact = exact and clause_exact

    candidates = _intersect_storage_key_lists(candidate_intersections) if candidate_intersections else None
    if should_candidates:
        required_candidateable_should = max(0, query.minimum_should_match - non_candidateable_should)
        if required_candidateable_should > 0:
            merged_should = _storage_keys_with_minimum_frequency(
                should_candidates,
                required_candidateable_should,
            )
            if candidates is None:
                candidates = merged_should
            else:
                merged_should_set = set(merged_should)
                candidates = [storage_key for storage_key in candidates if storage_key in merged_should_set]
    elif candidates is None:
        return CompoundCandidatePlan(
            candidates=None,
            backend=None,
            exact=False,
            should_candidates=tuple(tuple(values) for values in should_candidates),
            non_candidateable_should=non_candidateable_should,
        )

    if candidates is None:
        return CompoundCandidatePlan(
            candidates=None,
            backend=None,
            exact=False,
            should_candidates=tuple(tuple(values) for values in should_candidates),
            non_candidateable_should=non_candidateable_should,
        )

    if downstream_filter_storage_keys is not None:
        downstream_set = set(downstream_filter_storage_keys)
        candidates = [storage_key for storage_key in candidates if storage_key in downstream_set]
        exact = exact and downstream_filter_exact

    if query.must_not:
        excluded: set[str] = set()
        exclusion_resolver = must_not_resolver or candidate_resolver
        for clause in query.must_not:
            storage_keys, _backend, clause_exact = exclusion_resolver(clause)
            if storage_keys is None:
                exact = False
                continue
            if clause_exact:
                excluded.update(storage_keys)
            else:
                exact = False
        if excluded:
            candidates = [storage_key for storage_key in candidates if storage_key not in excluded]

    return CompoundCandidatePlan(
        candidates=tuple(candidates),
        backend="fts5-prefilter",
        exact=exact,
        should_candidates=tuple(tuple(values) for values in should_candidates),
        non_candidateable_should=non_candidateable_should,
    )


def compound_clause_candidate_state(
    *,
    definition: SearchIndexDefinition,
    clause: SearchQuery,
    candidate_resolver: CandidateResolver,
    downstream_filter_storage_keys: list[str] | None,
    downstream_filter_spec: dict[str, object] | None,
    downstream_filter_exact: bool,
) -> tuple[list[str] | None, str | None, bool, DownstreamFilterRefinement | None]:
    storage_keys, backend, clause_exact = candidate_resolver(clause)
    implied_path, implied_values = downstream_filter_implies_clause(
        clause,
        definition=definition,
        filter_spec=downstream_filter_spec,
    )
    if implied_path is None or downstream_filter_storage_keys is None:
        return storage_keys, backend, clause_exact, None
    refined_keys = downstream_filter_storage_keys
    if storage_keys is not None:
        refined_set = set(refined_keys)
        refined_keys = [storage_key for storage_key in storage_keys if storage_key in refined_set]
    refined_backend = "downstream-filter" if backend is None else f"{backend}+downstream-filter"
    refinement = DownstreamFilterRefinement(
        applied=True,
        path=implied_path,
        value_count=len(implied_values or ()),
        backend=refined_backend,
        exact=downstream_filter_exact and (clause_exact if storage_keys is not None else True),
    )
    return refined_keys, refined_backend, refinement.exact, refinement


def describe_compound_prefilter(
    definition: SearchIndexDefinition,
    query: SearchCompoundQuery,
    *,
    candidate_resolver: CandidateResolver,
    physical_name: str | None,
    downstream_filter: dict[str, object] | None = None,
    downstream_filter_storage_keys: list[str] | None = None,
) -> dict[str, object]:
    downstream_filter_spec = (
        downstream_filter.get("spec")
        if isinstance(downstream_filter, dict) and isinstance(downstream_filter.get("spec"), dict)
        else None
    )
    downstream_filter_exact = bool(isinstance(downstream_filter, dict) and downstream_filter.get("exact"))

    def _describe_clause(clause: SearchQuery) -> CompoundClausePlan:
        storage_keys, backend, exact, refinement = compound_clause_candidate_state(
            definition=definition,
            clause=clause,
            candidate_resolver=candidate_resolver,
            downstream_filter_storage_keys=downstream_filter_storage_keys,
            downstream_filter_spec=downstream_filter_spec,
            downstream_filter_exact=downstream_filter_exact,
        )
        return CompoundClausePlan(
            operator=search_query_operator_name(clause),
            candidateable=storage_keys is not None,
            exact=exact if storage_keys is not None else False,
            backend=backend,
            downstream_refinement=refinement,
        )

    must_plans = [_describe_clause(clause) for clause in query.must]
    filter_plans = [_describe_clause(clause) for clause in query.filter]
    should_plans = [_describe_clause(clause) for clause in query.should]
    must_not_plans = [_describe_clause(clause) for clause in query.must_not]
    non_candidateable_should = sum(1 for clause in should_plans if not clause.candidateable)
    partial_ranking = CompoundPartialRankingPlan(
        supported=compound_entry_ranking_supported(query, physical_name=physical_name),
        strategy="fts-materialized-entries" if compound_entry_ranking_supported(query, physical_name=physical_name) else None,
    )
    return {
        "must": [plan.to_dict() for plan in must_plans],
        "filter": [plan.to_dict() for plan in filter_plans],
        "should": [plan.to_dict() for plan in should_plans],
        "mustNot": [plan.to_dict() for plan in must_not_plans],
        "clauseClasses": {
            "must": [plan.clause_class for plan in must_plans],
            "filter": [plan.clause_class for plan in filter_plans],
            "should": [plan.clause_class for plan in should_plans],
            "mustNot": [plan.clause_class for plan in must_not_plans],
        },
        "downstreamFilter": deepcopy(downstream_filter) if downstream_filter is not None else None,
        "requiredCandidateableShould": max(0, query.minimum_should_match - non_candidateable_should),
        "candidateableShouldCount": sum(1 for clause in should_plans if clause.candidateable),
        "candidateableShouldOperators": [plan.operator for plan in should_plans if plan.candidateable],
        "partialRanking": partial_ranking.to_dict(),
    }


def textual_search_field_types(definition: SearchIndexDefinition) -> dict[str, str]:
    result: dict[str, str] = {}
    mappings = definition.definition.get("mappings")
    if not isinstance(mappings, dict):
        return result

    def _walk(node: dict[str, object], prefix: str = "") -> None:
        fields = node.get("fields")
        if not isinstance(fields, dict):
            return
        for field_name, field_spec in fields.items():
            if not isinstance(field_name, str) or not isinstance(field_spec, dict):
                continue
            path = f"{prefix}.{field_name}" if prefix else field_name
            mapping_type = field_spec.get("type", "document")
            if mapping_type == "document":
                _walk(field_spec, path)
                continue
            if isinstance(mapping_type, str):
                result[path] = mapping_type

    _walk(mappings)
    return result


def downstream_filter_implies_clause(
    clause: SearchQuery,
    *,
    definition: SearchIndexDefinition,
    filter_spec: dict[str, object] | None,
) -> tuple[str | None, list[object] | None]:
    if filter_spec is None:
        return None, None
    clauses = flatten_candidate_filter_clauses(filter_spec)
    search_paths = clause_search_paths(clause)
    if clauses is None or search_paths is None:
        return None, None
    for path, raw_value in clauses:
        if path not in search_paths:
            continue
        values: list[object]
        if raw_value == {"$exists": True}:
            if isinstance(clause, SearchExistsQuery):
                return path, [object()]
            continue
        if isinstance(raw_value, str):
            values = [raw_value]
        elif isinstance(raw_value, dict) and set(raw_value) == {"$in"} and isinstance(raw_value["$in"], list):
            if not raw_value["$in"] or not all(isinstance(item, str) for item in raw_value["$in"]):
                continue
            values = list(raw_value["$in"])
        else:
            continue
        if all(
            matches_search_query(
                document_for_search_path(path, value),
                definition=definition,
                query=clause,
                materialized=materialize_search_document(document_for_search_path(path, value), definition),
            )
            for value in values
        ):
            return path, values
    return None, None


def clause_search_paths(clause: SearchQuery) -> tuple[str, ...] | None:
    if isinstance(
        clause,
        (
            SearchTextQuery,
            SearchPhraseQuery,
            SearchAutocompleteQuery,
            SearchWildcardQuery,
            SearchRegexQuery,
            SearchExistsQuery,
            SearchInQuery,
            SearchEqualsQuery,
            SearchRangeQuery,
        ),
    ):
        return (clause.path,) if isinstance(clause, (SearchInQuery, SearchEqualsQuery, SearchRangeQuery)) else clause.paths
    return None


def document_for_search_path(path: str, value: object) -> Document:
    parts = [part for part in path.split(".") if part]
    document: dict[str, object] = {}
    current = document
    for part in parts[:-1]:
        nested: dict[str, object] = {}
        current[part] = nested
        current = nested
    if parts:
        current[parts[-1]] = value
    return document


def compound_entry_ranking_supported(
    query: SearchCompoundQuery,
    *,
    physical_name: str | None,
) -> bool:
    return bool(
        physical_name
        and query.should
        and not any(
            isinstance(clause, (SearchInQuery, SearchEqualsQuery, SearchRangeQuery, SearchNearQuery, SearchCompoundQuery))
            for clause in query.should
        )
    )


def _intersect_storage_key_lists(lists: list[list[str]]) -> list[str]:
    if not lists:
        return []
    allowed = set(lists[0])
    for values in lists[1:]:
        allowed &= set(values)
    return [storage_key for storage_key in lists[0] if storage_key in allowed]


def _storage_keys_with_minimum_frequency(
    lists: list[list[str]],
    minimum: int,
) -> list[str]:
    if minimum <= 1:
        return _union_storage_key_lists(lists)
    counts: dict[str, int] = {}
    order: dict[str, int] = {}
    for values in lists:
        for storage_key in values:
            counts[storage_key] = counts.get(storage_key, 0) + 1
            order.setdefault(storage_key, len(order))
    return [
        storage_key
        for storage_key, count in sorted(counts.items(), key=lambda item: order[item[0]])
        if count >= minimum
    ]


def _union_storage_key_lists(lists: list[list[str]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for values in lists:
        for storage_key in values:
            if storage_key in seen:
                continue
            seen.add(storage_key)
            ordered.append(storage_key)
    return ordered
