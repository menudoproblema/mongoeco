from __future__ import annotations

from collections import Counter
import heapq
import sqlite3
from dataclasses import dataclass
from typing import Protocol

from mongoeco.core.search import (
    MaterializedSearchDocument,
    SearchCompoundQuery,
    SearchNearQuery,
    SearchQuery,
    search_clause_ranking,
    search_compound_ranking,
    search_near_distance,
    tokenize_classic_text,
)
from mongoeco.engines._sqlite_compound_prefilter import compound_entry_ranking_supported
from mongoeco.types import Document, SearchIndexDefinition


_SEARCH_RUNTIME_DUMMY_DEFINITION = SearchIndexDefinition(
    {"mappings": {"dynamic": True}},
    name="local",
    index_type="search",
)


class _SQLiteCompoundRankingEngine(Protocol):
    _materialized_search_entry_cache: dict[
        tuple[str, str, str, int],
        dict[str, tuple[tuple[tuple[str, str], ...], MaterializedSearchDocument]],
    ]

    def _quote_identifier(self, identifier: str) -> str: ...
    def _search_backend_version(self, db_name: str, coll_name: str) -> int: ...


@dataclass(frozen=True, slots=True)
class CompoundTopKPrefilter:
    applied: bool
    before_count: int
    after_count: int
    strategy: str
    cutoff_matched_should: int | None = None
    cutoff_should_score: float | None = None
    cutoff_tier: dict[str, object] | None = None
    candidate_count_before_partial_ranking: int | None = None
    candidate_count_after_partial_ranking: int | None = None
    partial_ranking: dict[str, object] | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "applied": self.applied,
            "beforeCount": self.before_count,
            "afterCount": self.after_count,
            "strategy": self.strategy,
            "cutoffMatchedShould": self.cutoff_matched_should,
        }
        if self.cutoff_should_score is not None:
            payload["cutoffShouldScore"] = self.cutoff_should_score
        if self.cutoff_tier is not None:
            payload["cutoffTier"] = self.cutoff_tier
        if self.candidate_count_before_partial_ranking is not None:
            payload["candidateCountBeforePartialRanking"] = self.candidate_count_before_partial_ranking
        if self.candidate_count_after_partial_ranking is not None:
            payload["candidateCountAfterPartialRanking"] = self.candidate_count_after_partial_ranking
        if self.partial_ranking is not None:
            payload["partialRanking"] = self.partial_ranking
        return payload


def prune_candidate_storage_keys_for_topk(
    engine: _SQLiteCompoundRankingEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str | None,
    query: SearchQuery,
    candidate_storage_keys: list[str] | None,
    candidate_exact: bool,
    result_limit_hint: int | None,
    should_candidates: list[list[str]] | None = None,
    candidate_resolver,
) -> tuple[list[str] | None, dict[str, object] | None]:
    if (
        candidate_storage_keys is None
        or result_limit_hint is None
        or result_limit_hint <= 0
        or len(candidate_storage_keys) <= result_limit_hint
    ):
        return candidate_storage_keys, None
    before_count = len(candidate_storage_keys)
    if not candidate_exact:
        return candidate_storage_keys, None
    if not isinstance(query, SearchCompoundQuery) or not query.should:
        return (
            candidate_storage_keys[:result_limit_hint],
            CompoundTopKPrefilter(
                applied=True,
                before_count=before_count,
                after_count=min(before_count, result_limit_hint),
                strategy="stable-prefix",
            ).to_dict(),
        )

    candidate_set = set(candidate_storage_keys)
    resolved_should_candidates = list(should_candidates or [])
    if not resolved_should_candidates:
        for clause in query.should:
            storage_keys, _backend, clause_exact = candidate_resolver(clause)
            if storage_keys is None or not clause_exact:
                return candidate_storage_keys, None
            resolved_should_candidates.append(storage_keys)

    coarse_prefiltered_keys, matched_should_cutoff = prefilter_candidate_storage_keys_by_matched_should(
        candidate_storage_keys,
        resolved_should_candidates,
        result_limit_hint=result_limit_hint,
    )
    exact_score_input_keys = coarse_prefiltered_keys or candidate_storage_keys
    exact_should_scores = exact_candidateable_should_scores(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        query=query,
        candidate_storage_keys=exact_score_input_keys,
    )
    if exact_should_scores is not None:
        selected: set[str] = set()
        selected_count = 0
        cutoff: tuple[int, float] | None = None
        exact_tiers = sorted(
            {
                (
                    int(score["matchedShould"]),
                    round(float(score["shouldScore"]), 12),
                )
                for score in exact_should_scores.values()
            },
            reverse=True,
        )
        for tier_key in exact_tiers:
            tier = [
                storage_key
                for storage_key in exact_score_input_keys
                if (
                    int(exact_should_scores[storage_key]["matchedShould"]),
                    round(float(exact_should_scores[storage_key]["shouldScore"]), 12),
                ) == tier_key
            ]
            if not tier:
                continue
            selected.update(tier)
            selected_count += len(tier)
            cutoff = tier_key
            if selected_count >= result_limit_hint:
                break
        pruned = [storage_key for storage_key in exact_score_input_keys if storage_key in selected]
        if len(pruned) < before_count:
            return (
                pruned,
                CompoundTopKPrefilter(
                    applied=True,
                    before_count=before_count,
                    after_count=len(pruned),
                    strategy="exact-should-score-tier",
                    cutoff_matched_should=cutoff[0] if cutoff is not None else None,
                    cutoff_should_score=cutoff[1] if cutoff is not None else None,
                    cutoff_tier={
                        "matchedShould": cutoff[0] if cutoff is not None else None,
                        "shouldScore": cutoff[1] if cutoff is not None else None,
                    },
                    candidate_count_before_partial_ranking=before_count,
                    candidate_count_after_partial_ranking=len(exact_score_input_keys),
                    partial_ranking={
                        "strategy": "matched-should-prefilter+exact-should-score-tier",
                        "matchedShouldCutoff": matched_should_cutoff,
                    },
                ).to_dict(),
            )

    if isinstance(query, SearchCompoundQuery):
        pre_ranked = prune_candidate_storage_keys_with_candidateable_ranking(
            candidate_storage_keys,
            resolved_should_candidates,
            result_limit_hint=result_limit_hint,
        )
        if pre_ranked is not None:
            pruned, matched_cutoff = pre_ranked
            if len(pruned) < before_count:
                return (
                    pruned,
                    CompoundTopKPrefilter(
                        applied=True,
                        before_count=before_count,
                        after_count=len(pruned),
                        strategy="candidateable-should-ranking-tier",
                        cutoff_matched_should=matched_cutoff,
                        cutoff_tier={"matchedShould": matched_cutoff},
                    ).to_dict(),
                )

    match_counts: Counter[str] = Counter()
    for values in resolved_should_candidates:
        for storage_key in values:
            if storage_key in candidate_set:
                match_counts[storage_key] += 1

    selected: set[str] = set()
    selected_count = 0
    cutoff_matched_should: int | None = None
    for matched_should in sorted({match_counts.get(storage_key, 0) for storage_key in candidate_storage_keys}, reverse=True):
        tier = [storage_key for storage_key in candidate_storage_keys if match_counts.get(storage_key, 0) == matched_should]
        if not tier:
            continue
        selected.update(tier)
        selected_count += len(tier)
        cutoff_matched_should = matched_should
        if selected_count >= result_limit_hint:
            break

    pruned = [storage_key for storage_key in candidate_storage_keys if storage_key in selected]
    if len(pruned) >= before_count:
        return candidate_storage_keys, None
    return (
        pruned,
        CompoundTopKPrefilter(
            applied=True,
            before_count=before_count,
            after_count=len(pruned),
            strategy="matched-should-tier",
            cutoff_matched_should=cutoff_matched_should,
            cutoff_tier={"matchedShould": cutoff_matched_should},
        ).to_dict(),
    )


def prune_candidate_storage_keys_with_candidateable_ranking(
    candidate_storage_keys: list[str],
    should_candidates: list[list[str]],
    *,
    result_limit_hint: int,
) -> tuple[list[str], int | None] | None:
    if not should_candidates:
        return None
    candidate_set = set(candidate_storage_keys)
    match_counts: Counter[str] = Counter()
    coverage_counts: Counter[str] = Counter()
    for candidate_list in should_candidates:
        seen_in_clause: set[str] = set()
        for storage_key in candidate_list:
            if storage_key not in candidate_set or storage_key in seen_in_clause:
                continue
            seen_in_clause.add(storage_key)
            match_counts[storage_key] += 1
            coverage_counts[storage_key] += 1
    if not match_counts:
        return None
    selected: set[str] = set()
    selected_count = 0
    cutoff_matched_should: int | None = None
    tiers = sorted(
        {(match_counts.get(storage_key, 0), coverage_counts.get(storage_key, 0)) for storage_key in candidate_storage_keys},
        reverse=True,
    )
    for matched_should, coverage_count in tiers:
        tier = [
            storage_key
            for storage_key in candidate_storage_keys
            if (match_counts.get(storage_key, 0), coverage_counts.get(storage_key, 0)) == (matched_should, coverage_count)
        ]
        if not tier:
            continue
        selected.update(tier)
        selected_count += len(tier)
        cutoff_matched_should = matched_should
        if selected_count >= result_limit_hint:
            break
    pruned = [storage_key for storage_key in candidate_storage_keys if storage_key in selected]
    return pruned, cutoff_matched_should


def prefilter_candidate_storage_keys_by_matched_should(
    candidate_storage_keys: list[str],
    should_candidates: list[list[str]],
    *,
    result_limit_hint: int,
) -> tuple[list[str] | None, int | None]:
    if not should_candidates or result_limit_hint <= 0:
        return None, None
    candidate_set = set(candidate_storage_keys)
    match_counts: Counter[str] = Counter()
    for candidate_list in should_candidates:
        seen_in_clause: set[str] = set()
        for storage_key in candidate_list:
            if storage_key not in candidate_set or storage_key in seen_in_clause:
                continue
            seen_in_clause.add(storage_key)
            match_counts[storage_key] += 1
    if not match_counts:
        return None, None
    selected: set[str] = set()
    selected_count = 0
    cutoff_matched_should: int | None = None
    for matched_should in sorted({match_counts.get(storage_key, 0) for storage_key in candidate_storage_keys}, reverse=True):
        tier = [storage_key for storage_key in candidate_storage_keys if match_counts.get(storage_key, 0) == matched_should]
        if not tier:
            continue
        selected.update(tier)
        selected_count += len(tier)
        cutoff_matched_should = matched_should
        if selected_count >= result_limit_hint:
            break
    prefitered = [storage_key for storage_key in candidate_storage_keys if storage_key in selected]
    if len(prefitered) >= len(candidate_storage_keys):
        return None, cutoff_matched_should
    return prefitered, cutoff_matched_should


def exact_candidateable_should_scores(
    engine: _SQLiteCompoundRankingEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str | None,
    query: SearchCompoundQuery,
    candidate_storage_keys: list[str],
) -> dict[str, dict[str, float]] | None:
    if physical_name is None or not candidate_storage_keys or any(isinstance(clause, (SearchNearQuery, SearchCompoundQuery)) for clause in query.should):
        return None
    prepared_by_storage_key = load_materialized_search_documents_by_storage_key(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        storage_keys=candidate_storage_keys,
    )
    if not prepared_by_storage_key:
        return None
    scores: dict[str, dict[str, float]] = {}
    for storage_key in candidate_storage_keys:
        prepared = prepared_by_storage_key.get(storage_key, materialized_search_document_from_entries(()))
        matched_should = 0
        should_score = 0.0
        for clause in query.should:
            matched, clause_score, _near_distance = search_clause_ranking(
                {},
                definition=_SEARCH_RUNTIME_DUMMY_DEFINITION,
                query=clause,
                materialized=prepared,
            )
            if not matched:
                continue
            matched_should += 1
            should_score += clause_score
        scores[storage_key] = {
            "matchedShould": float(matched_should),
            "shouldScore": should_score,
        }
    return scores


def load_search_entries_by_storage_key(
    engine: _SQLiteCompoundRankingEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
    storage_keys: list[str],
) -> dict[str, tuple[tuple[str, str], ...]]:
    if not storage_keys:
        return {}
    cache_bucket = materialized_search_entry_cache_bucket(
        engine,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
    )
    missing_storage_keys = [storage_key for storage_key in storage_keys if storage_key not in cache_bucket]
    if missing_storage_keys:
        grouped_entries: dict[str, list[tuple[str, str]]] = {storage_key: [] for storage_key in missing_storage_keys}
        placeholders = ", ".join("?" for _ in missing_storage_keys)
        sql = (
            f"SELECT storage_key, field_path, content FROM {engine._quote_identifier(physical_name)} "
            f"WHERE storage_key IN ({placeholders})"
        )
        for storage_key, field_path, content in conn.execute(sql, tuple(missing_storage_keys)).fetchall():
            grouped_entries.setdefault(str(storage_key), []).append((str(field_path), str(content)))
        for storage_key in missing_storage_keys:
            entries = tuple(grouped_entries.get(storage_key, ()))
            cache_bucket[storage_key] = (entries, materialized_search_document_from_entries(entries))
    return {
        storage_key: cache_bucket[storage_key][0]
        for storage_key in storage_keys
        if storage_key in cache_bucket
    }


def load_materialized_search_documents_by_storage_key(
    engine: _SQLiteCompoundRankingEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
    storage_keys: list[str],
) -> dict[str, MaterializedSearchDocument]:
    load_search_entries_by_storage_key(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        storage_keys=storage_keys,
    )
    cache_bucket = materialized_search_entry_cache_bucket(
        engine,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
    )
    return {
        storage_key: cache_bucket[storage_key][1]
        for storage_key in storage_keys
        if storage_key in cache_bucket
    }


def materialized_search_entry_cache_bucket(
    engine: _SQLiteCompoundRankingEngine,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
) -> dict[str, tuple[tuple[tuple[str, str], ...], MaterializedSearchDocument]]:
    cache_key = (db_name, coll_name, physical_name, engine._search_backend_version(db_name, coll_name))
    return engine._materialized_search_entry_cache.setdefault(cache_key, {})


def materialized_search_document_from_entries(
    entries: list[tuple[str, str]] | tuple[tuple[str, str], ...],
) -> MaterializedSearchDocument:
    searchable_paths: set[str] = set()
    lowered_values: list[str] = []
    lowered_values_by_path: dict[str, list[str]] = {}
    token_counter_by_path: dict[str, Counter[str]] = {}
    token_counter: Counter[str] = Counter()
    token_sets_by_path: dict[str, set[str]] = {}
    token_set: set[str] = set()
    for field_path, value in entries:
        searchable_paths.add(field_path)
        lowered = value.lower()
        lowered_values.append(lowered)
        lowered_values_by_path.setdefault(field_path, []).append(lowered)
        tokens_counter = Counter(tokenize_classic_text(value))
        tokens = set(tokens_counter)
        token_counter.update(tokens_counter)
        token_counter_by_path.setdefault(field_path, Counter()).update(tokens_counter)
        token_set.update(tokens)
        token_sets_by_path.setdefault(field_path, set()).update(tokens)
    return MaterializedSearchDocument(
        entries=tuple(entries),
        searchable_paths=frozenset(searchable_paths),
        lowered_values=tuple(lowered_values),
        lowered_values_by_path={field_path: tuple(values) for field_path, values in lowered_values_by_path.items()},
        token_counter_by_path=token_counter_by_path,
        token_counter=token_counter,
        token_sets_by_path={field_path: frozenset(values) for field_path, values in token_sets_by_path.items()},
        token_set=frozenset(token_set),
    )


def sort_search_documents_for_query(
    documents: list[tuple[Document, SearchIndexDefinition, object | None]],
    *,
    query: SearchQuery,
) -> list[Document]:
    if isinstance(query, SearchNearQuery):
        ranked: list[tuple[float, Document]] = []
        for document, _definition, _materialized in documents:
            distance = search_near_distance(document, query=query)
            ranked.append((distance if distance is not None else float("inf"), document))
        ranked.sort(key=lambda item: item[0])
        return [document for _distance, document in ranked]
    if isinstance(query, SearchCompoundQuery) and query.should:
        ranked_compound: list[tuple[tuple[float, float, float], Document]] = []
        for document, definition, materialized in documents:
            matched_should, should_score, best_near_distance = search_compound_ranking(
                document,
                definition=definition,
                query=query,
                materialized=materialized,
            )
            ranked_compound.append(((-float(matched_should), -should_score, best_near_distance), document))
        ranked_compound.sort(key=lambda item: item[0])
        return [document for _score, document in ranked_compound]
    return [document for document, _definition, _materialized in documents]


def rank_compound_candidate_storage_keys_from_entries(
    engine: _SQLiteCompoundRankingEngine,
    conn: sqlite3.Connection,
    *,
    db_name: str,
    coll_name: str,
    physical_name: str,
    query: SearchCompoundQuery,
    candidate_storage_keys: list[str],
    result_limit_hint: int | None,
) -> list[str] | None:
    if not candidate_storage_keys:
        return []
    if not compound_entry_ranking_supported(query, physical_name=physical_name):
        return None
    prepared_by_storage_key = load_materialized_search_documents_by_storage_key(
        engine,
        conn,
        db_name=db_name,
        coll_name=coll_name,
        physical_name=physical_name,
        storage_keys=candidate_storage_keys,
    )
    ranked: list[tuple[tuple[float, float], int, str]] = []
    for order, storage_key in enumerate(candidate_storage_keys):
        prepared = prepared_by_storage_key.get(storage_key, materialized_search_document_from_entries(()))
        matched_should = 0
        should_score = 0.0
        for clause in query.should:
            matched, clause_score, _near_distance = search_clause_ranking(
                {},
                definition=_SEARCH_RUNTIME_DUMMY_DEFINITION,
                query=clause,
                materialized=prepared,
            )
            if not matched:
                continue
            matched_should += 1
            should_score += clause_score
        ranked.append(((-float(matched_should), -should_score), order, storage_key))
    if result_limit_hint is not None and result_limit_hint > 0 and len(ranked) > result_limit_hint:
        ranked = heapq.nsmallest(result_limit_hint, ranked, key=lambda item: item[:2])
    ranked.sort(key=lambda item: item[:2])
    return [storage_key for _rank, _order, storage_key in ranked]
