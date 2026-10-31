from __future__ import annotations

import datetime

from collections.abc import Callable, Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
from typing import Generic, TypeVar

from mongoeco.core.bson_ordering import Code
from mongoeco.core.bson_scalars import (
    normalize_utc_bson_datetime,
    numeric_comparison_decimal,
    wrap_bson_numeric,
)
from mongoeco.core.query_operators import (
    is_non_empty_document_clause_list,
    is_query_operator_key,
)


CandidateT = TypeVar("CandidateT")
_FilterValueKey = tuple[str, object]


@dataclass(frozen=True, slots=True)
class CandidateFilterSupport:
    operator: str
    supported: bool
    exact: bool


@dataclass(frozen=True, slots=True)
class CandidateFilterClause:
    path: str
    raw_clause: object
    support: CandidateFilterSupport


@dataclass(frozen=True, slots=True)
class CandidateFilterPlan:
    spec: dict[str, object]
    clauses: tuple[CandidateFilterClause, ...]
    shape: str | None
    supported_paths: tuple[str, ...]
    supported_operators: tuple[str, ...]
    supported_clause_count: int
    unsupported_clause_count: int
    exact: bool
    candidateable: bool

    def to_metadata(self, *, backend: str | None = None) -> dict[str, object]:
        return {
            "spec": deepcopy(self.spec),
            "candidateable": self.candidateable,
            "exact": self.exact,
            "backend": backend if self.candidateable else None,
            "supportedPaths": list(self.supported_paths),
            "supportedClauseCount": self.supported_clause_count,
            "unsupportedClauseCount": self.unsupported_clause_count,
            "supportedOperators": list(self.supported_operators),
            "booleanShape": self.shape,
        }


@dataclass(frozen=True, slots=True)
class CandidateFilterResult(Generic[CandidateT]):
    plan: CandidateFilterPlan
    matches: tuple[CandidateT, ...] | None

    def to_metadata(self, *, backend: str | None = None) -> dict[str, object]:
        return self.plan.to_metadata(backend=backend)


@dataclass(frozen=True, slots=True)
class _NodeEvaluation(Generic[CandidateT]):
    matches: tuple[CandidateT, ...] | None
    clauses: tuple[CandidateFilterClause, ...]
    supported_paths: tuple[str, ...]
    supported_operators: tuple[str, ...]
    supported_clause_count: int
    unsupported_clause_count: int
    exact: bool
    shape: str


def filter_value_key(value: object) -> _FilterValueKey | None:
    if value is None:
        return ("null", None)
    if isinstance(value, bool):
        return ("bool", value)
    numeric = wrap_bson_numeric(value)
    if numeric is not None:
        number = numeric_comparison_decimal(numeric.value)
        if not number.is_finite():
            return None
        return ("number", number)
    if isinstance(value, Code):
        return None
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, datetime.datetime):
        return ("datetime", normalize_utc_bson_datetime(value))
    if isinstance(value, datetime.date):
        return ("date", value)
    return None


def collect_filterable_values(
    value: object,
    *,
    prefix: str = "",
) -> list[tuple[str, set[_FilterValueKey]]]:
    entries: list[tuple[str, set[_FilterValueKey]]] = []
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or not key:
                continue
            path = f"{prefix}.{key}" if prefix else key
            entries.extend(collect_filterable_values(item, prefix=path))
        return entries
    if not prefix:
        return entries
    if isinstance(value, list):
        scalar_values = {
            normalized
            for item in value
            if (normalized := filter_value_key(item)) is not None
        }
        entries.append((prefix, scalar_values))
        return entries
    normalized = filter_value_key(value)
    entries.append((prefix, {normalized} if normalized is not None else set()))
    return entries


def value_key_matches_range(
    value_key: _FilterValueKey, clause: dict[str, object]
) -> bool:
    kind, value = value_key
    if kind not in {"number", "date", "datetime"}:
        return False
    if kind == "number":
        value = numeric_comparison_decimal(value)
    for operator, bound in clause.items():
        if operator not in {"$gt", "$gte", "$lt", "$lte"}:
            return False
        bound_key = filter_value_key(bound)
        if bound_key is None or bound_key[0] != kind:
            return False
        bound_value = bound_key[1]
        if operator == "$gt" and not (value > bound_value):
            return False
        if operator == "$gte" and not (value >= bound_value):
            return False
        if operator == "$lt" and not (value < bound_value):
            return False
        if operator == "$lte" and not (value <= bound_value):
            return False
    return True


def unrepresented_filter_paths(document: dict[str, object]) -> set[str]:
    """Top-level domains that the scalar postings cannot represent completely."""
    return {path for path, value in document.items() if filter_value_key(value) is None}


def scalar_filter_candidates[CandidateT](  # noqa: PLR0911, PLR0913 - explicit coverage and capability exits
    path: str,
    clause: object,
    *,
    all_candidates: set[CandidateT],
    exists_index: dict,
    value_index: dict,
    unrepresented_paths: frozenset[str],
) -> tuple[set[CandidateT] | None, str, bool]:
    """Resolve only lossless scalar postings; unknown domains imply no reduction.

    Arrays, object traversal and numeric positions stay in the canonical residual.
    Null membership also needs that residual because missing is not a posting.
    """
    operator = (
        "$exists"
        if isinstance(clause, dict) and set(clause) == {"$exists"}
        else "$in"
        if isinstance(clause, dict) and set(clause) == {"$in"}
        else "range"
        if isinstance(clause, dict)
        else "eq"
    )
    if "." in path or path in unrepresented_paths:
        return None, operator, False
    if operator == "$exists":
        if not isinstance(clause["$exists"], bool):
            return None, operator, False
        existing = set(exists_index.get(path, ())) & all_candidates
        return (
            (existing if clause["$exists"] else all_candidates - existing),
            operator,
            True,
        )
    postings = value_index.get(path, {})
    if operator == "$in":
        if not isinstance(clause["$in"], list):
            return None, operator, False
        values = clause["$in"]
    elif operator == "range":
        if not clause or any(
            key not in {"$gt", "$gte", "$lt", "$lte"}
            or (bound := filter_value_key(value)) is None
            or bound[0] not in {"number", "date", "datetime"}
            for key, value in clause.items()
        ):
            return None, operator, False
        return (
            {
                candidate
                for value, candidates in postings.items()
                if value_key_matches_range(value, clause)
                for candidate in candidates
                if candidate in all_candidates
            },
            operator,
            True,
        )
    else:
        values = [clause]
    matches = set()
    for value in values:
        key = filter_value_key(value)
        if key is None or key[0] == "null":
            return None, operator, False
        matches.update(postings.get(key, ()))
    return matches & all_candidates, operator, True


def flatten_candidate_filter_clauses(
    filter_spec: dict[str, object],
) -> tuple[tuple[str, object], ...] | None:
    node = _evaluate_candidate_filter_node(
        filter_spec,
        all_candidates=(),
        clause_resolver=lambda _path, _clause: ((), "eq", True),
        clauses_only=True,
    )
    if node is None:
        return None
    return tuple((clause.path, clause.raw_clause) for clause in node.clauses)


def evaluate_candidate_filter(
    filter_spec: dict[str, object],
    *,
    all_candidates: Sequence[CandidateT],
    clause_resolver: Callable[
        [str, object], tuple[Iterable[CandidateT] | None, str, bool]
    ],
    ordered_candidates: Sequence[CandidateT] | None = None,
) -> CandidateFilterResult[CandidateT] | None:
    node = _evaluate_candidate_filter_node(
        filter_spec,
        all_candidates=all_candidates,
        clause_resolver=clause_resolver,
        clauses_only=False,
    )
    if node is None:
        return None
    plan = CandidateFilterPlan(
        spec=deepcopy(filter_spec),
        clauses=node.clauses,
        shape=node.shape or None,
        supported_paths=node.supported_paths,
        supported_operators=node.supported_operators,
        supported_clause_count=node.supported_clause_count,
        unsupported_clause_count=node.unsupported_clause_count,
        exact=node.exact and node.unsupported_clause_count == 0,
        candidateable=node.matches is not None,
    )
    matches = node.matches
    if matches is not None and ordered_candidates is not None:
        allowed = set(matches)
        matches = tuple(
            candidate for candidate in ordered_candidates if candidate in allowed
        )
    return CandidateFilterResult(plan=plan, matches=matches)


def _evaluate_candidate_filter_node(
    filter_spec: dict[str, object],
    *,
    all_candidates: Sequence[CandidateT],
    clause_resolver: Callable[
        [str, object], tuple[Iterable[CandidateT] | None, str, bool]
    ],
    clauses_only: bool,
) -> _NodeEvaluation[CandidateT] | None:
    matches: tuple[CandidateT, ...] | None = (
        tuple(all_candidates) if clauses_only else None
    )
    supported_paths: list[str] = []
    supported_operators: list[str] = []
    clauses: list[CandidateFilterClause] = []
    supported_clause_count = 0
    unsupported_clause_count = 0
    exact = True
    shapes: list[str] = []

    def _ordered_intersection(
        left: tuple[CandidateT, ...], right: tuple[CandidateT, ...]
    ) -> tuple[CandidateT, ...]:
        allowed = set(right)
        return tuple(candidate for candidate in left if candidate in allowed)

    def _ordered_union(
        groups: list[tuple[CandidateT, ...]],
    ) -> tuple[CandidateT, ...]:
        seen: set[CandidateT] = set()
        ordered: list[CandidateT] = []
        for values in groups:
            for candidate in values:
                if candidate in seen:
                    continue
                seen.add(candidate)
                ordered.append(candidate)
        return tuple(ordered)

    for key, value in filter_spec.items():
        if key == "$and":
            if not is_non_empty_document_clause_list(value):
                return None
            shapes.append("$and")
            local_matches: tuple[CandidateT, ...] | None = None
            local_supported = False
            for item in value:
                nested = _evaluate_candidate_filter_node(
                    item,
                    all_candidates=all_candidates,
                    clause_resolver=clause_resolver,
                    clauses_only=clauses_only,
                )
                if nested is None:
                    return None
                clauses.extend(nested.clauses)
                supported_paths.extend(nested.supported_paths)
                supported_operators.extend(nested.supported_operators)
                supported_clause_count += nested.supported_clause_count
                unsupported_clause_count += nested.unsupported_clause_count
                exact = exact and nested.exact
                if not clauses_only and nested.matches is not None:
                    local_supported = True
                    local_matches = (
                        nested.matches
                        if local_matches is None
                        else _ordered_intersection(local_matches, nested.matches)
                    )
            if not clauses_only and local_supported:
                matches = (
                    local_matches
                    if matches is None
                    else _ordered_intersection(matches, local_matches or ())
                )
            continue
        if key == "$or":
            if not is_non_empty_document_clause_list(value):
                return None
            shapes.append("$or")
            branch_matches: list[tuple[CandidateT, ...]] = []
            branch_exact = True
            for item in value:
                nested = _evaluate_candidate_filter_node(
                    item,
                    all_candidates=all_candidates,
                    clause_resolver=clause_resolver,
                    clauses_only=clauses_only,
                )
                if nested is None:
                    return None
                clauses.extend(nested.clauses)
                supported_paths.extend(nested.supported_paths)
                supported_operators.extend(nested.supported_operators)
                supported_clause_count += nested.supported_clause_count
                unsupported_clause_count += nested.unsupported_clause_count
                branch_exact = branch_exact and nested.exact
                if not clauses_only:
                    if nested.matches is None:
                        return _NodeEvaluation(
                            matches=None,
                            clauses=tuple(clauses),
                            supported_paths=tuple(supported_paths),
                            supported_operators=tuple(supported_operators),
                            supported_clause_count=supported_clause_count,
                            unsupported_clause_count=unsupported_clause_count + 1,
                            exact=False,
                            shape="+".join(shapes) if shapes else "flat",
                        )
                    branch_matches.append(nested.matches)
            if not clauses_only:
                union = _ordered_union(branch_matches)
                matches = (
                    union if matches is None else _ordered_intersection(matches, union)
                )
                exact = exact and branch_exact
            continue
        if not isinstance(key, str) or is_query_operator_key(key):
            return None
        clause_matches, operator_name, clause_exact = clause_resolver(key, value)
        supported = clause_matches is not None
        clauses.append(
            CandidateFilterClause(
                path=key,
                raw_clause=deepcopy(value),
                support=CandidateFilterSupport(
                    operator=operator_name,
                    supported=supported,
                    exact=supported and clause_exact,
                ),
            )
        )
        if clauses_only:
            continue
        if clause_matches is None:
            unsupported_clause_count += 1
            exact = False
            continue
        exact = exact and clause_exact
        supported_paths.append(key)
        supported_operators.append(operator_name)
        supported_clause_count += 1
        ordered_clause_matches = tuple(clause_matches)
        matches = (
            ordered_clause_matches
            if matches is None
            else _ordered_intersection(matches, ordered_clause_matches)
        )

    return _NodeEvaluation(
        matches=matches,
        clauses=tuple(clauses),
        supported_paths=tuple(supported_paths),
        supported_operators=tuple(supported_operators),
        supported_clause_count=supported_clause_count,
        unsupported_clause_count=unsupported_clause_count,
        exact=exact,
        shape="+".join(shapes) if shapes else "flat",
    )
