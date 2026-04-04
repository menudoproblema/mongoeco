from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from copy import deepcopy
from dataclasses import dataclass
import datetime
import math
from typing import Generic, TypeVar

from mongoeco.core.paths import get_document_value


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
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number):
            return None
        return ("number", number)
    if isinstance(value, str):
        return ("string", value)
    if isinstance(value, datetime.datetime):
        return ("datetime", value)
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


def value_key_matches_range(value_key: _FilterValueKey, clause: dict[str, object]) -> bool:
    kind, value = value_key
    if kind not in {"number", "date", "datetime"}:
        return False
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


def matches_candidateable_filter(document: dict[str, object], filter_spec: dict[str, object]) -> bool | None:
    for key, value in filter_spec.items():
        if key == "$and":
            if not isinstance(value, list):
                return None
            for item in value:
                if not isinstance(item, dict):
                    return None
                matched = matches_candidateable_filter(document, item)
                if matched is None:
                    return None
                if not matched:
                    return False
            continue
        if key == "$or":
            if not isinstance(value, list) or not value:
                return None
            branch_supported = False
            for item in value:
                if not isinstance(item, dict):
                    return None
                matched = matches_candidateable_filter(document, item)
                if matched is None:
                    return None
                branch_supported = True
                if matched:
                    return True
            return False if branch_supported else None
        if key.startswith("$"):
            return None
        found, raw_value = get_document_value(document, key)
        candidate_values = raw_value if isinstance(raw_value, list) else [raw_value]
        if isinstance(value, dict) and set(value) == {"$exists"} and isinstance(value["$exists"], bool):
            if found != value["$exists"]:
                return False
            continue
        if not found:
            return False
        if isinstance(value, dict) and set(value) == {"$in"} and isinstance(value["$in"], list):
            normalized_candidates = {
                normalized
                for item in candidate_values
                if (normalized := filter_value_key(item)) is not None
            }
            normalized_filter_values = {
                normalized
                for item in value["$in"]
                if (normalized := filter_value_key(item)) is not None
            }
            if not normalized_filter_values or normalized_candidates.isdisjoint(normalized_filter_values):
                return False
            continue
        if isinstance(value, dict) and value and set(value).issubset({"$gt", "$gte", "$lt", "$lte"}):
            normalized_candidates = [
                normalized
                for item in candidate_values
                if (normalized := filter_value_key(item)) is not None
            ]
            if not normalized_candidates or not any(value_key_matches_range(item, value) for item in normalized_candidates):
                return False
            continue
        normalized_clause = filter_value_key(value)
        if normalized_clause is None:
            return None
        normalized_candidates = {
            normalized
            for item in candidate_values
            if (normalized := filter_value_key(item)) is not None
        }
        if normalized_clause not in normalized_candidates:
            return False
    return True


def flatten_candidate_filter_clauses(filter_spec: dict[str, object]) -> tuple[tuple[str, object], ...] | None:
    node = _evaluate_candidate_filter_node(
        filter_spec,
        all_candidates=(),
        clause_resolver=lambda _path, _clause: ((), "eq"),
        clauses_only=True,
    )
    if node is None:
        return None
    return tuple((clause.path, clause.raw_clause) for clause in node.clauses)


def evaluate_candidate_filter(
    filter_spec: dict[str, object],
    *,
    all_candidates: Sequence[CandidateT],
    clause_resolver: Callable[[str, object], tuple[Iterable[CandidateT] | None, str]],
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
        matches = tuple(candidate for candidate in ordered_candidates if candidate in allowed)
    return CandidateFilterResult(plan=plan, matches=matches)


def _evaluate_candidate_filter_node(
    filter_spec: dict[str, object],
    *,
    all_candidates: Sequence[CandidateT],
    clause_resolver: Callable[[str, object], tuple[Iterable[CandidateT] | None, str]],
    clauses_only: bool,
) -> _NodeEvaluation[CandidateT] | None:
    matches: tuple[CandidateT, ...] | None = tuple(all_candidates) if clauses_only else None
    supported_paths: list[str] = []
    supported_operators: list[str] = []
    clauses: list[CandidateFilterClause] = []
    supported_clause_count = 0
    unsupported_clause_count = 0
    exact = True
    shapes: list[str] = []

    def _ordered_intersection(left: tuple[CandidateT, ...], right: tuple[CandidateT, ...]) -> tuple[CandidateT, ...]:
        allowed = set(right)
        return tuple(candidate for candidate in left if candidate in allowed)

    def _ordered_union(groups: list[tuple[CandidateT, ...]]) -> tuple[CandidateT, ...]:
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
            if not isinstance(value, list):
                return None
            shapes.append("$and")
            local_matches: tuple[CandidateT, ...] | None = None
            local_supported = False
            for item in value:
                if not isinstance(item, dict):
                    return None
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
                    local_matches = nested.matches if local_matches is None else _ordered_intersection(local_matches, nested.matches)
            if not clauses_only and local_supported:
                matches = local_matches if matches is None else _ordered_intersection(matches, local_matches or ())
            continue
        if key == "$or":
            if not isinstance(value, list) or not value:
                return None
            shapes.append("$or")
            branch_matches: list[tuple[CandidateT, ...]] = []
            branch_exact = True
            for item in value:
                if not isinstance(item, dict):
                    return None
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
                matches = union if matches is None else _ordered_intersection(matches, union)
                exact = exact and branch_exact
            continue
        if key.startswith("$"):
            return None
        clause_matches, operator_name = clause_resolver(key, value)
        supported = clause_matches is not None
        clauses.append(
            CandidateFilterClause(
                path=key,
                raw_clause=deepcopy(value),
                support=CandidateFilterSupport(
                    operator=operator_name,
                    supported=supported,
                    exact=supported,
                ),
            )
        )
        if clauses_only:
            continue
        if clause_matches is None:
            unsupported_clause_count += 1
            exact = False
            continue
        supported_paths.append(key)
        supported_operators.append(operator_name)
        supported_clause_count += 1
        ordered_clause_matches = tuple(clause_matches)
        matches = ordered_clause_matches if matches is None else _ordered_intersection(matches, ordered_clause_matches)

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
