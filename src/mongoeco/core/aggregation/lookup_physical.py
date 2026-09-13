from __future__ import annotations

import datetime
import uuid

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from mongoeco.compat import (
    MONGODB_DIALECT_70,
    MONGODB_DIALECT_80,
    MongoDialect,
)
from mongoeco.core.bson_ordering import bson_equality_key
from mongoeco.core.bson_scalars import is_bson_numeric
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.work_control import iter_with_deadline
from mongoeco.types import (
    Binary,
    Document,
    Regex,
    Timestamp,
    UndefinedType,
    is_object_id_like,
)


if TYPE_CHECKING:
    from collections.abc import Callable, Hashable, Sequence

    from mongoeco.core.collation import CollationSpec


_SUPPORTED_DIALECTS = (MONGODB_DIALECT_70, MONGODB_DIALECT_80)


def _supports_hash_semantics(dialect: MongoDialect) -> bool:
    return any(dialect is supported for supported in _SUPPORTED_DIALECTS)


def _is_safe_hash_value(value: Any) -> bool:
    if (
        value is None
        or isinstance(value, (UndefinedType, bool, str, bytes))
        or is_bson_numeric(value)
        or isinstance(
            value,
            (Binary, uuid.UUID, datetime.datetime, Timestamp, Regex),
        )
        or is_object_id_like(value)
    ):
        return True
    if isinstance(value, dict):
        return all(
            isinstance(key, str) and _is_safe_hash_value(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return all(_is_safe_hash_value(item) for item in value)
    return False


def _lookup_hash_key(value: Any, dialect: MongoDialect) -> Hashable | None:
    if not _is_safe_hash_value(value):
        return None
    if value is None or (
        isinstance(value, UndefinedType)
        and dialect.policy.null_query_matches_undefined()
    ):
        return ("lookup-null",)
    return bson_equality_key(value)


@dataclass(frozen=True, slots=True)
class BoundedLookupHashPlan[T]:
    """Candidate index for a simple equality lookup.

    Every candidate is still checked by the canonical equality predicate. The
    index only removes pairs that cannot match under the built-in dialects.
    """

    foreign: Sequence[T]
    buckets: dict[Hashable, tuple[int, ...]]
    residual_indices: tuple[int, ...]
    dialect: MongoDialect
    deadline: float | None

    def candidate_indices(self, local_values: list[Any]) -> list[int] | None:
        normalized_values = local_values or [None]
        keys: set[Hashable] = set()
        for value in iter_with_deadline(normalized_values, self.deadline):
            key = _lookup_hash_key(value, self.dialect)
            if key is None:
                return None
            keys.add(key)

        indices = set(self.residual_indices)
        for key in keys:
            indices.update(self.buckets.get(key, ()))
        return sorted(indices)


def build_bounded_lookup_hash_plan[T](  # noqa: PLR0913
    foreign: Sequence[T],
    foreign_field: str,
    *,
    document_getter: Callable[[T], Document],
    dialect: MongoDialect,
    collation: CollationSpec | None,
    max_associations: int | None,
    deadline: float | None = None,
) -> BoundedLookupHashPlan[T] | None:
    """Build a hash candidate plan or return ``None`` for the nested-loop path."""
    if (
        max_associations is None
        or max_associations <= 0
        or collation is not None
        or not _supports_hash_semantics(dialect)
    ):
        return None

    buckets: dict[Hashable, list[int]] = {}
    residual_indices: list[int] = []
    association_count = 0
    for index, item in enumerate(iter_with_deadline(foreign, deadline)):
        values = QueryEngine.extract_values(document_getter(item), foreign_field) or [
            None
        ]
        keys: set[Hashable] = set()
        residual = False
        for value in iter_with_deadline(values, deadline):
            key = _lookup_hash_key(value, dialect)
            if key is None:
                residual = True
            else:
                keys.add(key)

        association_count += len(keys) + int(residual)
        if association_count > max_associations:
            return None
        for key in keys:
            buckets.setdefault(key, []).append(index)
        if residual:
            residual_indices.append(index)

    return BoundedLookupHashPlan(
        foreign=foreign,
        buckets={key: tuple(indices) for key, indices in buckets.items()},
        residual_indices=tuple(residual_indices),
        dialect=dialect,
        deadline=deadline,
    )


def explain_lookup_physical_plans(
    pipeline: Sequence[object],
    *,
    dialect: MongoDialect,
    collation: CollationSpec | None,
    max_associations: int | None,
) -> list[Document]:
    """Describe lookup candidates without claiming a runtime hash was built."""
    plans: list[Document] = []

    def visit(  # noqa: PLR0912
        stages: Sequence[object],
        path: tuple[object, ...],
    ) -> None:
        for index, stage in enumerate(stages):
            if not isinstance(stage, dict) or len(stage) != 1:
                continue
            operator, spec = next(iter(stage.items()))
            stage_path = (*path, index)
            if operator == "$lookup" and isinstance(spec, dict):
                if "pipeline" in spec:
                    strategy = "nested-loop"
                    reason = "pipeline-form"
                elif collation is not None:
                    strategy = "nested-loop"
                    reason = "collation"
                elif not _supports_hash_semantics(dialect):
                    strategy = "nested-loop"
                    reason = "custom-dialect"
                elif max_associations is None or max_associations <= 0:
                    strategy = "nested-loop"
                    reason = "no-association-budget"
                else:
                    strategy = "bounded-hash-candidate"
                    reason = "eligible-with-runtime-fallback"
                plan: Document = {
                    "stagePath": list(stage_path),
                    "strategy": strategy,
                    "reason": reason,
                }
                if strategy == "bounded-hash-candidate":
                    plan["maxAssociations"] = max_associations
                plans.append(plan)
                nested = spec.get("pipeline")
                if isinstance(nested, list):
                    visit(nested, (*stage_path, "$lookup.pipeline"))
            elif operator == "$facet" and isinstance(spec, dict):
                for branch, nested in spec.items():
                    if isinstance(nested, list):
                        visit(nested, (*stage_path, "$facet", branch))
            elif operator == "$unionWith" and isinstance(spec, dict):
                nested = spec.get("pipeline")
                if isinstance(nested, list):
                    visit(nested, (*stage_path, "$unionWith.pipeline"))

    visit(pipeline, ())
    return plans
