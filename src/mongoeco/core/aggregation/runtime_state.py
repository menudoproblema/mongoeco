from __future__ import annotations

import random

from copy import deepcopy
from typing import TYPE_CHECKING, Any

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.evaluation_environment import scoped_environment
from mongoeco.core.aggregation.grouping_stages import (
    _apply_bucket,
    _apply_bucket_auto,
    _apply_count,
    _apply_group,
    _apply_set_window_fields,
    _apply_sort_by_count,
)
from mongoeco.core.aggregation.lookup_physical import (
    build_bounded_lookup_hash_plan,
)
from mongoeco.core.aggregation.planning import (
    Pipeline,
    _projection_flag,
    _require_documents_stage,
    _require_non_negative_int,
    _require_projection_for_dialect,
    _require_sample_spec,
    _require_sort,
    _require_stage,
    _require_unset_spec,
)
from mongoeco.core.aggregation.runtime import (
    _CURRENT_COLLECTION_RESOLVER_KEY,
    _MISSING,
    _REMOVE,
    _apply_unwind,
    _evaluate_expression_with_missing,
    _lookup_matches,
    _require_lookup_spec,
    _require_pipeline_spec,
    _require_union_with_spec,
    _require_unwind_spec,
    evaluate_expression,
)
from mongoeco.core.aggregation.stages import apply_pipeline
from mongoeco.core.aggregation.transform_stages import (
    _apply_add_fields,
    _apply_match,
    _apply_project,
    _apply_replace_root,
    _apply_unset,
)
from mongoeco.core.work_control import iter_with_deadline
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operation_limits import enforce_deadline
from mongoeco.core.paths import (
    delete_document_value,
    get_document_value,
    set_document_value,
)
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    RuntimeMetadata,
    RuntimeVirtualField,
    ensure_runtime_state,
)
from mongoeco.core.sorting import sort_documents
from mongoeco.errors import OperationFailure


if TYPE_CHECKING:
    from mongoeco.core.collation import CollationSpec


_PRESERVING_STAGES = frozenset({"$match", "$sort", "$skip", "$limit", "$sample"})


def _paths_overlap(left: str, right: str) -> bool:
    return left == right or left.startswith(right + ".") or right.startswith(left + ".")


def _field_is_shadowed(state: RuntimeDocumentState, path: str) -> bool:
    found, _value = get_document_value(state.document, path)
    return found


def _detach_virtual_fields(
    state: RuntimeDocumentState,
    output: dict[str, Any],
    mappings: list[tuple[str, RuntimeVirtualField]],
    *,
    entries=None,
) -> RuntimeDocumentState:
    document = deepcopy(output)
    fields: list[RuntimeVirtualField] = []
    for destination, source in sorted(
        mappings,
        key=lambda item: item[0].count("."),
        reverse=True,
    ):
        found, value = get_document_value(document, destination)
        if not found:
            continue
        delete_document_value(document, destination)
        fields.append(
            RuntimeVirtualField(
                destination,
                value,
                source.source,
                source.policy,
            ),
        )
    metadata = RuntimeMetadata(
        entries=state.metadata.entries if entries is None else tuple(entries),
        virtual_fields=tuple(reversed(fields)),
    )
    return RuntimeDocumentState(document, metadata)


def _preserve_unshadowed_virtuals(
    state: RuntimeDocumentState,
    output: dict[str, Any],
    *,
    excluded_paths: set[str] | None = None,
) -> RuntimeDocumentState:
    excluded_paths = excluded_paths or set()
    mappings = [
        (field.path, field)
        for field in state.metadata.virtual_fields
        if not _field_is_shadowed(state, field.path)
        and not any(_paths_overlap(field.path, path) for path in excluded_paths)
    ]
    return _detach_virtual_fields(state, output, mappings)


def _apply_preserving_stage(  # noqa: PLR0913
    states: list[RuntimeDocumentState],
    operator: str,
    spec: object,
    *,
    variables: dict[str, Any] | None,
    dialect: MongoDialect,
    collation: CollationSpec | None,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    if operator == "$match":
        return [
            state
            for state in iter_with_deadline(states, deadline)
            if _apply_match(
                [state.public_document()],
                spec,
                variables,
                dialect=dialect,
                collation=collation,
                deadline=deadline,
            )
        ]
    if operator == "$skip":
        return states[_require_non_negative_int("$skip", spec) :]
    if operator == "$limit":
        return states[: _require_non_negative_int("$limit", spec)]
    if operator == "$sample":
        size = _require_sample_spec(spec)
        return random.sample(states, min(size, len(states))) if size else []

    views = [state.public_document() for state in iter_with_deadline(states, deadline)]
    owners = {id(view): state for view, state in zip(views, states, strict=True)}
    sorted_views = sort_documents(
        views,
        _require_sort(spec),
        dialect=dialect,
        collation=collation,
        deadline=deadline,
    )
    return [owners[id(view)] for view in sorted_views]


def _apply_project_states(
    states: list[RuntimeDocumentState],
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    projection = _require_projection_for_dialect(spec, dialect=dialect)
    flags = {
        path: _projection_flag(value, dialect=dialect)
        for path, value in projection.items()
    }
    computed_paths = {path for path, flag in flags.items() if flag is None}
    include_paths = {
        path for path, flag in flags.items() if flag == 1 and path != "_id"
    }
    exclude_paths = {
        path for path, flag in flags.items() if flag == 0 and path != "_id"
    }
    inclusion = bool(include_paths or computed_paths)
    result: list[RuntimeDocumentState] = []
    for state in iter_with_deadline(states, deadline):
        output = _apply_project(
            [state.public_document()],
            projection,
            variables,
            dialect=dialect,
            deadline=deadline,
        )[0]
        for path in computed_paths:
            value = _evaluate_expression_with_missing(
                state,
                projection[path],
                variables,
                dialect=dialect,
            )
            if value is _REMOVE:
                delete_document_value(output, path)
            elif value is not _MISSING:
                set_document_value(output, path, deepcopy(value))
        mappings: list[tuple[str, RuntimeVirtualField]] = []
        for field in state.metadata.virtual_fields:
            if _field_is_shadowed(state, field.path):
                continue
            if any(_paths_overlap(field.path, path) for path in computed_paths):
                continue
            if inclusion and not any(
                _paths_overlap(field.path, path) for path in include_paths
            ):
                continue
            if not inclusion and any(
                path == field.path or field.path.startswith(path + ".")
                for path in exclude_paths
            ):
                continue
            mappings.append((field.path, field))
        result.append(_detach_virtual_fields(state, output, mappings))
    return result


def _apply_add_fields_states(
    states: list[RuntimeDocumentState],
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    if not isinstance(spec, dict):
        return _apply_add_fields(
            [],
            spec,
            variables,
            dialect=dialect,
            deadline=deadline,
        )
    destinations = {path for path in spec if isinstance(path, str)}
    result: list[RuntimeDocumentState] = []
    for state in iter_with_deadline(states, deadline):
        output = _apply_add_fields(
            [state.public_document()],
            spec,
            variables,
            dialect=dialect,
            deadline=deadline,
        )[0]
        for path, expression in spec.items():
            value = _evaluate_expression_with_missing(
                state,
                expression,
                variables,
                dialect=dialect,
            )
            if value is _REMOVE:
                delete_document_value(output, path)
            elif value is not _MISSING:
                set_document_value(output, path, deepcopy(value))
        result.append(
            _preserve_unshadowed_virtuals(
                state,
                output,
                excluded_paths=destinations,
            ),
        )
    return result


def _apply_unset_states(
    states: list[RuntimeDocumentState],
    spec: object,
    *,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    excluded = set(_require_unset_spec(spec))
    result: list[RuntimeDocumentState] = []
    for state in iter_with_deadline(states, deadline):
        output = _apply_unset(
            [state.public_document()],
            spec,
            deadline=deadline,
        )[0]
        result.append(
            _preserve_unshadowed_virtuals(
                state,
                output,
                excluded_paths=excluded,
            ),
        )
    return result


def _apply_replace_root_states(
    states: list[RuntimeDocumentState],
    spec: object,
    variables: dict[str, Any] | None,
    *,
    dialect: MongoDialect,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    new_root_spec = spec.get("newRoot") if isinstance(spec, dict) else None
    result: list[RuntimeDocumentState] = []
    for state in iter_with_deadline(states, deadline):
        output = _apply_replace_root(
            [state.public_document()],
            spec,
            variables,
            dialect=dialect,
            deadline=deadline,
        )[0]
        if isinstance(new_root_spec, str) and new_root_spec in {
            "$$ROOT",
            "$$CURRENT",
        }:
            mappings = [
                (field.path, field)
                for field in state.metadata.virtual_fields
                if not _field_is_shadowed(state, field.path)
            ]
            result.append(_detach_virtual_fields(state, output, mappings))
            continue
        if isinstance(new_root_spec, str) and new_root_spec.startswith("$"):
            prefix = new_root_spec[1:]
            mappings = []
            for field in state.metadata.virtual_fields:
                if not field.path.startswith(prefix + "."):
                    continue
                destination = field.path[len(prefix) + 1 :]
                mappings.append((destination, field))
            result.append(_detach_virtual_fields(state, output, mappings, entries=()))
            continue
        mappings = _expression_virtual_mappings(new_root_spec, state)
        result.append(
            _detach_virtual_fields(state, output, mappings)
            if mappings
            else RuntimeDocumentState(output)
        )
    return result


def _expression_virtual_mappings(
    expression: object,
    state: RuntimeDocumentState,
    prefix: str = "",
) -> list[tuple[str, RuntimeVirtualField]]:
    if isinstance(expression, str) and expression in {"$$ROOT", "$$CURRENT"}:
        return [
            (
                f"{prefix}.{field.path}" if prefix else field.path,
                field,
            )
            for field in state.metadata.virtual_fields
            if not _field_is_shadowed(state, field.path)
        ]
    if isinstance(expression, str) and expression.startswith("$"):
        source_prefix = expression[1:]
        return [
            (
                (
                    f"{prefix}.{field.path[len(source_prefix) + 1 :]}"
                    if prefix
                    else field.path[len(source_prefix) + 1 :]
                ),
                field,
            )
            for field in state.metadata.virtual_fields
            if field.path.startswith(source_prefix + ".")
        ]
    if isinstance(expression, list):
        mappings: list[tuple[str, RuntimeVirtualField]] = []
        for index, item in enumerate(expression):
            destination = f"{prefix}.{index}" if prefix else str(index)
            mappings.extend(_expression_virtual_mappings(item, state, destination))
        return mappings
    if not isinstance(expression, dict) or any(
        isinstance(key, str) and key.startswith("$") for key in expression
    ):
        return []
    mappings = []
    for key, item in expression.items():
        destination = f"{prefix}.{key}" if prefix else str(key)
        mappings.extend(_expression_virtual_mappings(item, state, destination))
    return mappings


def _apply_unwind_states(
    states: list[RuntimeDocumentState],
    spec: object,
    *,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    path, _preserve, _include_array_index = _require_unwind_spec(spec)
    result: list[RuntimeDocumentState] = []
    for state in iter_with_deadline(states, deadline):
        outputs = _apply_unwind(
            [state.public_document()],
            spec,
            deadline=deadline,
        )
        found, source_value = state.resolve(path)
        source_is_array = found and isinstance(source_value, list)
        for output_index, output in enumerate(
            iter_with_deadline(outputs, deadline),
        ):
            mappings: list[tuple[str, RuntimeVirtualField]] = []
            for field in state.metadata.virtual_fields:
                if _field_is_shadowed(state, field.path):
                    continue
                destination = field.path
                if field.path.startswith(path + ".") and source_is_array:
                    suffix = field.path[len(path) + 1 :]
                    first, separator, rest = suffix.partition(".")
                    if first.isdigit():
                        if int(first) != output_index:
                            continue
                        destination = path + (f".{rest}" if separator else "")
                mappings.append((destination, field))
            result.append(_detach_virtual_fields(state, output, mappings))
    return result


def _nested_branch_state(
    fields: dict[str, list[RuntimeDocumentState]],
    *,
    deadline: float | None = None,
) -> RuntimeDocumentState:
    document: dict[str, Any] = {}
    virtuals: list[RuntimeVirtualField] = []
    for field_name, states in iter_with_deadline(fields.items(), deadline):
        document[field_name] = [
            state.persistence_document()
            for state in iter_with_deadline(states, deadline)
        ]
        for index, state in enumerate(iter_with_deadline(states, deadline)):
            virtuals.extend(
                (
                    RuntimeVirtualField(
                        f"{field_name}.{index}.{virtual.path}",
                        virtual.value,
                        virtual.source,
                        virtual.policy,
                    )
                    for virtual in state.metadata.virtual_fields
                ),
            )
    return RuntimeDocumentState(
        document,
        RuntimeMetadata(virtual_fields=tuple(virtuals)),
    )


def _apply_facet_states(
    states: list[RuntimeDocumentState],
    spec: object,
    **kwargs,
) -> list[RuntimeDocumentState]:
    if not isinstance(spec, dict):
        message = "$facet requires a document specification"
        raise OperationFailure(message)
    branches: dict[str, list[RuntimeDocumentState]] = {}
    deadline = kwargs.get("deadline")
    for field_name, pipeline in spec.items():
        if not isinstance(field_name, str):
            message = "$facet field names must be strings"
            raise OperationFailure(message)
        branches[field_name] = apply_pipeline_states(
            [deepcopy(state) for state in iter_with_deadline(states, deadline)],
            _require_pipeline_spec("$facet", pipeline),
            **kwargs,
        )
    return [_nested_branch_state(branches, deadline=deadline)]


def _apply_lookup_states(  # noqa: PLR0913
    states: list[RuntimeDocumentState],
    spec: object,
    *,
    collection_resolver,
    variables: dict[str, Any] | None,
    dialect: MongoDialect,
    collation: CollationSpec | None,
    spill_policy,
    lookup_hash_max_associations: int | None,
    deadline: float | None,
    **_kwargs,
) -> list[RuntimeDocumentState]:
    lookup = _require_lookup_spec(spec)
    if collection_resolver is None:
        message = "$lookup requires collection resolver support"
        raise OperationFailure(message)
    foreign_states = [
        ensure_runtime_state(document)
        for document in iter_with_deadline(
            collection_resolver(lookup["from"]) or [],
            deadline,
        )
    ]
    hash_plan = None
    if "pipeline" not in lookup:
        hash_plan = build_bounded_lookup_hash_plan(
            foreign_states,
            lookup["foreignField"],
            document_getter=lambda state: state.public_document(),
            dialect=dialect,
            collation=collation,
            max_associations=lookup_hash_max_associations,
            deadline=deadline,
        )
    result: list[RuntimeDocumentState] = []
    for state in iter_with_deadline(states, deadline):
        candidates = foreign_states
        if "localField" in lookup and "foreignField" in lookup:
            local_values = QueryEngine.extract_values(
                state.public_document(),
                lookup["localField"],
            )
            if hash_plan is not None:
                candidate_indices = hash_plan.candidate_indices(local_values)
                if candidate_indices is not None:
                    candidates = [foreign_states[index] for index in candidate_indices]
            candidates = [
                candidate
                for candidate in iter_with_deadline(candidates, deadline)
                if _lookup_matches(
                    local_values,
                    QueryEngine.extract_values(
                        candidate.public_document(),
                        lookup["foreignField"],
                    ),
                    dialect=dialect,
                    collation=collation,
                )
            ]
        if "pipeline" in lookup:
            scoped = scoped_environment(variables)
            for name, expression in lookup["let"].items():
                scoped[name] = evaluate_expression(
                    state.public_document(),
                    expression,
                    variables,
                    dialect=dialect,
                )
            matches = apply_pipeline_states(
                [deepcopy(candidate) for candidate in candidates],
                lookup["pipeline"],
                collection_resolver=collection_resolver,
                variables=scoped,
                dialect=dialect,
                collation=collation,
                spill_policy=spill_policy,
                lookup_hash_max_associations=lookup_hash_max_associations,
                deadline=deadline,
            )
        else:
            matches = [
                deepcopy(candidate)
                for candidate in iter_with_deadline(candidates, deadline)
            ]
        document = state.persistence_document()
        document[lookup["as"]] = [
            match.persistence_document()
            for match in iter_with_deadline(matches, deadline)
        ]
        virtuals = [
            virtual
            for virtual in iter_with_deadline(
                state.metadata.virtual_fields,
                deadline,
            )
            if not _paths_overlap(virtual.path, lookup["as"])
        ]
        for index, match in enumerate(iter_with_deadline(matches, deadline)):
            virtuals.extend(
                RuntimeVirtualField(
                    f"{lookup['as']}.{index}.{virtual.path}",
                    virtual.value,
                    virtual.source,
                    virtual.policy,
                )
                for virtual in iter_with_deadline(
                    match.metadata.virtual_fields,
                    deadline,
                )
            )
        result.append(
            RuntimeDocumentState(
                document,
                RuntimeMetadata(
                    entries=state.metadata.entries,
                    virtual_fields=tuple(virtuals),
                ),
            ),
        )
    return result


def _apply_union_states(
    states: list[RuntimeDocumentState],
    spec: object,
    *,
    collection_resolver,
    deadline: float | None,
    **kwargs,
) -> list[RuntimeDocumentState]:
    union = _require_union_with_spec(spec)
    if collection_resolver is None:
        message = "$unionWith requires collection resolver support"
        raise OperationFailure(message)
    resolver_key = union["coll"] or _CURRENT_COLLECTION_RESOLVER_KEY
    resolved = collection_resolver(resolver_key)
    foreign = (
        [deepcopy(state) for state in iter_with_deadline(states, deadline)]
        if resolved is None
        else [
            ensure_runtime_state(document)
            for document in iter_with_deadline(resolved, deadline)
        ]
    )
    if union["pipeline"]:
        foreign = apply_pipeline_states(
            foreign,
            union["pipeline"],
            collection_resolver=collection_resolver,
            deadline=deadline,
            **kwargs,
        )
    return [deepcopy(state) for state in iter_with_deadline(states, deadline)] + foreign


def apply_pipeline_states(  # noqa: PLR0912, PLR0913
    documents: list[RuntimeDocumentState] | list[dict[str, Any]],
    pipeline: Pipeline,
    *,
    collection_resolver=None,
    collection_stats_resolver=None,
    index_stats_resolver=None,
    current_op_resolver=None,
    plan_cache_stats_resolver=None,
    list_sessions_resolver=None,
    variables: dict[str, Any] | None = None,
    dialect: MongoDialect = MONGODB_DIALECT_70,
    collation: CollationSpec | None = None,
    spill_policy=None,
    lookup_hash_max_associations: int | None = None,
    deadline: float | None = None,
) -> list[RuntimeDocumentState]:
    """Execute a pipeline while keeping runtime provenance outside BSON values."""
    states = [
        ensure_runtime_state(document)
        for document in iter_with_deadline(documents, deadline)
    ]
    common = {
        "collection_resolver": collection_resolver,
        "collection_stats_resolver": collection_stats_resolver,
        "index_stats_resolver": index_stats_resolver,
        "current_op_resolver": current_op_resolver,
        "plan_cache_stats_resolver": plan_cache_stats_resolver,
        "list_sessions_resolver": list_sessions_resolver,
        "variables": variables,
        "dialect": dialect,
        "collation": collation,
        "spill_policy": spill_policy,
        "lookup_hash_max_associations": lookup_hash_max_associations,
        "deadline": deadline,
    }
    for stage in pipeline:
        enforce_deadline(deadline)
        operator, spec = _require_stage(stage)
        if operator in _PRESERVING_STAGES:
            states = _apply_preserving_stage(
                states,
                operator,
                spec,
                variables=variables,
                dialect=dialect,
                collation=collation,
                deadline=deadline,
            )
        elif operator == "$documents":
            states = [
                RuntimeDocumentState(item)
                for item in iter_with_deadline(
                    _require_documents_stage(spec),
                    deadline,
                )
            ]
        elif operator == "$project":
            states = _apply_project_states(
                states,
                spec,
                variables,
                dialect=dialect,
                deadline=deadline,
            )
        elif operator in {"$set", "$addFields"}:
            states = _apply_add_fields_states(
                states,
                spec,
                variables,
                dialect=dialect,
                deadline=deadline,
            )
        elif operator == "$unset":
            states = _apply_unset_states(states, spec, deadline=deadline)
        elif operator == "$unwind":
            states = _apply_unwind_states(states, spec, deadline=deadline)
        elif operator in {"$replaceRoot", "$replaceWith"}:
            normalized = spec if operator == "$replaceRoot" else {"newRoot": spec}
            states = _apply_replace_root_states(
                states,
                normalized,
                variables,
                dialect=dialect,
                deadline=deadline,
            )
        elif operator == "$facet":
            states = _apply_facet_states(states, spec, **common)
        elif operator == "$lookup":
            states = _apply_lookup_states(states, spec, **common)
        elif operator == "$unionWith":
            states = _apply_union_states(states, spec, **common)
        elif operator in {
            "$group",
            "$bucket",
            "$bucketAuto",
            "$count",
            "$sortByCount",
            "$setWindowFields",
        }:
            if operator == "$group":
                output = _apply_group(
                    states,
                    spec,
                    variables,
                    dialect=dialect,
                    collation=collation,
                    deadline=deadline,
                )
            elif operator == "$bucket":
                output = _apply_bucket(
                    states,
                    spec,
                    variables,
                    dialect=dialect,
                    collation=collation,
                    deadline=deadline,
                )
            elif operator == "$bucketAuto":
                output = _apply_bucket_auto(
                    states,
                    spec,
                    variables,
                    dialect=dialect,
                    collation=collation,
                    deadline=deadline,
                )
            elif operator == "$count":
                output = _apply_count(states, spec)
            elif operator == "$sortByCount":
                output = _apply_sort_by_count(
                    states,
                    spec,
                    variables,
                    dialect=dialect,
                    deadline=deadline,
                )
            else:
                plain = [
                    state.public_document()
                    for state in iter_with_deadline(states, deadline)
                ]
                output = _apply_set_window_fields(
                    plain,
                    spec,
                    variables,
                    dialect=dialect,
                    collation=collation,
                    deadline=deadline,
                )
            states = [
                RuntimeDocumentState(item)
                for item in iter_with_deadline(output, deadline)
            ]
        else:
            output = apply_pipeline(
                [state.persistence_document() for state in states],
                [stage],
                **common,
            )
            states = [
                RuntimeDocumentState(item)
                for item in iter_with_deadline(output, deadline)
            ]
    enforce_deadline(deadline)
    return states
