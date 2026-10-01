"""Validate logical pipelines before physical slicing or reading any rows."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.extensions import (
    _aggregation_stage_registry_version,
    get_registered_aggregation_stage_registration,
)
from mongoeco.core.aggregation.information_stages import (
    INFORMATION_STAGES,
    parse_information_spec,
)
from mongoeco.core.aggregation.planning import _require_stage
from mongoeco.core.aggregation.runtime import (
    _MISSING,
    _REMOVE,
    _require_lookup_spec,
    _require_union_with_spec,
)
from mongoeco.errors import OperationFailure


_UNSPECIFIED = object()
_FIRST_STAGES = INFORMATION_STAGES | {
    "$documents",
    "$geoNear",
    "$search",
    "$searchMeta",
    "$vectorSearch",
}


@dataclass(frozen=True, slots=True)
class StageAddress:
    path: tuple[object, ...]
    index: int


@dataclass(frozen=True, slots=True)
class ResourceRequest:
    collection: str | None
    operator: str
    scale: int = 1


@dataclass(frozen=True, slots=True)
class PreparationContext:
    collection: str | None
    scopes: tuple[str, ...]
    path: tuple[object, ...]
    registry_version: int


class PreparedStage(dict):
    """A stage retains its address even when a planner builds a new list."""

    def __init__(self, operator, spec, address, context):
        super().__init__({operator: spec})
        self.address = address
        self.context = context


class PreparedPipeline(list):
    """Owned stages whose slices retain logical stage addresses and dependencies."""

    def __init__(self, stages, addresses, requests, *, dialect, context):
        super().__init__(stages)
        self.addresses = tuple(addresses)
        self.requests = tuple(requests)
        self.dialect = dialect
        self.context = context

    @property
    def collection(self):
        return self.context.collection

    def __deepcopy__(self, memo):
        copied = PreparedPipeline(
            [],
            self.addresses,
            self.requests,
            dialect=self.dialect,
            context=self.context,
        )
        memo[id(self)] = copied
        copied.extend(deepcopy(stage, memo) for stage in self)
        return copied

    def __getitem__(self, index):
        value = super().__getitem__(index)
        if isinstance(index, slice):
            return PreparedPipeline(
                value,
                self.addresses[index],
                self.requests,
                dialect=self.dialect,
                context=self.context,
            )
        return value


def _copy_spec(value):
    if isinstance(value, PreparedPipeline):
        return deepcopy(value)
    if isinstance(value, dict):
        return {key: _copy_spec(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_spec(item) for item in value]
    return deepcopy(value, {id(_MISSING): _MISSING, id(_REMOVE): _REMOVE})


def _validate_placement(operator, address, scopes):
    if operator in _FIRST_STAGES and address.index != 0:
        message = f"{operator} is only valid as the first pipeline stage"
        raise OperationFailure(message)
    if "$facet" in scopes and operator in INFORMATION_STAGES | {
        "$facet",
        "$documents",
        "$geoNear",
    }:
        message = f"{operator} is not allowed inside $facet"
        raise OperationFailure(message)
    if scopes and operator in {"$out", "$merge"}:
        message = f"{operator} is not allowed inside {scopes[-1]}"
        raise OperationFailure(message)


def _prepare_join(operator, spec, *, dialect, address, context):
    if operator == "$lookup":
        spec = _require_lookup_spec(spec)
        foreign = spec["from"]
        if foreign is None:
            spec.pop("from")
    else:
        spec = _require_union_with_spec(spec)
        foreign = spec["coll"] if spec["coll"] is not None else context.collection
        if _documents_source(spec["pipeline"]):
            foreign = None
    if "pipeline" in spec:
        spec["pipeline"] = prepare_pipeline(
            spec["pipeline"],
            dialect=dialect,
            collection=foreign,
            scope=(*context.scopes, operator),
            path=(*address.path, f"{operator}.pipeline"),
        )
        namespace = spec.get("from") if operator == "$lookup" else spec["coll"]
        _validate_documents_namespace(namespace, spec["pipeline"], dialect, operator)
    return spec


def _prepare_facet(spec, *, dialect, address, context):
    if not isinstance(spec, dict):
        message = "$facet requires a document specification"
        raise OperationFailure(message)
    for branch, nested in spec.items():
        if not isinstance(branch, str):
            message = "$facet field names must be strings"
            raise OperationFailure(message)
        if not isinstance(nested, list):
            message = "$facet requires a pipeline list"
            raise OperationFailure(message)
        spec[branch] = prepare_pipeline(
            nested,
            dialect=dialect,
            collection=context.collection,
            scope=(*context.scopes, "$facet"),
            path=(*address.path, "$facet", branch),
        )
    return spec


def _resource_children(operator, spec, collection):
    """Recognize builtin resource fields without imposing an extension grammar."""
    if operator == "$facet" and isinstance(spec, dict):
        return [
            (nested, collection) for nested in spec.values() if isinstance(nested, list)
        ]
    if operator not in {"$lookup", "$unionWith"} or not isinstance(spec, dict):
        return []
    nested = spec.get("pipeline")
    if not isinstance(nested, list):
        return []
    foreign = (
        spec.get("from") if operator == "$lookup" else spec.get("coll") or collection
    )
    if operator == "$unionWith" and _documents_source(nested):
        foreign = None
    return [(nested, foreign if isinstance(foreign, str) else None)]


def _documents_source(pipeline):
    return bool(
        isinstance(pipeline, list)
        and pipeline
        and isinstance(pipeline[0], dict)
        and "$documents" in pipeline[0]
    )


def _discover_stage_requests(operator, spec, collection):
    """Acquire the same resources for builtins and extensions delegating to them."""
    requests = []
    if operator in INFORMATION_STAGES:
        storage = (
            spec.get("storageStats")
            if operator == "$collStats" and isinstance(spec, dict)
            else None
        )
        scale = storage.get("scale", 1) if isinstance(storage, dict) else 1
        if not isinstance(scale, int) or isinstance(scale, bool) or scale <= 0:
            scale = 1
        requests.append(ResourceRequest(collection, operator, scale))
    elif operator == "$lookup" and isinstance(spec, dict):
        foreign = spec.get("from")
        if isinstance(foreign, str):
            requests.append(ResourceRequest(foreign, "documents"))
    elif operator == "$unionWith":
        foreign = spec if isinstance(spec, str) else None
        if isinstance(spec, dict) and not _documents_source(spec.get("pipeline", [])):
            foreign = spec.get("coll") or collection
        if isinstance(foreign, str):
            requests.append(ResourceRequest(foreign, "documents"))
    for nested, foreign in _resource_children(operator, spec, collection):
        if isinstance(nested, PreparedPipeline) and nested.collection == foreign:
            requests.extend(nested.requests)
            continue
        for stage in nested:
            if isinstance(stage, dict) and len(stage) == 1:
                child_operator, child_spec = next(iter(stage.items()))
                requests.extend(
                    _discover_stage_requests(child_operator, child_spec, foreign)
                )
    return requests


def prepare_pipeline(
    pipeline,
    *,
    dialect=MONGODB_DIALECT_70,
    collection=_UNSPECIFIED,
    scope=_UNSPECIFIED,
    path=_UNSPECIFIED,
):
    """Parse built-in join shapes before selecting a physical execution strategy."""
    previous = (
        pipeline.context
        if isinstance(pipeline, PreparedPipeline)
        else getattr(pipeline[0], "context", None)
        if pipeline
        else None
    )
    if collection is _UNSPECIFIED:
        collection = previous.collection if previous is not None else None
    if scope is _UNSPECIFIED:
        scopes = previous.scopes if previous is not None else ()
    else:
        scopes = (
            (() if scope == "root" else (scope,))
            if isinstance(scope, str)
            else tuple(scope)
        )
    if path is _UNSPECIFIED:
        path = previous.path if previous is not None else ()
    context = PreparationContext(
        collection, scopes, path, _aggregation_stage_registry_version()
    )
    relocating = previous is not None and (
        previous.collection != collection
        or previous.scopes != scopes
        or previous.path != path
    )
    if (
        isinstance(pipeline, PreparedPipeline)
        and pipeline.dialect is dialect
        and pipeline.context == context
    ):
        return pipeline
    stages, addresses, requests = [], [], []
    for index, stage in enumerate(pipeline):
        operator, raw_spec = _require_stage(stage)
        spec = _copy_spec(raw_spec)
        address = (
            StageAddress((*path, index), index)
            if relocating
            else getattr(stage, "address", StageAddress((*path, index), index))
        )
        if get_registered_aggregation_stage_registration(operator) is None:
            _validate_placement(operator, address, scopes)
            if operator in {"$lookup", "$unionWith"}:
                spec = _prepare_join(
                    operator, spec, dialect=dialect, address=address, context=context
                )
            elif operator == "$facet":
                spec = _prepare_facet(
                    spec, dialect=dialect, address=address, context=context
                )
            elif operator in INFORMATION_STAGES:
                parse_information_spec(operator, spec)
        requests.extend(_discover_stage_requests(operator, spec, collection))
        stages.append(PreparedStage(operator, spec, address, context))
        addresses.append(address)
    return PreparedPipeline(
        stages, addresses, requests, dialect=dialect, context=context
    )


def _validate_documents_namespace(
    collection, pipeline, dialect: MongoDialect, operator
):
    if (
        collection is not None
        and pipeline
        and "$documents" in pipeline[0]
        and dialect.server_version.startswith("8.")
    ):
        message = f"{operator} with $documents must omit its collection namespace"
        raise OperationFailure(message)
