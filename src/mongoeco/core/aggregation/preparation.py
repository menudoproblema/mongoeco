"""Validate logical pipelines before physical slicing or reading any rows."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.core.aggregation.accumulators import _validate_empty_group_fields
from mongoeco.core.aggregation.array_string_expressions import (
    array_variable_names,
    validate_trim_chars,
)
from mongoeco.core.aggregation.extensions import (
    _aggregation_stage_registry_version,
    get_registered_aggregation_expression_operator,
    get_registered_aggregation_stage_registration,
)
from mongoeco.core.aggregation.grouping_stages import validate_date_range_window
from mongoeco.core.aggregation.information_stages import (
    INFORMATION_STAGES,
    parse_information_spec,
)
from mongoeco.core.aggregation.planning import (
    _require_stage,
    _validate_densify_partition_paths,
    _validate_densify_range,
)
from mongoeco.core.aggregation.runtime import (
    _MISSING,
    _REMOVE,
    _parse_variable_reference,
    _validate_variable_path,
    _require_lookup_spec,
    _require_union_with_spec,
)
from mongoeco.core.aggregation.scalar_expressions import validate_convert_spec
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
    variables: frozenset[str] = frozenset()


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
            variables=context.variables | frozenset(spec.get("let", {})),
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
            variables=context.variables,
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


_GLOBAL_EXPRESSION_VARIABLES = frozenset(
    {
        "ROOT",
        "CURRENT",
        "NOW",
        "REMOVE",
        "KEEP",
        "PRUNE",
        "DESCEND",
        "CLUSTER_TIME",
    }
)


def _validate_array_expression_scope(operator, spec, dialect, variables):
    names = array_variable_names(operator, spec, dialect=dialect)
    _validate_expression_variables(spec["input"], dialect, variables)
    if operator == "$reduce":
        _validate_expression_variables(spec["initialValue"], dialect, variables)
    if operator == "$filter" and "limit" in spec:
        _validate_expression_variables(spec["limit"], dialect, variables)
    body = spec["cond"] if operator == "$filter" else spec["in"]
    _validate_expression_variables(body, dialect, variables | frozenset(names.values()))


def _validate_variable_reference(expression, variables, dialect):
    if expression.startswith("$$"):
        name, path = _parse_variable_reference(expression)
        if name == "CLUSTER_TIME" and dialect.behavior_flag(
            "rejects_cluster_time_expression", default=False
        ):
            message = (
                "Builtin variable '$$CLUSTER_TIME' is not available "
                "on a standalone runtime"
            )
            raise OperationFailure(
                message, code=10071200, details={"codeName": "Location10071200"}
            )
        if name not in variables | _GLOBAL_EXPRESSION_VARIABLES:
            message = f"Use of undefined variable: {name}"
            raise OperationFailure(
                message, code=17276, details={"codeName": "Location17276"}
            )
        _validate_variable_path(expression, path)


def _validate_expression_variables(expression, dialect, variables):
    if isinstance(expression, str):
        _validate_variable_reference(expression, variables, dialect)
        return
    if isinstance(expression, list):
        for item in expression:
            _validate_expression_variables(item, dialect, variables)
        return
    if not isinstance(expression, dict):
        return
    if len(expression) == 1:
        operator, spec = next(iter(expression.items()))
        if (
            operator in {"$literal", "$const"}
            or get_registered_aggregation_expression_operator(operator) is not None
        ):
            return
        if operator in {"$map", "$filter", "$reduce"}:
            _validate_array_expression_scope(operator, spec, dialect, variables)
            return
        if (
            operator == "$let"
            and isinstance(spec, dict)
            and isinstance(spec.get("vars"), dict)
        ):
            for value in spec["vars"].values():
                _validate_expression_variables(value, dialect, variables)
            _validate_expression_variables(
                spec.get("in"), dialect, variables | frozenset(spec["vars"])
            )
            return
    for value in expression.values():
        _validate_expression_variables(value, dialect, variables)
    if "$convert" in expression and dialect.behavior_flag(
        "uses_extended_conversions", default=False
    ):
        validate_convert_spec(expression["$convert"])
    _validate_static_trim_expression(expression, dialect)


def _validate_static_trim_expression(expression, dialect):
    for operator in ("$trim", "$ltrim", "$rtrim"):
        spec = expression.get(operator)
        if not isinstance(spec, dict) or spec.get("input") is None:
            continue
        chars = spec.get("chars")
        constant = isinstance(chars, dict) and (
            set(chars) == {"$literal"} or set(chars) == {"$const"}
        )
        if constant:
            chars = next(iter(chars.values()))
        if isinstance(chars, str) and (constant or not chars.startswith("$")):
            validate_trim_chars(chars, dialect=dialect)


def _validate_match_expression_variables(spec, dialect, variables):
    if not isinstance(spec, dict):
        return
    for key, value in spec.items():
        if key == "$expr":
            _validate_expression_variables(value, dialect, variables)
        elif key in {"$and", "$or", "$nor"} and isinstance(value, list):
            for clause in value:
                _validate_match_expression_variables(clause, dialect, variables)


def _validate_stage_expression_variables(operator, spec, dialect, variables):
    if not dialect.behavior_flag("validates_aggregation_syntax_early", default=False):
        return
    if operator == "$match":
        _validate_match_expression_variables(spec, dialect, variables)
    elif operator in {
        "$project",
        "$set",
        "$addFields",
        "$group",
        "$redact",
        "$replaceRoot",
        "$replaceWith",
    }:
        _validate_expression_variables(spec, dialect, variables)
    elif operator == "$lookup" and isinstance(spec, dict):
        _validate_expression_variables(spec.get("let", {}), dialect, variables)
    elif operator in {"$bucket", "$bucketAuto"} and isinstance(spec, dict):
        _validate_expression_variables(spec.get("groupBy"), dialect, variables)
        _validate_expression_variables(spec.get("output", {}), dialect, variables)
    elif operator == "$setWindowFields" and isinstance(spec, dict):
        if dialect.behavior_flag("supports_date_range_windows", default=False):
            outputs = spec.get("output", {})
            if not isinstance(outputs, dict):
                message = "$setWindowFields output must be a document"
                raise OperationFailure(
                    message, code=14, details={"codeName": "TypeMismatch"}
                )
            for output in outputs.values():
                if isinstance(output, dict):
                    validate_date_range_window(output.get("window"))
        _validate_expression_variables(spec.get("partitionBy"), dialect, variables)
        _validate_expression_variables(spec.get("output", {}), dialect, variables)


def _prepare_builtin_stage(operator, spec, *, dialect, address, context):
    _validate_placement(operator, address, context.scopes)
    if operator in {"$lookup", "$unionWith"}:
        spec = _prepare_join(
            operator, spec, dialect=dialect, address=address, context=context
        )
    elif operator == "$facet":
        spec = _prepare_facet(spec, dialect=dialect, address=address, context=context)
    elif operator in INFORMATION_STAGES:
        parse_information_spec(operator, spec)
    elif operator == "$group":
        _validate_empty_group_fields(spec, dialect=dialect)
    elif operator == "$densify":
        _validate_densify_partition_paths(spec, dialect=dialect)
        if isinstance(spec, dict):
            _validate_densify_range(spec.get("range"))
    _validate_stage_expression_variables(operator, spec, dialect, context.variables)
    return spec


def prepare_pipeline(  # noqa: PLR0913 - logical address and lexical scope boundary
    pipeline,
    *,
    dialect=MONGODB_DIALECT_70,
    collection=_UNSPECIFIED,
    scope=_UNSPECIFIED,
    path=_UNSPECIFIED,
    variables=_UNSPECIFIED,
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
    if variables is _UNSPECIFIED:
        variables = previous.variables if previous is not None else frozenset()
    context = PreparationContext(
        collection,
        scopes,
        path,
        _aggregation_stage_registry_version(),
        frozenset(variables),
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
            spec = _prepare_builtin_stage(
                operator, spec, dialect=dialect, address=address, context=context
            )
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
        and dialect.behavior_flag(
            "documents_join_omits_collection_namespace", default=False
        )
    ):
        message = f"{operator} with $documents must omit its collection namespace"
        raise OperationFailure(message)
