"""Validate logical pipelines before physical slicing or reading any rows."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass

from mongoeco.compat import MONGODB_DIALECT_70, MongoDialect
from mongoeco.compat._catalog_operators import (
    ARRAY_ACCESS_EXPRESSION_ARGUMENTS,
    CONTROL_EXPRESSION_ARGUMENTS,
    SCALAR_EXPRESSION_ARGUMENTS,
    SUPPORTED_AGGREGATION_STAGES,
)
from mongoeco.core._search_contract import SEARCH_STAGE_OPERATORS
from mongoeco.core.aggregation.accumulators import (
    _prepare_accumulator_specs,
    _validate_empty_group_fields,
)
from mongoeco.core.aggregation.array_string_expressions import (
    array_variable_names,
    validate_trim_chars,
)
from mongoeco.core.aggregation.control_object_expressions import (
    parse_field_expression,
    prepare_field_expression,
)
from mongoeco.core.aggregation.evaluation_environment import environment_for_document
from mongoeco.core.aggregation.extensions import (
    _aggregation_registry_version,
    get_registered_aggregation_expression_operator,
    get_registered_aggregation_stage_registration,
)
from mongoeco.core.aggregation.group_average import (
    group_average_prefix_compatible,
    group_average_uses_sum,
)
from mongoeco.core.aggregation.grouping_stages import validate_date_range_window
from mongoeco.core.aggregation.information_stages import (
    INFORMATION_STAGES,
    parse_information_spec,
)
from mongoeco.core.aggregation.planning import (
    _require_group_spec,
    _require_projection_for_dialect,
    _require_replace_root_spec,
    _require_stage,
    _validate_densify_partition_paths,
    _validate_densify_range,
)
from mongoeco.core.aggregation.runtime import (
    _parse_variable_reference,
    _require_expression_args,
    _require_lookup_spec,
    _require_union_with_spec,
    _validate_variable_path,
    evaluate_expression,
)
from mongoeco.core.aggregation.scalar_expressions import validate_convert_spec
from mongoeco.core.aggregation.write_validation import validate_merge_mode_pair
from mongoeco.core.bson_values import (
    binary_parts,
    code_parts,
    document_mapping,
    is_bson_string,
)
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.error_documents import command_error_document, error_message
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.server_errors import operation_error, server_error
from mongoeco.errors import OperationFailure


_UNSPECIFIED = object()
_MATCH_EXPRESSION_DOCUMENT_CODE = 15983
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
        self.group_average_uses_sum = False


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


_UNDEFINED_VARIABLE_CODE = 17276


def _copy_spec(value):
    if isinstance(value, PreparedPipeline):
        return deepcopy(value)
    if isinstance(value, dict):
        return {key: _copy_spec(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_copy_spec(item) for item in value]
    return deepcopy(value)


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
    requires_document = _validate_expression_variables(
        spec["input"], dialect, variables
    )
    if operator == "$reduce":
        requires_document |= _validate_expression_variables(
            spec["initialValue"], dialect, variables
        )
    if operator == "$filter" and "limit" in spec:
        requires_document |= _validate_expression_variables(
            spec["limit"], dialect, variables
        )
    body = spec["cond"] if operator == "$filter" else spec["in"]
    return requires_document | _validate_expression_variables(
        body,
        dialect,
        variables
        | frozenset(names.values())
        | {spec.get("as", "this")}
        | ({"value"} if operator == "$reduce" else set()),
    )


def _validate_variable_reference(expression, variables, dialect):
    if expression.startswith("$$"):
        name, path = _parse_variable_reference(expression)
        if name == "CLUSTER_TIME":
            message = (
                "Builtin variable '$$CLUSTER_TIME' is not available "
                "on a standalone runtime"
            )
            code = (
                10071200
                if dialect.behavior_flag(
                    "rejects_cluster_time_expression", default=False
                )
                else 51144
            )
            raise server_error(message, code=code, reason="cluster_time_unavailable")
        if name not in variables | _GLOBAL_EXPRESSION_VARIABLES:
            message = f"Use of undefined variable: {name}"
            raise OperationFailure(
                message,
                code=17276,
                details={
                    "ok": 0.0,
                    "code": 17276,
                    "codeName": "Location17276",
                    "errmsg": f"Use of undefined variable: {name}",
                },
            )
        _validate_variable_path(expression, path)


def _validate_expression_values(values, dialect, variables):
    requires_document = False
    for value in values:
        requires_document |= _validate_expression_variables(value, dialect, variables)
    return requires_document


def _expression_scalar_requires_document(expression, dialect, variables):
    if not is_bson_string(expression):
        return False
    _validate_variable_reference(expression, variables, dialect)
    if expression.startswith("$$"):
        name, _path = _parse_variable_reference(expression)
        return name in {"ROOT", "CURRENT"} and name not in variables
    return expression.startswith("$") and "CURRENT" not in variables


def _validate_expression_variables(expression, dialect, variables):  # noqa: PLR0911 - distinct expression grammars and lexical scopes
    if not isinstance(expression, (dict, list)):
        return _expression_scalar_requires_document(expression, dialect, variables)
    if isinstance(expression, list):
        return _validate_expression_values(expression, dialect, variables)
    if len(expression) == 1:
        operator, spec = next(iter(expression.items()))
        if (
            operator in {"$literal", "$const"}
            or get_registered_aggregation_expression_operator(operator) is not None
        ):
            return False
        if operator in {"$getField", "$setField", "$unsetField"}:
            dependencies = []
            parse_field_expression(
                operator,
                spec,
                parse_operand=lambda operand: dependencies.append(
                    prepare_expression(
                        operand,
                        dialect=dialect,
                        variables=variables,
                        fold_constants=False,
                    )
                ),
            )
            return any(dependencies)
        if operator in {"$map", "$filter", "$reduce"}:
            return _validate_array_expression_scope(operator, spec, dialect, variables)
        if (
            operator == "$let"
            and isinstance(spec, dict)
            and isinstance(spec.get("vars"), dict)
        ):
            requires_document = _validate_expression_values(
                spec["vars"].values(), dialect, variables
            )
            return requires_document | _validate_expression_variables(
                spec.get("in"), dialect, variables | frozenset(spec["vars"])
            )
    requires_document = _validate_expression_values(
        expression.values(), dialect, variables
    )
    if "$convert" in expression and dialect.behavior_flag(
        "uses_extended_conversions", default=False
    ):
        validate_convert_spec(expression["$convert"])
    _validate_static_trim_expression(expression, dialect)
    return requires_document


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
    if (
        operator == "$setWindowFields"
        and isinstance(spec, dict)
        and not isinstance(spec.get("output", {}), dict)
    ):
        message = "$setWindowFields output must be a document"
        raise OperationFailure(message, code=14)
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
        outputs = spec.get("output", {})
        if dialect.behavior_flag("supports_date_range_windows", default=False):
            for output in outputs.values():
                if isinstance(output, dict):
                    validate_date_range_window(output.get("window"))
        _validate_expression_variables(spec.get("partitionBy"), dialect, variables)
        _validate_expression_variables(spec.get("output", {}), dialect, variables)


def _validate_stage_expression_roots(operator, spec, dialect):
    if operator != "$project" or not isinstance(spec, dict):
        return
    for expression in spec.values():
        if not isinstance(expression, dict) or len(expression) != 1:
            continue
        name = next(iter(expression))
        if (
            not isinstance(name, str)
            or not name.startswith("$")
            or name in {"$const", "$meta"}
            or dialect.supports_aggregation_expression_operator(name)
            or get_registered_aggregation_expression_operator(name) is not None
        ):
            continue
        message, code = (
            ("Cannot use $elemMatch in this context", 168)
            if name == "$elemMatch"
            else (f"Unknown expression {name}", 31325)
        )
        raise server_error(message, code=code, reason="expression_unknown")


def _validate_stage_structure(operator, spec, dialect):
    # Recognition is distinct from a physical dialect's capabilities. Search
    # stages are parsed by their existing local planner, not Community Server.
    if (
        operator not in SUPPORTED_AGGREGATION_STAGES
        and operator not in SEARCH_STAGE_OPERATORS
    ):
        message = f"Unrecognized pipeline stage name: '{operator}'"
        raise server_error(message, code=40324)
    if operator == "$match" and not isinstance(spec, dict):
        message = "$match requires an expression object"
        raise server_error(message, code=15959)
    if operator == "$project":
        _require_projection_for_dialect(spec, dialect=dialect)
    elif operator == "$replaceRoot":
        _require_replace_root_spec(spec)


def _prepare_builtin_stage(operator, spec, *, dialect, address, context):
    if operator in {"$lookup", "$unionWith"}:
        spec = _prepare_join(
            operator, spec, dialect=dialect, address=address, context=context
        )
    elif operator == "$facet":
        spec = _prepare_facet(spec, dialect=dialect, address=address, context=context)
    elif operator in INFORMATION_STAGES:
        parse_information_spec(operator, spec)
    elif operator == "$group":
        _require_group_spec(spec)
        _validate_empty_group_fields(spec, dialect=dialect)
        if isinstance(spec, dict):
            _prepare_accumulator_specs(
                {name: value for name, value in spec.items() if name != "_id"},
                dialect=dialect,
            )
    elif operator == "$densify":
        _validate_densify_partition_paths(spec, dialect=dialect)
        if isinstance(spec, dict):
            _validate_densify_range(spec.get("range"))
    if operator == "$match":
        from mongoeco.core.query_plan import (  # noqa: PLC0415 - expression/query preparation cycle
            compile_filter,
        )

        try:
            compile_filter(
                spec, dialect=dialect, variables=dict.fromkeys(context.variables)
            )
        except OperationFailure as error:
            raise operation_error(error, "aggregate_match", dialect.key) from error
    _validate_stage_expression_variables(operator, spec, dialect, context.variables)
    if dialect.behavior_flag(
        "validates_aggregation_syntax_early", default=True
    ) and operator in {
        "$set",
        "$addFields",
        "$project",
        "$replaceRoot",
        "$replaceWith",
    }:
        _validate_stage_expression_roots(operator, spec, dialect)
        prepare_expression(
            spec["newRoot"] if operator == "$replaceRoot" else spec,
            dialect=dialect,
            variables=context.variables,
            fold_constants="update" not in context.scopes,
        )
    return spec


def _parse_stage_containers(pipeline, dialect, context, relocating):
    parsed = []
    for index, stage in enumerate(pipeline):
        operator, raw_spec = _require_stage(stage)
        address = (
            StageAddress((*context.path, index), index)
            if relocating
            else getattr(stage, "address", StageAddress((*context.path, index), index))
        )
        builtin = get_registered_aggregation_stage_registration(operator) is None
        if builtin:
            _validate_stage_structure(operator, raw_spec, dialect)
            _validate_placement(operator, address, context.scopes)
            if operator == "$merge":
                validate_merge_mode_pair(raw_spec)
        parsed.append((stage, operator, raw_spec, address, builtin))
    return parsed


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
        _aggregation_registry_version(),
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
    # Parse every stage container before optimizing any stage expression.
    # In particular, a later invalid write policy precedes a constant-fold error.
    parsed = _parse_stage_containers(pipeline, dialect, context, relocating)
    stages, addresses, requests = [], [], []
    average_prefix_compatible = (
        dialect.server_version in {"8.0", "9.0"}
        and not ({"$facet", "$lookup"} & set(scopes))
    )
    for stage, operator, raw_spec, address, builtin in parsed:
        spec = _copy_spec(raw_spec)
        if builtin:
            try:
                spec = _prepare_builtin_stage(
                    operator, spec, dialect=dialect, address=address, context=context
                )
            except OperationFailure as error:
                diagnostic = getattr(error, "_server_diagnostic", None)
                parsing = (
                    diagnostic is not None
                    and diagnostic.reason
                    in {
                        "expression_unknown",
                        "expression_arity",
                        "expression_structure",
                    }
                    and operator in {"$project", "$set", "$addFields"}
                )
                if (
                    error.code != _UNDEFINED_VARIABLE_CODE or operator == "$match"
                ) and not parsing:
                    raise
                message = f"Invalid {operator} :: caused by :: {error_message(error)}"
                if parsing:
                    raise server_error(
                        message, code=error.code, reason="expression_parse"
                    ) from error
                details = command_error_document(error)
                details["errmsg"] = message
                raise OperationFailure(
                    message, code=error.code, details=details
                ) from error
        requests.extend(_discover_stage_requests(operator, spec, collection))
        prepared_stage = PreparedStage(operator, spec, address, context)
        if operator == "$group":
            prepared_stage.group_average_uses_sum = (
                stage.group_average_uses_sum
                if isinstance(stage, PreparedStage) and not relocating
                and stage.context == context
                else average_prefix_compatible
                and group_average_uses_sum(spec, dialect=dialect)
            )
        average_prefix_compatible = (
            average_prefix_compatible
            and group_average_prefix_compatible(operator, spec, dialect=dialect)
        )
        stages.append(prepared_stage)
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


# Preparation is structural; constant evaluation is restricted to pure scalar
# expressions and is enabled only in native phases that optimize constants.
_EXPRESSION_ARITIES = {
    **dict.fromkeys(
        (
            "$divide",
            "$mod",
            "$subtract",
            "$pow",
            "$log",
            "$split",
        ),
        (2, 2),
    ),
    **dict.fromkeys(
        (
            "$abs",
            "$exp",
            "$ln",
            "$log10",
            "$sqrt",
            "$floor",
            "$ceil",
            "$toInt",
        ),
        (1, 1),
    ),
    "$round": (1, 2),
    "$trunc": (1, 2),
    "$range": (2, 3),
    **SCALAR_EXPRESSION_ARGUMENTS,
    **CONTROL_EXPRESSION_ARGUMENTS,
    **ARRAY_ACCESS_EXPRESSION_ARGUMENTS,
}
_PURE_CONSTANT_OPERATORS = frozenset(_EXPRESSION_ARITIES) - {"$range"} | {
    "$regexMatch",
    "$literal",
    "$const",
    "$add",
    "$multiply",
    "$concat",
    "$and",
    "$or",
    "$cond",
    "$convert",
}


def _pure_constant(value):
    if is_bson_string(value):
        return not value.startswith("$")
    if isinstance(value, list):
        return all(_pure_constant(item) for item in value)
    if not isinstance(value, dict):
        return True
    if len(value) == 1 and next(iter(value)) in {"$literal", "$const"}:
        return True
    return all(
        (
            isinstance(key, str)
            and (not key.startswith("$") or key in _PURE_CONSTANT_OPERATORS)
        )
        and _pure_constant(item)
        for key, item in value.items()
    )


def prepare_expression(
    expression, *, dialect, variables=(), fold_constants=False, document_code=40181
):
    """Validate expression syntax and lexical scope without requiring a row."""
    requires_document = False
    if variables is not None:
        requires_document = _validate_expression_variables(
            expression, dialect, frozenset(variables)
        )
    try:
        _prepare_expression_structure(
            expression,
            dialect,
            fold_constants=fold_constants,
            document_code=document_code,
        )
    except OperationFailure as error:
        diagnostic = getattr(error, "_server_diagnostic", None)
        if diagnostic is None or diagnostic.reason != "expression_type":
            raise
        prefix = (
            "Failed to optimize expression :: caused by :: "
            if document_code == _MATCH_EXPRESSION_DOCUMENT_CODE
            else "Failed to optimize pipeline :: caused by :: "
        )
        raise server_error(
            prefix + diagnostic.message,
            code=diagnostic.code,
            reason="expression_optimization",
        ) from error
    return requires_document


def _validate_literal_binary_lengths(value):
    """Expression constants have stricter subtype rules than stored fields."""
    document = document_mapping(value)
    code = code_parts(value)
    if document is not None:
        for item in document.values():
            _validate_literal_binary_lengths(item)
    elif code is not None and code[1] is not None:
        _validate_literal_binary_lengths(code[1])
    elif isinstance(value, list):
        for item in value:
            _validate_literal_binary_lengths(item)
    else:
        parts = binary_parts(value)
        required_length = 16
        if (
            parts is not None and parts[0] in {3, 5}
            and len(parts[1]) != required_length
        ):
            message = (
                f"BinData subtype {parts[0]} requires exactly 16 bytes "
                "as an expression literal"
            )
            raise server_error(message, code=9, reason="expression_binary_literal")


def _prepare_expression_structure(
    expression, dialect, *, fold_constants, document_code
):

    if isinstance(expression, list):
        for value in expression:
            _prepare_expression_structure(
                value,
                dialect,
                fold_constants=fold_constants,
                document_code=document_code,
            )
        return
    document = document_mapping(expression)
    if document is None:
        _validate_literal_binary_lengths(expression)
        return
    expression = document
    operator = next(iter(expression)) if len(expression) == 1 else None
    if len(expression) > 1 and any(
        isinstance(key, str) and key.startswith("$") for key in expression
    ):
        message = "An expression specification must contain exactly one field"
        raise server_error(message, code=document_code)
    if operator in {"$literal", "$const"}:
        _validate_literal_binary_lengths(expression[operator])
        return
    if (
        operator
        and get_registered_aggregation_expression_operator(operator) is not None
    ):
        return
    if (
        isinstance(operator, str)
        and operator.startswith("$")
        and not (
            dialect.supports_aggregation_expression_operator(operator)
            or operator == "$meta"
        )
    ):
        message = f"Unrecognized expression '{operator}'"
        raise server_error(message, code=168, reason="expression_unknown")
    if operator in _EXPRESSION_ARITIES:
        _require_expression_args(
            operator,
            expression[operator],
            min_args=_EXPRESSION_ARITIES[operator][0],
            max_args=_EXPRESSION_ARITIES[operator][1],
        )
    if operator in {"$getField", "$setField", "$unsetField"}:
        parse_field_expression(
            operator,
            expression[operator],
            parse_operand=lambda operand: _prepare_expression_structure(
                operand, dialect, fold_constants=False, document_code=document_code
            ),
        )
        prepare_field_expression(operator, expression[operator], dialect=dialect)
        return
    for value in expression.values():
        _prepare_expression_structure(
            value, dialect, fold_constants=fold_constants, document_code=document_code
        )
    if (
        fold_constants
        and operator in _PURE_CONSTANT_OPERATORS
        and _pure_constant(expression)
    ):
        evaluate_expression({}, expression, dialect=dialect)


def normalize_command_let(bindings):
    """Check command-variable names without interpreting evaluated internal frames."""
    if bindings is None or isinstance(bindings, ExpressionExecutionContext):
        return bindings
    if not isinstance(bindings, dict):
        message = "let must be a dict"
        raise TypeError(message)
    for name in bindings:
        if name in {"NOW", "CLUSTER_TIME"}:
            message = f"Attempt to set internal constant: {name}"
            raise server_error(message, code=4738901)
        if not isinstance(name, str) or not name:
            reason = "empty variable names are not allowed"
        elif name != "CURRENT" and not ("a" <= name[0] <= "z" or not name[0].isascii()):
            reason = (
                f"'{name}' starts with an invalid character for a user variable name"
            )
        else:
            invalid = next(
                (
                    char
                    for char in name[1:]
                    if char.isascii() and not (char.isalnum() or char == "_")
                ),
                None,
            )
            if invalid is None:
                continue
            reason = (
                f"'{name}' contains an invalid character for a variable name: "
                f"'{invalid}'"
            )
        raise server_error("Invalid 'let' parameter :: caused by :: " + reason, code=9)
    return DocumentCodec.to_internal(bindings)


def _command_let_expressions(bindings, *, variables, dialect):
    for name, expression in bindings.items():
        if prepare_expression(expression, dialect=dialect, variables=variables):
            message = (
                "Command let Expression tried to access a field, "
                "but this is not allowed because"
                "Command let Expressions run before the query examines any documents."
            )
            raise server_error(message, code=4890500)
        yield name, expression
        variables = variables | {name}


def validate_command_let_dependencies(bindings, *, dialect):
    if bindings is None or isinstance(bindings, ExpressionExecutionContext):
        return
    for _name, _expression in _command_let_expressions(
        bindings, variables=frozenset(), dialect=dialect
    ):
        pass


def prepare_command_let(bindings, *, context, dialect, write=False):
    """Evaluate a closed command program once; overlays remain literal values."""
    bindings = normalize_command_let(bindings)
    if isinstance(bindings, ExpressionExecutionContext):
        return bindings
    if not bindings:
        return context
    result = context
    for name, expression in _command_let_expressions(
        bindings, variables=frozenset(context.bindings), dialect=dialect
    ):
        frame = environment_for_document({}, result)
        if not write and not dialect.behavior_flag(
            "supports_now_in_read_command_let", default=True
        ):
            frame.unavailable_system_variables = frozenset({"NOW"})
        value = evaluate_expression({}, expression, frame, dialect=dialect)
        result = result.with_bindings({name: value})
    return ExpressionExecutionContext(
        result.bindings,
        now=context.now,
        command_let=bindings,
        command_let_base=context.bindings,
    )
