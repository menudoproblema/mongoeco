from importlib import import_module

_ACCUMULATOR_EXPORTS = (
    "_ACCUMULATOR_FLAGS_KEY",
    "_accumulator_flags",
    "_apply_accumulators",
    "_finalize_accumulators",
    "_initialize_accumulators",
)

_RUNTIME_EXPORTS = (
    "AggregationStageContext",
    "_CURRENT_COLLECTION_RESOLVER_KEY",
    "_MISSING",
    "_aggregation_key",
    "_bson_document_size",
    "_expression_truthy",
    "_resolve_aggregation_field_path",
    "evaluate_expression",
)

_PLANNING_EXPORTS = (
    "AggregationPushdown",
    "Pipeline",
    "PipelineStage",
    "_is_simple_projection",
    "_match_spec_contains_expr",
    "_require_projection",
    "split_pushdown_pipeline",
)

_COMPILED_PIPELINE_EXPORTS = (
    "CompiledPipelinePlan",
    "compile_pipeline",
)

_EXTENSION_EXPORTS = (
    "AggregationExpressionExtensionContext",
    "AggregationExpressionExtensionHandler",
    "AggregationStageExecutionMode",
    "AggregationStageExtensionHandler",
    "get_registered_aggregation_expression_operator",
    "get_registered_aggregation_stage",
    "register_aggregation_expression_operator",
    "register_aggregation_stage",
    "registered_aggregation_expression_operator",
    "registered_aggregation_stage",
    "unregister_aggregation_expression_operator",
    "unregister_aggregation_stage",
)

_SPILL_EXPORTS = (
    "BLOCKING_AGGREGATION_STAGES",
    "AggregationSpillPolicy",
)

_COST_EXPORTS = ("AggregationCostPolicy",)

_GROUPING_EXPORTS = ("_apply_group",)

_STAGE_EXPORTS = (
    "AGGREGATION_STAGE_HANDLERS",
    "AGGREGATION_STAGE_SPECS",
    "AggregationStageHandler",
    "AggregationStageSpec",
    "apply_pipeline",
    "get_aggregation_stage_spec",
    "has_materializing_aggregation_stage",
    "is_streamable_aggregation_stage",
)

__all__ = [
    "AGGREGATION_STAGE_HANDLERS",
    "AGGREGATION_STAGE_SPECS",
    "AggregationPushdown",
    "AggregationExpressionExtensionContext",
    "AggregationExpressionExtensionHandler",
    "AggregationStageExecutionMode",
    "AggregationStageContext",
    "AggregationStageExtensionHandler",
    "AggregationStageHandler",
    "AggregationStageSpec",
    "AggregationSpillPolicy",
    "AggregationCostPolicy",
    "BLOCKING_AGGREGATION_STAGES",
    "CompiledPipelinePlan",
    "Pipeline",
    "PipelineStage",
    "_ACCUMULATOR_FLAGS_KEY",
    "_CURRENT_COLLECTION_RESOLVER_KEY",
    "_MISSING",
    "_accumulator_flags",
    "_aggregation_key",
    "_apply_accumulators",
    "_apply_group",
    "_bson_document_size",
    "_expression_truthy",
    "_finalize_accumulators",
    "_initialize_accumulators",
    "_is_simple_projection",
    "_match_spec_contains_expr",
    "_require_projection",
    "_resolve_aggregation_field_path",
    "apply_pipeline",
    "compile_pipeline",
    "evaluate_expression",
    "get_aggregation_stage_spec",
    "has_materializing_aggregation_stage",
    "get_registered_aggregation_expression_operator",
    "get_registered_aggregation_stage",
    "is_streamable_aggregation_stage",
    "register_aggregation_expression_operator",
    "register_aggregation_stage",
    "registered_aggregation_expression_operator",
    "registered_aggregation_stage",
    "split_pushdown_pipeline",
    "unregister_aggregation_expression_operator",
    "unregister_aggregation_stage",
]

_EXPORT_MODULES = {
    **dict.fromkeys(_ACCUMULATOR_EXPORTS, "mongoeco.core.aggregation.accumulators"),
    **dict.fromkeys(_RUNTIME_EXPORTS, "mongoeco.core.aggregation.runtime"),
    **dict.fromkeys(_PLANNING_EXPORTS, "mongoeco.core.aggregation.planning"),
    **dict.fromkeys(
        _COMPILED_PIPELINE_EXPORTS,
        "mongoeco.core.aggregation.compiled_pipeline",
    ),
    **dict.fromkeys(_EXTENSION_EXPORTS, "mongoeco.core.aggregation.extensions"),
    **dict.fromkeys(_SPILL_EXPORTS, "mongoeco.core.aggregation.spill"),
    **dict.fromkeys(_COST_EXPORTS, "mongoeco.core.aggregation.cost"),
    **dict.fromkeys(_GROUPING_EXPORTS, "mongoeco.core.aggregation.grouping_stages"),
    **dict.fromkeys(_STAGE_EXPORTS, "mongoeco.core.aggregation.stages"),
}


def __getattr__(name: str):
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(name)
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted({*globals(), *__all__})
