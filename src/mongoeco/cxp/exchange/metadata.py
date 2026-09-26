"""Mongoeco-owned shape checks for metadata before exchange projection."""

from __future__ import annotations

from typing import Literal

import msgspec


class MongoReadMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    embedded: bool | None = None
    sync: bool | None = None
    async_: bool | None = msgspec.field(name="async", default=None)
    query_field_operators: tuple[str, ...] | None = msgspec.field(
        name="queryFieldOperators", default=None
    )
    query_top_level_operators: tuple[str, ...] | None = msgspec.field(
        name="queryTopLevelOperators", default=None
    )
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )


READ_OPERATION_NAMES = frozenset(
    {"find", "find_one", "count_documents", "estimated_document_count", "distinct"}
)
READ_OPERATION_FIELDS = frozenset(
    {
        "acceptedNoopOptions",
        "acceptsBatchSize",
        "acceptsCollation",
        "acceptsComment",
        "acceptsFieldPath",
        "acceptsFilter",
        "acceptsHint",
        "acceptsLet",
        "acceptsLimit",
        "acceptsMaxTimeMs",
        "acceptsProjection",
        "acceptsSkip",
        "acceptsSort",
        "collectionScoped",
        "supportedOptions",
        "supportsExplain",
        "supportsSession",
        "unsupportedOptions",
    }
)


class MongoReadOperationMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    accepted_noop_options: tuple[str, ...] | None = msgspec.field(
        name="acceptedNoopOptions", default=None
    )
    accepts_batch_size: bool | None = msgspec.field(
        name="acceptsBatchSize", default=None
    )
    accepts_collation: bool | None = msgspec.field(
        name="acceptsCollation", default=None
    )
    accepts_comment: bool | None = msgspec.field(name="acceptsComment", default=None)
    accepts_field_path: bool | None = msgspec.field(
        name="acceptsFieldPath", default=None
    )
    accepts_filter: bool | None = msgspec.field(name="acceptsFilter", default=None)
    accepts_hint: bool | None = msgspec.field(name="acceptsHint", default=None)
    accepts_let: bool | None = msgspec.field(name="acceptsLet", default=None)
    accepts_limit: bool | None = msgspec.field(name="acceptsLimit", default=None)
    accepts_max_time_ms: bool | None = msgspec.field(
        name="acceptsMaxTimeMs", default=None
    )
    accepts_projection: bool | None = msgspec.field(
        name="acceptsProjection", default=None
    )
    accepts_skip: bool | None = msgspec.field(name="acceptsSkip", default=None)
    accepts_sort: bool | None = msgspec.field(name="acceptsSort", default=None)
    collection_scoped: bool | None = msgspec.field(
        name="collectionScoped", default=None
    )
    result_type: str | None = msgspec.field(name="resultType", default=None)
    supported_options: tuple[str, ...] | None = msgspec.field(
        name="supportedOptions", default=None
    )
    supports_explain: bool | None = msgspec.field(
        name="supportsExplain", default=None
    )
    supports_session: bool | None = msgspec.field(
        name="supportsSession", default=None
    )
    unsupported_options: tuple[str, ...] | None = msgspec.field(
        name="unsupportedOptions", default=None
    )


class MongoWriteMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    embedded: bool | None = None
    sync: bool | None = None
    async_: bool | None = msgspec.field(name="async", default=None)
    update_operators: tuple[str, ...] | None = msgspec.field(
        name="updateOperators", default=None
    )
    supports_pipeline_update: bool | None = msgspec.field(
        name="supportsPipelineUpdate", default=None
    )
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )


class MongoTransactionsMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    embedded: bool | None = None
    sync: bool | None = None
    async_: bool | None = msgspec.field(name="async", default=None)
    distributed: bool | None = None
    mode: str | None = None


class MongoChangeStreamsMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    implementation: str | None = None
    distributed: bool | None = None
    persistent: bool | None = None
    resumable: bool | None = None
    resumable_across_client_restarts: bool | None = msgspec.field(
        name="resumableAcrossClientRestarts", default=None
    )
    resumable_across_processes: bool | None = msgspec.field(
        name="resumableAcrossProcesses", default=None
    )
    resumable_across_nodes: bool | None = msgspec.field(
        name="resumableAcrossNodes", default=None
    )
    bounded_history: bool | None = msgspec.field(name="boundedHistory", default=None)


class MongoAggregationMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    supported_stages: tuple[str, ...] = msgspec.field(name="supportedStages")
    supported_expression_operators: tuple[str, ...] = msgspec.field(
        name="supportedExpressionOperators", default=()
    )
    supported_group_accumulators: tuple[str, ...] = msgspec.field(
        name="supportedGroupAccumulators", default=()
    )
    supported_window_accumulators: tuple[str, ...] = msgspec.field(
        name="supportedWindowAccumulators", default=()
    )
    embedded: bool | None = None
    sync: bool | None = None
    async_: bool | None = msgspec.field(name="async", default=None)
    explainable: bool | None = None
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )


class MongoSearchMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    operators: tuple[str, ...]
    aggregate_stage: Literal["$search"] = msgspec.field(
        name="aggregateStage", default="$search"
    )
    field_mappings: tuple[str, ...] | None = msgspec.field(
        name="fieldMappings", default=None
    )
    structured_field_mappings: tuple[str, ...] | None = msgspec.field(
        name="structuredFieldMappings", default=None
    )
    textual_field_mappings: tuple[str, ...] | None = msgspec.field(
        name="textualFieldMappings", default=None
    )
    exact_filter_field_mappings: tuple[str, ...] | None = msgspec.field(
        name="exactFilterFieldMappings", default=None
    )
    structured_parent_path_operators: tuple[str, ...] | None = msgspec.field(
        name="structuredParentPathOperators", default=None
    )
    explain_features: tuple[str, ...] | None = msgspec.field(
        name="explainFeatures", default=None
    )
    operator_semantics: dict[str, object] | None = msgspec.field(
        name="operatorSemantics", default=None
    )
    text_search_tier: str | None = msgspec.field(name="textSearchTier", default=None)
    stage_options: dict[str, object] | None = msgspec.field(
        name="stageOptions", default=None
    )
    advanced_atlas_like_gaps: tuple[str, ...] | None = msgspec.field(
        name="advancedAtlasLikeGaps", default=None
    )
    sqlite_backends: tuple[str, ...] | None = msgspec.field(
        name="sqliteBackends", default=None
    )
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )
    note: str | None = None


class MongoVectorSearchMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    similarities: tuple[str, ...]
    aggregate_stage: Literal["$vectorSearch"] = msgspec.field(
        name="aggregateStage", default="$vectorSearch"
    )
    backend: str | None = None
    mode: str | None = None
    filter_mode: str | None = msgspec.field(name="filterMode", default=None)
    fallback: str | None = None
    hybrid_filter_modes: tuple[str, ...] | None = msgspec.field(
        name="hybridFilterModes", default=None
    )
    explain_features: tuple[str, ...] | None = msgspec.field(
        name="explainFeatures", default=None
    )
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )
    note: str | None = None


class MongoCollationMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    backend: dict[str, object] = msgspec.field(default_factory=dict)
    capabilities: dict[str, object] = msgspec.field(default_factory=dict)
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )


class MongoPersistenceMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    persistent: bool
    storage_engine: str = msgspec.field(name="storageEngine")
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )


class MongoTopologyDiscoveryMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    topology_type: str = msgspec.field(name="topologyType")
    server_count: int = msgspec.field(name="serverCount")
    sdam: dict[str, object] = msgspec.field(default_factory=dict)
    operation_metadata: dict[str, object] | None = msgspec.field(
        name="operationMetadata", default=None
    )


METADATA_SCHEMAS: dict[str, type[msgspec.Struct]] = {
    "read": MongoReadMetadata,
    "write": MongoWriteMetadata,
    "transactions": MongoTransactionsMetadata,
    "change_streams": MongoChangeStreamsMetadata,
    "aggregation": MongoAggregationMetadata,
    "search": MongoSearchMetadata,
    "vector_search": MongoVectorSearchMetadata,
    "collation": MongoCollationMetadata,
    "persistence": MongoPersistenceMetadata,
    "topology_discovery": MongoTopologyDiscoveryMetadata,
}


def validate_mongodb_metadata(capability: str, metadata: dict[str, object]) -> None:
    """Check authored values, retaining only authored keys for compatibility."""
    if not isinstance(metadata, dict) or not all(
        isinstance(key, str) for key in metadata
    ):
        message = "MongoDB capability metadata must be a string-keyed object"
        raise ValueError(message)
    schema = METADATA_SCHEMAS.get(capability)
    if schema is not None:
        try:
            msgspec.convert(msgspec.to_builtins(metadata), type=schema, strict=True)
            if capability == "read":
                operations = metadata.get("operationMetadata")
                if isinstance(operations, dict):
                    if set(operations) - READ_OPERATION_NAMES:
                        message = "Unknown read operation metadata"
                        raise ValueError(message)
                    for operation in operations.values():
                        if not isinstance(operation, dict) or not all(
                            isinstance(key, str) for key in operation
                        ):
                            message = "Read operation metadata must be an object"
                            raise ValueError(message)
                        msgspec.convert(
                            msgspec.to_builtins(operation),
                            type=MongoReadOperationMetadata,
                            strict=True,
                        )
        except (
            TypeError,
            ValueError,
            msgspec.ValidationError,
            RecursionError,
        ) as error:
            message = f"Invalid MongoDB {capability} metadata"
            raise ValueError(message) from error
