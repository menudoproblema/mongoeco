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


WRITE_OPERATION_NAMES = frozenset(
    {
        "insert_one",
        "insert_many",
        "update_one",
        "update_many",
        "replace_one",
        "delete_one",
        "delete_many",
        "bulk_write",
    }
)
WRITE_OPERATION_FIELDS = frozenset(
    {
        "acceptedNoopOptions",
        "acceptsArrayFilters",
        "acceptsCollation",
        "acceptsComment",
        "acceptsDocument",
        "acceptsDocuments",
        "acceptsFilter",
        "acceptsHint",
        "acceptsLet",
        "acceptsOrderedExecution",
        "acceptsReplacementDocument",
        "acceptsSort",
        "acceptsUpdateDocument",
        "acceptsWriteModels",
        "collectionScoped",
        "supportedOptions",
        "supportedUpdateOperators",
        "supportsExplain",
        "supportsOrderedExecution",
        "supportsPipelineUpdate",
        "supportsReplacementDocument",
        "supportsSession",
        "supportsUpsert",
        "unsupportedOptions",
    }
)


class MongoWriteOperationMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    accepted_noop_options: tuple[str, ...] | None = msgspec.field(
        name="acceptedNoopOptions", default=None
    )
    accepts_array_filters: bool | None = msgspec.field(
        name="acceptsArrayFilters", default=None
    )
    accepts_collation: bool | None = msgspec.field(
        name="acceptsCollation", default=None
    )
    accepts_comment: bool | None = msgspec.field(name="acceptsComment", default=None)
    accepts_document: bool | None = msgspec.field(name="acceptsDocument", default=None)
    accepts_documents: bool | None = msgspec.field(
        name="acceptsDocuments", default=None
    )
    accepts_filter: bool | None = msgspec.field(name="acceptsFilter", default=None)
    accepts_hint: bool | None = msgspec.field(name="acceptsHint", default=None)
    accepts_let: bool | None = msgspec.field(name="acceptsLet", default=None)
    accepts_ordered_execution: bool | None = msgspec.field(
        name="acceptsOrderedExecution", default=None
    )
    accepts_replacement_document: bool | None = msgspec.field(
        name="acceptsReplacementDocument", default=None
    )
    accepts_sort: bool | None = msgspec.field(name="acceptsSort", default=None)
    accepts_update_document: bool | None = msgspec.field(
        name="acceptsUpdateDocument", default=None
    )
    accepts_write_models: bool | None = msgspec.field(
        name="acceptsWriteModels", default=None
    )
    collection_scoped: bool | None = msgspec.field(
        name="collectionScoped", default=None
    )
    result_type: str | None = msgspec.field(name="resultType", default=None)
    supported_options: tuple[str, ...] | None = msgspec.field(
        name="supportedOptions", default=None
    )
    supported_update_operators: tuple[str, ...] | None = msgspec.field(
        name="supportedUpdateOperators", default=None
    )
    supports_explain: bool | None = msgspec.field(
        name="supportsExplain", default=None
    )
    supports_ordered_execution: bool | None = msgspec.field(
        name="supportsOrderedExecution", default=None
    )
    supports_pipeline_update: bool | None = msgspec.field(
        name="supportsPipelineUpdate", default=None
    )
    supports_replacement_document: bool | None = msgspec.field(
        name="supportsReplacementDocument", default=None
    )
    supports_session: bool | None = msgspec.field(
        name="supportsSession", default=None
    )
    supports_upsert: bool | None = msgspec.field(name="supportsUpsert", default=None)
    unsupported_options: tuple[str, ...] | None = msgspec.field(
        name="unsupportedOptions", default=None
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


AGGREGATION_OPERATION_FIELDS = frozenset(
    {
        "acceptedNoopOptions",
        "acceptsPipeline",
        "supportedExpressionOperators",
        "supportedGroupAccumulators",
        "supportedOptions",
        "supportedStages",
        "supportedWindowAccumulators",
        "supportsCollectionScope",
        "supportsDatabaseScope",
        "supportsExplain",
        "supportsLeadingSearchStage",
        "supportsLeadingVectorSearchStage",
        "supportsSession",
        "unsupportedOptions",
    }
)


class MongoAggregationOperationMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    accepted_noop_options: tuple[str, ...] | None = msgspec.field(
        name="acceptedNoopOptions", default=None
    )
    accepts_pipeline: bool | None = msgspec.field(name="acceptsPipeline", default=None)
    result_type: str | None = msgspec.field(name="resultType", default=None)
    supported_expression_operators: tuple[str, ...] | None = msgspec.field(
        name="supportedExpressionOperators", default=None
    )
    supported_group_accumulators: tuple[str, ...] | None = msgspec.field(
        name="supportedGroupAccumulators", default=None
    )
    supported_options: tuple[str, ...] | None = msgspec.field(
        name="supportedOptions", default=None
    )
    supported_stages: tuple[str, ...] | None = msgspec.field(
        name="supportedStages", default=None
    )
    supported_window_accumulators: tuple[str, ...] | None = msgspec.field(
        name="supportedWindowAccumulators", default=None
    )
    supports_collection_scope: bool | None = msgspec.field(
        name="supportsCollectionScope", default=None
    )
    supports_database_scope: bool | None = msgspec.field(
        name="supportsDatabaseScope", default=None
    )
    supports_explain: bool | None = msgspec.field(
        name="supportsExplain", default=None
    )
    supports_leading_search_stage: bool | None = msgspec.field(
        name="supportsLeadingSearchStage", default=None
    )
    supports_leading_vector_search_stage: bool | None = msgspec.field(
        name="supportsLeadingVectorSearchStage", default=None
    )
    supports_session: bool | None = msgspec.field(name="supportsSession", default=None)
    unsupported_options: tuple[str, ...] | None = msgspec.field(
        name="unsupportedOptions", default=None
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


SEARCH_OPERATION_FIELDS = frozenset(
    {
        "acceptedNoopOptions",
        "acceptsPipeline",
        "aggregateStage",
        "operators",
        "requiresLeadingStage",
        "supportedOptions",
        "supportsCollectionScope",
        "supportsDatabaseScope",
        "supportsExplain",
        "supportsSession",
        "unsupportedOptions",
    }
)


class MongoSearchOperationMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    accepted_noop_options: tuple[str, ...] | None = msgspec.field(
        name="acceptedNoopOptions", default=None
    )
    accepts_pipeline: bool | None = msgspec.field(name="acceptsPipeline", default=None)
    aggregate_stage: str | None = msgspec.field(name="aggregateStage", default=None)
    operators: tuple[str, ...] | None = None
    requires_leading_stage: bool | None = msgspec.field(
        name="requiresLeadingStage", default=None
    )
    result_type: str | None = msgspec.field(name="resultType", default=None)
    stage_options: dict[str, object] | None = msgspec.field(
        name="stageOptions", default=None
    )
    supported_options: tuple[str, ...] | None = msgspec.field(
        name="supportedOptions", default=None
    )
    supports_collection_scope: bool | None = msgspec.field(
        name="supportsCollectionScope", default=None
    )
    supports_database_scope: bool | None = msgspec.field(
        name="supportsDatabaseScope", default=None
    )
    supports_explain: bool | None = msgspec.field(
        name="supportsExplain", default=None
    )
    supports_session: bool | None = msgspec.field(name="supportsSession", default=None)
    unsupported_options: tuple[str, ...] | None = msgspec.field(
        name="unsupportedOptions", default=None
    )


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


VECTOR_SEARCH_OPERATION_FIELDS = frozenset(
    {
        "acceptedNoopOptions",
        "acceptsPipeline",
        "aggregateStage",
        "explainFeatures",
        "hybridFilterModes",
        "requiresLeadingStage",
        "scoreField",
        "similarities",
        "supportedOptions",
        "supportsCollectionScope",
        "supportsDatabaseScope",
        "supportsExplain",
        "supportsSession",
        "unsupportedOptions",
    }
)


class MongoVectorSearchOperationMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    accepted_noop_options: tuple[str, ...] | None = msgspec.field(
        name="acceptedNoopOptions", default=None
    )
    accepts_pipeline: bool | None = msgspec.field(name="acceptsPipeline", default=None)
    aggregate_stage: str | None = msgspec.field(name="aggregateStage", default=None)
    explain_features: tuple[str, ...] | None = msgspec.field(
        name="explainFeatures", default=None
    )
    hybrid_filter_modes: tuple[str, ...] | None = msgspec.field(
        name="hybridFilterModes", default=None
    )
    requires_leading_stage: bool | None = msgspec.field(
        name="requiresLeadingStage", default=None
    )
    result_type: str | None = msgspec.field(name="resultType", default=None)
    score_field: str | None = msgspec.field(name="scoreField", default=None)
    similarities: tuple[str, ...] | None = None
    supported_options: tuple[str, ...] | None = msgspec.field(
        name="supportedOptions", default=None
    )
    supports_collection_scope: bool | None = msgspec.field(
        name="supportsCollectionScope", default=None
    )
    supports_database_scope: bool | None = msgspec.field(
        name="supportsDatabaseScope", default=None
    )
    supports_explain: bool | None = msgspec.field(
        name="supportsExplain", default=None
    )
    supports_session: bool | None = msgspec.field(name="supportsSession", default=None)
    unsupported_options: tuple[str, ...] | None = msgspec.field(
        name="unsupportedOptions", default=None
    )


COLLATION_BACKEND_FIELDS = frozenset(
    {
        "advancedOptionsAvailable",
        "availableBackends",
        "selectedBackend",
        "unicodeAvailable",
    }
)
COLLATION_CAPABILITY_FIELDS = frozenset(
    {
        "advancedOptionsRequireIcu",
        "fallbackBackend",
        "optionalIcuBackend",
        "supportedLocales",
        "supportsCaseLevel",
        "supportsNumericOrdering",
    }
)


class MongoCollationBackendMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    advanced_options_available: bool | None = msgspec.field(
        name="advancedOptionsAvailable", default=None
    )
    available_backends: tuple[str, ...] | None = msgspec.field(
        name="availableBackends", default=None
    )
    selected_backend: str | None = msgspec.field(name="selectedBackend", default=None)
    unicode_available: bool | None = msgspec.field(
        name="unicodeAvailable", default=None
    )


class MongoCollationCapabilitiesMetadata(
    msgspec.Struct, frozen=True, forbid_unknown_fields=True
):
    advanced_options_require_icu: tuple[str, ...] | None = msgspec.field(
        name="advancedOptionsRequireIcu", default=None
    )
    fallback_backend: str | None = msgspec.field(name="fallbackBackend", default=None)
    optional_icu_backend: bool | None = msgspec.field(
        name="optionalIcuBackend", default=None
    )
    supported_locales: tuple[str, ...] | None = msgspec.field(
        name="supportedLocales", default=None
    )
    supported_strengths: tuple[int, ...] | None = msgspec.field(
        name="supportedStrengths", default=None
    )
    supports_case_level: bool | None = msgspec.field(
        name="supportsCaseLevel", default=None
    )
    supports_numeric_ordering: bool | None = msgspec.field(
        name="supportsNumericOrdering", default=None
    )


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


SDAM_FIELDS = frozenset(
    {
        "distributedMonitoring",
        "electionMetadataAware",
        "fullSdam",
        "helloMemberDiscovery",
        "longPollingHello",
        "serverHealthTracking",
        "topologyVersionAware",
    }
)


class MongoSdamMetadata(msgspec.Struct, frozen=True, forbid_unknown_fields=True):
    distributed_monitoring: bool | None = msgspec.field(
        name="distributedMonitoring", default=None
    )
    election_metadata_aware: bool | None = msgspec.field(
        name="electionMetadataAware", default=None
    )
    full_sdam: bool | None = msgspec.field(name="fullSdam", default=None)
    hello_member_discovery: bool | None = msgspec.field(
        name="helloMemberDiscovery", default=None
    )
    long_polling_hello: bool | None = msgspec.field(
        name="longPollingHello", default=None
    )
    server_health_tracking: bool | None = msgspec.field(
        name="serverHealthTracking", default=None
    )
    topology_version_aware: bool | None = msgspec.field(
        name="topologyVersionAware", default=None
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
OPERATION_SCHEMAS: dict[
    str, tuple[frozenset[str], type[msgspec.Struct]]
] = {
    "read": (READ_OPERATION_NAMES, MongoReadOperationMetadata),
    "write": (WRITE_OPERATION_NAMES, MongoWriteOperationMetadata),
    "aggregation": (frozenset({"aggregate"}), MongoAggregationOperationMetadata),
    "search": (frozenset({"aggregate"}), MongoSearchOperationMetadata),
    "vector_search": (frozenset({"aggregate"}), MongoVectorSearchOperationMetadata),
}


def _validate_operation_metadata(
    capability: str, metadata: dict[str, object]
) -> None:
    contract = OPERATION_SCHEMAS.get(capability)
    if contract is None:
        return
    operations = metadata.get("operationMetadata")
    if operations is None:
        return
    if not isinstance(operations, dict) or not all(
        isinstance(name, str) for name in operations
    ):
        message = "Operation metadata must be a string-keyed object"
        raise ValueError(message)
    names, schema = contract
    if set(operations) - names:
        message = "Unknown operation metadata"
        raise ValueError(message)
    for operation in operations.values():
        if not isinstance(operation, dict) or not all(
            isinstance(key, str) for key in operation
        ):
            message = "Operation metadata must be a string-keyed object"
            raise ValueError(message)
        msgspec.convert(msgspec.to_builtins(operation), type=schema, strict=True)


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
            if capability == "topology_discovery" and "sdam" in metadata:
                msgspec.convert(
                    msgspec.to_builtins(metadata["sdam"]),
                    type=MongoSdamMetadata,
                    strict=True,
                )
            if capability == "collation":
                for field, nested_schema in (
                    ("backend", MongoCollationBackendMetadata),
                    ("capabilities", MongoCollationCapabilitiesMetadata),
                ):
                    if field in metadata:
                        msgspec.convert(
                            msgspec.to_builtins(metadata[field]),
                            type=nested_schema,
                            strict=True,
                        )
            _validate_operation_metadata(capability, metadata)
        except (
            TypeError,
            ValueError,
            msgspec.ValidationError,
            RecursionError,
        ) as error:
            message = f"Invalid MongoDB {capability} metadata"
            raise ValueError(message) from error
