"""Registration data shared by native capture and semantic replay consumers."""

from dataclasses import dataclass

from tests.differential.semantic_absence_consumer_cases import (
    SEED as ABSENCE_SEED,
    SEMANTIC_ABSENCE_CONSUMER_CASES,
)
from tests.differential.semantic_aggregation_response_cases import (
    SEED as AGGREGATION_RESPONSE_SEED,
    SEMANTIC_AGGREGATION_RESPONSE_CASES,
)
from tests.differential.semantic_average_route_cases import (
    SEED as AVERAGE_ROUTE_SEED,
    SEMANTIC_AVERAGE_ROUTE_CASES,
)
from tests.differential.semantic_binary_cases import (
    SEED as BINARY_SEED,
    SEMANTIC_BINARY_CASES,
)
from tests.differential.semantic_binary_edge_cases import (
    SEED as BINARY_EDGE_SEED,
    SEMANTIC_BINARY_EDGE_CASES,
)
from tests.differential.semantic_binary_surface_cases import (
    SEED as BINARY_SURFACE_SEED,
    SEMANTIC_BINARY_SURFACE_CASES,
)
from tests.differential.semantic_bson_admin_read_cases import (
    SEED as BSON_ADMIN_READ_SEED,
    SEMANTIC_BSON_ADMIN_READ_CASES,
)
from tests.differential.semantic_bson_admission_cases import (
    SEED as BSON_ADMISSION_SEED,
    SEMANTIC_BSON_ADMISSION_CASES,
)
from tests.differential.semantic_bson_bulk_cases import (
    SEED as BSON_BULK_SEED,
    SEMANTIC_BSON_BULK_CASES,
)
from tests.differential.semantic_bson_catalog_cases import (
    SEED as BSON_CATALOG_SEED,
    SEMANTIC_BSON_CATALOG_CASES,
)
from tests.differential.semantic_bson_command_cases import (
    SEED as BSON_COMMAND_SEED,
    SEMANTIC_BSON_COMMAND_CASES,
)
from tests.differential.semantic_bson_command_flag_cases import (
    SEED as BSON_COMMAND_FLAG_SEED,
    SEMANTIC_BSON_COMMAND_FLAG_CASES,
)
from tests.differential.semantic_bson_consumer_cases import (
    SEED as BSON_CONSUMER_SEED,
    SEMANTIC_BSON_CONSUMER_CASES,
)
from tests.differential.semantic_bson_dispatch_cases import (
    SEED as BSON_DISPATCH_SEED,
    SEMANTIC_BSON_DISPATCH_CASES,
)
from tests.differential.semantic_bson_grammar_context_cases import (
    SEED as BSON_GRAMMAR_CONTEXT_SEED,
    SEMANTIC_BSON_GRAMMAR_CONTEXT_CASES,
)
from tests.differential.semantic_bson_index_admin_cases import (
    SEED as BSON_INDEX_ADMIN_SEED,
    SEMANTIC_BSON_INDEX_ADMIN_CASES,
)
from tests.differential.semantic_bson_insert_cases import (
    SEED as BSON_INSERT_SEED,
    SEMANTIC_BSON_INSERT_CASES,
)
from tests.differential.semantic_bson_mutation_cases import (
    SEED as BSON_MUTATION_SEED,
    SEMANTIC_BSON_MUTATION_CASES,
)
from tests.differential.semantic_bson_namespace_cases import (
    SEED as BSON_NAMESPACE_SEED,
    SEMANTIC_BSON_NAMESPACE_CASES,
)
from tests.differential.semantic_bson_request_cases import (
    SEED as BSON_REQUEST_SEED,
    SEMANTIC_BSON_REQUEST_CASES,
)
from tests.differential.semantic_bson_search_input_cases import (
    SEED as BSON_SEARCH_INPUT_SEED,
    SEMANTIC_BSON_SEARCH_INPUT_CASES,
)
from tests.differential.semantic_bson_size_cases import (
    SEED as BSON_SIZE_SEED,
    SEMANTIC_BSON_SIZE_CASES,
)
from tests.differential.semantic_bson_uuid_policy_cases import (
    SEED as BSON_UUID_POLICY_SEED,
    SEMANTIC_BSON_UUID_POLICY_CASES,
)
from tests.differential.semantic_command_batch_cases import (
    SEED as COMMAND_BATCH_SEED,
    SEMANTIC_COMMAND_BATCH_CASES,
)
from tests.differential.semantic_command_let_cases import (
    SEED as COMMAND_LET_SEED,
    SEMANTIC_COMMAND_LET_CASES,
)
from tests.differential.semantic_cursor_consumption_cases import (
    SEED as CURSOR_CONSUMPTION_SEED,
    SEMANTIC_CURSOR_CONSUMPTION_CASES,
)
from tests.differential.semantic_datetime_cases import (
    SEED as DATETIME_SEED,
    SEMANTIC_DATETIME_CASES,
)
from tests.differential.semantic_dbref_grammar_cases import (
    SEED as DBREF_GRAMMAR_SEED,
    SEMANTIC_DBREF_GRAMMAR_CASES,
)
from tests.differential.semantic_decimal_advanced_cases import (
    CONTROL_SEED as DECIMAL_CONTROL_SEED,
    EDGE_SEED as DECIMAL_ADVANCED_EDGE_SEED,
    GENERATED_SEED as DECIMAL_GENERATED_SEED,
    SEED as DECIMAL_ADVANCED_SEED,
    SEMANTIC_DECIMAL_ADVANCED_CASES,
    SEMANTIC_DECIMAL_ADVANCED_EDGE_CASES,
    SEMANTIC_DECIMAL_CONTROL_CASES,
    SEMANTIC_DECIMAL_GENERATED_CASES,
)
from tests.differential.semantic_decimal_cases import (
    EDGE_SEED as DECIMAL_EDGE_SEED,
    SEED as DECIMAL_SEED,
    SEMANTIC_DECIMAL_CASES,
    SEMANTIC_DECIMAL_EDGE_CASES,
)
from tests.differential.semantic_decimal_conversion_cases import (
    SEED as DECIMAL_CONVERSION_SEED,
    SEMANTIC_DECIMAL_CONVERSION_CASES,
)
from tests.differential.semantic_document_factory_cases import (
    SEED as DOCUMENT_FACTORY_SEED,
    SEMANTIC_DOCUMENT_FACTORY_CASES,
)
from tests.differential.semantic_document_order_cases import (
    SEED as DOCUMENT_ORDER_SEED,
    SEMANTIC_DOCUMENT_ORDER_CASES,
)
from tests.differential.semantic_expression_grammar_cases import (
    SEED as EXPRESSION_GRAMMAR_SEED,
    SEMANTIC_EXPRESSION_GRAMMAR_CASES,
)
from tests.differential.semantic_find_projection_cases import (
    SEED as FIND_PROJECTION_SEED,
    SEMANTIC_FIND_PROJECTION_CASES,
)
from tests.differential.semantic_group_cases import SEMANTIC_GROUP_CASES
from tests.differential.semantic_materialized_context_cases import (
    SEED as MATERIALIZED_CONTEXT_SEED,
    SEMANTIC_MATERIALIZED_CONTEXT_CASES,
)
from tests.differential.semantic_merge_key_cases import (
    SEED as MERGE_KEY_SEED,
    SEMANTIC_MERGE_KEY_CASES,
)
from tests.differential.semantic_missing_cases import (
    SEED as MISSING_SEED,
    SEMANTIC_MISSING_CASES,
)
from tests.differential.semantic_mod_cases import (
    SEED as MOD_SEED,
    SEMANTIC_MOD_CASES,
)
from tests.differential.semantic_numeric_cases import (
    SEED as NUMERIC_SEED,
    SEMANTIC_NUMERIC_CASES,
)
from tests.differential.semantic_numeric_predicate_cases import (
    SEED as NUMERIC_PREDICATE_SEED,
    SEMANTIC_NUMERIC_PREDICATE_CASES,
)
from tests.differential.semantic_object_grammar_cases import (
    SEED as OBJECT_GRAMMAR_SEED,
    SEMANTIC_OBJECT_GRAMMAR_CASES,
)
from tests.differential.semantic_overflow_cases import (
    NUMERIC_CONTROL_SEED,
    SEED as OVERFLOW_SEED,
    SEMANTIC_NUMERIC_CONTROL_CASES,
    SEMANTIC_OVERFLOW_CASES,
)
from tests.differential.semantic_overflow_write_cases import (
    SEMANTIC_OVERFLOW_WRITE_CASES,
    WRITE_SEED as OVERFLOW_WRITE_SEED,
)
from tests.differential.semantic_query_batch_callback_cases import (
    SEED as QUERY_BATCH_CALLBACK_SEED,
    SEMANTIC_QUERY_BATCH_CALLBACK_CASES,
)
from tests.differential.semantic_query_matching_cases import (
    SEED as QUERY_MATCHING_SEED,
    SEMANTIC_QUERY_MATCHING_CASES,
)
from tests.differential.semantic_raw_response_cases import (
    SEED as RAW_RESPONSE_SEED,
    SEMANTIC_RAW_RESPONSE_CASES,
)
from tests.differential.semantic_rename_command_cases import (
    SEED as RENAME_COMMAND_SEED,
    SEMANTIC_RENAME_COMMAND_CASES,
)
from tests.differential.semantic_response_command_cases import (
    SEED as RESPONSE_COMMAND_SEED,
    SEMANTIC_RESPONSE_COMMAND_CASES,
)
from tests.differential.semantic_response_frontier_cases import (
    SEED as RESPONSE_FRONTIER_SEED,
    SEMANTIC_RESPONSE_FRONTIER_CASES,
)
from tests.differential.semantic_schema_cases import (
    SEED as SCHEMA_SEED,
    SEMANTIC_SCHEMA_CASES,
)
from tests.differential.semantic_search_bson_cases import (
    INITIAL_SEED as SEARCH_BSON_SEED,
    SEMANTIC_SEARCH_BSON_CASES,
)
from tests.differential.semantic_search_eligibility_cases import (
    SEED as SEARCH_ELIGIBILITY_SEED,
    SEMANTIC_SEARCH_ELIGIBILITY_CASES,
)
from tests.differential.semantic_sum_cases import (
    SEED as SUM_SEED,
    SEMANTIC_SUM_CASES,
    SEMANTIC_SUM_STREAM_CASES,
    STREAM_SEED as SUM_STREAM_SEED,
)
from tests.differential.semantic_ttl_publication_cases import (
    SEED as TTL_PUBLICATION_SEED,
    SEMANTIC_TTL_PUBLICATION_CASES,
)
from tests.differential.semantic_type_query_cases import (
    SEED as TYPE_QUERY_SEED,
    SEMANTIC_TYPE_QUERY_CASES,
)
from tests.differential.semantic_update_entrypoint_cases import (
    SEED as UPDATE_ENTRYPOINT_SEED,
    SEMANTIC_UPDATE_ENTRYPOINT_CASES,
)
from tests.differential.semantic_update_event_cases import (
    SEED as UPDATE_EVENT_SEED,
    SEMANTIC_UPDATE_EVENT_CASES,
)
from tests.differential.semantic_update_identity_cases import (
    SEED as UPDATE_IDENTITY_SEED,
    SEMANTIC_UPDATE_IDENTITY_CASES,
)
from tests.differential.semantic_watch_batch_cases import (
    SEED as WATCH_BATCH_SEED,
    SEMANTIC_WATCH_BATCH_CASES,
)
from tests.differential.semantic_watch_cases import (
    SEED as WATCH_SEED,
    SEMANTIC_WATCH_CASES,
)
from tests.differential.semantic_watch_checkpoint_cases import (
    SEED as WATCH_CHECKPOINT_SEED,
    SEMANTIC_WATCH_CHECKPOINT_CASES,
)
from tests.differential.semantic_watch_image_cases import (
    SEED as WATCH_IMAGE_SEED,
    SEMANTIC_WATCH_IMAGE_CASES,
)
from tests.differential.semantic_watch_lifecycle_cases import (
    SEED as WATCH_LIFECYCLE_SEED,
    SEMANTIC_WATCH_LIFECYCLE_CASES,
)
from tests.differential.semantic_watch_namespace_image_cases import (
    SEED as WATCH_NAMESPACE_IMAGE_SEED,
    SEMANTIC_WATCH_NAMESPACE_IMAGE_CASES,
)
from tests.differential.semantic_watch_protocol_cases import (
    SEED as WATCH_PROTOCOL_SEED,
    SEMANTIC_WATCH_PROTOCOL_CASES,
)
from tests.differential.semantic_watch_rename_cases import (
    SEED as WATCH_RENAME_SEED,
    SEMANTIC_WATCH_RENAME_CASES,
)
from tests.differential.semantic_watch_token_cases import (
    SEED as WATCH_TOKEN_SEED,
    SEMANTIC_WATCH_TOKEN_CASES,
)
from tests.differential.semantic_write_effect_cases import (
    SEED as WRITE_EFFECT_SEED,
    SEMANTIC_WRITE_EFFECT_CASES,
)
from tests.differential.semantic_write_factory_cases import (
    SEED as WRITE_FACTORY_SEED,
    SEMANTIC_WRITE_FACTORY_CASES,
)
from tests.differential.semantic_write_factory_event_cases import (
    SEED as WRITE_FACTORY_EVENT_SEED,
    SEMANTIC_WRITE_FACTORY_EVENT_CASES,
)
from tests.differential.semantic_write_response_cases import (
    SEED as WRITE_RESPONSE_SEED,
    SEMANTIC_WRITE_RESPONSE_CASES,
)


@dataclass(frozen=True)
class SemanticCorpus:
    case_set: str
    module: str
    seed: list
    cases: tuple
    observation_encoding: str = "bson"
    surfaces: tuple[str, ...] = ("sync", "async", "wire-sync", "wire-async")
    requires_replica_set: bool = False
    requires_standalone: bool = False


SEMANTIC_CORPORA = {
    "materialized_context": SemanticCorpus(
        "materialized-context",
        "semantic_materialized_context_cases",
        MATERIALIZED_CONTEXT_SEED,
        SEMANTIC_MATERIALIZED_CONTEXT_CASES,
        requires_standalone=True,
    ),
    "write_factory_event": SemanticCorpus(
        "write-factory-event",
        "semantic_write_factory_event_cases",
        WRITE_FACTORY_EVENT_SEED,
        SEMANTIC_WRITE_FACTORY_EVENT_CASES,
        surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "write_factory": SemanticCorpus(
        "write-factory",
        "semantic_write_factory_cases",
        WRITE_FACTORY_SEED,
        SEMANTIC_WRITE_FACTORY_CASES,
        requires_standalone=True,
    ),
    "write_response": SemanticCorpus(
        "write-response",
        "semantic_write_response_cases",
        WRITE_RESPONSE_SEED,
        SEMANTIC_WRITE_RESPONSE_CASES,
        requires_standalone=True,
    ),
    "query_batch_callbacks": SemanticCorpus(
        "query-batch-callbacks",
        "semantic_query_batch_callback_cases",
        QUERY_BATCH_CALLBACK_SEED,
        SEMANTIC_QUERY_BATCH_CALLBACK_CASES,
        requires_standalone=True,
    ),
    "command_batch": SemanticCorpus(
        "command-batch",
        "semantic_command_batch_cases",
        COMMAND_BATCH_SEED,
        SEMANTIC_COMMAND_BATCH_CASES,
        surfaces=("wire-sync", "wire-async"),
        requires_standalone=True,
    ),
    "bson_consumer": SemanticCorpus(
        "bson-consumer",
        "semantic_bson_consumer_cases",
        BSON_CONSUMER_SEED,
        SEMANTIC_BSON_CONSUMER_CASES,
    ),
    "cursor_consumption": SemanticCorpus(
        "cursor-consumption",
        "semantic_cursor_consumption_cases",
        CURSOR_CONSUMPTION_SEED,
        SEMANTIC_CURSOR_CONSUMPTION_CASES,
    ),
    "document_factory": SemanticCorpus(
        "document-factory",
        "semantic_document_factory_cases",
        DOCUMENT_FACTORY_SEED,
        SEMANTIC_DOCUMENT_FACTORY_CASES,
        observation_encoding="canonical-json",
        requires_standalone=True,
    ),
    "bson_dispatch": SemanticCorpus(
        "bson-dispatch",
        "semantic_bson_dispatch_cases",
        BSON_DISPATCH_SEED,
        SEMANTIC_BSON_DISPATCH_CASES,
        observation_encoding="canonical-json",
    ),
    "response_command": SemanticCorpus(
        "response-command", "semantic_response_command_cases",
        RESPONSE_COMMAND_SEED, SEMANTIC_RESPONSE_COMMAND_CASES,
        surfaces=("wire-sync", "wire-async"),
    ),
    "response_frontier": SemanticCorpus(
        "response-frontier", "semantic_response_frontier_cases",
        RESPONSE_FRONTIER_SEED, SEMANTIC_RESPONSE_FRONTIER_CASES,
    ),
    "raw_response": SemanticCorpus(
        "raw-response", "semantic_raw_response_cases",
        RAW_RESPONSE_SEED, SEMANTIC_RAW_RESPONSE_CASES,
    ),
    "aggregation_response": SemanticCorpus(
        "aggregation-response", "semantic_aggregation_response_cases",
        AGGREGATION_RESPONSE_SEED, SEMANTIC_AGGREGATION_RESPONSE_CASES,
    ),
    "bson_grammar_context": SemanticCorpus(
        "bson-grammar-context", "semantic_bson_grammar_context_cases",
        BSON_GRAMMAR_CONTEXT_SEED, SEMANTIC_BSON_GRAMMAR_CONTEXT_CASES,
    ),
    "merge_key": SemanticCorpus(
        "merge-key", "semantic_merge_key_cases", MERGE_KEY_SEED,
        SEMANTIC_MERGE_KEY_CASES,
    ),
    "binary_surface": SemanticCorpus(
        "binary-surface", "semantic_binary_surface_cases", BINARY_SURFACE_SEED,
        SEMANTIC_BINARY_SURFACE_CASES,
    ),
    "watch_namespace_image": SemanticCorpus(
        "watch-namespace-image", "semantic_watch_namespace_image_cases",
        WATCH_NAMESPACE_IMAGE_SEED, SEMANTIC_WATCH_NAMESPACE_IMAGE_CASES,
        surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch_batch": SemanticCorpus(
        "watch-batch", "semantic_watch_batch_cases", WATCH_BATCH_SEED,
        SEMANTIC_WATCH_BATCH_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch_checkpoint": SemanticCorpus(
        "watch-checkpoint", "semantic_watch_checkpoint_cases", WATCH_CHECKPOINT_SEED,
        SEMANTIC_WATCH_CHECKPOINT_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch_protocol": SemanticCorpus(
        "watch-protocol", "semantic_watch_protocol_cases", WATCH_PROTOCOL_SEED,
        SEMANTIC_WATCH_PROTOCOL_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch_token": SemanticCorpus(
        "watch-token", "semantic_watch_token_cases", WATCH_TOKEN_SEED,
        SEMANTIC_WATCH_TOKEN_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "rename_command": SemanticCorpus(
        "rename-command", "semantic_rename_command_cases", RENAME_COMMAND_SEED,
        SEMANTIC_RENAME_COMMAND_CASES,
    ),
    "watch_rename": SemanticCorpus(
        "watch-rename", "semantic_watch_rename_cases", WATCH_RENAME_SEED,
        SEMANTIC_WATCH_RENAME_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch_lifecycle": SemanticCorpus(
        "watch-lifecycle", "semantic_watch_lifecycle_cases", WATCH_LIFECYCLE_SEED,
        SEMANTIC_WATCH_LIFECYCLE_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch_image": SemanticCorpus(
        "watch-image", "semantic_watch_image_cases", WATCH_IMAGE_SEED,
        SEMANTIC_WATCH_IMAGE_CASES, surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "watch": SemanticCorpus(
        "watch",
        "semantic_watch_cases",
        WATCH_SEED,
        SEMANTIC_WATCH_CASES,
        surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "update_entrypoint": SemanticCorpus(
        "update-entrypoint",
        "semantic_update_entrypoint_cases",
        UPDATE_ENTRYPOINT_SEED,
        SEMANTIC_UPDATE_ENTRYPOINT_CASES,
        surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "update_event": SemanticCorpus(
        "update-event",
        "semantic_update_event_cases",
        UPDATE_EVENT_SEED,
        SEMANTIC_UPDATE_EVENT_CASES,
        surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "bson_size": SemanticCorpus(
        "bson-size",
        "semantic_bson_size_cases",
        BSON_SIZE_SEED,
        SEMANTIC_BSON_SIZE_CASES,
    ),
    "write_effect": SemanticCorpus(
        "write-effect",
        "semantic_write_effect_cases",
        WRITE_EFFECT_SEED,
        SEMANTIC_WRITE_EFFECT_CASES,
    ),
    "type_query": SemanticCorpus(
        "type-query",
        "semantic_type_query_cases",
        TYPE_QUERY_SEED,
        SEMANTIC_TYPE_QUERY_CASES,
    ),
    "dbref_grammar": SemanticCorpus(
        "dbref-grammar",
        "semantic_dbref_grammar_cases",
        DBREF_GRAMMAR_SEED,
        SEMANTIC_DBREF_GRAMMAR_CASES,
    ),
    "binary_edges": SemanticCorpus(
        "binary-edges",
        "semantic_binary_edge_cases",
        BINARY_EDGE_SEED,
        SEMANTIC_BINARY_EDGE_CASES,
    ),
    "ttl_publication": SemanticCorpus(
        "ttl-publication",
        "semantic_ttl_publication_cases",
        TTL_PUBLICATION_SEED,
        SEMANTIC_TTL_PUBLICATION_CASES,
        surfaces=("sync", "async"),
        requires_replica_set=True,
    ),
    "datetime": SemanticCorpus(
        "datetime",
        "semantic_datetime_cases",
        DATETIME_SEED,
        SEMANTIC_DATETIME_CASES,
    ),
    "update_identity": SemanticCorpus(
        "update-identity",
        "semantic_update_identity_cases",
        UPDATE_IDENTITY_SEED,
        SEMANTIC_UPDATE_IDENTITY_CASES,
    ),
    "binary": SemanticCorpus(
        "binary",
        "semantic_binary_cases",
        BINARY_SEED,
        SEMANTIC_BINARY_CASES,
    ),
    "mod": SemanticCorpus(
        "mod",
        "semantic_mod_cases",
        MOD_SEED,
        SEMANTIC_MOD_CASES,
        observation_encoding="canonical-json",
    ),
    "search_bson": SemanticCorpus(
        "search-bson", "semantic_search_bson_cases", SEARCH_BSON_SEED,
        SEMANTIC_SEARCH_BSON_CASES,
    ),
    "search_eligibility": SemanticCorpus(
        "search-eligibility",
        "semantic_search_eligibility_cases",
        SEARCH_ELIGIBILITY_SEED,
        SEMANTIC_SEARCH_ELIGIBILITY_CASES,
    ),
    "schema": SemanticCorpus(
        "schema",
        "semantic_schema_cases",
        SCHEMA_SEED,
        SEMANTIC_SCHEMA_CASES,
        observation_encoding="canonical-json",
    ),
    "query_matching": SemanticCorpus(
        "query-matching",
        "semantic_query_matching_cases",
        QUERY_MATCHING_SEED,
        SEMANTIC_QUERY_MATCHING_CASES,
    ),
    "object_grammar": SemanticCorpus(
        "object-grammar",
        "semantic_object_grammar_cases",
        OBJECT_GRAMMAR_SEED,
        SEMANTIC_OBJECT_GRAMMAR_CASES,
    ),
    "command_let": SemanticCorpus(
        "command-let",
        "semantic_command_let_cases",
        COMMAND_LET_SEED,
        SEMANTIC_COMMAND_LET_CASES,
        observation_encoding="canonical-json",
    ),
    "expression_grammar": SemanticCorpus(
        "expression-grammar",
        "semantic_expression_grammar_cases",
        EXPRESSION_GRAMMAR_SEED,
        SEMANTIC_EXPRESSION_GRAMMAR_CASES,
    ),
    "overflow_numeric_controls": SemanticCorpus(
        "overflow-numeric-controls",
        "semantic_overflow_cases",
        NUMERIC_CONTROL_SEED,
        SEMANTIC_NUMERIC_CONTROL_CASES,
    ),
    "overflow": SemanticCorpus(
        "overflow-values",
        "semantic_overflow_cases",
        OVERFLOW_SEED,
        SEMANTIC_OVERFLOW_CASES,
    ),
    "overflow_writes": SemanticCorpus(
        "overflow-writes",
        "semantic_overflow_write_cases",
        OVERFLOW_WRITE_SEED,
        SEMANTIC_OVERFLOW_WRITE_CASES,
        observation_encoding="canonical-json",
    ),
    "average_routes": SemanticCorpus(
        "average-routes",
        "semantic_average_route_cases",
        AVERAGE_ROUTE_SEED,
        SEMANTIC_AVERAGE_ROUTE_CASES,
    ),
    "sum_stream": SemanticCorpus(
        "sum-stream",
        "semantic_sum_cases",
        SUM_STREAM_SEED,
        SEMANTIC_SUM_STREAM_CASES,
    ),
    "sum": SemanticCorpus(
        "sum-values", "semantic_sum_cases", SUM_SEED, SEMANTIC_SUM_CASES
    ),
    "decimal_controls": SemanticCorpus(
        "decimal-controls",
        "semantic_decimal_advanced_cases",
        DECIMAL_CONTROL_SEED,
        SEMANTIC_DECIMAL_CONTROL_CASES,
    ),
    "decimal_generated": SemanticCorpus(
        "decimal-generated",
        "semantic_decimal_advanced_cases",
        DECIMAL_GENERATED_SEED,
        SEMANTIC_DECIMAL_GENERATED_CASES,
    ),
    "decimal_advanced": SemanticCorpus(
        "decimal-advanced",
        "semantic_decimal_advanced_cases",
        DECIMAL_ADVANCED_SEED,
        SEMANTIC_DECIMAL_ADVANCED_CASES,
    ),
    "decimal_advanced_edges": SemanticCorpus(
        "decimal-advanced-edges",
        "semantic_decimal_advanced_cases",
        DECIMAL_ADVANCED_EDGE_SEED,
        SEMANTIC_DECIMAL_ADVANCED_EDGE_CASES,
    ),
    "decimal_edges": SemanticCorpus(
        "decimal-edges",
        "semantic_decimal_cases",
        DECIMAL_EDGE_SEED,
        SEMANTIC_DECIMAL_EDGE_CASES,
    ),
    "bson_command_flags": SemanticCorpus(
        "bson-command-flags", "semantic_bson_command_flag_cases",
        BSON_COMMAND_FLAG_SEED,
        SEMANTIC_BSON_COMMAND_FLAG_CASES,
    ),
    "find_projection": SemanticCorpus(
        "find-projection",
        "semantic_find_projection_cases",
        FIND_PROJECTION_SEED,
        SEMANTIC_FIND_PROJECTION_CASES,
    ),
    "bson_uuid_policy": SemanticCorpus(
        "bson-uuid-policy",
        "semantic_bson_uuid_policy_cases",
        BSON_UUID_POLICY_SEED,
        SEMANTIC_BSON_UUID_POLICY_CASES,
    ),
    "bson_search_input": SemanticCorpus(
        "bson-search-input", "semantic_bson_search_input_cases", BSON_SEARCH_INPUT_SEED,
        SEMANTIC_BSON_SEARCH_INPUT_CASES,
    ),
    "bson_namespace": SemanticCorpus(
        "bson-namespace", "semantic_bson_namespace_cases", BSON_NAMESPACE_SEED,
        SEMANTIC_BSON_NAMESPACE_CASES,
    ),
    "bson_admin_read": SemanticCorpus(
        "bson-admin-read", "semantic_bson_admin_read_cases", BSON_ADMIN_READ_SEED,
        SEMANTIC_BSON_ADMIN_READ_CASES,
    ),
    "bson_index_admin": SemanticCorpus(
        "bson-index-admin", "semantic_bson_index_admin_cases", BSON_INDEX_ADMIN_SEED,
        SEMANTIC_BSON_INDEX_ADMIN_CASES,
    ),
    "bson_catalog": SemanticCorpus(
        "bson-catalog", "semantic_bson_catalog_cases", BSON_CATALOG_SEED,
        SEMANTIC_BSON_CATALOG_CASES,
    ),
    "bson_command": SemanticCorpus(
        "bson-command", "semantic_bson_command_cases", BSON_COMMAND_SEED,
        SEMANTIC_BSON_COMMAND_CASES,
    ),
    "bson_bulk": SemanticCorpus(
        "bson-bulk", "semantic_bson_bulk_cases", BSON_BULK_SEED,
        SEMANTIC_BSON_BULK_CASES, observation_encoding="canonical-json",
    ),
    "bson_mutation": SemanticCorpus(
        "bson-mutation", "semantic_bson_mutation_cases",
        BSON_MUTATION_SEED, SEMANTIC_BSON_MUTATION_CASES,
        observation_encoding="canonical-json",
    ),
    "bson_insert": SemanticCorpus(
        "bson-insert", "semantic_bson_insert_cases",
        BSON_INSERT_SEED, SEMANTIC_BSON_INSERT_CASES,
    ),
    "bson_request": SemanticCorpus(
        "bson-request", "semantic_bson_request_cases",
        BSON_REQUEST_SEED, SEMANTIC_BSON_REQUEST_CASES,
    ),
    "bson_admission": SemanticCorpus(
        "bson-admission", "semantic_bson_admission_cases",
        BSON_ADMISSION_SEED, SEMANTIC_BSON_ADMISSION_CASES,
    ),
    "decimal_conversion": SemanticCorpus(
        "decimal-conversion", "semantic_decimal_conversion_cases",
        DECIMAL_CONVERSION_SEED, SEMANTIC_DECIMAL_CONVERSION_CASES,
    ),
    "decimal": SemanticCorpus(
        "decimal-values",
        "semantic_decimal_cases",
        DECIMAL_SEED,
        SEMANTIC_DECIMAL_CASES,
    ),
    "document_order": SemanticCorpus(
        "document-order",
        "semantic_document_order_cases",
        DOCUMENT_ORDER_SEED,
        SEMANTIC_DOCUMENT_ORDER_CASES,
    ),
    "group_parity": SemanticCorpus(
        "semantic-fixes",
        "semantic_group_cases",
        [{"_id": 1}, {"_id": 2}],
        SEMANTIC_GROUP_CASES,
    ),
    "missing": SemanticCorpus(
        "missing-values",
        "semantic_missing_cases",
        MISSING_SEED,
        SEMANTIC_MISSING_CASES,
    ),
    "absence_consumers": SemanticCorpus(
        "absence-consumers",
        "semantic_absence_consumer_cases",
        ABSENCE_SEED,
        SEMANTIC_ABSENCE_CONSUMER_CASES,
    ),
    "numeric": SemanticCorpus(
        "numeric-values",
        "semantic_numeric_cases",
        NUMERIC_SEED,
        SEMANTIC_NUMERIC_CASES,
    ),
    "numeric_predicates": SemanticCorpus(
        "numeric-predicates",
        "semantic_numeric_predicate_cases",
        NUMERIC_PREDICATE_SEED,
        SEMANTIC_NUMERIC_PREDICATE_CASES,
    ),
}
