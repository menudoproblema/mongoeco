MONGODB_DIALECT_HOOK_NAMES = (
    "null_query_matches_undefined",
    "rejects_empty_group_fields",
    "validates_densify_partition_paths",
    "validates_aggregation_syntax_early",
    "documents_join_omits_collection_namespace",
    "supports_array_index_variables",
    "uses_extended_conversions",
    "uses_server_densify_bounds",
    "densify_equal_bounds_are_empty",
    "densify_full_nonadvancing_step_errors",
    "densify_empty_partition_generates_values",
    "supports_date_range_windows",
    "limits_trim_chars",
    "rejects_cluster_time_expression",
    "list_indexes_includes_simple_collation",
    "rejects_unimplemented_wildcard_projection",
    "merge_insert_preserves_incoming_field_order",
)

PYMONGO_PROFILE_HOOK_NAMES = (
    "supports_update_one_sort",
    "rejects_reserved_aggregation_keywords",
)

DEFAULT_MONGODB_DIALECT = "7.0"
DEFAULT_PYMONGO_PROFILE = "4.9"
AUTO_INSTALLED_PYMONGO_PROFILE = "auto-installed"
STRICT_AUTO_INSTALLED_PYMONGO_PROFILE = "strict-auto-installed"

MONGODB_CAP_NULL_QUERY_MATCHES_UNDEFINED = "query.null_matches_undefined"
PYMONGO_CAP_UPDATE_ONE_SORT = "update_one.sort"
PYMONGO_CAP_RESERVED_AGGREGATION_KEYWORDS = "aggregate.reserved_keywords_validation"
PYMONGO_CAP_SRV_ALLOWED_HOSTS_SUFFIX = "uri.srv_allowed_hosts_suffix"
