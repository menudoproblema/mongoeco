import unittest

from tests.unit.core._sqlite_helper_test_cases import SQLiteInternalHelperTests as _BaseSQLiteHelperTests

_BaseSQLiteHelperTests.__test__ = False


def _build_case(name: str, methods: list[str]) -> type[unittest.TestCase]:
    namespace = {"__module__": __name__, "_connection": _BaseSQLiteHelperTests._connection}
    for method_name in methods:
        namespace[method_name] = getattr(_BaseSQLiteHelperTests, method_name)
    return type(name, (unittest.TestCase,), namespace)


SQLiteSearchRuntimeHelperTests = _build_case(
    "SQLiteSearchRuntimeHelperTests",
    [
        "test_sqlite_engine_module_keeps_admin_runtime_boundary",
        "test_sqlite_runtime_diagnostics_handles_missing_search_index_table",
        "test_sqlite_explain_contract_helpers_cover_hint_and_fallback_shapes",
        "test_sqlite_search_runtime_exact_should_score_prefilter_and_vector_prefilter_helpers",
        "test_sqlite_search_runtime_helper_branches_and_sync_wrappers",
        "test_sqlite_search_runtime_additional_compound_and_downstream_helper_paths",
        "test_sqlite_search_backend_cache_helpers_prune_stale_entries",
        "test_sqlite_search_helpers_cover_listing_and_python_fallback",
        "test_sqlite_array_comparison_and_search_entry_helpers_cover_fallback_paths",
        "test_sqlite_entry_rebuild_and_snapshot_helpers_cover_none_and_skip_paths",
    ],
)
