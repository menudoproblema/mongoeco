import unittest

from tests.unit.core._sqlite_helper_test_cases import SQLiteInternalHelperTests as _BaseSQLiteHelperTests

_BaseSQLiteHelperTests.__test__ = False


def _build_case(name: str, methods: list[str]) -> type[unittest.TestCase]:
    namespace = {"__module__": __name__, "_connection": _BaseSQLiteHelperTests._connection}
    for method_name in methods:
        namespace[method_name] = getattr(_BaseSQLiteHelperTests, method_name)
    return type(name, (unittest.TestCase,), namespace)


SQLiteAdminHelperTests = _build_case(
    "SQLiteAdminHelperTests",
    [
        "test_sqlite_profile_namespace_paths_delegate_to_admin_runtime",
        "test_sqlite_namespace_admin_and_modify_ops_cover_remaining_error_branches",
        "test_sqlite_runtime_metric_and_active_operation_helpers_delegate_cleanly",
        "test_sqlite_session_sync_helper_delegates_to_session_runtime",
        "test_sqlite_admin_runtime_covers_collection_existence_and_options_helpers",
        "test_sqlite_admin_runtime_profile_helpers_and_cache_cleanup",
        "test_sqlite_admin_runtime_delegates_namespace_entrypoints_with_engine_wiring",
        "test_sqlite_index_admin_helpers_cover_builtin_id_and_rollback_paths",
        "test_sqlite_search_admin_helpers_cover_documents_and_conflicts",
        "test_sqlite_namespace_admin_helpers_update_catalog_and_invalidate_runtime",
        "test_sqlite_engine_delegates_namespace_admin_runtime_boundaries",
        "test_sqlite_write_helpers_cover_duplicate_snapshot_and_rollback_paths",
    ],
)
