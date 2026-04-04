import unittest

from tests.unit.core._sqlite_helper_test_cases import SQLiteInternalHelperTests as _BaseSQLiteHelperTests

_BaseSQLiteHelperTests.__test__ = False


def _build_case(name: str, methods: list[str]) -> type[unittest.TestCase]:
    namespace = {"__module__": __name__, "_connection": _BaseSQLiteHelperTests._connection}
    for method_name in methods:
        namespace[method_name] = getattr(_BaseSQLiteHelperTests, method_name)
    return type(name, (unittest.TestCase,), namespace)


SQLiteReadHelperTests = _build_case(
    "SQLiteReadHelperTests",
    [
        "test_sqlite_read_execution_helpers_cover_none_and_guard_paths",
        "test_sqlite_read_execution_plan_helpers_cover_limit_branches",
        "test_sqlite_scalar_and_multikey_helper_branches_cover_remaining_guard_paths",
        "test_sqlite_query_plan_multikey_translation_covers_match_all_fallbacks_and_range_variants",
        "test_sqlite_physical_index_helpers_commit_only_when_needed",
        "test_sqlite_scalar_sort_helpers_cover_cache_lookup_and_sql_construction",
        "test_sqlite_plan_and_dbref_helpers_cover_recursive_and_cached_paths",
    ],
)
