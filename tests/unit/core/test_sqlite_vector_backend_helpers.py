import unittest

from tests.unit.core._sqlite_helper_test_cases import SQLiteInternalHelperTests as _BaseSQLiteHelperTests

_BaseSQLiteHelperTests.__test__ = False


def _build_case(name: str, methods: list[str]) -> type[unittest.TestCase]:
    namespace = {"__module__": __name__, "_connection": _BaseSQLiteHelperTests._connection}
    for method_name in methods:
        namespace[method_name] = getattr(_BaseSQLiteHelperTests, method_name)
    return type(name, (unittest.TestCase,), namespace)


SQLiteVectorBackendHelperTests = _build_case(
    "SQLiteVectorBackendHelperTests",
    [
        "test_sqlite_vector_backend_covers_build_search_and_error_paths",
        "test_sqlite_vector_backend_filter_prefilter_supports_in_exists_and_reports_unsupported",
        "test_sqlite_vector_backend_low_level_filter_helpers_cover_conservative_branches",
    ],
)
