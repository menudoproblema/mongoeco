import fnmatch
import os
import unittest

from tests.differential.cases import REAL_PARITY_CASES


TARGET_MODULES = {
    "7.0": "tests.differential.mongodb7_real_parity",
    "8.0": "tests.differential.mongodb8_real_parity",
}


def resolve_target(argv: list[str]) -> str:
    if len(argv) >= 2:
        target = argv[1]
    else:
        target = os.getenv("MONGOECO_REAL_MONGODB_TARGET", "7.0")

    if target not in TARGET_MODULES:
        supported = ", ".join(sorted(TARGET_MODULES))
        raise SystemExit(f"unsupported MongoDB target {target!r}; expected one of: {supported}")
    return target


def resolve_case_filter(argv: list[str]) -> str | None:
    if len(argv) >= 3:
        return argv[2]
    return os.getenv("MONGOECO_REAL_MONGODB_CASE")


def available_case_names(target_version: tuple[int, int] | None = None) -> list[str]:
    selected = [
        case.name
        for case in REAL_PARITY_CASES
        if target_version is None or case.supports(target_version)
    ]
    return sorted(selected)


def build_suite(target: str, case_filter: str | None = None) -> unittest.TestSuite:
    suite = unittest.defaultTestLoader.loadTestsFromName(TARGET_MODULES[target])
    if not case_filter:
        return suite

    filtered = unittest.TestSuite()
    patterns = [fragment.strip() for fragment in case_filter.split(",") if fragment.strip()]
    for test in _iter_cases(suite):
        method_name = getattr(test, "_testMethodName", "")
        case_name = method_name.removeprefix("test_").removesuffix("_matches_real_mongodb")
        if any(fnmatch.fnmatch(case_name, pattern) for pattern in patterns):
            filtered.addTest(test)
    return filtered


def _iter_cases(suite: unittest.TestSuite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from _iter_cases(test)
        else:
            yield test
