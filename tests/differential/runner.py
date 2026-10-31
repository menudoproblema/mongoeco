import fnmatch
import os
import unittest

from tests.differential.cases import REAL_PARITY_CASES


TARGET_MODULES = {
    "7.0": ("tests.differential.mongodb7_real_parity.MongoDB7RealParityTests"),
    "8.0": ("tests.differential.mongodb8_real_parity.MongoDB8RealParityTests"),
    "9.0": ("tests.differential.mongodb9_real_parity.MongoDB9RealParityTests"),
}


def resolve_target(argv: list[str]) -> str:
    if len(argv) >= 2:
        target = argv[1]
    else:
        target = os.getenv("MONGOECO_REAL_MONGODB_TARGET", "7.0")

    if target not in TARGET_MODULES:
        supported = ", ".join(sorted(TARGET_MODULES))
        raise SystemExit(
            f"unsupported MongoDB target {target!r}; expected one of: {supported}",
        )
    return target


def resolve_case_filter(argv: list[str]) -> str | None:
    if len(argv) >= 3:
        return argv[2]
    return os.getenv("MONGOECO_REAL_MONGODB_CASE")


def select_cases(target_version=None, case_filter=None, *, partition="all"):
    """Canonical selection for listing, execution, capture inventory and reports."""
    if partition not in {"all", "standalone", "replica-set"}:
        message = f"unknown differential partition: {partition}"
        raise ValueError(message)
    patterns = [
        value.strip() for value in (case_filter or "*").split(",") if value.strip()
    ]
    return tuple(
        case
        for case in REAL_PARITY_CASES
        if (target_version is None or case.supports(target_version))
        and (
            partition == "all"
            or case.requires_replica_set == (partition == "replica-set")
        )
        and any(fnmatch.fnmatch(case.name, pattern) for pattern in patterns)
    )


def available_case_names(target_version=None, case_filter=None, *, partition="all"):
    return sorted(
        case.name
        for case in select_cases(target_version, case_filter, partition=partition)
    )


def build_suite(
    target: str, case_filter=None, *, partition="all"
) -> unittest.TestSuite:
    suite = unittest.defaultTestLoader.loadTestsFromName(TARGET_MODULES[target])
    target_version = tuple(int(part) for part in target.split("."))
    applicable = set(
        available_case_names(target_version, case_filter, partition=partition)
    )
    filtered = unittest.TestSuite()
    for test in _iter_cases(suite):
        method_name = getattr(test, "_testMethodName", "")
        case_name = method_name.removeprefix("test_").removesuffix(
            "_matches_real_mongodb"
        )
        if case_name in applicable:
            filtered.addTest(test)
    if filtered.countTestCases() == 0:
        message = (
            f"case filter matched no differential cases: {case_filter} ({partition})"
        )
        raise ValueError(message)
    if filtered.countTestCases() != len(applicable):
        message = "differential suite did not load every selected case"
        raise ValueError(message)
    return filtered


def _iter_cases(suite: unittest.TestSuite):
    for test in suite:
        if isinstance(test, unittest.TestSuite):
            yield from _iter_cases(test)
        else:
            yield test
