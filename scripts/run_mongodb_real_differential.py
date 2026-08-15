import argparse
import fnmatch
import importlib
import json
import os
import random
import sys
import time
import unittest

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

REAL_PARITY_CASES = importlib.import_module(
    "tests.differential.cases",
).REAL_PARITY_CASES
_differential_runner = importlib.import_module("tests.differential.runner")
available_case_names = _differential_runner.available_case_names
build_suite = _differential_runner.build_suite


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=("7.0", "8.0"))
    parser.add_argument("case_filter", nargs="?")
    parser.add_argument("--list-cases", action="store_true")
    parser.add_argument("--json-report", type=Path)
    parser.add_argument("--junit-dir", type=Path)
    parser.add_argument(
        "--seed",
        type=int,
        default=int(os.getenv("MONGOECO_DIFFERENTIAL_SEED", "42")),
    )
    return parser.parse_args(argv[1:])


def _failure_document(item: tuple[object, str]) -> dict[str, str]:
    test, traceback = item
    return {"test": str(test), "traceback": traceback}


def main(argv: list[str] | None = None) -> int:
    args = sys.argv if argv is None else argv
    parsed = _parse_args(args)
    if parsed.list_cases:
        target = parsed.target
        major, minor = (int(part) for part in target.split(".", 1))
        for case_name in available_case_names((major, minor)):
            sys.stdout.write(f"{case_name}\n")
        return 0

    if not os.getenv("MONGOECO_REAL_MONGODB_URI"):
        sys.stderr.write("MONGOECO_REAL_MONGODB_URI is not configured\n")
        return 2

    random.seed(parsed.seed)
    started_at = time.time()
    try:
        suite = build_suite(parsed.target, parsed.case_filter)
    except ValueError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    if suite.countTestCases() == 0:
        sys.stderr.write("differential suite resolved to zero tests\n")
        return 2
    if parsed.junit_dir is not None:
        try:
            xmlrunner = importlib.import_module("xmlrunner")
        except ImportError as error:
            message = "unittest-xml-reporting is required for --junit-dir"
            raise SystemExit(message) from error
        parsed.junit_dir.mkdir(parents=True, exist_ok=True)
        runner = xmlrunner.XMLTestRunner(
            output=str(parsed.junit_dir),
            verbosity=2,
        )
    else:
        runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    if parsed.json_report is not None:
        parsed.json_report.parent.mkdir(parents=True, exist_ok=True)
        major, minor = (int(part) for part in parsed.target.split(".", 1))
        manifests = [
            case.to_manifest()
            for case in REAL_PARITY_CASES
            if case.supports((major, minor))
            and (
                parsed.case_filter is None
                or any(
                    fnmatch.fnmatch(case.name, pattern.strip())
                    for pattern in parsed.case_filter.split(",")
                    if pattern.strip()
                )
            )
        ]
        parsed.json_report.write_text(
            json.dumps(
                {
                    "target": parsed.target,
                    "case_filter": parsed.case_filter,
                    "seed": parsed.seed,
                    "duration_seconds": time.time() - started_at,
                    "tests_run": result.testsRun,
                    "successful": result.wasSuccessful(),
                    "failures": [_failure_document(item) for item in result.failures],
                    "errors": [_failure_document(item) for item in result.errors],
                    "skipped": [
                        {"test": str(test), "reason": reason}
                        for test, reason in result.skipped
                    ],
                    "cases": manifests,
                },
                default=str,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())
