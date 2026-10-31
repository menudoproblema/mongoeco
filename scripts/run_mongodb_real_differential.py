import argparse
import importlib
import json
import os
import platform
import random
import sys
import time
import unittest

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if (
    os.environ.get("MONGOECO_TEST_INSTALLED_ARTIFACT") != "1"
    and str(SRC_ROOT) not in sys.path
):
    sys.path.insert(0, str(SRC_ROOT))

REAL_PARITY_CASES = importlib.import_module(
    "tests.differential.cases",
).REAL_PARITY_CASES
_differential_runner = importlib.import_module("tests.differential.runner")
available_case_names = _differential_runner.available_case_names
build_suite = _differential_runner.build_suite


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("target", choices=tuple(_differential_runner.TARGET_MODULES))
    parser.add_argument("case_filter", nargs="?")
    listing = parser.add_mutually_exclusive_group()
    listing.add_argument("--list-cases", action="store_true")
    listing.add_argument("--list-corpora", action="store_true")
    parser.add_argument(
        "--partition", choices=("all", "standalone", "replica-set"), default="all"
    )
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


def inspect_server_runtime(
    uri: str, target: str, *, require_replica_set: bool = False
) -> dict[str, object]:
    """Require the actual server and FCV, never just a configured target."""
    import pymongo  # noqa: PLC0415 - optional dependency only needed for real integration

    with pymongo.MongoClient(
        uri, serverSelectionTimeoutMS=3000, socketTimeoutMS=3000
    ) as client:
        client.admin.command("ping")
        build_info = client.server_info()
        version = build_info.get("versionArray", [])
        expected = [int(part) for part in target.split(".")]
        if version[:2] != expected:
            message = f"requires MongoDB {target}.x, got {version!r}"
            raise RuntimeError(message)
        fcv = client.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        if fcv.get("version") != target or any(
            key in fcv for key in ("targetVersion", "previousVersion")
        ):
            message = f"requires stable FCV {target}, got {fcv!r}"
            raise RuntimeError(message)
        hello = client.admin.command("hello") if require_replica_set else None
        if require_replica_set and not (
            hello.get("setName") and hello.get("isWritablePrimary")
        ):
            message = "requires a writable replica-set primary"
            raise RuntimeError(message)
    runtime = {
        "build_info": build_info,
        "fcv": fcv,
        "pymongo": pymongo.version,
        "python": platform.python_version(),
    }
    if hello is not None:
        runtime["hello"] = hello
    return runtime


def main(argv: list[str] | None = None) -> int:
    args = sys.argv if argv is None else argv
    parsed = _parse_args(args)
    version = tuple(int(part) for part in parsed.target.split("."))
    selected = _differential_runner.select_cases(
        version, parsed.case_filter, partition=parsed.partition
    )
    if parsed.list_cases:
        for case_name in sorted(case.name for case in selected):
            sys.stdout.write(f"{case_name}\n")
        return 0
    if parsed.list_corpora:
        corpora = importlib.import_module(
            "tests.differential.semantic_corpora"
        ).SEMANTIC_CORPORA
        names = {case.name for case in selected}
        json.dump(
            [
                {"case_set": corpus.case_set, "fixture_key": key}
                for key, corpus in corpora.items()
                if names.intersection(name for name, _ in corpus.cases)
            ],
            sys.stdout,
            indent=2,
        )
        sys.stdout.write("\n")
        return 0

    if not os.getenv("MONGOECO_REAL_MONGODB_URI"):
        sys.stderr.write("MONGOECO_REAL_MONGODB_URI is not configured\n")
        return 2

    random.seed(parsed.seed)
    started_at = time.time()
    try:
        suite = build_suite(
            parsed.target, parsed.case_filter, partition=parsed.partition
        )
    except ValueError as error:
        sys.stderr.write(f"{error}\n")
        return 2
    expected_tests = suite.countTestCases()
    if expected_tests == 0:
        sys.stderr.write("differential suite resolved to zero tests\n")
        return 2
    runtime: dict[str, object] = {}
    preflight_error = None
    try:
        runtime = inspect_server_runtime(
            os.environ["MONGOECO_REAL_MONGODB_URI"],
            parsed.target,
            require_replica_set=any(case.requires_replica_set for case in selected),
        )
    except Exception as error:
        preflight_error = {"error_type": type(error).__name__, "error": str(error)}

        def fail_preflight(cause=error):
            raise cause

        suite = unittest.TestSuite(
            [
                unittest.FunctionTestCase(
                    fail_preflight, description="real MongoDB preflight"
                )
            ]
        )
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
    successful = (
        preflight_error is None
        and result.wasSuccessful()
        and result.testsRun == expected_tests
        and not result.skipped
    )
    if parsed.json_report is not None:
        parsed.json_report.parent.mkdir(parents=True, exist_ok=True)
        manifests = [case.to_manifest() for case in selected]
        parsed.json_report.write_text(
            json.dumps(
                {
                    "target": parsed.target,
                    "case_filter": parsed.case_filter,
                    "partition": parsed.partition,
                    "seed": parsed.seed,
                    "duration_seconds": time.time() - started_at,
                    "expected_tests": expected_tests,
                    "tests_run": result.testsRun if preflight_error is None else 0,
                    "successful": successful,
                    "runtime": runtime,
                    "preflight_error": preflight_error,
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
    return 0 if successful else 1


if __name__ == "__main__":
    raise SystemExit(main())
