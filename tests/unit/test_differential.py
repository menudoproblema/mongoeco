import importlib.util
import json
import tempfile
import unittest

from pathlib import Path
from typing import ClassVar
from unittest.mock import MagicMock, patch

from scripts.capture_differential_replay_golden import _capture_expectation
from scripts.run_mongodb_real_differential import (
    inspect_server_runtime,
    main as differential_main,
)

from tests.differential._real_parity_base import MongoRealParityBase
from tests.differential.cases import (
    REAL_CAPTURE_PENDING_CASES,
    REAL_PARITY_CASES,
    RealParityCase,
)
from tests.differential.runner import available_case_names, build_suite


def test_native_capture_normalizes_only_generated_error_namespaces():
    namespace = "mongoeco_replay_capture_" + "a" * 32
    value = {"ok": False, "code": 9, "message": f"error on {namespace}.cases"}
    result = _capture_expectation([value])
    assert result == [
        {
            "ok": False,
            "code": 9,
            "message": "error on mongoeco_replay_capture_UUID.cases",
        }
    ]
    assert value["message"] == f"error on {namespace}.cases"
    assert _capture_expectation({"ok": True, "message": namespace}) == {
        "ok": True,
        "message": namespace,
    }
    successful_result = {"ok": True, "result": [value]}
    assert _capture_expectation(successful_result) == successful_result


class _MongoClientStub:
    seen_dialects: ClassVar[list[str | None]] = []

    def __init__(self, _engine, *, mongodb_dialect=None, **_kwargs):
        type(self).seen_dialects.append(mongodb_dialect)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get_database(self, _name):
        return self

    def get_collection(self, _name):
        return self

    def insert_one(self, _document):
        return None


class MongoRealParityBaseUnitTests(unittest.TestCase):
    def test_cli_preflight_failure_is_reported_as_failure_not_skip(self):
        result, report = self._run_cli(
            unittest.TestSuite([unittest.FunctionTestCase(lambda: None)]),
            preflight_error=RuntimeError("cannot connect to real MongoDB"),
        )
        self.assertEqual(result, 1)
        self.assertFalse(report["successful"])
        self.assertEqual(report["tests_run"], 0)
        self.assertEqual(report["preflight_error"]["error_type"], "RuntimeError")
        self.assertTrue(report["errors"])

    def test_cli_unexpected_skips_fail_the_gate(self):
        def skipped():
            message = "connection failed after preflight"
            raise unittest.SkipTest(message)

        result, report = self._run_cli(
            unittest.TestSuite([unittest.FunctionTestCase(skipped)])
        )
        self.assertEqual(result, 1)
        self.assertFalse(report["successful"])
        self.assertEqual(len(report["skipped"]), 1)

    def test_cli_reports_runtime_for_successful_nonempty_suite(self):
        result, report = self._run_cli(
            unittest.TestSuite([unittest.FunctionTestCase(lambda: None)])
        )
        self.assertEqual(result, 0)
        self.assertTrue(report["successful"])
        self.assertEqual(report["tests_run"], 1)
        self.assertEqual(report["expected_tests"], 1)
        self.assertEqual(report["runtime"], {"pymongo": "4.18.2"})

    def test_cli_rejects_empty_suite_before_connecting(self):
        with (
            patch.dict("os.environ", {"MONGOECO_REAL_MONGODB_URI": "mongodb://unused"}),
            patch(
                "scripts.run_mongodb_real_differential.build_suite",
                return_value=unittest.TestSuite(),
            ),
            patch(
                "scripts.run_mongodb_real_differential.inspect_server_runtime"
            ) as inspect,
        ):
            self.assertEqual(differential_main(["runner", "7.0"]), 2)
            inspect.assert_not_called()

    @unittest.skipUnless(
        importlib.util.find_spec("pymongo"), "requires optional PyMongo"
    )
    def test_preflight_rejects_wrong_server_and_unstable_fcv(self):
        for version, fcv in (
            ([8, 0, 32], {"version": "8.0"}),
            ([9, 0, 2], {"version": "8.3"}),
            ([9, 0, 2], {"version": "9.0", "targetVersion": "9.0"}),
        ):
            with self.subTest(version=version, fcv=fcv):
                client = MagicMock()
                client.__enter__.return_value = client
                client.server_info.return_value = {"versionArray": version}
                client.admin.command.return_value = {"featureCompatibilityVersion": fcv}
                with (
                    patch("pymongo.MongoClient", return_value=client),
                    self.assertRaisesRegex(RuntimeError, "requires"),
                ):
                    inspect_server_runtime("mongodb://unused", "9.0")
                client.__exit__.assert_called_once()

    def _run_cli(self, suite, *, preflight_error=None):
        with tempfile.TemporaryDirectory() as directory:
            report_path = Path(directory) / "report.json"
            with (
                patch.dict(
                    "os.environ", {"MONGOECO_REAL_MONGODB_URI": "mongodb://unused"}
                ),
                patch(
                    "scripts.run_mongodb_real_differential.build_suite",
                    return_value=suite,
                ),
                patch(
                    "scripts.run_mongodb_real_differential.inspect_server_runtime",
                    return_value={"pymongo": "4.18.2"},
                    side_effect=preflight_error,
                ),
            ):
                result = differential_main(
                    ["runner", "7.0", "--json-report", str(report_path)]
                )
            return result, json.loads(report_path.read_text())

    def test_assert_matches_real_uses_target_dialect_for_mongoeco_client(self):
        class Harness(MongoRealParityBase):
            TARGET_VERSION = (8, 0)

        harness = Harness(methodName="runTest")
        harness._real_client = type(
            "RealClientStub",
            (),
            {
                "__getitem__": lambda self, _name: self,
                "insert_one": lambda self, _document: None,
                "drop_database": lambda self, _name: None,
            },
        )()

        _MongoClientStub.seen_dialects = []
        case = RealParityCase(
            name="stub_case",
            seed_documents=[{"_id": "1"}],
            action=lambda _collection: "ok",
        )

        with patch(
            "tests.differential._real_parity_base.MongoClient", _MongoClientStub
        ):
            harness._assert_matches_real_case(case)

        self.assertEqual(_MongoClientStub.seen_dialects, ["8.0", "8.0"])

    def test_real_parity_cases_are_exposed_as_dynamic_tests(self):
        dynamic_names = {
            name
            for name in dir(MongoRealParityBase)
            if name.startswith("test_") and name.endswith("_matches_real_mongodb")
        }

        expected_names = {
            f"test_{case.name}_matches_real_mongodb" for case in REAL_PARITY_CASES
        }

        self.assertTrue(expected_names)
        self.assertEqual(dynamic_names, expected_names)

    def test_pending_capture_cases_are_real_executable_cases(self):
        executable_names = {case.name for case in REAL_PARITY_CASES}

        self.assertFalse(REAL_CAPTURE_PENDING_CASES)
        self.assertLessEqual(REAL_CAPTURE_PENDING_CASES, executable_names)

    def test_available_case_names_filters_by_target_version(self):
        all_cases = available_case_names()
        targeted_cases = available_case_names((7, 0))

        self.assertTrue(all_cases)
        self.assertEqual(all_cases, targeted_cases)

    def test_build_suite_can_filter_case_pattern(self):
        suite = build_suite("7.0", "find_expr_*")
        names = {test._testMethodName for test in self._iter_tests(suite)}

        self.assertIn("test_find_expr_compare_fields_matches_real_mongodb", names)
        self.assertIn("test_find_expr_truthiness_array_matches_real_mongodb", names)
        self.assertNotIn(
            "test_find_subdocument_order_sensitive_equality_matches_real_mongodb", names
        )

    def test_build_suite_rejects_filters_that_match_no_cases(self):
        with self.assertRaisesRegex(ValueError, "matched no differential cases"):
            build_suite("7.0", "definitely-not-a-case")

    def test_differential_cli_returns_usage_error_for_empty_filter(self):
        with (
            patch.dict(
                "os.environ",
                {"MONGOECO_REAL_MONGODB_URI": "mongodb://unused"},
            ),
            patch(
                "scripts.run_mongodb_real_differential.build_suite",
                side_effect=ValueError("case filter matched no differential cases"),
            ),
        ):
            result = differential_main(
                ["run_mongodb_real_differential.py", "7.0", "missing"],
            )

        self.assertEqual(result, 2)

    def _iter_tests(self, suite: unittest.TestSuite):
        for test in suite:
            if isinstance(test, unittest.TestSuite):
                yield from self._iter_tests(test)
            else:
                yield test


def test_native_index_build_ids_are_incidental_but_cause_and_values_are_not():
    first = {
        "ok": False,
        "code": 16755,
        "code_name": "Location16755",
        "error_labels": [],
        "message": (
            "Index build failed: 4a175f24-5491-4aa2-8f83-d5b2c976a98c: "
            "Collection mongoeco_replay_capture_15806d4025ed48f188a9744be0d94bff.cases "
            "( 99b952c8-aecf-48e7-95ef-275f98d80772 ) :: caused by :: "
            "Point must only contain numeric elements; document UUID "
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        ),
    }
    second = {
        **first,
        "message": (
            "Index build failed: 4feabd8f-b89f-4f0e-8d54-aba5147997dd: "
            "Collection mongoeco_replay_capture_dfd626035a774d4f86d29521132df615.cases "
            "( 4f3e0a2a-66a5-402f-9d7e-f93dd4f8901c ) :: caused by :: "
            "Point must only contain numeric elements; document UUID "
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
        ),
    }
    assert _capture_expectation(first) == _capture_expectation(second)
    for changed in (
        {**second, "code": 1},
        {**second, "error_labels": ["RetryableWriteError"]},
        {**second, "message": second["message"] + " changed cause"},
        {
            **second,
            "message": second["message"].replace(
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            ),
        },
    ):
        assert _capture_expectation(first) != _capture_expectation(changed)
    assert _capture_expectation({"ok": True, "result": first["message"]}) == {
        "ok": True,
        "result": first["message"],
    }


def test_index_scan_duration_is_normalized_without_hiding_scan_or_document_data():
    message = (
        "Index build failed: 4a175f24-5491-4aa2-8f83-d5b2c976a98c: "
        "Collection mongoeco_replay_capture_15806d4025ed48f188a9744be0d94bff.cases "
        "( 99b952c8-aecf-48e7-95ef-275f98d80772 ) :: caused by :: "
        "collection scan stopped. totalRecords: 0; durationMillis: 1ms; "
        "phase: collection scan; collectionScanPosition: (None); "
        "readSource: kNoTimestamp :: caused by :: invalid geometry; "
        "document value durationMillis: 7ms"
    )
    original = {"ok": False, "code": 16755, "message": message}
    changed_time = {
        **original,
        "message": message.replace("durationMillis: 1ms", "durationMillis: 12ms"),
    }
    assert _capture_expectation(original) == _capture_expectation(changed_time)
    for changed_message in (
        message.replace("totalRecords: 0", "totalRecords: 1"),
        message.replace("phase: collection scan", "phase: other"),
        message.replace("invalid geometry", "different failure"),
        message.replace(
            "document value durationMillis: 7ms", "document value durationMillis: 8ms"
        ),
    ):
        assert _capture_expectation(original) != _capture_expectation(
            {**original, "message": changed_message}
        )
