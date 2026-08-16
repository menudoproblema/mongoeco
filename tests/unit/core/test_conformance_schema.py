from __future__ import annotations

import json
import unittest

from copy import deepcopy
from pathlib import Path

from jsonschema import Draft202012Validator, ValidationError

from mongoeco.conformance import (
    CONFORMANCE_REPORT_SCHEMA_VERSION,
    ConformanceCheckResult,
    ConformancePhase,
    ConformanceProfile,
    ConformanceReport,
    ConformanceStatus,
    conformance_report_schema,
)


class ConformanceReportSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        self.schema = conformance_report_schema()
        Draft202012Validator.check_schema(self.schema)
        self.validator = Draft202012Validator(self.schema)

    @staticmethod
    def _report(*checks: ConformanceCheckResult) -> dict[str, object]:
        return ConformanceReport("external", "spi-v2", checks).to_document()

    def test_schema_validates_public_report_states(self) -> None:
        checks = (
            ConformanceCheckResult(
                ConformanceProfile.SPI_V2_CORE,
                "passed",
                status=ConformanceStatus.PASSED,
                capability="spi-v2",
            ),
            ConformanceCheckResult(
                ConformanceProfile.SPI_V2_CORE,
                "failed",
                status=ConformanceStatus.FAILED,
                capability="typed-outcomes",
                detail="contract mismatch",
            ),
            ConformanceCheckResult(
                ConformanceProfile.SPI_V2_CLOCK,
                "error",
                status=ConformanceStatus.ERROR,
                capability="injected-clock",
                detail="unexpected exception",
            ),
            ConformanceCheckResult(
                ConformanceProfile.SEARCH_V1,
                "not-applicable",
                status=ConformanceStatus.NOT_APPLICABLE,
                capability="search-v1",
                detail="capability is not declared",
            ),
        )

        self.validator.validate(self._report(*checks))

    def test_schema_validates_cleanup_failure(self) -> None:
        check = ConformanceCheckResult(
            ConformanceProfile.SPI_V2_CORE,
            "cleanup",
            status=ConformanceStatus.ERROR,
            capability="spi-v2",
            phase=ConformancePhase.CLEANUP,
            detail="cleanup RuntimeError",
            cleanup_error="RuntimeError: cleanup failed",
        )

        self.validator.validate(self._report(check))

    def test_schema_rejects_contradictory_check_states(self) -> None:
        document = self._report(
            ConformanceCheckResult(
                ConformanceProfile.SPI_V2_CORE,
                "passed",
                status=ConformanceStatus.PASSED,
                capability="spi-v2",
            ),
        )
        invalid_documents = []
        for field, value in (
            ("passed", False),
            ("applicable", False),
            ("detail", "impossible"),
            ("cleanupError", "impossible"),
        ):
            invalid = deepcopy(document)
            invalid["checks"][0][field] = value
            invalid_documents.append(invalid)

        for invalid in invalid_documents:
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                self.validator.validate(invalid)

    def test_schema_rejects_contradictory_report_states(self) -> None:
        passed = self._report(
            ConformanceCheckResult(
                ConformanceProfile.SPI_V2_CORE,
                "passed",
                status=ConformanceStatus.PASSED,
                capability="spi-v2",
            ),
        )
        invalid_documents = []

        false_success = deepcopy(passed)
        false_success["passed"] = False
        invalid_documents.append(false_success)

        empty_success = deepcopy(passed)
        empty_success["checks"] = []
        empty_success["summary"]["passed"] = 0
        invalid_documents.append(empty_success)

        hidden_failure = deepcopy(passed)
        hidden_failure["summary"]["failed"] = 1
        invalid_documents.append(hidden_failure)

        for invalid in invalid_documents:
            with self.subTest(invalid=invalid), self.assertRaises(ValidationError):
                self.validator.validate(invalid)

    def test_checked_in_v1_compatibility_fixture_remains_valid(self) -> None:
        fixture = json.loads(
            Path("tests/fixtures/conformance_report_v1.json").read_text(
                encoding="utf-8",
            ),
        )

        self.validator.validate(fixture)
        self.assertEqual(
            fixture["schemaVersion"],
            CONFORMANCE_REPORT_SCHEMA_VERSION,
        )

    def test_schema_is_owned_versioned_and_json_serializable(self) -> None:
        first = conformance_report_schema()
        second = conformance_report_schema()
        first["title"] = "mutated"

        self.assertNotEqual(first, second)
        self.assertEqual(
            second["properties"]["schemaVersion"]["const"],
            CONFORMANCE_REPORT_SCHEMA_VERSION,
        )
        json.dumps(second)
