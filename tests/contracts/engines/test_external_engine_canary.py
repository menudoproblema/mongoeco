from __future__ import annotations

import ast
import unittest

from pathlib import Path

from jsonschema import Draft202012Validator

from mongoeco.conformance import (
    ConformanceStatus,
    EngineConformanceProvider,
    conformance_report_schema,
    run_engine_conformance,
)

from tests.consumer.engine_canary import ExternalCanaryEngine


class ExternalEngineCanaryTests(unittest.IsolatedAsyncioTestCase):
    async def test_independent_engine_passes_every_declared_capability(self) -> None:
        report = await run_engine_conformance(
            EngineConformanceProvider("external-canary", ExternalCanaryEngine),
        )

        report.require_success()
        Draft202012Validator(conformance_report_schema()).validate(
            report.to_document(),
        )
        self.assertTrue(
            all(
                check.status is ConformanceStatus.PASSED
                for check in report.checks
                if check.applicable
            ),
        )
        self.assertGreaterEqual(len(report.inapplicable), 4)

    def test_canary_imports_only_the_curated_engine_surface(self) -> None:
        path = Path(__file__).parents[2] / "consumer" / "engine_canary.py"
        module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        mongoeco_imports = {
            node.module
            for node in ast.walk(module)
            if isinstance(node, ast.ImportFrom)
            and node.module is not None
            and node.module.startswith("mongoeco")
        }

        self.assertEqual(mongoeco_imports, {"mongoeco.engines"})
