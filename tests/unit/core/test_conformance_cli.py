from __future__ import annotations

import contextlib
import io
import json
import os
import runpy
import subprocess
import sys
import tempfile
import unittest

from pathlib import Path
from unittest.mock import patch

from jsonschema import Draft202012Validator

from mongoeco.conformance import cli as conformance_cli, conformance_report_schema
from mongoeco.conformance.cli import _parse_profiles
from mongoeco.conformance.models import ConformanceProfile


class ConformanceCliTests(unittest.TestCase):
    @staticmethod
    def _run(
        *arguments: str, cwd: Path | None = None
    ) -> subprocess.CompletedProcess[str]:
        environment = dict(os.environ)
        source_root = str(Path(__file__).resolve().parents[3] / "src")
        project_root = str(Path(__file__).resolve().parents[3])
        environment["PYTHONPATH"] = os.pathsep.join((source_root, project_root))
        return subprocess.run(  # noqa: S603 - fixed interpreter and module invocation
            [sys.executable, "-m", "mongoeco.conformance", *arguments],
            cwd=cwd,
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )

    def test_memory_json_report_is_schema_valid(self) -> None:
        result = self._run(
            "mongoeco.engines:MemoryEngine",
            "--format",
            "json",
            "--require-success",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        document = json.loads(result.stdout)
        Draft202012Validator(conformance_report_schema()).validate(document)
        self.assertTrue(document["passed"])

    def test_sqlite_human_report_and_profile_selection(self) -> None:
        result = self._run(
            "mongoeco.engines:SQLiteEngine",
            "--profile",
            "spi-v2-core",
            "--require-success",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("PASS", result.stdout)
        self.assertIn("spi-v2", result.stdout)

    def test_profile_parser_accepts_repeated_and_comma_separated_values(self) -> None:
        self.assertEqual(
            _parse_profiles(["spi-v2-core,search-v1", "spi-v2-core"]),
            (
                ConformanceProfile.SPI_V2_CORE,
                ConformanceProfile.SEARCH_V1,
            ),
        )

    def test_async_canary_factory_preserves_not_applicable_checks(self) -> None:
        result = self._run(
            "tests.consumer.conformance_cli_fixtures:async_canary_factory",
            "--format",
            "json",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        document = json.loads(result.stdout)
        self.assertGreater(document["summary"]["not-applicable"], 0)

    def test_output_file_receives_json_without_stdout_noise(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.json"
            result = self._run(
                "mongoeco.engines:MemoryEngine",
                "--profile",
                "spi-v2-core",
                "--format",
                "json",
                "--output",
                str(output),
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(result.stdout, "")
            self.assertTrue(json.loads(output.read_text(encoding="utf-8"))["passed"])

    def test_load_errors_have_stable_exit_code(self) -> None:
        for provider in (
            "missing-module:factory",
            "tests.consumer.conformance_cli_fixtures:missing",
            "tests.consumer.conformance_cli_fixtures:not_callable",
        ):
            with self.subTest(provider=provider):
                result = self._run(provider)
                self.assertEqual(result.returncode, 2)
                self.assertIn("load error", result.stderr)

    def test_contract_failure_is_distinct_from_internal_error(self) -> None:
        failed = self._run(
            "builtins:object",
            "--format",
            "json",
            "--require-success",
        )
        self.assertEqual(failed.returncode, 1, failed.stderr)
        self.assertFalse(json.loads(failed.stdout)["passed"])

        internal = self._run(
            "tests.consumer.conformance_cli_fixtures:raising_factory",
        )
        self.assertEqual(internal.returncode, 3)
        self.assertIn("internal error", internal.stderr)

    def test_cleanup_failure_is_reported_as_contract_failure(self) -> None:
        result = self._run(
            "tests.consumer.engine_canary:ExternalCanaryEngine",
            "--cleanup",
            "tests.consumer.conformance_cli_fixtures:cleanup_failure",
            "--format",
            "json",
            "--require-success",
        )
        self.assertEqual(result.returncode, 1, result.stderr)
        document = json.loads(result.stdout)
        self.assertTrue(any(check["cleanupError"] for check in document["checks"]))

    def test_timeout_has_stable_exit_code(self) -> None:
        result = self._run(
            "tests.consumer.conformance_cli_fixtures:SlowCanaryEngine",
            "--timeout",
            "0.01",
        )
        self.assertEqual(result.returncode, 4)
        self.assertIn("timed out", result.stderr)

    def test_schema_failure_has_stable_exit_code(self) -> None:
        diagnostics = io.StringIO()
        with (
            patch.object(
                conformance_cli,
                "conformance_report_schema",
                return_value={"not": {}},
            ),
            contextlib.redirect_stderr(diagnostics),
        ):
            exit_code = conformance_cli.main(
                ["mongoeco.engines:MemoryEngine", "--profile", "spi-v2-core"]
            )
        self.assertEqual(exit_code, 5)
        self.assertIn("schema error", diagnostics.getvalue())

    def test_direct_main_covers_success_contract_load_internal_and_timeout(
        self,
    ) -> None:
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            success = conformance_cli.main(
                [
                    "mongoeco.engines:MemoryEngine",
                    "--profile",
                    "spi-v2-core",
                    "--format",
                    "json",
                    "--require-success",
                ]
            )
        self.assertEqual(success, 0)
        self.assertTrue(json.loads(output.getvalue())["passed"])

        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            failed = conformance_cli.main(
                ["builtins:object", "--format", "json", "--require-success"]
            )
        self.assertEqual(failed, 1)
        self.assertFalse(json.loads(output.getvalue())["passed"])

        for arguments, expected, fragment in (
            (["invalid"], 2, "load error"),
            (
                ["tests.consumer.conformance_cli_fixtures:raising_factory"],
                3,
                "internal error",
            ),
            (
                [
                    "tests.consumer.conformance_cli_fixtures:SlowCanaryEngine",
                    "--timeout",
                    "0.001",
                ],
                4,
                "timed out",
            ),
        ):
            with self.subTest(arguments=arguments):
                diagnostics = io.StringIO()
                with contextlib.redirect_stderr(diagnostics):
                    exit_code = conformance_cli.main(arguments)
                self.assertEqual(exit_code, expected)
                self.assertIn(fragment, diagnostics.getvalue())

    def test_direct_main_writes_file_and_rejects_invalid_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "report.txt"
            exit_code = conformance_cli.main(
                [
                    "mongoeco.engines:MemoryEngine",
                    "--profile",
                    "spi-v2-core",
                    "--output",
                    str(output),
                ]
            )
            self.assertEqual(exit_code, 0)
            self.assertIn("PASS", output.read_text(encoding="utf-8"))

        diagnostics = io.StringIO()
        with contextlib.redirect_stderr(diagnostics):
            exit_code = conformance_cli.main(
                [
                    "mongoeco.engines:MemoryEngine",
                    "--cleanup",
                    "tests.consumer.conformance_cli_fixtures:not_callable",
                ]
            )
        self.assertEqual(exit_code, 2)
        self.assertIn("cleanup is not callable", diagnostics.getvalue())

    def test_schema_validation_requires_jsonschema(self) -> None:
        real_import = conformance_cli.importlib.import_module

        def import_without_jsonschema(name: str):
            if name == "jsonschema":
                raise ImportError
            return real_import(name)

        with (
            patch.object(
                conformance_cli.importlib,
                "import_module",
                side_effect=import_without_jsonschema,
            ),
            self.assertRaisesRegex(RuntimeError, "engine-testing extra"),
        ):
            conformance_cli._validate_report({})

    def test_module_entry_point_delegates_to_cli_main(self) -> None:
        with (
            patch.object(conformance_cli, "main", return_value=7),
            self.assertRaises(SystemExit) as raised,
        ):
            runpy.run_module("mongoeco.conformance", run_name="__main__")
        self.assertEqual(raised.exception.code, 7)
