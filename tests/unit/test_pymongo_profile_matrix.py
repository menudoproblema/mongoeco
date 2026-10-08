import importlib.util
import json
import subprocess
import tempfile
import unittest

from pathlib import Path
from unittest.mock import patch

from mongoeco.compat import PYMONGO_PROFILES


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_pymongo_profile_matrix.py"


def load_matrix_script():
    spec = importlib.util.spec_from_file_location(
        "run_pymongo_profile_matrix", SCRIPT_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PyMongoProfileMatrixScriptTests(unittest.TestCase):
    @unittest.skipUnless(
        importlib.util.find_spec("pymongo"), "requires real PyMongo exceptions"
    )
    def test_probe_distinguishes_validation_from_execution(self):
        from pymongo.errors import (  # noqa: PLC0415 - optional driver test
            ConfigurationError,
            ServerSelectionTimeoutError,
        )

        module = load_matrix_script()

        def fail(error):
            def call():
                raise error

            return call

        for error, accepted, status in (
            (ConfigurationError("reserved option"), False, "configuration-rejected"),
            (TypeError("invalid argument"), False, "signature-rejected"),
            (
                ServerSelectionTimeoutError("no probe server"),
                True,
                "arguments-accepted",
            ),
            (RuntimeError("unexpected failure"), None, "indeterminate"),
        ):
            with self.subTest(error=type(error).__name__):
                result = module.probe_call(fail(error))
                self.assertIs(result["accepted"], accepted)
                self.assertEqual(result["status"], status)
        self.assertEqual(module.probe_call(lambda: None)["status"], "completed")

    def test_summary_refuses_indeterminate_evidence(self):
        module = load_matrix_script()
        with self.assertRaisesRegex(ValueError, "Indeterminate probe"):
            module.summarize_results(
                {"4.18.2": {"aggregate.aggregate": {"accepted": None}}}
            )

    def test_cached_environment_with_wrong_driver_is_rejected(self):
        module = load_matrix_script()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            python_bin = root / "4.18.2" / "bin" / "python"
            python_bin.parent.mkdir(parents=True)
            python_bin.touch()
            result = subprocess.CompletedProcess([], 0, stdout="4.17.0\n")
            with (
                patch.object(module, "run", return_value=result),
                self.assertRaisesRegex(RuntimeError, "has 4.17.0, expected 4.18.2"),
            ):
                module.ensure_venv("4.18.2", root, "python")

    def test_summary_output_matches_curated_fixture_shape(self):
        module = load_matrix_script()
        unsupported = {
            "update_one.max_time_ms",
            "update_many.max_time_ms",
            "replace_one.max_time_ms",
            "delete_one.max_time_ms",
            "delete_many.max_time_ms",
        }
        deltas = {
            "update_one.sort",
            "replace_one.sort",
            "bulk_write.sort",
            "bulk_write.replace_sort",
        }
        results = {}
        for version in ("4.9.2", "4.11.3", "4.17.0"):
            version_results = {}
            for check in module.CHECK_ORDER:
                accepted = check not in unsupported and (
                    check not in deltas or version != "4.9.2"
                )
                version_results[check] = {
                    "accepted": accepted,
                    "error_type": None if accepted else "TypeError",
                    "error": None if accepted else "unexpected keyword argument",
                }
            results[version] = version_results

        summary = module.summarize_results(results)

        self.assertEqual(
            summary["generated_from"],
            ["PyMongo 4.9.2", "PyMongo 4.11.3", "PyMongo 4.17.0"],
        )
        self.assertEqual(
            summary["confirmed_profile_deltas"],
            {
                "4.11_plus": [
                    "update_one.sort",
                    "replace_one.sort",
                    "bulk_write.UpdateOne.sort",
                    "bulk_write.ReplaceOne.sort",
                ],
            },
        )
        self.assertEqual(
            summary["confirmed_unsupported_in_4_9_to_4_17"],
            {
                "update_one": ["max_time_ms"],
                "update_many": ["max_time_ms"],
                "replace_one": ["max_time_ms"],
                "delete_one": ["max_time_ms"],
                "delete_many": ["max_time_ms"],
            },
        )
        self.assertEqual(
            summary["confirmed_baseline_4_9_plus"]["bulk_write"],
            ["comment", "let", "UpdateOne.hint", "DeleteOne.hint"],
        )

    def test_fixture_covers_all_official_pymongo_profiles(self):
        fixture_path = (
            PROJECT_ROOT / "tests" / "fixtures" / "pymongo_profile_matrix.json"
        )
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        generated_from = fixture["generated_from"]

        for profile_key in PYMONGO_PROFILES:
            with self.subTest(profile=profile_key):
                self.assertTrue(
                    any(
                        item.startswith(f"PyMongo {profile_key}.")
                        for item in generated_from
                    ),
                    generated_from,
                )


if __name__ == "__main__":
    unittest.main()
