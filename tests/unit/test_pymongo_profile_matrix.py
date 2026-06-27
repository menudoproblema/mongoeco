import importlib.util
import json
from pathlib import Path
import unittest

from mongoeco.compat import PYMONGO_PROFILES


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = PROJECT_ROOT / "scripts" / "run_pymongo_profile_matrix.py"


def load_matrix_script():
    spec = importlib.util.spec_from_file_location("run_pymongo_profile_matrix", SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class PyMongoProfileMatrixScriptTests(unittest.TestCase):
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
        fixture_path = PROJECT_ROOT / "tests" / "fixtures" / "pymongo_profile_matrix.json"
        fixture = json.loads(fixture_path.read_text(encoding="utf-8"))
        generated_from = fixture["generated_from"]

        for profile_key in PYMONGO_PROFILES:
            with self.subTest(profile=profile_key):
                self.assertTrue(
                    any(item.startswith(f"PyMongo {profile_key}.") for item in generated_from),
                    generated_from,
                )


if __name__ == "__main__":
    unittest.main()
