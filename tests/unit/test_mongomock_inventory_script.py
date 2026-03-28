from __future__ import annotations

import importlib.util
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "inventory_mongomock_suite.py"
    spec = importlib.util.spec_from_file_location("inventory_mongomock_suite", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MongomockInventoryScriptTests(unittest.TestCase):
    def test_inventory_suite_extracts_test_functions_and_methods(self):
        module = _load_script_module()
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            tests_dir = root / "tests"
            tests_dir.mkdir()
            (tests_dir / "test_collection_api.py").write_text(
                textwrap.dedent(
                    """
                    def test_top_level_case():
                        assert True

                    class CollectionTests:
                        def test_method_case(self):
                            assert True
                    """
                ),
                encoding="utf-8",
            )

            payload = module.inventory_suite(tests_dir)

        self.assertEqual(payload["files"], 1)
        self.assertEqual(payload["total_cases"], 2)
        self.assertEqual(payload["category_counts"]["collection"], 2)
        case_names = {case["test_name"] for case in payload["cases"]}
        self.assertEqual(case_names, {"test_top_level_case", "test_method_case"})

    def test_infer_category_uses_filename_heuristics(self):
        module = _load_script_module()

        category = module.infer_category(Path("tests/test_aggregate_cursor.py"))

        self.assertEqual(category, "aggregation")
