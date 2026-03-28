from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path


def _load_script_module():
    script_path = Path(__file__).resolve().parents[2] / "scripts" / "build_mongomock_matrix.py"
    spec = importlib.util.spec_from_file_location("build_mongomock_matrix", script_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class MongomockMatrixScriptTests(unittest.TestCase):
    def test_build_matrix_creates_rows_for_every_inventory_case(self):
        module = _load_script_module()

        matrix = module.build_matrix(
            {
                "root": "/tmp/mongomock/tests",
                "cases": [
                    {
                        "file": "test__collection_api.py",
                        "class_name": None,
                        "test_name": "test_top_level_case",
                        "category": "collection",
                    },
                    {
                        "file": "test__collection_api.py",
                        "class_name": "CollectionTests",
                        "test_name": "test_method_case",
                        "category": "collection",
                    },
                ],
            }
        )

        self.assertEqual(matrix["inventory_root"], "/tmp/mongomock/tests")
        self.assertEqual(matrix["inventory_total_cases"], 2)
        self.assertEqual(matrix["status_counts"], {"review-needed": 2})
        self.assertEqual(
            [case["id"] for case in matrix["cases"]],
            [
                "test__collection_api.py::test_top_level_case",
                "test__collection_api.py::CollectionTests::test_method_case",
            ],
        )
        self.assertTrue(all(case["coverage"] == "unknown" for case in matrix["cases"]))

    def test_build_matrix_applies_category_rules(self):
        module = _load_script_module()

        matrix = module.build_matrix(
            {
                "root": "/tmp/mongomock/tests",
                "cases": [
                    {
                        "file": "test__gridfs.py",
                        "class_name": None,
                        "test_name": "test_gridfs_case",
                        "category": "_gridfs",
                    },
                    {
                        "file": "test_api.py",
                        "class_name": None,
                        "test_name": "test_collection_case",
                        "category": "collection",
                    },
                ],
            },
            rules={
                "category_rules": {
                    "_gridfs": {
                        "status": "outside-scope",
                        "note": "GridFS fuera de alcance.",
                    }
                }
            },
        )

        self.assertEqual(matrix["status_counts"], {"outside-scope": 1, "review-needed": 1})
        self.assertEqual(matrix["cases"][0]["status"], "outside-scope")
        self.assertEqual(matrix["cases"][0]["coverage"], "outside-scope")
        self.assertEqual(matrix["cases"][1]["status"], "review-needed")

    def test_build_matrix_rejects_non_document_cases(self):
        module = _load_script_module()

        with self.assertRaises(TypeError):
            module.build_matrix({"cases": ["invalid"]})

    def test_main_writes_json_output(self):
        module = _load_script_module()

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            inventory_path = root / "inventory.json"
            output_path = root / "matrix.json"
            inventory_path.write_text(
                (
                    '{"root": "/tmp/mongomock/tests", "cases": '
                    '[{"file": "test_api.py", "class_name": null, "test_name": "test_case", "category": "collection"}]}'
                ),
                encoding="utf-8",
            )

            exit_code = module.main([str(inventory_path), "--output", str(output_path)])

            self.assertEqual(exit_code, 0)
            payload = output_path.read_text(encoding="utf-8")
            self.assertIn('"inventory_total_cases": 1', payload)
            self.assertIn('"status": "review-needed"', payload)
