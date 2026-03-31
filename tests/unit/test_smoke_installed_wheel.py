from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys
import unittest


_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "smoke_installed_wheel.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("smoke_installed_wheel", _SCRIPT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load smoke script from {_SCRIPT_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SmokeInstalledWheelScriptTests(unittest.TestCase):
    def test_resolve_distribution_supports_wheel_and_sdist(self):
        module = _load_module()

        wheel = module._resolve_distribution(None, kind="wheel")
        sdist = module._resolve_distribution(None, kind="sdist")

        self.assertEqual(wheel.suffix, ".whl")
        self.assertEqual(sdist.suffixes[-2:], [".tar", ".gz"])

    def test_smoke_script_checks_unicode_collation_runtime(self):
        module = _load_module()
        script = module._smoke_script()

        self.assertIn("unicode_collation_available()", script)
        self.assertIn('compare_with_collation("Álvaro", "alvaro"', script)
        self.assertIn('compare_with_collation("file2", "file10"', script)

    def test_smoke_script_runs_in_repo_python_context(self):
        module = _load_module()
        repo_root = _SCRIPT_PATH.parents[1]
        env = dict(os.environ)
        existing_pythonpath = env.get("PYTHONPATH")
        source_root = str(repo_root / "src")
        env["PYTHONPATH"] = (
            source_root
            if not existing_pythonpath
            else f"{source_root}{os.pathsep}{existing_pythonpath}"
        )

        completed = subprocess.run(
            [sys.executable, "-c", module._smoke_script()],
            cwd=repo_root,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )

        self.assertIn("unicode_collation True", completed.stdout)
        self.assertIn("find_one", completed.stdout)

    def test_script_rejects_wheel_and_sdist_at_the_same_time(self):
        completed = subprocess.run(
            [sys.executable, str(_SCRIPT_PATH), "--wheel", "a.whl", "--sdist", "b.tar.gz"],
            check=False,
            capture_output=True,
            text=True,
        )

        self.assertNotEqual(completed.returncode, 0)
        self.assertIn("Usa solo una de --wheel o --sdist", completed.stderr or completed.stdout)
