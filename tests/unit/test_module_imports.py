import os
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path


class ModuleImportSmokeTests(unittest.TestCase):
    @staticmethod
    def _repo_root() -> Path:
        return Path(__file__).resolve().parents[2]

    @classmethod
    def _source_modules(cls) -> list[str]:
        src_root = cls._repo_root() / "src" / "mongoeco"
        modules: list[str] = []
        for path in sorted(src_root.rglob("*.py")):
            rel_path = path.relative_to(cls._repo_root() / "src").with_suffix("")
            if path.name == "__init__.py":
                module = ".".join(rel_path.parts[:-1])
            else:
                module = ".".join(rel_path.parts)
            modules.append(module)
        return modules

    def test_every_source_module_imports_in_a_clean_interpreter(self):
        repo_root = self._repo_root()
        source_root = repo_root / "src"
        pythonpath_entries = [str(source_root)]
        if existing_pythonpath := os.environ.get("PYTHONPATH"):
            pythonpath_entries.append(existing_pythonpath)
        env = os.environ | {"PYTHONPATH": os.pathsep.join(pythonpath_entries)}

        for module_name in self._source_modules():
            with self.subTest(module=module_name):
                result = subprocess.run(
                    [
                        sys.executable,
                        "-c",
                        textwrap.dedent(
                            """
                            import importlib
                            import sys

                            importlib.import_module(sys.argv[1])
                            """
                        ),
                        module_name,
                    ],
                    cwd=repo_root,
                    capture_output=True,
                    text=True,
                    env=env,
                    check=False,
                )

                if result.returncode != 0:
                    self.fail(
                        f"Import failed for {module_name}\n"
                        f"stdout:\n{result.stdout}\n"
                        f"stderr:\n{result.stderr}"
                    )
