import os
import ast
import builtins
import subprocess
import sys
import textwrap
import unittest
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


class ModuleImportSmokeTests(unittest.TestCase):
    def test_pytest_helpers_require_only_the_optional_runner(self):
        environment = dict(os.environ)
        if environment.get("MONGOECO_TEST_INSTALLED_ARTIFACT") != "1":
            environment["PYTHONPATH"] = str(self._repo_root() / "src")
        code = textwrap.dedent("""
            import importlib
            import importlib.abc
            import sys

            class MissingPytest(importlib.abc.MetaPathFinder):
                def find_spec(self, fullname, path=None, target=None):
                    if fullname == "pytest" or fullname.startswith("pytest."):
                        raise ModuleNotFoundError("pytest is optional", name="pytest")
                    return None

            sys.meta_path.insert(0, MissingPytest())
            import mongoeco
            assert "pytest" not in sys.modules
            for module in (
                "mongoeco._pytest_diagnostics", "mongoeco.pytest_plugin",
                "mongoeco.pytest_asyncio", "mongoeco.pytest_anyio",
            ):
                try:
                    importlib.import_module(module)
                except ModuleNotFoundError as error:
                    assert error.name == "pytest", (module, error)
                else:
                    raise AssertionError(module)
        """)
        result = subprocess.run(  # noqa: S603 - fixed optional-import regression
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_core_import_does_not_load_pytest(self):
        environment = dict(os.environ)
        if environment.get("MONGOECO_TEST_INSTALLED_ARTIFACT") != "1":
            environment["PYTHONPATH"] = str(self._repo_root() / "src")
        code = "import mongoeco, sys; assert 'pytest' not in sys.modules"
        result = subprocess.run(  # noqa: S603 - fixed interpreter and import assertion
            [sys.executable, "-c", code],
            capture_output=True,
            text=True,
            env=environment,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

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

    @classmethod
    def _source_files(cls) -> list[Path]:
        src_root = cls._repo_root() / "src" / "mongoeco"
        return sorted(src_root.rglob("*.py"))

    @staticmethod
    def _iter_annotation_names(annotation: ast.AST) -> Iterable[str]:
        for node in ast.walk(annotation):
            if isinstance(node, ast.Name):
                yield node.id

    @staticmethod
    def _module_has_future_annotations(module: ast.Module) -> bool:
        for node in module.body:
            if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
                if isinstance(node.value.value, str):
                    continue
            if isinstance(node, ast.ImportFrom) and node.module == "__future__":
                return any(alias.name == "annotations" for alias in node.names)
            break
        return False

    @staticmethod
    def _module_bound_names(module: ast.Module) -> set[str]:
        bound_names = set(dir(builtins))
        for node in module.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    bound_names.add(alias.asname or alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    if alias.name != "*":
                        bound_names.add(alias.asname or alias.name)
            elif isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef | ast.ClassDef):
                bound_names.add(node.name)
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        bound_names.add(target.id)
            elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
                bound_names.add(node.target.id)
        return bound_names

    @classmethod
    def _annotation_resolution_errors(cls, path: Path) -> list[str]:
        module = ast.parse(path.read_text(), filename=str(path))
        if cls._module_has_future_annotations(module):
            return []

        bound_names = cls._module_bound_names(module)
        missing: set[str] = set()

        for node in ast.walk(module):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                annotations = [
                    arg.annotation
                    for arg in (
                        node.args.posonlyargs
                        + node.args.args
                        + node.args.kwonlyargs
                    )
                    if arg.annotation is not None
                ]
                if node.args.vararg and node.args.vararg.annotation is not None:
                    annotations.append(node.args.vararg.annotation)
                if node.args.kwarg and node.args.kwarg.annotation is not None:
                    annotations.append(node.args.kwarg.annotation)
                if node.returns is not None:
                    annotations.append(node.returns)
            elif isinstance(node, ast.AnnAssign):
                annotations = [node.annotation]
            else:
                continue

            for annotation in annotations:
                for name in cls._iter_annotation_names(annotation):
                    if name not in bound_names:
                        missing.add(name)

        if not missing:
            return []
        return [f"{path.relative_to(cls._repo_root())}: {', '.join(sorted(missing))}"]

    def test_every_source_module_imports_in_a_clean_interpreter(self):
        repo_root = self._repo_root()
        source_root = repo_root / "src"
        env = dict(os.environ)
        if env.get('MONGOECO_TEST_INSTALLED_ARTIFACT') == '1':
            env.pop('PYTHONPATH', None)
        else:
            pythonpath_entries = [str(source_root)]
            if existing_pythonpath := os.environ.get("PYTHONPATH"):
                pythonpath_entries.append(existing_pythonpath)
            env['PYTHONPATH'] = os.pathsep.join(pythonpath_entries)
        module_names = self._source_modules()

        def _import_module(module_name: str) -> subprocess.CompletedProcess[str]:
            return subprocess.run(
                [
                    sys.executable,
                    "-c",
                    textwrap.dedent(
                        """
                        import importlib
                        import importlib.util
                        import sys

                        optional = {
                            "mongoeco._pytest_diagnostics": {"pytest"},
                            "mongoeco.pytest_plugin": {"pytest"},
                            "mongoeco.pytest_asyncio": {"pytest", "pytest_asyncio"},
                            "mongoeco.pytest_anyio": {"pytest"},
                        }
                        try:
                            importlib.import_module(sys.argv[1])
                        except ModuleNotFoundError as error:
                            # Explicit optional entrypoints require their runner.
                            # A missing core dependency or any other import error
                            # remains a failure in both minimal and full installs.
                            assert error.name in optional.get(sys.argv[1], set())
                            assert importlib.util.find_spec(error.name) is None
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

        with ThreadPoolExecutor(max_workers=min(16, len(module_names) or 1)) as executor:
            results = dict(zip(module_names, executor.map(_import_module, module_names)))

        for module_name in module_names:
            with self.subTest(module=module_name):
                result = results[module_name]
                if result.returncode != 0:
                    self.fail(
                        f"Import failed for {module_name}\n"
                        f"stdout:\n{result.stdout}\n"
                        f"stderr:\n{result.stderr}"
                    )

    def test_modules_without_future_annotations_do_not_reference_unbound_names(self):
        errors: list[str] = []
        for path in self._source_files():
            errors.extend(self._annotation_resolution_errors(path))

        self.assertEqual(
            errors,
            [],
            "Unbound names found in runtime-evaluated annotations:\n"
            + "\n".join(errors),
        )
