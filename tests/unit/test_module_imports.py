import os
import ast
import builtins
import subprocess
import sys
import textwrap
import unittest
from collections.abc import Iterable
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
