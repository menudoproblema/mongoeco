"""Run the real testkit in an isolated dependency and semantic-removal overlay."""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile

from pathlib import Path


class SemanticRemovalOverlay(ast.NodeTransformer):
    """Remove only semantic correction entry points, preserving owner adaptation."""

    def visit_FunctionDef(self, node):
        if node.name == "_with_array_filter_update":
            node.body = ast.parse("return args, kwargs").body
        if node.name == "_normalize_mongodb_filter":
            node.body = ast.parse("return _normalize_bson_datetime(filter_value)").body
        return self.generic_visit(node)

    def visit_AsyncFunctionDef(self, node):
        if node.name == "bulk_write":
            node.body = [
                statement
                for statement in node.body
                if not (
                    isinstance(statement, ast.If)
                    and isinstance(statement.test, ast.Name)
                    and statement.test.id == "args"
                )
            ]
        return self.generic_visit(node)

    def visit_Call(self, node):
        if (
            isinstance(node.func, ast.Name)
            and node.func.id == "_lower_unsupported_aggregation_expressions"
        ):
            return self.visit(node.args[0])
        return self.generic_visit(node)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--consumer-root", type=Path, required=True)
    parser.add_argument("--source-root", type=Path, required=True)
    parser.add_argument("--consumer-module", required=True)
    parser.add_argument("--consumer-tests", type=Path, required=True)
    parser.add_argument("--consumer-python", type=Path, required=True)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--uris", nargs=3, required=True, metavar="URI")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    args.output = args.output.resolve()
    source = (args.consumer_root / args.source_root).resolve()
    module_path = Path(*args.consumer_module.split(".")).with_suffix(".py")
    original = source / module_path
    initial_hash = hashlib.sha256(original.read_bytes()).hexdigest()
    with tempfile.TemporaryDirectory(prefix="mongoeco-consumer-") as directory:
        scratch = Path(directory)
        shutil.copytree(
            source, scratch / "src", ignore=shutil.ignore_patterns("__pycache__")
        )
        overlay = scratch / "src" / module_path
        tree = SemanticRemovalOverlay().visit(ast.parse(original.read_text()))
        overlay.write_text(ast.unparse(ast.fix_missing_locations(tree)) + "\n")
        subprocess.run(  # noqa: S603 - explicit interpreter and argument vector; no shell
            [
                str(args.consumer_python),
                "-m",
                "venv",
                "--without-pip",
                str(scratch / "env"),
            ],
            check=True,
        )
        python = scratch / "env/bin/python"
        paths = json.loads(
            subprocess.check_output(  # noqa: S603 - explicit consumer interpreter
                [
                    str(args.consumer_python),
                    "-c",
                    "import sys,json;print(json.dumps(sys.path))",
                ],
                text=True,
            )
        )
        paths = [
            path
            for path in paths
            if path
            and str(source) not in path
            and not path.endswith(".zip")
            and "lib-dynload" not in path
        ]
        site = Path(
            subprocess.check_output(  # noqa: S603 - explicit consumer interpreter
                [
                    str(python),
                    "-c",
                    "import sysconfig;print(sysconfig.get_path('purelib'))",
                ],
                text=True,
            ).strip()
        )
        (site / "consumer-overlay.pth").write_text(
            str(scratch / "src") + "\n" + "\n".join(paths) + "\n"
        )
        subprocess.run(  # noqa: S603 - explicit interpreter and argument vector; no shell
            [
                sys.executable,
                "-m",
                "pip",
                "--python",
                str(python),
                "install",
                "--ignore-installed",
                "--no-deps",
                str(args.wheel.resolve()),
                "pymongo==4.19.0",
            ],
            check=True,
        )
        subprocess.run(  # noqa: S603 - explicit interpreter and argument vector; no shell
            [
                str(python),
                str(root / "tests/consumers/write_canary.py"),
                "--consumer-root",
                str(args.consumer_root.resolve()),
                "--consumer-module",
                args.consumer_module,
                "--consumer-tests",
                str(args.consumer_tests),
                "--overlay-root",
                str(scratch),
                "--output",
                str(args.output),
                "--uris",
                *args.uris,
            ],
            check=True,
            env={
                **os.environ,
                "MONGOECO_TEST_INSTALLED_ARTIFACT": "1",
                "PYTHONPATH": str(root),
            },
        )
        data = json.loads(args.output.read_text())
        data["overlay"] = {
            "original_sha256": initial_hash,
            "overlay_sha256": hashlib.sha256(overlay.read_bytes()).hexdigest(),
            "wheel_sha256": hashlib.sha256(args.wheel.read_bytes()).hexdigest(),
            "consumer_source_modified": False,
        }
        args.output.write_text(json.dumps(data, indent=2) + "\n")
    if hashlib.sha256(original.read_bytes()).hexdigest() != initial_hash:
        message = "consumer source changed during isolated canary"
        raise RuntimeError(message)


if __name__ == "__main__":
    main()
