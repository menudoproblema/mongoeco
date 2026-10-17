"""Observe residual v2 maintenance candidates without opening engine resources."""

from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile

from importlib.metadata import version
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TARGETS = (
    "_iter_scan_documents_sync",
    "_count_matching_documents_sync",
    "EngineUpdateSemantics",
)
SOURCES = (
    "src/mongoeco/api/_async/cursor.py",
    "src/mongoeco/api/_async/collection.py",
    "src/mongoeco/api/_async/_collection_runtime.py",
    "src/mongoeco/api/operations.py",
    "src/mongoeco/engines/base.py",
    "src/mongoeco/engines/adapter.py",
    "src/mongoeco/engines/semantic_core.py",
    "src/mongoeco/engines/sqlite.py",
    "src/mongoeco/engines/_sqlite_reader.py",
)
PROBE = """from mongoeco.api._async.cursor import AsyncCursor

def inspect(cursor: AsyncCursor) -> None:
    reveal_type(cursor._base_operation())
    reveal_type(cursor._operation_with_overrides(limit=1))
    reveal_type(cursor._base_semantics())
    reveal_type(cursor._semantics_with_overrides(limit=1))
    cursor._base_operation().missing_member()
    cursor._operation_with_overrides(limit=1).missing_member()
"""


def references() -> dict[str, list[dict[str, object]]]:
    result: dict[str, list[dict[str, object]]] = {name: [] for name in TARGETS}
    for directory in ("src", "tests", "scripts", "benchmarks", "examples"):
        for path in sorted((ROOT / directory).rglob("*.py")):
            tree = ast.parse(path.read_text(encoding="utf-8"))
            direct_calls = {
                id(node.func) for node in ast.walk(tree) if isinstance(node, ast.Call)
            }
            for node in ast.walk(tree):
                name = (
                    node.attr
                    if isinstance(node, ast.Attribute)
                    else node.id
                    if isinstance(node, ast.Name)
                    else None
                )
                if name in result:
                    result[name].append(
                        {
                            "path": path.relative_to(ROOT).as_posix(),
                            "line": node.lineno,
                            "kind": "call" if id(node) in direct_calls else "reference",
                        }
                    )
    return result


def typing_probe() -> dict[str, object]:
    work = ROOT / ".tmp/spi-v2-maintenance"
    work.mkdir(parents=True, exist_ok=True)
    environment = dict(os.environ, MYPYPATH=str(ROOT / "src"))
    results: dict[str, object] = {}
    with tempfile.TemporaryDirectory(prefix="typing-", dir=work) as temporary:
        probe = Path(temporary) / "probe.py"
        probe.write_text(PROBE, encoding="utf-8")
        for mode in ("ordinary", "strict"):
            command = [
                sys.executable,
                "-m",
                "mypy",
                "--follow-imports=silent",
                "--no-incremental",
                "--show-error-codes",
                "--no-error-summary",
                "--cache-dir",
                str(work / "mypy-cache"),
            ]
            if mode == "strict":
                command.append("--strict")
            completed = subprocess.run(  # noqa: S603 - fixed local mypy probe
                [*command, str(probe)],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
                check=False,
            )
            output = completed.stdout.replace(str(probe), "<probe>").replace(
                probe.relative_to(ROOT).as_posix(), "<probe>"
            )
            diagnostics = re.findall(r"error: .* \[([^]]+)]", output)
            expected = (
                ["attr-defined"]
                if mode == "ordinary"
                else [
                    "no-untyped-call",
                    "attr-defined",
                ]
            )
            if completed.returncode != 1 or diagnostics != expected:
                message = f"unexpected {mode} typing result: {output}"
                raise RuntimeError(message)
            results[mode] = {
                "expected_exit_code": completed.returncode,
                "error_codes": diagnostics,
                "output": output,
            }
    return results


def main() -> None:
    report = {
        "schema": "mongoeco-spi-v2-maintenance-observation/v1",
        "mypy_version": version("mypy"),
        "sources_sha256": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in SOURCES
        },
        "references": references(),
        "typing_probe_source": PROBE,
        "typing": typing_probe(),
        "limits": [
            "AST reference census does not resolve dynamic dispatch or external uses.",
            "Expected negative typing diagnostics demonstrate a local annotation gap.",
            "No engine resources, sockets or native MongoDB servers are opened.",
            "No runtime correction, latency gain or v3 adoption is accredited.",
        ],
    }
    sys.stdout.write(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
