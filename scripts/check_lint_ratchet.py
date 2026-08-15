#!/usr/bin/env python3

from __future__ import annotations

import argparse
import ast
import json
import shutil
import subprocess
import sys
import tempfile

from collections import Counter
from functools import cache
from pathlib import Path


_LINT_ROOTS = ("src", "tests", "scripts")


def _run_git(
    *args: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    git = shutil.which("git")
    if git is None:
        message = "git executable is required"
        raise RuntimeError(message)
    return subprocess.run(  # noqa: S603 - explicit git arguments
        [git, *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _ruff_version() -> str:
    result = subprocess.run(
        [sys.executable, "-m", "ruff", "--version"],
        check=True,
        capture_output=True,
        text=True,
    )
    version = result.stdout.strip().removeprefix("ruff ")
    if not version:
        message = "could not determine the installed Ruff version"
        raise RuntimeError(message)
    return version


def _changed_path_map(base_ref: str) -> dict[Path, Path | None]:
    result = _run_git(
        "diff",
        "--name-status",
        "--find-renames",
        "--diff-filter=ACMR",
        base_ref,
        "--",
        *_LINT_ROOTS,
    )
    paths: dict[Path, Path | None] = {}
    for line in result.stdout.splitlines():
        fields = line.split("\t")
        status = fields[0]
        if status.startswith("R"):
            base_path, current_path = map(Path, fields[1:3])
        else:
            current_path = Path(fields[1])
            base_path = None if status == "A" else current_path
        if current_path.suffix == ".py":
            paths[current_path] = base_path

    untracked = _run_git(
        "ls-files",
        "--others",
        "--exclude-standard",
        "--",
        *_LINT_ROOTS,
    )
    for raw_path in untracked.stdout.splitlines():
        path = Path(raw_path)
        if path.suffix == ".py":
            paths[path] = None
    return paths


def _materialize_base(
    root: Path,
    base_ref: str,
    paths: dict[Path, Path | None],
) -> list[Path]:
    config = _run_git("show", f"{base_ref}:pyproject.toml")
    (root / "pyproject.toml").write_text(config.stdout, encoding="utf-8")
    materialized = []
    for current_path, base_path in paths.items():
        if base_path is None:
            continue
        content = _run_git(
            "show",
            f"{base_ref}:{base_path.as_posix()}",
            check=False,
        )
        if content.returncode != 0:
            continue
        target = root / current_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(content.stdout, encoding="utf-8")
        materialized.append(current_path)
    return materialized


def _ruff_diagnostics(
    paths: list[Path],
    *,
    cwd: Path,
) -> list[dict[str, object]]:
    if not paths:
        return []
    result = subprocess.run(  # noqa: S603 - current Python executable
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--output-format",
            "json",
            *[str(path) for path in paths],
        ],
        cwd=cwd,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode not in {0, 1}:
        sys.stderr.write(result.stderr)
        raise SystemExit(result.returncode)
    return json.loads(result.stdout)


def _diagnostic_key(
    diagnostic: dict[str, object],
    *,
    root: Path,
) -> tuple[str, ...]:
    filename = Path(str(diagnostic["filename"]))
    if not filename.is_absolute():
        filename = root / filename
    filename = filename.resolve()
    try:
        path = str(filename.relative_to(root.resolve()))
    except ValueError:
        path = str(filename)
    return (
        path,
        str(diagnostic.get("code", "")),
        _diagnostic_anchor(diagnostic, filename),
    )


def _diagnostic_anchor(
    diagnostic: dict[str, object],
    filename: Path,
) -> str:
    location = diagnostic.get("location")
    row = int(location.get("row", 0)) if isinstance(location, dict) else 0
    source = _read_source(filename)
    if source is None:
        return ""
    # The ratchet controls debt cardinality per symbol; it is not a diff
    # engine. This remains stable across formatting and internal refactors.
    return _enclosing_symbol(source, row)


@cache
def _read_source(filename: Path) -> str | None:
    try:
        return filename.read_text(encoding="utf-8")
    except OSError:
        return None


def _enclosing_symbol(source: str, row: int) -> str:
    candidates = [
        (end_row - start_row, qualified)
        for start_row, end_row, qualified in _symbol_ranges(source)
        if start_row <= row <= end_row
    ]
    return min(candidates, default=(0, "<module>"))[1]


@cache
def _symbol_ranges(source: str) -> tuple[tuple[int, int, str], ...]:
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return ()
    ranges: list[tuple[int, int, str]] = []

    def visit(node: ast.AST, parents: tuple[str, ...] = ()) -> None:
        next_parents = parents
        if isinstance(
            node,
            (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef),
        ):
            end_row = getattr(node, "end_lineno", node.lineno)
            qualified = ".".join((*parents, node.name))
            ranges.append((node.lineno, end_row, qualified))
            next_parents = (*parents, node.name)
        for child in ast.iter_child_nodes(node):
            visit(child, next_parents)

    visit(tree)
    return tuple(ranges)


def _load_baseline(path: Path) -> Counter[tuple[str, ...]]:
    if not path.exists():
        return Counter()
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        message = "lint baseline must include Ruff version metadata"
        raise TypeError(message)
    expected_version = payload.get("ruff_version")
    installed_version = _ruff_version()
    if expected_version != installed_version:
        message = (
            f"lint baseline requires Ruff {expected_version}, "
            f"found {installed_version}"
        )
        raise RuntimeError(message)
    entries = payload.get("diagnostics")
    if not isinstance(entries, list):
        message = "lint baseline diagnostics must be a list"
        raise TypeError(message)
    return Counter(
        {
            (
                str(entry["path"]),
                str(entry["code"]),
                str(entry["source"]),
            ): int(entry["count"])
            for entry in entries
        },
    )


def _write_baseline(
    path: Path,
    diagnostics: list[dict[str, object]],
    *,
    root: Path,
) -> None:
    counts = Counter(_diagnostic_key(item, root=root) for item in diagnostics)
    entries = [
        {
            "path": key[0],
            "code": key[1],
            "source": key[2],
            "count": count,
        }
        for key, count in sorted(counts.items())
    ]
    payload = {
        "ruff_version": _ruff_version(),
        "diagnostics": entries,
    }
    path.write_text(
        f"{json.dumps(payload, indent=2, ensure_ascii=True)}\n",
        encoding="utf-8",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Reject Ruff diagnostics added in changed Python files.",
    )
    parser.add_argument(
        "--base-ref",
        default="HEAD^",
        help="Git ref used as the lint baseline (default: HEAD^).",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("scripts/ruff_ratchet_baseline.json"),
        help="Known diagnostic deltas accepted as migration debt.",
    )
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Accept the diagnostics added relative to the base ref.",
    )
    args = parser.parse_args()

    project_root = Path.cwd().resolve()
    changed_paths = _changed_path_map(args.base_ref)
    current_paths = sorted(
        path for path in changed_paths if (project_root / path).exists()
    )
    current_diagnostics = _ruff_diagnostics(
        current_paths,
        cwd=project_root,
    )
    with tempfile.TemporaryDirectory(prefix="mongoeco-ruff-base-") as temp:
        base_root = Path(temp)
        base_paths = _materialize_base(
            base_root,
            args.base_ref,
            changed_paths,
        )
        base_diagnostics = _ruff_diagnostics(base_paths, cwd=base_root)
        base_counts = Counter(
            _diagnostic_key(item, root=base_root) for item in base_diagnostics
        )

    introduced = []
    for diagnostic in current_diagnostics:
        key = _diagnostic_key(diagnostic, root=project_root)
        if base_counts[key] > 0:
            base_counts[key] -= 1
        else:
            introduced.append(diagnostic)

    if args.update_baseline:
        _write_baseline(
            args.baseline,
            introduced,
            root=project_root,
        )
        sys.stdout.write(
            f"wrote {len(introduced)} diagnostic deltas to {args.baseline}\n",
        )
        return 0

    try:
        accepted = _load_baseline(args.baseline)
    except (RuntimeError, TypeError) as exc:
        sys.stderr.write(f"{exc}\n")
        return 2
    rejected = []
    for diagnostic in introduced:
        key = _diagnostic_key(diagnostic, root=project_root)
        if accepted[key] > 0:
            accepted[key] -= 1
        else:
            rejected.append(diagnostic)

    if not rejected:
        sys.stdout.write(
            f"lint ratchet passed for {len(current_paths)} changed Python files\n",
        )
        return 0

    for diagnostic in rejected:
        location = diagnostic["location"]
        sys.stdout.write(
            f'{diagnostic["filename"]}:'
            f'{location["row"]}:{location["column"]}: '
            f'{diagnostic["code"]} {diagnostic["message"]}\n',
        )
    sys.stdout.write(
        f"{len(rejected)} Ruff violation(s) introduced in changed files\n",
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
