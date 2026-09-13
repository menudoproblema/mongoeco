from __future__ import annotations

import sys

from pathlib import Path
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from collections.abc import Sequence


def resolve_subject_root(
    argv: Sequence[str],
    *,
    cwd: Path | None = None,
) -> Path | None:
    """Resolve the benchmark subject before importing its package."""
    raw_root: str | None = None
    for index, argument in enumerate(argv):
        if argument == "--subject-root":
            try:
                raw_root = argv[index + 1]
            except IndexError as error:
                message = "--subject-root requires a path"
                raise SystemExit(message) from error
        elif argument.startswith("--subject-root="):
            raw_root = argument.partition("=")[2]
    if raw_root is None:
        return None
    if not raw_root:
        message = "--subject-root requires a non-empty path"
        raise SystemExit(message)

    base = cwd or Path.cwd()
    root = Path(raw_root).expanduser()
    if not root.is_absolute():
        root = base / root
    root = root.resolve()
    package = root / "src" / "mongoeco" / "__init__.py"
    if not package.is_file():
        message = f"--subject-root must contain src/mongoeco/__init__.py: {root}"
        raise SystemExit(message)
    return root


def activate_subject_root(argv: Sequence[str]) -> Path | None:
    """Prepend the selected subject source tree to the import path."""
    root = resolve_subject_root(argv)
    if root is None:
        return None
    source_root = str(root / "src")
    if source_root in sys.path:
        sys.path.remove(source_root)
    sys.path.insert(0, source_root)
    return root


def require_imported_subject(root: Path, module_file: str | None) -> Path:
    """Fail closed when Python loaded MongoEco from a different location."""
    if module_file is None:
        message = "the imported mongoeco module has no filesystem origin"
        raise SystemExit(message)
    imported = Path(module_file).resolve()
    expected = (root / "src" / "mongoeco").resolve()
    if not imported.is_relative_to(expected):
        message = (
            "--subject-root did not provide the imported mongoeco package: "
            f"expected under {expected}, loaded {imported}"
        )
        raise SystemExit(message)
    return imported
