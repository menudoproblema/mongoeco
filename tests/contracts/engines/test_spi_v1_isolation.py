from __future__ import annotations

import ast
import json

from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
SOURCE_ROOT = ROOT / "src/mongoeco"
ALLOWLIST = ROOT / "tests/fixtures/spi_v1_reference_allowlist.json"


def _contains_exact_marker(path: Path, marker: str) -> bool:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == marker:
            return True
        if isinstance(node, ast.Attribute) and node.attr == marker:
            return True
        if (
            isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
            and node.name == marker
        ):
            return True
        if isinstance(node, ast.Constant) and node.value == marker:
            return True
        if isinstance(node, ast.alias) and node.name.rsplit(".", 1)[-1] == marker:
            return True
    return False


def test_spi_v1_references_remain_inside_reviewed_legacy_boundaries() -> None:
    expected = json.loads(ALLOWLIST.read_text(encoding="utf-8"))
    observed = {}
    source_files = tuple(SOURCE_ROOT.rglob("*.py"))
    for marker in expected:
        observed[marker] = sorted(
            str(path.relative_to(ROOT))
            for path in source_files
            if _contains_exact_marker(path, marker)
        )
    assert observed == expected


def test_public_api_layer_has_no_spi_v1_vocabulary() -> None:
    forbidden = {
        "LegacyEngineAdapter",
        "capture_document",
        "capture_documents",
        "put_document",
        "put_documents_bulk",
        "supports_injected_clock",
    }
    violations: list[str] = []
    for path in (SOURCE_ROOT / "api").rglob("*.py"):
        violations.extend(
            f"{path.relative_to(ROOT)}: {marker}"
            for marker in forbidden
            if _contains_exact_marker(path, marker)
        )
    assert violations == []
