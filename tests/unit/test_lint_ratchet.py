import importlib.util

from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "check_lint_ratchet.py"
SPEC = importlib.util.spec_from_file_location("check_lint_ratchet", SCRIPT)
assert SPEC is not None
assert SPEC.loader is not None
RATCHET = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RATCHET)


def _diagnostic(path: Path, row: int) -> dict[str, object]:
    return {
        "filename": str(path),
        "code": "PLR0912",
        "message": "Too many branches (13 > 12)",
        "location": {"row": row, "column": 1},
    }


def test_structural_diagnostics_are_anchored_to_the_enclosing_symbol(
    tmp_path: Path,
) -> None:
    RATCHET._symbol_ranges.cache_clear()
    module = tmp_path / "module.py"
    module.write_text(
        "def first():\n    return 1\n\ndef second():\n    return 2\n",
        encoding="utf-8",
    )

    first = RATCHET._diagnostic_key(_diagnostic(module, 1), root=tmp_path)
    second = RATCHET._diagnostic_key(_diagnostic(module, 4), root=tmp_path)

    assert first != second
    assert first[-1] == "first"
    assert second[-1] == "second"
    assert RATCHET._symbol_ranges.cache_info().misses == 1


def test_line_diagnostics_are_counted_per_symbol(
    tmp_path: Path,
) -> None:
    module = tmp_path / "module.py"
    module.write_text(
        "def sample():\n    value = 1\n    return value\n",
        encoding="utf-8",
    )
    diagnostic = {
        "filename": str(module),
        "code": "E501",
        "message": "Line too long",
        "location": {"row": 2, "column": 1},
    }

    key = RATCHET._diagnostic_key(diagnostic, root=tmp_path)

    assert key[-1] == "sample"


def test_symbol_anchor_ignores_formatting_and_statement_changes(
    tmp_path: Path,
) -> None:
    compact = tmp_path / "compact.py"
    formatted = tmp_path / "formatted.py"
    distinct = tmp_path / "distinct.py"
    compact.write_text("result = call('value', option=True)\n", encoding="utf-8")
    formatted.write_text(
        'result = call(\n    "value",\n    option=True,\n)\n',
        encoding="utf-8",
    )
    distinct.write_text("result = call('other', option=True)\n", encoding="utf-8")

    def line_diagnostic(path: Path, row: int) -> dict[str, object]:
        return {
            "filename": str(path),
            "code": "E501",
            "message": "Line too long",
            "location": {"row": row, "column": 1},
        }

    compact_key = RATCHET._diagnostic_key(
        line_diagnostic(compact, 1),
        root=tmp_path,
    )
    formatted_key = RATCHET._diagnostic_key(
        line_diagnostic(formatted, 2),
        root=tmp_path,
    )
    distinct_key = RATCHET._diagnostic_key(
        line_diagnostic(distinct, 1),
        root=tmp_path,
    )

    assert compact_key[-1] == formatted_key[-1]
    assert compact_key[-1] == distinct_key[-1]
