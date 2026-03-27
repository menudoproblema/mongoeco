#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mongoeco.compat import export_full_compat_catalog


def _render_markdown(catalog: dict[str, object]) -> str:
    lines: list[str] = ["# Compat Catalog", ""]

    defaults = catalog["defaults"]
    assert isinstance(defaults, dict)
    lines.append("## Defaults")
    for key, value in defaults.items():
        lines.append(f"- `{key}`: `{value}`")
    lines.append("")

    hooks = catalog["hooks"]
    assert isinstance(hooks, dict)
    lines.append("## Hooks")
    for key, values in hooks.items():
        rendered = ", ".join(f"`{value}`" for value in values) or "_none_"
        lines.append(f"- `{key}`: {rendered}")
    lines.append("")

    supported_majors = catalog["supported_majors"]
    assert isinstance(supported_majors, dict)
    lines.append("## Supported Majors")
    for key, values in supported_majors.items():
        rendered = ", ".join(str(value) for value in values)
        lines.append(f"- `{key}`: {rendered}")
    lines.append("")

    def _render_section(title: str, entries: dict[str, object]) -> None:
        lines.append(f"## {title}")
        for key, value in entries.items():
            assert isinstance(value, dict)
            lines.append(f"### `{key}`")
            for field_name, field_value in value.items():
                if isinstance(field_value, list):
                    rendered = ", ".join(f"`{item}`" for item in field_value) or "_empty_"
                else:
                    rendered = f"`{field_value}`"
                lines.append(f"- `{field_name}`: {rendered}")
            lines.append("")

    mongodb_dialects = catalog["mongodb_dialects"]
    assert isinstance(mongodb_dialects, dict)
    _render_section("MongoDB Dialects", mongodb_dialects)

    pymongo_profiles = catalog["pymongo_profiles"]
    assert isinstance(pymongo_profiles, dict)
    _render_section("PyMongo Profiles", pymongo_profiles)

    operation_options = catalog["operation_options"]
    assert isinstance(operation_options, dict)
    lines.append("## Operation Options")
    for operation, options in operation_options.items():
        assert isinstance(options, dict)
        lines.append(f"### `{operation}`")
        for option, support in options.items():
            assert isinstance(support, dict)
            rendered = ", ".join(
                f"`{field}`={json.dumps(value)}"
                for field, value in support.items()
            )
            lines.append(f"- `{option}`: {rendered}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Exporta el catálogo de compatibilidad de mongoeco.")
    parser.add_argument(
        "--format",
        choices=("json", "markdown"),
        default="json",
        help="Formato de salida.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Ruta de salida. Si se omite, escribe a stdout.",
    )
    args = parser.parse_args()

    catalog = export_full_compat_catalog()
    if args.format == "json":
        rendered = json.dumps(catalog, indent=2, sort_keys=True) + "\n"
    else:
        rendered = _render_markdown(catalog)

    if args.output is None:
        print(rendered, end="")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
