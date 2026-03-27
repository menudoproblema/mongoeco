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

from mongoeco.compat import export_full_compat_catalog, export_full_compat_catalog_markdown


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
        rendered = export_full_compat_catalog_markdown()

    if args.output is None:
        print(rendered, end="")
        return 0

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
