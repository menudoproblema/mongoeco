#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if (
    os.environ.get("MONGOECO_TEST_INSTALLED_ARTIFACT") != "1"
    and str(SRC_ROOT) not in sys.path
):
    sys.path.insert(0, str(SRC_ROOT))

from mongoeco.compat import (
    export_full_compat_catalog,
    export_full_compat_catalog_markdown,
)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Export the current exchange compatibility view."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "dist" / "compat-catalog-current",
        help="Destination for current exports; historical fixtures are read-only.",
    )
    args = parser.parse_args()
    output_dir = args.output_dir.resolve()
    historical = (PROJECT_ROOT / "tests" / "fixtures").resolve()
    if output_dir == historical or historical in output_dir.parents:
        message = "Historical compatibility fixtures are read-only"
        raise SystemExit(message)
    output_dir.mkdir(parents=True, exist_ok=True)

    (output_dir / "compat_catalog_exchange_snapshot.json").write_text(
        json.dumps(export_full_compat_catalog(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (output_dir / "compat_catalog_exchange_snapshot.md").write_text(
        export_full_compat_catalog_markdown(),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
