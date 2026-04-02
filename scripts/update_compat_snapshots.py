#!/usr/bin/env python3

from __future__ import annotations

import json
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from mongoeco.compat import export_full_compat_catalog, export_full_compat_catalog_markdown


def main() -> int:
    fixtures_dir = PROJECT_ROOT / "tests" / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)

    (fixtures_dir / "compat_catalog_snapshot.json").write_text(
        json.dumps(export_full_compat_catalog(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    (fixtures_dir / "compat_catalog_snapshot.md").write_text(
        export_full_compat_catalog_markdown(),
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
