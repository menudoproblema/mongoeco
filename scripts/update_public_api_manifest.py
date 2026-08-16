from __future__ import annotations

import argparse
import json
import os
import sys

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
if (
    os.environ.get("MONGOECO_TEST_INSTALLED_ARTIFACT") != "1"
    and str(SRC_ROOT) not in sys.path
):
    sys.path.insert(0, str(SRC_ROOT))

from mongoeco.compat import (  # noqa: E402
    compare_public_api_manifests,
    public_api_manifest,
)


DEFAULT_OUTPUT = PROJECT_ROOT / "tests/fixtures/public_api_manifest_v1.json"


def _render(document: dict[str, object]) -> str:
    return json.dumps(document, indent=2, sort_keys=True) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate or verify MongoEco's public API manifest."
    )
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--compare", type=Path)
    args = parser.parse_args()

    generated = public_api_manifest()
    if args.compare is not None:
        baseline = json.loads(args.compare.read_text(encoding="utf-8"))
        changes = compare_public_api_manifests(baseline, generated)
        sys.stdout.write(json.dumps(changes, indent=2, sort_keys=True) + "\n")
        return int(bool(changes))
    rendered = _render(generated)
    if args.check:
        if not args.output.is_file():
            sys.stderr.write(f"public API manifest is missing: {args.output}\n")
            return 1
        current = args.output.read_text(encoding="utf-8")
        if current == rendered:
            sys.stdout.write("public API manifest is current\n")
            return 0
        baseline = json.loads(current)
        changes = compare_public_api_manifests(baseline, generated)
        sys.stderr.write(json.dumps(changes, indent=2, sort_keys=True) + "\n")
        return 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(f"updated {args.output}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
