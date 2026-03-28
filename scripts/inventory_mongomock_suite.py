from __future__ import annotations

import argparse
import ast
import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class MongomockTestCase:
    file: str
    node_type: str
    test_name: str
    class_name: str | None
    line: int
    category: str
    status: str = "unmapped"
    note: str = ""


def infer_category(path: Path) -> str:
    lowered = path.as_posix().lower()
    name = path.name.lower()
    if "aggregate" in lowered:
        return "aggregation"
    if "objectid" in lowered or "bson" in lowered:
        return "bson"
    if "filter" in lowered or "query" in lowered:
        return "query"
    if "update" in lowered:
        return "update"
    if "index" in lowered:
        return "indexes"
    if "command" in lowered or "database" in lowered or "mongo_client" in lowered:
        return "admin-client"
    if "collection" in lowered:
        return "collection"
    if "cursor" in lowered:
        return "cursor"
    if "write" in lowered or "insert" in lowered or "replace" in lowered or "remove" in lowered:
        return "writes"
    if "helpers" in lowered or "utils" in lowered:
        return "helpers"
    return name.removeprefix("test_").removesuffix(".py") or "misc"


def extract_cases_from_file(path: Path, root: Path) -> list[MongomockTestCase]:
    module = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    category = infer_category(path.relative_to(root))
    cases: list[MongomockTestCase] = []
    for node in module.body:
        if isinstance(node, ast.FunctionDef) and node.name.startswith("test"):
            cases.append(
                MongomockTestCase(
                    file=str(path.relative_to(root)),
                    node_type="function",
                    test_name=node.name,
                    class_name=None,
                    line=node.lineno,
                    category=category,
                )
            )
        elif isinstance(node, ast.ClassDef):
            for child in node.body:
                if isinstance(child, ast.FunctionDef) and child.name.startswith("test"):
                    cases.append(
                        MongomockTestCase(
                            file=str(path.relative_to(root)),
                            node_type="method",
                            test_name=child.name,
                            class_name=node.name,
                            line=child.lineno,
                            category=category,
                        )
                    )
    return cases


def inventory_suite(root: Path) -> dict[str, object]:
    files = sorted(
        path for path in root.rglob("test*.py") if path.is_file() and "__pycache__" not in path.parts
    )
    cases: list[MongomockTestCase] = []
    for path in files:
        cases.extend(extract_cases_from_file(path, root))
    categories: dict[str, int] = {}
    for case in cases:
        categories[case.category] = categories.get(case.category, 0) + 1
    return {
        "root": str(root),
        "files": len(files),
        "cases": [asdict(case) for case in cases],
        "category_counts": dict(sorted(categories.items())),
        "total_cases": len(cases),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Inventaria una checkout local de la suite de tests de mongomock."
    )
    parser.add_argument(
        "root",
        help="Ruta al directorio raíz de tests de mongomock o al repo completo.",
    )
    parser.add_argument(
        "--output",
        default=".tmp/mongomock-suite-inventory.json",
        help="Ruta de salida JSON.",
    )
    args = parser.parse_args(argv)

    root = Path(args.root).expanduser().resolve()
    tests_root = root / "tests" if (root / "tests").is_dir() else root
    if not tests_root.is_dir():
        raise SystemExit(f"tests root not found: {tests_root}")

    payload = inventory_suite(tests_root)
    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Wrote {payload['total_cases']} cases across {payload['files']} files to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
