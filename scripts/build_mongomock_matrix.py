from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_STATUS = "review-needed"


def build_matrix(inventory: dict[str, object]) -> dict[str, object]:
    cases = inventory.get("cases", [])
    if not isinstance(cases, list):
        raise TypeError("inventory cases must be a list")

    matrix_cases: list[dict[str, object]] = []
    for case in cases:
        if not isinstance(case, dict):
            raise TypeError("inventory cases must contain documents")
        file_name = case.get("file")
        class_name = case.get("class_name")
        test_name = case.get("test_name")
        if not isinstance(file_name, str) or not isinstance(test_name, str):
            raise TypeError("inventory case must contain file and test_name strings")
        case_id = "::".join(
            part
            for part in (file_name, class_name if isinstance(class_name, str) else None, test_name)
            if part
        )
        matrix_cases.append(
            {
                "id": case_id,
                "file": file_name,
                "class_name": class_name,
                "test_name": test_name,
                "category": case.get("category", "unknown"),
                "status": DEFAULT_STATUS,
                "coverage": "unknown",
                "note": "",
            }
        )

    return {
        "inventory_root": inventory.get("root"),
        "inventory_total_cases": len(matrix_cases),
        "status_counts": {DEFAULT_STATUS: len(matrix_cases)},
        "cases": matrix_cases,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Construye una matriz exhaustiva base a partir del inventario de tests de mongomock."
    )
    parser.add_argument("inventory", help="Ruta al JSON generado por inventory_mongomock_suite.py")
    parser.add_argument(
        "--output",
        default=".tmp/mongomock-suite-matrix.json",
        help="Ruta de salida JSON.",
    )
    args = parser.parse_args(argv)

    inventory_path = Path(args.inventory).expanduser().resolve()
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    matrix = build_matrix(inventory)

    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(matrix, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Wrote {matrix['inventory_total_cases']} matrix rows to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
