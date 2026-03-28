from __future__ import annotations

import argparse
import json
from pathlib import Path


DEFAULT_STATUS = "review-needed"
RULE_STATUSES = {
    "covered",
    "equivalent",
    "not-covered",
    "outside-scope",
    DEFAULT_STATUS,
}


def _load_rules(path: Path | None) -> dict[str, object]:
    if path is None:
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("rules document must be an object")
    return payload


def _resolve_case_rule(
    case: dict[str, object],
    *,
    case_id: str,
    rules: dict[str, object],
) -> tuple[str, str]:
    case_rules = rules.get("case_rules", {})
    if isinstance(case_rules, dict):
        rule = case_rules.get(case_id)
        if isinstance(rule, dict):
            status = rule.get("status")
            note = rule.get("note", "")
            if isinstance(status, str) and status in RULE_STATUSES and isinstance(note, str):
                return status, note

    category_rules = rules.get("category_rules", {})
    if isinstance(category_rules, dict):
        category = case.get("category")
        rule = category_rules.get(category)
        if isinstance(rule, dict):
            status = rule.get("status")
            note = rule.get("note", "")
            if isinstance(status, str) and status in RULE_STATUSES and isinstance(note, str):
                return status, note

    test_name_contains_rules = rules.get("test_name_contains_rules", [])
    if isinstance(test_name_contains_rules, list):
        test_name = case.get("test_name")
        if isinstance(test_name, str):
            lowered = test_name.lower()
            for rule in test_name_contains_rules:
                if not isinstance(rule, dict):
                    continue
                pattern = rule.get("pattern")
                status = rule.get("status")
                note = rule.get("note", "")
                if (
                    isinstance(pattern, str)
                    and pattern
                    and pattern.lower() in lowered
                    and isinstance(status, str)
                    and status in RULE_STATUSES
                    and isinstance(note, str)
                ):
                    return status, note

    return DEFAULT_STATUS, ""


def build_matrix(
    inventory: dict[str, object],
    *,
    rules: dict[str, object] | None = None,
) -> dict[str, object]:
    cases = inventory.get("cases", [])
    if not isinstance(cases, list):
        raise TypeError("inventory cases must be a list")
    resolved_rules = {} if rules is None else rules

    matrix_cases: list[dict[str, object]] = []
    status_counts: dict[str, int] = {}
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
        status, note = _resolve_case_rule(case, case_id=case_id, rules=resolved_rules)
        coverage = "unknown" if status != "outside-scope" else "outside-scope"
        matrix_cases.append(
            {
                "id": case_id,
                "file": file_name,
                "class_name": class_name,
                "test_name": test_name,
                "category": case.get("category", "unknown"),
                "status": status,
                "coverage": coverage,
                "note": note,
            }
        )
        status_counts[status] = status_counts.get(status, 0) + 1

    return {
        "inventory_root": inventory.get("root"),
        "inventory_total_cases": len(matrix_cases),
        "status_counts": dict(sorted(status_counts.items())),
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
    parser.add_argument(
        "--rules",
        default=None,
        help="Ruta opcional a reglas de triage por categoria o caso.",
    )
    args = parser.parse_args(argv)

    inventory_path = Path(args.inventory).expanduser().resolve()
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    rules_path = Path(args.rules).expanduser().resolve() if args.rules is not None else None
    matrix = build_matrix(inventory, rules=_load_rules(rules_path))

    output = Path(args.output).expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(matrix, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Wrote {matrix['inventory_total_cases']} matrix rows to {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
