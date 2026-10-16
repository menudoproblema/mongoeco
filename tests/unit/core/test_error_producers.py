"""New internal server failures need identity; legacy emitters remain inventoried."""

from __future__ import annotations

import ast
import hashlib
import json

from collections import Counter
from pathlib import Path


ROOT = Path(__file__).parents[3]
BASELINE = ROOT / "tests/fixtures/legacy_error_emitters.json"


def uncoded_emitters(source, filename):
    found = Counter()
    scopes = []

    class Inventory(ast.NodeVisitor):
        def visit_FunctionDef(self, node):
            scopes.append(node.name)
            self.generic_visit(node)
            scopes.pop()

        def visit_AsyncFunctionDef(self, node):
            self.visit_FunctionDef(node)

        def visit_ClassDef(self, node):
            scopes.append(node.name)
            self.generic_visit(node)
            scopes.pop()

        def visit_Call(self, node):
            if isinstance(node.func, ast.Name) and node.func.id == "OperationFailure":
                code = next(
                    (key.value for key in node.keywords if key.arg == "code"), None
                )
                if code is None or (
                    isinstance(code, ast.Constant) and code.value is None
                ):
                    # Python 3.15 omits expression contexts from ast.dump.
                    # They carry no diagnostic identity; normalize on every Python.
                    for child in ast.walk(node):
                        if hasattr(child, "ctx"):
                            del child.ctx
                    digest = hashlib.sha256(ast.dump(node).encode()).hexdigest()
                    found[f"{filename}:{'.'.join(scopes)}:{digest}"] += 1
            self.generic_visit(node)

    Inventory().visit(ast.parse(source))
    return found


def test_new_internal_operation_failures_cannot_omit_server_codes():
    actual = Counter()
    for path in (ROOT / "src/mongoeco").rglob("*.py"):
        actual.update(uncoded_emitters(path.read_text(), str(path.relative_to(ROOT))))
    baseline = Counter(json.loads(BASELINE.read_text())["emitters"])
    assert not actual - baseline, "New uncoded server failures: " + repr(
        actual - baseline
    )


def test_inventory_distinguishes_missing_none_and_explicit_codes():
    missing = uncoded_emitters('raise OperationFailure("bad")', "sample")
    explicit_none = uncoded_emitters(
        'raise OperationFailure("bad", code=None)', "sample"
    )
    assert sum(missing.values()) == sum(explicit_none.values()) == 1
    assert missing != explicit_none
    assert not uncoded_emitters('raise OperationFailure("bad", code=2)', "sample")
