"""Observe the v2 baseline; does not implement or enable an SPI successor."""

from __future__ import annotations

import ast
import hashlib
import json
import platform
import shutil
import subprocess
import sys

from dataclasses import fields, replace
from pathlib import Path
from unittest.mock import patch

import mongoeco

from mongoeco.api import operations
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search_execution import SearchRequest
from mongoeco.engines import EngineCapabilities
from mongoeco.engines.semantic_core import EngineFindSemantics, EngineUpdateSemantics


ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).with_name("baseline.json")
SOURCE_PATHS = (
    "src/mongoeco/api/operations.py",
    "src/mongoeco/core/operation_context.py",
    "src/mongoeco/core/search_execution.py",
    "src/mongoeco/engines/adapter.py",
    "src/mongoeco/engines/base.py",
    "src/mongoeco/engines/capabilities.py",
    "src/mongoeco/engines/semantic_core.py",
    "src/mongoeco/engines/results.py",
    "src/mongoeco/engines/snapshots.py",
    "tests/consumer/engine_canary.py",
)
CALL_NAMES = {
    "EngineSpiAdapter",
    "compile_find_semantics",
    "compile_find_semantics_from_operation",
    "compile_update_semantics",
}


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def rejected(call):
    try:
        call()
    except (ValueError, TypeError, RuntimeError) as error:
        return {"type": type(error).__name__, "message": str(error)}
    message = "expected rejection"
    raise AssertionError(message)


def observe_binding():
    factories = {
        "find": ("compile_find_operation", ({},), {}),
        "update": (
            "compile_update_operation",
            ({},),
            {"update_spec": {"$set": {"value": 1}}},
        ),
        "aggregate": ("compile_aggregate_operation", ([],), {}),
    }
    rows = {}
    for name, (compiler_name, args, kwargs) in factories.items():
        compiler = getattr(operations, compiler_name)
        operation = compiler(*args, **kwargs)
        same = OperationContext.create(dialect=MONGODB_DIALECT_70)
        changed = OperationContext.create(dialect=MONGODB_DIALECT_80)
        with patch.object(operations, compiler_name, wraps=compiler) as spy:
            bound = operation.bind(same)
            unchanged_compilations = spy.call_count
            require(bound.bind(same) is bound, "binding must be idempotent")
            rebound_rejection = rejected(
                lambda bound=bound, changed=changed: bound.bind(changed)
            )
            spy.reset_mock()
            recompiled = operation.bind(changed)
            changed_compilations = spy.call_count
        duplicate_rejection = rejected(
            lambda bound=bound: replace(bound, dialect=MONGODB_DIALECT_80)
        )
        require(operation.context is None, "original must stay unbound")
        require(bound.context is same, "binding must preserve identity")
        require(recompiled.context is changed, "compilation must preserve identity")
        require(recompiled.dialect is changed.dialect, "compilation must use context")
        require(unchanged_compilations == 0, "equivalent semantics must reuse plan")
        require(changed_compilations == 1, "changed semantics must recompile")
        rows[name] = {
            "unbound_state_constructible": True,
            "same_context_binding_is_idempotent": True,
            "equivalent_context_additional_compilations": unchanged_compilations,
            "different_dialect_additional_compilations": changed_compilations,
            "different_context_rebinding": rebound_rejection,
            "divergent_duplicate_dialect": duplicate_rejection,
        }
    return rows


def main():
    # Runtime probes must inspect exactly the source being inventoried.
    require(
        Path(mongoeco.__file__).resolve() == ROOT / "src/mongoeco/__init__.py",
        "observer requires the inventoried source checkout",
    )
    references = []
    for relative in sorted((ROOT / "src/mongoeco").rglob("*.py")):
        tree = ast.parse(relative.read_text(encoding="utf-8"))
        references.extend(
            {
                "path": relative.relative_to(ROOT).as_posix(),
                "line": node.lineno,
                "callee": node.func.id,
            }
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id in CALL_NAMES
        )
    types = (
        operations.FindOperation,
        operations.UpdateOperation,
        operations.AggregateOperation,
        EngineFindSemantics,
        EngineUpdateSemantics,
        SearchRequest,
    )
    shapes = {}
    for cls in types:
        names = [field.name for field in fields(cls)]
        shapes[cls.__name__] = {
            "context_fields": [
                n for n in names if n in {"context", "operation_context"}
            ],
            "semantic_duplicate_fields": [
                n for n in names if n in {"dialect", "collation", "let", "variables"}
            ],
        }
    git = shutil.which("git")
    require(git is not None, "git executable required")
    document = {
        "schema": "mongoeco-spi-v3-baseline/1",
        "source_revision": subprocess.check_output(  # noqa: S603 - resolved git, fixed arguments
            [git, "rev-parse", "HEAD"], cwd=ROOT, text=True
        ).strip(),
        "mongoeco_version": mongoeco.__version__,
        "python_version": platform.python_version(),
        "source_sha256": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in SOURCE_PATHS
        },
        "supported_spi": {
            "2": EngineCapabilities(spi_version=2).spi_version,
            "1": rejected(lambda: EngineCapabilities(spi_version=1)),
            "3": rejected(lambda: EngineCapabilities(spi_version=3)),
        },
        "operation_shapes": shapes,
        "binding": observe_binding(),
        "syntactic_call_sites": sorted(
            references, key=lambda row: (row["path"], row["line"])
        ),
        "limits": [
            "Named AST calls only; not a semantic call graph or adoption census.",
            "Compilation counts cover the three documented probes, not throughput.",
            "No SPI v3 implementation, native MongoDB run or performance comparison.",
        ],
    }
    OUTPUT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    sys.stdout.write(
        json.dumps(
            {
                "output": OUTPUT.relative_to(ROOT).as_posix(),
                "syntactic_call_sites": len(references),
                "binding_probes": 3,
            }
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
