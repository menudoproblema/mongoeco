"""Reproduce design-review claims against unchanged SPI v2, without sockets."""

from __future__ import annotations

import ast
import asyncio
import hashlib
import json
import platform
import sys

from dataclasses import replace
from pathlib import Path
from unittest.mock import patch

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api import operations
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.collation import normalize_collation
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines import __all__ as engine_exports, semantic_core
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


ROOT = Path(__file__).resolve().parents[3]
OUTPUT = Path(__file__).with_name("review-probes.json")


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def rejection(call):
    try:
        call()
    except (TypeError, ValueError, RuntimeError) as error:
        return type(error).__name__
    return None


def structural_probes():
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_70,
        collation={"locale": "en", "strength": 2},
        bindings={"tenant": 1},
    )
    operation = operations.compile_find_operation({"x": 1})
    partial = operation.with_overrides(context=context)
    canonical = semantic_core.compile_find_semantics_from_operation(partial)
    require(partial.collation is None, "probe must retain absent duplicate")
    require(canonical.collation == context.collation, "compiler must use context")
    require(canonical.variables is context.expressions, "compiler must use bindings")
    guard_matrix = {
        "operation_absent_collation_and_let_with_nonempty_context": "accepted",
        "read_semantics_cleared_collation": rejection(
            lambda: replace(canonical, collation=None)
        ),
        "read_semantics_compilation_resolves_context": True,
    }
    require(
        guard_matrix["read_semantics_cleared_collation"] == "ValueError",
        "read semantics guard must reject divergent collation",
    )

    original = operations.compile_find_operation({"x": 1})
    other = operations.compile_find_operation({"x": 2})
    grafted = original.with_overrides(plan=other.plan)
    grafted_semantics = semantic_core.compile_find_semantics_from_operation(grafted)
    require(grafted.filter_spec == {"x": 1}, "original filter must survive")
    require(
        grafted_semantics.compiled_query.match({"x": 2}), "grafted plan must execute"
    )
    require(
        not grafted_semantics.compiled_query.match({"x": 1}), "plan differs from spec"
    )

    unbound = operations.compile_find_operation({}, dialect=MONGODB_DIALECT_80)
    defaulted = semantic_core.compile_find_semantics_from_operation(unbound)
    explicit = semantic_core.compile_find_semantics_from_operation(
        unbound, dialect=unbound.dialect
    )
    require(
        defaulted.dialect is MONGODB_DIALECT_70, "baseline changed: implicit dialect"
    )
    require(
        explicit.dialect is MONGODB_DIALECT_80, "explicit dialect must be respected"
    )

    path = ROOT / "src/mongoeco/engines/base.py"
    tree = ast.parse(path.read_text())
    planning = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "AsyncReadPlanningEngine"
    )
    method = next(
        node
        for node in planning.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "plan_find_execution"
    )
    annotation = next(
        arg.annotation for arg in method.args.args if arg.arg == "operation"
    )
    require(
        ast.unparse(annotation) == "FindOperation", "planning consumes FindOperation"
    )
    return {
        "guards": guard_matrix,
        "foreign_plan_with_overrides": {
            "accepted": True,
            "specification": {"x": 1},
            "executed_match": {"x": 2},
            "scope": "direct construction/helper; no facade injection demonstrated",
        },
        "unbound_dialect_conversion": {
            "operation": "8.0",
            "implicit_conversion": "7.0",
            "explicit_conversion": "8.0",
            "scope": "unbound helper; bound facade paths resolve from context",
        },
        "curated_exports": {
            name: name in engine_exports
            for name in (
                "FindOperation",
                "UpdateOperation",
                "AggregateOperation",
                "EngineFindSemantics",
                "EngineUpdateSemantics",
            )
        },
        "unexported_but_in_spi_planning": "FindOperation",
        "operation_context_contains_deadline": any(
            "deadline" in name or "timeout" in name
            for name in context.__dataclass_fields__
        ),
        "context_collation_is_normalized": context.collation
        == normalize_collation({"locale": "en", "strength": 2}),
    }


async def cursor_probe():
    async with AsyncMongoClient(MemoryEngine()) as client:
        cursor = client.review.items.find({}).max_time_ms(1000)
        context = cursor._operation_context
        require(cursor._semantics_cache is None, "find construction must remain lazy")
        first_deadline, next_deadline = 100.0, 200.0
        with patch.object(
            semantic_core,
            "operation_deadline",
            side_effect=[first_deadline, next_deadline],
        ) as spy:
            first = cursor._base_semantics()
            again = cursor._base_semantics()
            cursor.rewind()
            reset = cursor._base_semantics()
        require(first is again, "same execution must reuse semantics")
        require(
            first.deadline == first_deadline and reset.deadline == next_deadline,
            "rewind must recapture deadline on next semantic preparation",
        )
        require(
            cursor._operation_context is context, "rewind must retain operation context"
        )
        clone = cursor.clone()
        require(
            clone._operation_context is not context, "clone must capture a new context"
        )
        await clone.close()
        await cursor.close()
        return {
            "find_construction_deadline_calls": 0,
            "two_semantics_reads_then_rewind_deadline_calls": spy.call_count,
            "rewind_keeps_operation_context": True,
            "rewind_recaptures_deadline": True,
            "clone_creates_new_context": True,
            "clock_patch": "local deadline function; no global clock patch",
        }


def count_selector_compilations(spy, selector):
    return sum(
        1
        for call in spy.call_args_list
        if call.args
        and call.args[0] == selector
        and (call.args[1] if len(call.args) > 1 else call.kwargs.get("plan")) is None
    )


async def multi_probe(engine_name, surface, count):
    factory = (
        MemoryEngine if engine_name == "memory" else lambda: SQLiteEngine(":memory:")
    )
    selector = {"group": "a"}
    documents = [{"_id": index, "group": "a"} for index in range(count)]
    original = semantic_core.ensure_query_plan
    if surface == "async":
        async with AsyncMongoClient(factory()) as client:
            collection = client.review.items
            await collection.insert_many(documents)
            with patch.object(
                semantic_core, "ensure_query_plan", wraps=original
            ) as spy:
                result = await collection.update_many(selector, {"$set": {"value": 1}})
    else:
        with MongoClient(factory()) as client:
            collection = client.review.items
            collection.insert_many(documents)
            with patch.object(
                semantic_core, "ensure_query_plan", wraps=original
            ) as spy:
                result = collection.update_many(selector, {"$set": {"value": 1}})
    calls = count_selector_compilations(spy, selector)
    expected = count if engine_name == "memory" else 2 * count
    require(
        calls == expected, f"selector count: {engine_name}/{surface}/{count}: {calls}"
    )
    require(
        result.matched_count == count and result.modified_count == count,
        "probe must update every selected document",
    )
    return {
        "engine": engine_name,
        "surface": surface,
        "documents": count,
        "selector_plan_requests_without_plan": calls,
        "matched_count": result.matched_count,
        "modified_count": result.modified_count,
        "scope": "N semantic compiler entries; not latency or AST instruction counts",
    }


async def main():
    base = json.loads(Path(__file__).with_name("baseline.json").read_text())
    for path, expected in base["source_sha256"].items():
        require(
            hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == expected,
            "baseline source changed",
        )
    extra_paths = (
        "src/mongoeco/api/_async/cursor.py",
        "src/mongoeco/api/_async/_collection_modify.py",
        "src/mongoeco/api/_async/_collection_runtime.py",
        "src/mongoeco/api/_async/aggregation_cursor.py",
        "src/mongoeco/engines/memory.py",
        "src/mongoeco/engines/sqlite.py",
        "src/mongoeco/engines/_sqlite_modify_ops.py",
        "src/mongoeco/engines/__init__.py",
        "src/mongoeco/core/operation_limits.py",
    )
    document = {
        "schema": "mongoeco-spi-proposal-review/1",
        "source_revision": base["source_revision"],
        "python": platform.python_version(),
        "structural": structural_probes(),
        "cursor": await cursor_probe(),
        "update_many": [
            await multi_probe(engine, surface, count)
            for engine in ("memory", "sqlite")
            for surface in ("sync", "async")
            for count in (1, 3)
        ],
        "additional_source_sha256": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in extra_paths
        },
        "limits": [
            "No code fixes, no SPI v3, no native MongoDB or sockets.",
            "SQLite uses in-memory databases; compilation counters are not benchmarks.",
        ],
    }
    OUTPUT.write_text(json.dumps(document, indent=2) + "\n")
    sys.stdout.write(
        json.dumps(
            {
                "output": OUTPUT.relative_to(ROOT).as_posix(),
                "multi_cells": len(document["update_many"]),
            }
        )
        + "\n"
    )


if __name__ == "__main__":
    asyncio.run(main())
