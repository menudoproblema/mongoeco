"""Inspect the existing v2 boundary without modifying production sources."""

from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import json
import subprocess
import sys
import tempfile

from collections import Counter
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))

import mongoeco  # noqa: E402

from mongoeco import AsyncMongoClient, MongoClient  # noqa: E402
from mongoeco.api.operations import (  # noqa: E402
    compile_find_operation,
    compile_update_operation,
)
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80  # noqa: E402
from mongoeco.conformance import (  # noqa: E402
    EngineConformanceProvider,
    run_engine_conformance,
)
from mongoeco.core.operation_context import OperationContext  # noqa: E402
from mongoeco.engines import (  # noqa: E402
    MemoryEngine,
    SQLiteEngine,
    memory,
    semantic_core,
    sqlite,
)
from mongoeco.engines.adapter import EngineSpiAdapter  # noqa: E402
from mongoeco.types import UNDEFINED  # noqa: E402

from tests.consumer.engine_canary import ExternalCanaryEngine  # noqa: E402


SOURCES = (
    "src/mongoeco/engines/adapter.py",
    "src/mongoeco/engines/base.py",
    "src/mongoeco/engines/capabilities.py",
    "src/mongoeco/engines/semantic_core.py",
    "src/mongoeco/engines/memory.py",
    "src/mongoeco/engines/sqlite.py",
    "src/mongoeco/engines/_sqlite_modify_ops.py",
    "src/mongoeco/api/operations.py",
    "src/mongoeco/api/_async/_collection_modify.py",
    "src/mongoeco/api/_async/_collection_runtime.py",
    "src/mongoeco/conformance/runner.py",
    "src/mongoeco/conformance/models.py",
    "src/mongoeco/core/query_plan.py",
    "src/mongoeco/core/compiled_query.py",
    "tests/consumer/engine_canary.py",
)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


async def selector_case(factory, action, target_eligible):
    engine = factory()
    connect = getattr(engine, "connect", None)
    if connect is not None:
        await connect()
    try:
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)
        for identifier, eligible in (("a", 1), ("b", int(target_eligible))):
            await engine.insert_document(
                "probe",
                "items",
                {"_id": identifier, "eligible": eligible},
                overwrite=False,
                operation_context=context,
            )
        operation = compile_update_operation(
            {"_id": "b"},
            update_spec={"$set": {"changed": True}} if action == "update" else None,
        ).bind(context)
        adapter = EngineSpiAdapter(engine)
        outcome = await getattr(adapter, action + "_outcome")(
            "probe",
            "items",
            operation,
            selector_filter={"eligible": 1},
            operation_context=context,
        )
        affected = (
            outcome.after_document if action == "update" else outcome.deleted_document
        )
        state = [
            await engine.get_document("probe", "items", key, operation_context=context)
            for key in ("a", "b")
        ]
        count = outcome.matched_count if action == "update" else outcome.deleted_count
        expected_id = "b" if target_eligible else None
        actual_id = affected.get("_id") if affected else None
        return {
            "expected_id": expected_id,
            "actual_id": actual_id,
            "count": count,
            "state": state,
            "matches_contract": actual_id == expected_id
            and count == int(target_eligible),
        }
    finally:
        disconnect = getattr(engine, "disconnect", None)
        if disconnect is not None:
            await disconnect()


def attribute_reads(factory, surface):
    reads = Counter()
    module = memory if factory is MemoryEngine else sqlite
    original = semantic_core.compile_update_semantics

    class ObservedSemantics:
        def __init__(self, value):
            self.value = value

        def __getattr__(self, key):
            reads[key] += 1
            return getattr(self.value, key)

    def compile_observed(*args, **kwargs):
        return ObservedSemantics(original(*args, **kwargs))

    async def ac():
        async with AsyncMongoClient(factory()) as client:
            c = client.probe.items
            await c.insert_many([{"_id": "a", "n": 1}, {"_id": "b", "n": 2}])
            await c.update_many({}, {"$inc": {"n": 1}})
            await c.replace_one({"_id": "a"}, {"n": 4})
            await c.update_one({"_id": "b"}, [{"$set": {"n": 5}}])
            await c.update_one({"_id": "new"}, {"$set": {"n": 6}}, upsert=True)

    with patch.object(module, "compile_update_semantics", side_effect=compile_observed):
        if surface == "async":
            asyncio.run(ac())
        else:
            with MongoClient(factory()) as client:
                c = client.probe.items
                c.insert_many([{"_id": "a", "n": 1}, {"_id": "b", "n": 2}])
                c.update_many({}, {"$inc": {"n": 1}})
                c.replace_one({"_id": "a"}, {"n": 4})
                c.update_one({"_id": "b"}, [{"$set": {"n": 5}}])
                c.update_one({"_id": "new"}, {"$set": {"n": 6}}, upsert=True)
    return dict(sorted(reads.items()))


async def unbound_delete(factory, *, explicit_dialect):
    engine = factory()
    await engine.connect()
    try:
        context = OperationContext.create(dialect=MONGODB_DIALECT_80)
        await engine.insert_document(
            "probe", "items", {"_id": "a", "v": UNDEFINED}, operation_context=context
        )
        operation = compile_update_operation({"_id": "a"}, dialect=MONGODB_DIALECT_80)
        options = {"dialect": MONGODB_DIALECT_80} if explicit_dialect else {}
        result = await engine.delete_with_operation(
            "probe", "items", operation, selector_filter={"v": None}, **options
        )
        return result.deleted_count
    finally:
        await engine.disconnect()


def typing_probe():
    snippets = {
        "variadic_update": (
            'await adapter.update_outcome("db", "items", object(), '
            'upsert="invalid", operation_context="invalid", '
            "replacement_document=[1])"
        ),
        "variadic_read": (
            "await adapter.count_documents(42, None, object(), "
            'operation_context="invalid")'
        ),
        "typed_control": (
            'await adapter.plan_find_execution(42, None, object(), dialect="invalid")'
        ),
        "coordinator_typed_method_through_field": (
            "from mongoeco.api._async._collection_runtime "
            "import CollectionRuntimeCoordinator\n"
            "    from typing import cast\n"
            "    runtime = cast(CollectionRuntimeCoordinator, object())\n"
            "    reveal_type(runtime._engine_spi)\n"
            "    if runtime._engine_spi is not None:\n"
            "        reveal_type(runtime._engine_spi)\n"
            "        await runtime._engine_spi.plan_find_execution(42, None, object())"
        ),
    }
    rows = {}
    with tempfile.TemporaryDirectory(prefix="mongoeco-spi-typing-") as temporary:
        for name, statement in snippets.items():
            path = Path(temporary) / (name + ".py")
            path.write_text(
                "from mongoeco.engines.adapter import EngineSpiAdapter\n"
                "async def call(adapter: EngineSpiAdapter) -> None:\n    "
                + statement
                + "\n"
            )
            result = subprocess.run(  # noqa: S603 - fixed local mypy inputs
                [
                    sys.executable,
                    "-m",
                    "mypy",
                    "--strict",
                    "--follow-imports=silent",
                    "--ignore-missing-imports",
                    str(path),
                ],
                text=True,
                capture_output=True,
                check=False,
            )
            rows[name] = {
                "exit_code": result.returncode,
                "output": result.stdout.replace(str(path), "<probe.py>"),
                "stderr": result.stderr.replace(temporary, "<temporary>"),
            }
    return rows


def static_inventory():
    tree = ast.parse((ROOT / SOURCES[0]).read_text())
    cls = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "EngineSpiAdapter"
    )
    variadic = [
        node.name
        for node in cls.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        and (node.args.vararg is not None or node.args.kwarg is not None)
    ]
    names = ("_iter_scan_documents_sync", "_count_matching_documents_sync")
    references = {name: [] for name in names}
    for folder in ("src", "tests", "scripts", "benchmarks", "examples"):
        for path in (ROOT / folder).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text())):
                if isinstance(node, ast.Attribute) and node.attr in references:
                    references[node.attr].append(
                        {"file": path.relative_to(ROOT).as_posix(), "line": node.lineno}
                    )
    return {
        "variadic_adapter_entrypoints": variadic,
        "private_reader_references": references,
    }


def reachable_read_matcher():
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_80,
        bindings={"tenant": 2},
        collation={"locale": "en", "strength": 2},
    )
    operation = compile_find_operation(
        {"label": "a", "$expr": {"$eq": ["$tenant", "$$tenant"]}},
        dialect=MONGODB_DIALECT_80,
        variables=context.expressions,
        collation=context.collation,
    ).bind(context)
    semantics = semantic_core.compile_find_semantics_from_operation(operation)
    matcher = semantics.compiled_query
    require(matcher is not None, "canonical read producer did not supply a matcher")
    values = {
        "matching": matcher.match(
            {"label": "A", "tenant": 2}, variables=semantics.variables
        ),
        "wrong_binding": matcher.match(
            {"label": "A", "tenant": 3}, variables=semantics.variables
        ),
    }
    require(values == {"matching": True, "wrong_binding": False}, values)
    return {
        "access": (
            "semantics.compiled_query.match(document, variables=semantics.variables)"
        ),
        "result": values,
        "scope": "reachable through an exported DTO field; filtering only",
        "limits": [
            "compiled_query is optional in the public DTO constructor",
            "does not execute projection, sort, paging, deadlines or mutations",
            "reachability alone does not settle the stability of every method",
        ],
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    package = Path(mongoeco.__file__).resolve().parent
    require("site-packages" in package.parts, "use an installed artifact")
    for name in SOURCES:
        if name.startswith("src/"):
            require(
                (ROOT / name).read_bytes()
                == (package.parent / name.removeprefix("src/")).read_bytes(),
                "installed source differs: " + name,
            )
    identities = {
        name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in SOURCES
    }

    async def cases():
        rows = {}
        for label, factory in (
            ("memory", MemoryEngine),
            ("sqlite", SQLiteEngine),
            ("independent-canary", ExternalCanaryEngine),
        ):
            rows[label] = {}
            for action in ("update", "delete"):
                for eligible in (False, True):
                    rows[label][
                        f"{action}/target_eligible={eligible}"
                    ] = await selector_case(factory, action, eligible)
        report = await run_engine_conformance(
            EngineConformanceProvider("independent-canary", ExternalCanaryEngine)
        )
        return rows, report.to_document()["summary"]

    selectors, canary_summary = asyncio.run(cases())
    reads = {
        f"{label}/{surface}": attribute_reads(factory, surface)
        for label, factory in (("memory", MemoryEngine), ("sqlite", SQLiteEngine))
        for surface in ("sync", "async")
    }
    deletes = {
        label: {
            "implicit": asyncio.run(unbound_delete(factory, explicit_dialect=False)),
            "explicit_8_0": asyncio.run(unbound_delete(factory, explicit_dialect=True)),
        }
        for label, factory in (("memory", MemoryEngine), ("sqlite", SQLiteEngine))
    }
    require(
        all(
            row["matches_contract"]
            for label in ("memory", "sqlite")
            for row in selectors[label].values()
        ),
        "built-in selector control failed",
    )
    for name, digest in identities.items():
        require(
            hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest,
            "source changed during probe: " + name,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(
            {
                "schema": "mongoeco-spi-v2-followup-probes/v1",
                "installed_version": mongoeco.__version__,
                "sources_sha256": identities,
                "independent_canary_default_conformance": canary_summary,
                "selector_identity_cases": selectors,
                "internal_update_semantics_attribute_reads": reads,
                "unbound_builtin_delete": {
                    "scope": (
                        "direct convenience call omitting operation_context "
                        "required by the v2 protocol; no facade defect demonstrated"
                    ),
                    "observations": deletes,
                },
                "typing": typing_probe(),
                "static": static_inventory(),
                "reachable_read_matcher": reachable_read_matcher(),
                "limits": [
                    "No production code changed.",
                    "No native MongoDB or global suite rerun for this proposal.",
                    (
                        "Attribute traces characterize the selected scenarios, "
                        "not every possible dynamic consumer."
                    ),
                ],
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
