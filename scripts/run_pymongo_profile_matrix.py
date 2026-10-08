#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import textwrap

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_VERSIONS = ("4.9.2", "4.11.3", "4.13.2", "4.17.0", "4.18.2")
WRITE_METHODS = (
    "update_one",
    "update_many",
    "replace_one",
    "delete_one",
    "delete_many",
    "find_one_and_update",
    "find_one_and_replace",
    "find_one_and_delete",
    "bulk_write",
)
AGGREGATE_OPTIONS = ("hint", "comment", "maxTimeMS", "batchSize", "let")
CHECK_ORDER = (
    "update_one.sort",
    "update_one.hint",
    "update_one.comment",
    "update_one.max_time_ms",
    "update_one.let",
    "update_many.hint",
    "update_many.comment",
    "update_many.max_time_ms",
    "update_many.let",
    "replace_one.sort",
    "replace_one.hint",
    "replace_one.comment",
    "replace_one.max_time_ms",
    "replace_one.let",
    "delete_one.hint",
    "delete_one.comment",
    "delete_one.max_time_ms",
    "delete_one.let",
    "delete_many.hint",
    "delete_many.comment",
    "delete_many.max_time_ms",
    "delete_many.let",
    "find_one_and_update.hint",
    "find_one_and_update.comment",
    "find_one_and_update.max_time_ms",
    "find_one_and_update.let",
    "find_one_and_replace.hint",
    "find_one_and_replace.comment",
    "find_one_and_replace.max_time_ms",
    "find_one_and_replace.let",
    "find_one_and_delete.hint",
    "find_one_and_delete.comment",
    "find_one_and_delete.max_time_ms",
    "find_one_and_delete.let",
    "aggregate.hint",
    "aggregate.comment",
    "aggregate.max_time_ms",
    "aggregate.batch_size",
    "aggregate.let",
    "bulk_write.comment",
    "bulk_write.let",
    "bulk_write.hint",
    "bulk_write.sort",
    "bulk_write.delete_hint",
    "bulk_write.replace_sort",
    "aggregate.aggregate",
    "aggregate.duplicate_pipeline",
    "aggregate_raw_batches.aggregate",
    "aggregate_raw_batches.duplicate_pipeline",
    "list_search_indexes.aggregate",
    "list_search_indexes.pipeline",
)
CHECK_LABELS = {
    "bulk_write.hint": ("bulk_write", "UpdateOne.hint"),
    "bulk_write.sort": ("bulk_write", "UpdateOne.sort"),
    "bulk_write.delete_hint": ("bulk_write", "DeleteOne.hint"),
    "bulk_write.replace_sort": ("bulk_write", "ReplaceOne.sort"),
}


def probe_call(call) -> dict[str, object]:
    """Measure argument validation, not successful execution on a server."""
    from pymongo.errors import (  # noqa: PLC0415 - driver lives in the isolated probe environment
        ConfigurationError,
        ServerSelectionTimeoutError,
    )

    try:
        call()
    except ServerSelectionTimeoutError as exc:
        # This probe intentionally has no server. Reaching server selection
        # proves only that the helper accepted the supplied arguments.
        return {
            "accepted": True,
            "status": "arguments-accepted",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    except (TypeError, ConfigurationError) as exc:
        status = (
            "configuration-rejected"
            if isinstance(exc, ConfigurationError)
            else "signature-rejected"
        )
        return {
            "accepted": False,
            "status": status,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    except Exception as exc:
        return {
            "accepted": None,
            "status": "indeterminate",
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
    return {"accepted": True, "status": "completed", "error_type": None, "error": None}


PROBE_SCRIPT = textwrap.dedent(
    """
    import json

    from pymongo import MongoClient
    from pymongo.operations import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne
    from scripts.run_pymongo_profile_matrix import probe_call


    client = MongoClient("mongodb://127.0.0.1:1", connect=False, serverSelectionTimeoutMS=1)
    collection = client.get_database("db").get_collection("coll")

    checks = {
        "update_one.sort": lambda: collection.update_one({"x": 1}, {"$set": {"y": 1}}, sort=[("rank", 1)]),
        "update_one.hint": lambda: collection.update_one({"x": 1}, {"$set": {"y": 1}}, hint="_id_"),
        "update_one.comment": lambda: collection.update_one({"x": 1}, {"$set": {"y": 1}}, comment="trace"),
        "update_one.max_time_ms": lambda: collection.update_one({"x": 1}, {"$set": {"y": 1}}, max_time_ms=5),
        "update_one.let": lambda: collection.update_one({"x": 1}, {"$set": {"y": 1}}, let={"tenant": "a"}),
        "update_many.hint": lambda: collection.update_many({"x": 1}, {"$set": {"y": 1}}, hint="_id_"),
        "update_many.comment": lambda: collection.update_many({"x": 1}, {"$set": {"y": 1}}, comment="trace"),
        "update_many.max_time_ms": lambda: collection.update_many({"x": 1}, {"$set": {"y": 1}}, max_time_ms=5),
        "update_many.let": lambda: collection.update_many({"x": 1}, {"$set": {"y": 1}}, let={"tenant": "a"}),
        "replace_one.sort": lambda: collection.replace_one({"x": 1}, {"x": 1}, sort=[("rank", 1)]),
        "replace_one.hint": lambda: collection.replace_one({"x": 1}, {"x": 1}, hint="_id_"),
        "replace_one.comment": lambda: collection.replace_one({"x": 1}, {"x": 1}, comment="trace"),
        "replace_one.max_time_ms": lambda: collection.replace_one({"x": 1}, {"x": 1}, max_time_ms=5),
        "replace_one.let": lambda: collection.replace_one({"x": 1}, {"x": 1}, let={"tenant": "a"}),
        "delete_one.hint": lambda: collection.delete_one({"x": 1}, hint="_id_"),
        "delete_one.comment": lambda: collection.delete_one({"x": 1}, comment="trace"),
        "delete_one.max_time_ms": lambda: collection.delete_one({"x": 1}, max_time_ms=5),
        "delete_one.let": lambda: collection.delete_one({"x": 1}, let={"tenant": "a"}),
        "delete_many.hint": lambda: collection.delete_many({"x": 1}, hint="_id_"),
        "delete_many.comment": lambda: collection.delete_many({"x": 1}, comment="trace"),
        "delete_many.max_time_ms": lambda: collection.delete_many({"x": 1}, max_time_ms=5),
        "delete_many.let": lambda: collection.delete_many({"x": 1}, let={"tenant": "a"}),
        "find_one_and_update.hint": lambda: collection.find_one_and_update({"x": 1}, {"$set": {"y": 1}}, hint="_id_"),
        "find_one_and_update.comment": lambda: collection.find_one_and_update({"x": 1}, {"$set": {"y": 1}}, comment="trace"),
        "find_one_and_update.max_time_ms": lambda: collection.find_one_and_update({"x": 1}, {"$set": {"y": 1}}, max_time_ms=5),
        "find_one_and_update.let": lambda: collection.find_one_and_update({"x": 1}, {"$set": {"y": 1}}, let={"tenant": "a"}),
        "find_one_and_replace.hint": lambda: collection.find_one_and_replace({"x": 1}, {"x": 1}, hint="_id_"),
        "find_one_and_replace.comment": lambda: collection.find_one_and_replace({"x": 1}, {"x": 1}, comment="trace"),
        "find_one_and_replace.max_time_ms": lambda: collection.find_one_and_replace({"x": 1}, {"x": 1}, max_time_ms=5),
        "find_one_and_replace.let": lambda: collection.find_one_and_replace({"x": 1}, {"x": 1}, let={"tenant": "a"}),
        "find_one_and_delete.hint": lambda: collection.find_one_and_delete({"x": 1}, hint="_id_"),
        "find_one_and_delete.comment": lambda: collection.find_one_and_delete({"x": 1}, comment="trace"),
        "find_one_and_delete.max_time_ms": lambda: collection.find_one_and_delete({"x": 1}, max_time_ms=5),
        "find_one_and_delete.let": lambda: collection.find_one_and_delete({"x": 1}, let={"tenant": "a"}),
        "aggregate.hint": lambda: collection.aggregate([], hint="_id_"),
        "aggregate.comment": lambda: collection.aggregate([], comment="trace"),
        "aggregate.max_time_ms": lambda: collection.aggregate([], maxTimeMS=5),
        "aggregate.batch_size": lambda: collection.aggregate([], batchSize=5),
        "aggregate.let": lambda: collection.aggregate([], let={"tenant": "a"}),
        "aggregate.aggregate": lambda: collection.aggregate([], aggregate="other"),
        "aggregate.duplicate_pipeline": lambda: collection.aggregate([], pipeline=[]),
        "aggregate_raw_batches.aggregate": lambda: (
            collection.aggregate_raw_batches([], aggregate="other")
        ),
        "aggregate_raw_batches.duplicate_pipeline": lambda: (
            collection.aggregate_raw_batches([], pipeline=[])
        ),
        "list_search_indexes.aggregate": lambda: (
            collection.list_search_indexes(aggregate="other")
        ),
        "list_search_indexes.pipeline": lambda: (
            collection.list_search_indexes(pipeline=[])
        ),
        "bulk_write.comment": lambda: collection.bulk_write([InsertOne({"x": 1})], comment="trace"),
        "bulk_write.let": lambda: collection.bulk_write([InsertOne({"x": 1})], let={"tenant": "a"}),
        "bulk_write.hint": lambda: collection.bulk_write([UpdateOne({"x": 1}, {"$set": {"y": 1}}, hint="_id_")]),
        "bulk_write.sort": lambda: collection.bulk_write([UpdateOne({"x": 1}, {"$set": {"y": 1}}, sort=[("rank", 1)])]),
        "bulk_write.delete_hint": lambda: collection.bulk_write([DeleteOne({"x": 1}, hint="_id_")]),
        "bulk_write.replace_sort": lambda: collection.bulk_write([ReplaceOne({"x": 1}, {"x": 1}, sort=[("rank", 1)])]),
    }

    print(json.dumps({name: probe_call(check) for name, check in checks.items()}, indent=2, sort_keys=True))
    """
)


def run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    # Each probe must import and install its own driver, independently of the
    # caller's source path or CI constraint on the main environment.
    for name in ("PYTHONPATH", "PYTHONHOME", "PIP_CONSTRAINT"):
        environment.pop(name, None)
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=True,
        env=environment,
    )


def ensure_venv(version: str, root: Path, python: str) -> Path:
    venv_dir = root / version
    python_bin = venv_dir / "bin" / "python"
    if not python_bin.exists():
        run([python, "-m", "venv", str(venv_dir)])
        run([str(python_bin), "-m", "pip", "install", "--upgrade", "pip"])
        run([str(python_bin), "-m", "pip", "install", f"pymongo=={version}"])
    actual = run(
        [str(python_bin), "-c", "import pymongo; print(pymongo.version)"]
    ).stdout.strip()
    if actual != version:
        message = f"PyMongo environment {venv_dir} has {actual}, expected {version}"
        raise RuntimeError(message)
    return python_bin


def probe_version(version: str, root: Path, python: str) -> dict[str, object]:
    python_bin = ensure_venv(version, root, python)
    result = run([str(python_bin), "-c", PROBE_SCRIPT], cwd=PROJECT_ROOT)
    results = json.loads(result.stdout)
    for check in results:
        _accepted({version: results}, version, check)
    return results


def _version_sort_key(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split(".") if part.isdigit())


def _minor_key(version: str) -> str:
    major, minor, *_rest = version.split(".")
    return f"{major}_{minor}"


def _minor_label(version: str) -> str:
    major, minor, *_rest = version.split(".")
    return f"{major}.{minor}"


def _check_sort_key(check: str) -> tuple[int, str]:
    try:
        return (CHECK_ORDER.index(check), check)
    except ValueError:
        return (len(CHECK_ORDER), check)


def _check_label(check: str) -> tuple[str, str]:
    mapped = CHECK_LABELS.get(check)
    if mapped is not None:
        return mapped
    group, _, option = check.partition(".")
    return group, option


def _accepted(results: dict[str, dict[str, object]], version: str, check: str) -> bool:
    result = results[version][check]
    if not isinstance(result, dict):
        raise TypeError(f"Unexpected probe result for {version} {check}: {result!r}")
    accepted = result["accepted"]
    if not isinstance(accepted, bool):
        message = f"Indeterminate probe for {version} {check}: {result!r}"
        raise ValueError(message)
    return accepted


def _all_checks(results: dict[str, dict[str, object]]) -> list[str]:
    checks = {
        check for version_results in results.values() for check in version_results
    }
    return sorted(checks, key=_check_sort_key)


def _group_checks(checks: list[str]) -> dict[str, list[str]]:
    grouped = {method: [] for method in (*WRITE_METHODS, "aggregate")}
    extras: dict[str, list[str]] = {}
    for check in checks:
        group, label = _check_label(check)
        target = grouped.get(group)
        if target is None:
            target = extras.setdefault(group, [])
        target.append(label)
    return {group: labels for group, labels in {**grouped, **extras}.items() if labels}


def _delta_label(check: str) -> str:
    group, label = _check_label(check)
    return f"{group}.{label}"


def summarize_results(results: dict[str, dict[str, object]]) -> dict[str, object]:
    versions = sorted(results, key=_version_sort_key)
    checks = _all_checks(results)
    accepted_by_version = {
        version: {check for check in checks if _accepted(results, version, check)}
        for version in versions
    }
    all_versions = [accepted_by_version[version] for version in versions]
    baseline = [
        check for check in checks if all(check in accepted for accepted in all_versions)
    ]
    unsupported = [
        check
        for check in checks
        if all(check not in accepted for accepted in all_versions)
    ]
    deltas: dict[str, list[str]] = {}
    rejections: dict[str, list[str]] = {}
    for index, version in enumerate(versions[1:], start=1):
        delta_checks = [
            check
            for check in checks
            if check not in baseline
            and check not in unsupported
            and all(
                check not in accepted_by_version[previous]
                for previous in versions[:index]
            )
            and all(
                check in accepted_by_version[current] for current in versions[index:]
            )
        ]
        if delta_checks:
            deltas[f"{_minor_label(version)}_plus"] = [
                _delta_label(check) for check in delta_checks
            ]
        rejected_checks = [
            check
            for check in checks
            if all(check in accepted_by_version[previous] for previous in versions[:index])
            and all(check not in accepted_by_version[current] for current in versions[index:])
        ]
        if rejected_checks:
            rejections[f"{_minor_label(version)}_plus"] = [
                _delta_label(check) for check in rejected_checks
            ]

    return {
        "generated_from": [f"PyMongo {version}" for version in versions],
        "scope": {
            "write_methods": list(WRITE_METHODS),
            "aggregate": list(AGGREGATE_OPTIONS),
        },
        f"confirmed_baseline_{_minor_key(versions[0])}_plus": _group_checks(baseline),
        "confirmed_profile_deltas": deltas,
        "confirmed_profile_rejections": rejections,
        f"confirmed_unsupported_in_{_minor_key(versions[0])}_to_{_minor_key(versions[-1])}": _group_checks(
            unsupported
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Contrasta la superficie PyMongo real contra las opciones modeladas por mongoeco.",
    )
    parser.add_argument(
        "--versions",
        nargs="+",
        default=list(DEFAULT_VERSIONS),
        help="Versiones de PyMongo a instalar y contrastar.",
    )
    parser.add_argument(
        "--root",
        default=".tmp/pymongo-profile-matrix",
        help="Directorio donde se crearán los entornos virtuales.",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="Intérprete base para crear los entornos.",
    )
    parser.add_argument(
        "--keep",
        action="store_true",
        help="Conserva los entornos creados tras la ejecución.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Fichero opcional donde guardar el JSON de resultados.",
    )
    parser.add_argument(
        "--summary-output",
        default=None,
        help="Fichero opcional donde guardar el resumen estable usado como fixture.",
    )
    args = parser.parse_args()

    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    results = {
        version: probe_version(version, root, args.python) for version in args.versions
    }

    payload = json.dumps(results, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    if args.summary_output:
        summary = json.dumps(summarize_results(results), indent=2, sort_keys=False)
        Path(args.summary_output).write_text(summary + "\n", encoding="utf-8")
    print(payload)

    if not args.keep:
        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
