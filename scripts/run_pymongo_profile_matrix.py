#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path


DEFAULT_VERSIONS = ("4.9.2", "4.11.3", "4.13.2")

PROBE_SCRIPT = textwrap.dedent(
    """
    import json

    from pymongo import MongoClient
    from pymongo.operations import DeleteMany, DeleteOne, InsertOne, ReplaceOne, UpdateMany, UpdateOne


    def accepted(label, call):
        try:
            call()
        except TypeError as exc:
            message = str(exc)
            return {
                "accepted": "unexpected keyword argument" not in message,
                "error_type": type(exc).__name__,
                "error": message,
            }
        except Exception as exc:
            return {
                "accepted": True,
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        return {
            "accepted": True,
            "error_type": None,
            "error": None,
        }


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
        "bulk_write.comment": lambda: collection.bulk_write([InsertOne({"x": 1})], comment="trace"),
        "bulk_write.let": lambda: collection.bulk_write([InsertOne({"x": 1})], let={"tenant": "a"}),
        "bulk_write.hint": lambda: collection.bulk_write([UpdateOne({"x": 1}, {"$set": {"y": 1}}, hint="_id_")]),
        "bulk_write.sort": lambda: collection.bulk_write([UpdateOne({"x": 1}, {"$set": {"y": 1}}, sort=[("rank", 1)])]),
        "bulk_write.delete_hint": lambda: collection.bulk_write([DeleteOne({"x": 1}, hint="_id_")]),
        "bulk_write.replace_sort": lambda: collection.bulk_write([ReplaceOne({"x": 1}, {"x": 1}, sort=[("rank", 1)])]),
    }

    print(json.dumps({name: accepted(name, check) for name, check in checks.items()}, indent=2, sort_keys=True))
    """
)


def run(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        text=True,
        capture_output=True,
        check=True,
    )


def ensure_venv(version: str, root: Path, python: str) -> Path:
    venv_dir = root / version
    python_bin = venv_dir / "bin" / "python"
    if python_bin.exists():
        return python_bin

    run([python, "-m", "venv", str(venv_dir)])
    run([str(python_bin), "-m", "pip", "install", "--upgrade", "pip"])
    run([str(python_bin), "-m", "pip", "install", f"pymongo=={version}"])
    return python_bin


def probe_version(version: str, root: Path, python: str) -> dict[str, object]:
    python_bin = ensure_venv(version, root, python)
    result = run([str(python_bin), "-c", PROBE_SCRIPT])
    return json.loads(result.stdout)


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
    args = parser.parse_args()

    root = Path(args.root).resolve()
    root.mkdir(parents=True, exist_ok=True)

    results = {
        version: probe_version(version, root, args.python)
        for version in args.versions
    }

    payload = json.dumps(results, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(payload + "\n", encoding="utf-8")
    print(payload)

    if not args.keep:
        shutil.rmtree(root, ignore_errors=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
