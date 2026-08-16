from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import subprocess
import sys
import tempfile
import venv

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = PROJECT_ROOT / "tests/fixtures/sqlite/mongoeco-4.5.0-bridge.sqlite"
DEFAULT_METADATA = PROJECT_ROOT / "tests/fixtures/sqlite/mongoeco-4.5.0-bridge.json"
PAYLOAD = PROJECT_ROOT / "scripts/_sqlite_45_fixture_payload.py"
FIXTURE_CONSTRAINTS = PROJECT_ROOT / "requirements/sqlite-45-fixture-constraints.txt"
MONGOECO_45_WHEEL_SHA256 = (
    "f168ab9f4172abbf1a7e35f8996c3e01463a26557b213028c83ef64d102a2fd3"
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_metadata(
    output: Path,
    metadata_path: Path,
    wheel: Path,
    runtime: dict[str, object],
) -> None:
    document = {
        "schemaVersion": "mongoeco-sqlite-fixture/v1",
        "fixture": output.name,
        "fixtureSha256": _sha256(output),
        "generator": {
            "package": "mongoeco",
            "version": "4.5.0",
            "artifact": wheel.name,
            "artifactSha256": MONGOECO_45_WHEEL_SHA256,
            "source": "PyPI mongoeco==4.5.0",
            "payload": "scripts/_sqlite_45_fixture_payload.py",
            "constraints": "requirements/sqlite-45-fixture-constraints.txt",
            "runtime": runtime,
        },
        "database": {
            "namespace": "bridge.items",
            "documentCount": 3,
            "scalarIndexes": ["kind_1"],
            "searchIndexes": ["by_text"],
            "outboxSchemaVersion": 4,
            "consumer": "bridge-durable",
            "consumerCheckpoint": 2,
            "pendingSequences": [3],
            "operationId": "mongoeco-4.5.0-bridge-operation",
        },
    }
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _runtime_metadata(python: Path) -> dict[str, object]:
    script = """
import importlib.metadata
import json
import platform
import sqlite3
import sys

packages = (
    "cxp",
    "msgspec",
    "mongoeco",
    "numpy",
    "numkong",
    "pyuca",
    "shapely",
    "tqdm",
    "usearch",
)
print(json.dumps({
    "python": platform.python_version(),
    "pythonImplementation": platform.python_implementation(),
    "sqlite": sqlite3.sqlite_version,
    "platform": platform.platform(),
    "packages": {name: importlib.metadata.version(name) for name in packages},
}, sort_keys=True))
"""
    result = subprocess.run(  # noqa: S603 - isolated verified interpreter
        [str(python), "-c", script],
        check=True,
        capture_output=True,
        text=True,
    )
    document = json.loads(result.stdout)
    if not isinstance(document, dict):
        message = "fixture runtime metadata must be a JSON object"
        raise RuntimeError(message)
    return document


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Generate the frozen SQLite compatibility fixture using the official "
            "MongoEco 4.5.0 wheel."
        ),
    )
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--metadata", type=Path, default=DEFAULT_METADATA)
    args = parser.parse_args()

    wheel = args.wheel.expanduser().resolve()
    if not wheel.is_file():
        raise FileNotFoundError(wheel)
    observed_hash = _sha256(wheel)
    if observed_hash != MONGOECO_45_WHEEL_SHA256:
        message = (
            "MongoEco 4.5.0 wheel SHA-256 mismatch: "
            f"expected {MONGOECO_45_WHEEL_SHA256}, observed {observed_hash}"
        )
        raise RuntimeError(message)
    output = args.output.expanduser().resolve()
    metadata_path = args.metadata.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)

    environment_root = Path(tempfile.mkdtemp(prefix="mongoeco-45-fixture-"))
    try:
        venv.EnvBuilder(with_pip=True).create(environment_root)
        python = environment_root / (
            "Scripts/python.exe" if sys.platform == "win32" else "bin/python"
        )
        subprocess.run(  # noqa: S603 - interpreter and wheel paths are verified
            [
                str(python),
                "-m",
                "pip",
                "install",
                "--constraint",
                str(FIXTURE_CONSTRAINTS),
                str(wheel),
            ],
            check=True,
        )
        subprocess.run(  # noqa: S603 - interpreter and payload paths are controlled
            [str(python), str(PAYLOAD), str(output)],
            cwd=Path(tempfile.gettempdir()),
            check=True,
        )
        runtime = _runtime_metadata(python)
    finally:
        shutil.rmtree(environment_root, ignore_errors=True)
    _write_metadata(output, metadata_path, wheel, runtime)
    sys.stdout.write(f"fixture_sha256={_sha256(output)}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
