"""Create the preparation fixture using the verified published 4.10.1 wheel."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import platform
import sqlite3
import zipfile

from pathlib import Path

import _sqlite_45_fixture_payload as payload

import mongoeco


WHEEL_SHA256 = "0b4419ec41777a2914e1a331473c5bc2dc3e9770c31a61061b894d89135a7ddd"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    module = Path(mongoeco.__file__).resolve()
    if mongoeco.__version__ != "4.10.1" or "site-packages" not in module.parts:
        message = "fixture requires the installed published 4.10.1 wheel"
        raise RuntimeError(message)
    if hashlib.sha256(args.wheel.read_bytes()).hexdigest() != WHEEL_SHA256:
        message = "published wheel hash mismatch"
        raise RuntimeError(message)
    with zipfile.ZipFile(args.wheel) as archive:
        for name in archive.namelist():
            if name.startswith("mongoeco/") and (
                module.parent.parent / name
            ).read_bytes() != archive.read(name):
                message = "installed runtime differs from published wheel"
                raise RuntimeError(message)
    if args.output.exists():
        message = "refusing to overwrite an existing fixture"
        raise FileExistsError(message)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload.FIXTURE_OPERATION_ID = "mongoeco-4.10.1-preparation-operation"
    asyncio.run(payload._generate(args.output))
    metadata = {
        "schemaVersion": "mongoeco-sqlite-fixture/v1",
        "fixture": args.output.name,
        "fixtureSha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "generator": {
            "version": "4.10.1",
            "artifactSha256": WHEEL_SHA256,
            "python": platform.python_version(),
            "sqlite": sqlite3.sqlite_version,
            "fromSitePackages": True,
            "payload": "scripts/_sqlite_45_fixture_payload.py:_generate",
            "payloadSha256": hashlib.sha256(
                Path(payload.__file__).read_bytes()
            ).hexdigest(),
        },
        "database": {
            "namespace": "bridge.items",
            "documentCount": 3,
            "consumerCheckpoint": 2,
            "pendingSequences": [3],
            "operationId": payload.FIXTURE_OPERATION_ID,
        },
    }
    args.output.with_suffix(".json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
