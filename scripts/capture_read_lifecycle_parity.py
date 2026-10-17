"""Capture independent public read lifecycle parity with native PyMongo."""
# ruff: noqa: E402 - standalone observer imports repository test data only

import argparse
import asyncio
import importlib
import json
import sys
import uuid

from pathlib import Path

import pymongo


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


from scripts._capture_identity import SourceIdentity


INPUTS = (
    "scripts/capture_read_lifecycle_parity.py",
    "scripts/_capture_identity.py",
    "tests/differential/read_lifecycle_cases.py",
    "tests/support_read_lifecycle.py",
    "tests/support_error_phases.py",
    "tests/support_unacknowledged.py",
    "tests/support_write_parity.py",
)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=("7.0", "8.0", "9.0"), required=True)
    parser.add_argument("--uri", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    identity = SourceIdentity(ROOT, INPUTS)
    comparable = importlib.import_module("tests.support_error_phases").comparable
    observer = importlib.import_module("tests.support_read_lifecycle")
    pools = importlib.import_module("tests.support_unacknowledged")
    observe_reads, observe_async_reads = (
        observer.observe_reads,
        observer.observe_async_reads,
    )
    pool_type, client_options = pools.PoolObservation, pools.client_options
    database = "read_reference_" + uuid.uuid4().hex
    with pymongo.MongoClient(args.uri, serverSelectionTimeoutMS=3000) as control:
        build = control.server_info()
        fcv = control.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        if not build["version"].startswith(args.target + ".") or fcv != {
            "version": args.target
        }:
            message = "unexpected native version or FCV"
            raise RuntimeError(message)
        pool = pool_type()
        try:
            with pymongo.MongoClient(args.uri, **client_options(pool)) as client:
                sync = observe_reads(client, database)
            control.drop_database(database)

            async def run():
                async_pool = pool_type()
                async with pymongo.AsyncMongoClient(
                    args.uri, **client_options(async_pool)
                ) as client:
                    observations = await observe_async_reads(client, database)
                return observations, async_pool.observation()

            asynchronous, async_pool = asyncio.run(run())
        finally:
            control.drop_database(database)
        if database in control.list_database_names():
            message = "native cleanup failed"
            raise RuntimeError(message)
    if comparable(sync) != comparable(asynchronous):
        message = "native sync and async differ"
        raise RuntimeError(message)
    if (
        pool.observation() != {"physical_connections": 1, "reused": True}
        or async_pool != pool.observation()
    ):
        message = "native physical connection was not reused"
        raise RuntimeError(message)
    if any(name == "mongoeco" or name.startswith("mongoeco.") for name in sys.modules):
        message = "native observer imported the candidate"
        raise RuntimeError(message)
    result = {
        "target": args.target,
        "version": build["version"],
        "fcv": fcv,
        "sdk": pymongo.version,
        "native_sync_async_equal": True,
        "native_physical_connection_reuse": True,
        "observations": sync,
        "inputs_sha256": identity.verify(),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    sys.stdout.write(
        f"MongoDB {build['version']}: {len(sync)} read lifecycle cases; "
        "cleanup and reuse verified\n"
    )


if __name__ == "__main__":
    main()
