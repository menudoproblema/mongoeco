"""Capture native unacknowledged writes and response-requiring controls."""

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid

from pathlib import Path

import pymongo


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tests.differential.unacknowledged_cases import WRITE_COMMAND_CASES  # noqa: E402
from tests.support_unacknowledged import (  # noqa: E402
    RAW_CASES,
    PoolObservation,
    client_options,
    observe,
    observe_async,
    observe_raw,
)


PATCHES = {"7.0": "7.0.43", "8.0": "8.0.32", "9.0": "9.0.2"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


async def asynchronous(uri, database):
    results = {}
    for case in WRITE_COMMAND_CASES:
        pool = PoolObservation()
        async with pymongo.AsyncMongoClient(uri, **client_options(pool)) as client:
            results[case.id] = await observe_async(
                client, database, case.id, case, pool
            )
    return results


async def raw(uri, database):
    return {
        f"more_{more}_error_{fails}": await observe_raw(
            uri, database, f"raw_{more}_{fails}", more=more, fails=fails
        )
        for more, fails in RAW_CASES
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=PATCHES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    uri = os.environ["MONGOECO_REAL_MONGODB_URI"]
    name = f"mongoeco_unacknowledged_{uuid.uuid4().hex}"
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as control:
        build = control.server_info()
        require(build["version"] == PATCHES[args.target], "wrong MongoDB patch")
        fcv = control.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        require(fcv == {"version": args.target}, "wrong FCV")
        try:
            sync = {}
            for case in WRITE_COMMAND_CASES:
                pool = PoolObservation()
                with pymongo.MongoClient(uri, **client_options(pool)) as client:
                    sync[case.id] = observe(client, name, case.id, case, pool)
            async_results = asyncio.run(asynchronous(uri, name + "_async"))
            raw_results = asyncio.run(raw(uri, name + "_raw"))
        finally:
            for database in (name, name + "_async", name + "_raw"):
                control.drop_database(database)
                require(database not in control.list_database_names(), "cleanup failed")
    require(sync == async_results, "native sync/async observations differ")
    require(
        all(
            item["connection"] == {"physical_connections": 1, "reused": True}
            for item in sync.values()
        ),
        "native connection was not reused",
    )
    require(
        not any(n == "mongoeco" or n.startswith("mongoeco.") for n in sys.modules),
        "native capture imported the candidate",
    )
    capture = {
        "target": args.target,
        "build_info": build,
        "fcv": fcv,
        "sdk": pymongo.version,
        "source_sha256": {
            relative: hashlib.sha256((ROOT / relative).read_bytes()).hexdigest()
            for relative in (
                "tests/differential/unacknowledged_cases.py",
                "tests/differential/write_preflight_cases.py",
                "tests/support_unacknowledged.py",
                "tests/support_write_parity.py",
            )
        },
        "native_facades": ["sync", "async"],
        "observations": sync,
        "raw": raw_results,
    }
    if args.check is not None:
        expected = json.loads(args.check.read_text())
        require(
            sync == expected["observations"] and raw_results == expected["raw"],
            "native capture differs",
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(capture, indent=2) + "\n")
    print(  # noqa: T201 - capture receipt
        f"MongoDB {build['version']} / PyMongo {pymongo.version}: "
        f"{len(sync)} cases per facade and {len(raw_results)} raw; "
        "native sync/async equal, physical connection reused and cleanup verified"
    )


if __name__ == "__main__":
    main()
