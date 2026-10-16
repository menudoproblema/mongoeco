"""Capture API option results and comments through real PyMongo sync/async."""

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

from tests.differential.api_option_cases import API_OPTION_CASES  # noqa: E402
from tests.support_api_options import (  # noqa: E402
    observe_api_case,
    observe_async_api_case,
)


PATCHES = {"7.0": "7.0.43", "8.0": "8.0.32", "9.0": "9.0.2"}


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


async def capture_async(uri, name):
    async with pymongo.AsyncMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        database = client[name]
        try:
            await database.command({"profile": 2, "slowms": 0})
            return {
                case.id: await observe_async_api_case(database, case.id, case)
                for case in API_OPTION_CASES
            }
        finally:
            await client.drop_database(name)
            require(
                name not in await client.list_database_names(), "async cleanup failed"
            )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=PATCHES, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    uri = os.environ["MONGOECO_REAL_MONGODB_URI"]
    name = f"mongoeco_api_options_{uuid.uuid4().hex}"
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        build = client.server_info()
        require(build["version"] == PATCHES[args.target], "wrong server patch")
        fcv = client.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        require(fcv == {"version": args.target}, "wrong FCV")
        database = client[name]
        try:
            database.command({"profile": 2, "slowms": 0})
            sync = {
                case.id: observe_api_case(database, case.id, case)
                for case in API_OPTION_CASES
            }
        finally:
            client.drop_database(name)
            require(name not in client.list_database_names(), "sync cleanup failed")
    asynchronous = asyncio.run(capture_async(uri, name + "_async"))
    require(sync == asynchronous, "native sync/async observations differ")
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
                "tests/differential/api_option_cases.py",
                "tests/support_api_options.py",
                "tests/support_write_parity.py",
            )
        },
        "native_facades": ["sync", "async"],
        "observations": sync,
    }
    if args.check is not None:
        require(
            sync == json.loads(args.check.read_text())["observations"],
            "native capture differs",
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(capture, indent=2) + "\n")
    print(  # noqa: T201 - native capture receipt
        f"MongoDB {build['version']} / PyMongo {pymongo.version}: "
        f"{len(sync)} cases per facade; native sync/async equal; cleanup verified"
    )


if __name__ == "__main__":
    main()
