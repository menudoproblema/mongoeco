# ruff: noqa: E402 - bootstrap repository imports for standalone capture
"""Capture independent native command-phase references with real PyMongo clients."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


_REQUESTS_PER_CASE = 2


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import pymongo

from tests.differential.engine_preparation_cases import PREPARATION_CASES
from tests.differential.error_absence_cases import EXTRA_CASES
from tests.differential.error_composition_cases import COMPOSITION_CASES
from tests.differential.error_phase_cases import CASES
from tests.support_error_phases import (
    comparable,
    observe_async_error_phases,
    observe_error_phases,
)
from tests.support_unacknowledged import PoolObservation, client_options


INPUTS = (
    "tests/differential/error_phase_cases.py",
    "tests/differential/error_absence_cases.py",
    "tests/differential/error_composition_cases.py",
    "tests/differential/write_cases.py",
    "tests/support_error_phases.py",
    "tests/support_write_parity.py",
)


def capture(  # noqa: PLR0913 - capture inputs and explicit corpus selection
    target,
    uri,
    *,
    output=None,
    check=None,
    baseline=None,
    corpus="errors",
    batch_size=64,
):
    cases = (
        PREPARATION_CASES
        if corpus == "preparation"
        else CASES + EXTRA_CASES + COMPOSITION_CASES
    )
    inputs = (
        (*INPUTS, "tests/differential/engine_preparation_cases.py")
        if corpus == "preparation"
        else INPUTS
    )
    name = "phase_reference_" + uuid.uuid4().hex[:16]
    batches = [
        cases[start : start + batch_size] for start in range(0, len(cases), batch_size)
    ]
    pool = PoolObservation()
    with pymongo.MongoClient(
        uri, **{**client_options(pool), "socketTimeoutMS": 30000}
    ) as client:
        build = client.server_info()
        require((build["version"].startswith(target + ".")), build["version"])
        fcv = client.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        sync = {}
        for batch in batches:
            try:
                sync.update(observe_error_phases(client, name, batch))
            finally:
                client.drop_database(name)
    require(
        (pool.observation() == {"physical_connections": 1, "reused": True}),
        "Sync client did not reuse one physical connection",
    )

    async def ac():
        pool = PoolObservation()
        async with pymongo.AsyncMongoClient(
            uri, **{**client_options(pool), "socketTimeoutMS": 30000}
        ) as client:
            rows = {}
            for batch in batches:
                try:
                    rows.update(await observe_async_error_phases(client, name, batch))
                finally:
                    await client.drop_database(name)
        require(
            (pool.observation() == {"physical_connections": 1, "reused": True}),
            "Async client did not reuse one physical connection",
        )
        return rows

    asynchronous = asyncio.run(ac())
    require(
        (comparable(sync) == comparable(asynchronous)),
        "Native sync/async observations differ",
    )
    require(
        (
            all(
                isinstance(row, list)
                and len(row) == _REQUESTS_PER_CASE
                and not any("state_error" in stage for stage in row)
                and "error" not in row[1]
                for row in sync.values()
            )
        ),
        "Native setup, state observation or recovery failed",
    )
    require(
        (
            not any(
                key == "mongoeco" or key.startswith("mongoeco.") for key in sys.modules
            )
        ),
        "Native capture imported the candidate",
    )
    result = {
        "mongodb": build["version"],
        "fcv": fcv,
        "sdk": pymongo.version,
        "native_sync_async_equal": True,
        "native_physical_connection_reuse": True,
        "capture_batches": len(batches),
        "inputs_sha256": {
            path: hashlib.sha256((ROOT / path).read_bytes()).hexdigest()
            for path in inputs
        },
        "observations": sync,
    }
    fixture_prefix = "engine_preparation" if corpus == "preparation" else "error_phases"
    path = output or (
        ROOT
        / "tests/fixtures"
        / f"{fixture_prefix}_mongodb_{target.replace('.', '_')}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2) + "\n")
    if check is not None:
        check_reference(result, target, check, baseline=baseline)
    print(target, len(cases), "native scenarios captured", flush=True)  # noqa: T201 - capture receipt


def check_reference(result, target, check, *, baseline=None):
    observations = result["observations"]
    expected = json.loads(check.read_text())
    if baseline is not None:
        reference = json.loads(baseline.read_text())
        expected = expected["targets"][target]
        require(
            expected["baseline_fixture_sha256"]
            == hashlib.sha256(baseline.read_bytes()).hexdigest(),
            "Driver delta baseline changed",
        )
        require(expected["sdk"] == result["sdk"], "Driver delta SDK changed")
        require(
            expected["compared_scenarios"] == len(result["observations"]),
            "Driver delta corpus changed",
        )
        observations = {
            name: value
            for name, value in result["observations"].items()
            if comparable(value) != comparable(reference["observations"][name])
        }
        expected["observations"] = expected["overrides"]
    require(
        expected["inputs_sha256"] == result["inputs_sha256"],
        "Corpus inputs changed",
    )
    require(
        comparable(expected["observations"]) == comparable(observations),
        f"Native error-phase reference changed for {target}",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--uris",
        type=Path,
        help="JSON mapping dialect to private MongoDB URI",
    )
    parser.add_argument("--target", choices=("7.0", "8.0", "9.0"))
    parser.add_argument("--uri", default=os.getenv("MONGOECO_REAL_MONGODB_URI"))
    parser.add_argument("--corpus", choices=("errors", "preparation"), default="errors")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--check", type=Path)
    parser.add_argument(
        "--baseline", type=Path, help="Native reference for an SDK delta check"
    )
    args = parser.parse_args()
    if args.uris is None:
        if not args.target or not args.uri:
            parser.error("provide --uris or both --target and --uri")
        if args.baseline is not None and args.check is None:
            parser.error("--baseline requires --check")
        capture(
            args.target,
            args.uri,
            output=args.output,
            check=args.check,
            baseline=args.baseline,
            corpus=args.corpus,
        )
        return
    if any((args.target, args.output, args.check, args.baseline)):
        parser.error("--uris cannot be combined with single-target options")
    uris = json.loads(args.uris.read_text())
    with ThreadPoolExecutor(max_workers=3) as executor:
        list(
            executor.map(
                lambda row: capture(
                    row[0],
                    row[1]["uri"] if isinstance(row[1], dict) else row[1],
                    corpus=args.corpus,
                ),
                uris.items(),
            )
        )


if __name__ == "__main__":
    main()
