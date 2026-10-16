"""Capture the finite write corpus exclusively through a real PyMongo server."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import uuid

from pathlib import Path

import pymongo


sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tests.differential.find_modify_cases import FIND_MODIFY_CASES
from tests.differential.shared_error_cases import SHARED_ERROR_CASES
from tests.differential.write_cases import WRITE_CASES
from tests.differential.write_identity_cases import (
    IDENTITY_CASES,
    RAW_IDENTITY_CASE_IDS,
)
from tests.differential.write_operation_cases import WRITE_OPERATION_CASES
from tests.differential.write_query_cases import RAW_QUERY_CASES
from tests.support_collation_diagnostics import (
    observe_async_collation_keys,
    observe_collation_keys,
)
from tests.support_unacknowledged import PoolObservation
from tests.support_write_histories import HISTORY_CASES, observe_history
from tests.support_write_parity import (
    RAW_CASE_IDS,
    observe_async_raw_write,
    observe_async_write_case,
    observe_raw_write,
    observe_write_case,
)


PATCHES = {"7.0": "7.0.43", "8.0": "8.0.32", "9.0": "9.0.2"}
IDENTITY_CASE_COUNT = 321
RAW_IDENTITY_CASE_COUNT = 180
RAW_QUERY_CASE_COUNT = 28
OPERATION_CASE_COUNT = 42
FIND_MODIFY_CASE_COUNT = 38
SHARED_ERROR_CASE_COUNT = 173


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


async def capture_async(uri, name):
    pool = PoolObservation()
    async with pymongo.AsyncMongoClient(
        uri, maxPoolSize=1, event_listeners=[pool], serverSelectionTimeoutMS=3000
    ) as client:
        db = client[name]
        try:
            result = []
            for cases in (
                WRITE_CASES,
                IDENTITY_CASES,
                WRITE_OPERATION_CASES,
                FIND_MODIFY_CASES,
                SHARED_ERROR_CASES,
            ):
                result.append(
                    {
                        case.id: await observe_async_write_case(
                            db,
                            f"c{len(result)}_{case.id}",
                            case,
                            error_contract=cases is SHARED_ERROR_CASES,
                        )
                        for case in cases
                    }
                )
            result.append(
                {
                    case.id: await observe_async_raw_write(db, "raw_" + case.id, case)
                    for case in WRITE_CASES
                    if case.id in RAW_CASE_IDS
                }
            )
            result.append(
                {
                    case.id: await observe_async_raw_write(
                        db, "id_raw_" + case.id, case
                    )
                    for case in IDENTITY_CASES
                    if case.id in RAW_IDENTITY_CASE_IDS
                }
            )
            result.append(
                {
                    case.id: await observe_async_raw_write(db, "query_" + case.id, case)
                    for case in RAW_QUERY_CASES
                }
            )
            result.append(await observe_async_collation_keys(db))
            require(
                pool.observation()["reused"],
                "native async physical connection not reused",
            )
            return result
        finally:
            await client.drop_database(name)
            require(
                name not in await client.list_database_names(), "async cleanup failed"
            )


def verify_inventory(root):
    plan = json.loads((root / "docs/write-parity-corpus.json").read_text())
    require(
        {case.id for case in WRITE_CASES} == {row["id"] for row in plan["sequential"]},
        "corpus inventory differs",
    )
    require(
        (len(WRITE_CASES), len(HISTORY_CASES), len(RAW_CASE_IDS)) == (170, 6, 8),
        "unexpected corpus counts",
    )
    require(
        len(IDENTITY_CASES)
        == len({case.id for case in IDENTITY_CASES})
        == IDENTITY_CASE_COUNT,
        "unexpected identity corpus inventory",
    )
    require(
        len(WRITE_OPERATION_CASES)
        == len({case.id for case in WRITE_OPERATION_CASES})
        == OPERATION_CASE_COUNT,
        "unexpected operation payload corpus inventory",
    )
    require(
        len(FIND_MODIFY_CASES)
        == len({case.id for case in FIND_MODIFY_CASES})
        == FIND_MODIFY_CASE_COUNT,
        "unexpected findAndModify command-error corpus inventory",
    )
    require(
        len(RAW_IDENTITY_CASE_IDS) == RAW_IDENTITY_CASE_COUNT
        and set(RAW_IDENTITY_CASE_IDS) <= {case.id for case in IDENTITY_CASES},
        "unexpected raw identity corpus inventory",
    )
    require(
        len(RAW_QUERY_CASES)
        == len({case.id for case in RAW_QUERY_CASES})
        == RAW_QUERY_CASE_COUNT,
        "unexpected raw query corpus inventory",
    )
    require(
        len(SHARED_ERROR_CASES) == SHARED_ERROR_CASE_COUNT,
        "unexpected shared-error inventory",
    )
    return plan


def main():  # noqa: PLR0915 - native capture owns setup, facade comparison and cleanup
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=PATCHES, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--histories-dir", type=Path, required=True)
    parser.add_argument("--allow-local-patch", action="store_true")
    parser.add_argument("--check-dir", type=Path)
    args = parser.parse_args()
    uri = os.environ["MONGOECO_REAL_MONGODB_URI"]
    root = Path(__file__).resolve().parents[1]
    catalog = root / "tests/differential/write_cases.py"
    plan = verify_inventory(root)
    inputs = {
        path: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in (
            *sorted((root / "tests/differential").glob("*.py")),
            root / "tests/support_write_parity.py",
            root / "tests/support_collation_diagnostics.py",
        )
    }
    pool = PoolObservation()
    with pymongo.MongoClient(
        uri, maxPoolSize=1, event_listeners=[pool], serverSelectionTimeoutMS=3000
    ) as client:
        build = client.server_info()
        require(
            build["versionArray"][:2] == [int(x) for x in args.target.split(".")],
            "wrong server version",
        )
        require(
            args.allow_local_patch or build["version"] == PATCHES[args.target],
            f"wrong admission patch: {build['version']}",
        )
        fcv = client.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        require(fcv == {"version": args.target}, "wrong FCV")
        meta = {
            "target": args.target,
            "build_info": build,
            "fcv": fcv,
            "sdk": pymongo.version,
            "native_facades": ["sync", "async"],
            "native_physical_connection_reuse": True,
            "catalog_sha256": hashlib.sha256(catalog.read_bytes()).hexdigest(),
            "observer_sha256": hashlib.sha256(
                (root / "tests/support_write_parity.py").read_bytes()
            ).hexdigest(),
            "legacy_fixture_sha256": hashlib.sha256(
                (root / "tests/fixtures/positional_update_mongodb8.json").read_bytes()
            ).hexdigest(),
        }
        require(
            meta["legacy_fixture_sha256"] == plan["legacy_fixture_sha256"],
            "historical inputs changed",
        )
        name = f"mongoeco_write_capture_{uuid.uuid4().hex}"
        db = client[name]
        try:
            observations = {
                case.id: observe_write_case(db, case.id, case) for case in WRITE_CASES
            }
            identity = {
                case.id: observe_write_case(db, "identity_" + case.id, case)
                for case in IDENTITY_CASES
            }
            operations = {
                case.id: observe_write_case(db, "op_" + case.id, case)
                for case in WRITE_OPERATION_CASES
            }
            find_modify = {
                case.id: observe_write_case(db, "fm_" + case.id, case)
                for case in FIND_MODIFY_CASES
            }
            shared_errors = {
                case.id: observe_write_case(
                    db, "shared_" + case.id, case, error_contract=True
                )
                for case in SHARED_ERROR_CASES
            }
            require(
                pool.observation()["reused"],
                "native sync physical connection not reused",
            )
            raw = {
                case.id: observe_raw_write(db, "raw_" + case.id, case)
                for case in WRITE_CASES
                if case.id in RAW_CASE_IDS
            }
            identity_raw = {
                case.id: observe_raw_write(db, "id_raw_" + case.id, case)
                for case in IDENTITY_CASES
                if case.id in RAW_IDENTITY_CASE_IDS
            }
            queries = {
                case.id: observe_raw_write(db, case.id, case)
                for case in RAW_QUERY_CASES
            }
            collation_keys = observe_collation_keys(db)
            histories = []
            for identifier in HISTORY_CASES:
                for repetition in range(10):
                    collection = db[f"{identifier}_{repetition}"]
                    histories.append(
                        {
                            "repetition": repetition,
                            **observe_history([collection, collection], identifier),
                        }
                    )
        finally:
            client.drop_database(name)
            require(name not in client.list_database_names(), "cleanup failed")
        require(
            [
                observations,
                identity,
                operations,
                find_modify,
                shared_errors,
                raw,
                identity_raw,
                queries,
                collation_keys,
            ]
            == asyncio.run(capture_async(uri, name + "_async")),
            "native sync/async observations differ",
        )
        require(
            all(
                hashlib.sha256(path.read_bytes()).hexdigest() == digest
                for path, digest in inputs.items()
            ),
            "capture inputs changed during the native observation",
        )
        args.output_dir.mkdir(parents=True, exist_ok=True)
        args.histories_dir.mkdir(parents=True, exist_ok=True)
        suffix = args.target.replace(".", "_")
        require(
            not any(
                name == "mongoeco" or name.startswith("mongoeco.")
                for name in sys.modules
            ),
            "native capture imported the candidate",
        )
        for filename, data in (
            (f"write_parity_mongodb_{suffix}.json", observations),
            (f"write_raw_mongodb_{suffix}.json", raw),
            (f"write_identity_mongodb_{suffix}.json", identity),
            (f"write_operations_mongodb_{suffix}.json", operations),
            (f"write_queries_mongodb_{suffix}.json", queries),
            (f"find_modify_mongodb_{suffix}.json", find_modify),
            (f"shared_errors_mongodb_{suffix}.json", shared_errors),
            (f"collation_keys_mongodb_{suffix}.json", collation_keys),
        ):
            if args.check_dir is not None:
                expected = json.loads((args.check_dir / filename).read_text())
                require(
                    data == expected["observations"],
                    f"native capture differs: {filename}",
                )
            capture = {**meta, "observations": data}
            if data is collation_keys:
                capture["collation_catalog_sha256"] = hashlib.sha256(
                    (
                        root / "tests/differential/collation_diagnostic_cases.py"
                    ).read_bytes()
                ).hexdigest()
                capture["collation_observer_sha256"] = hashlib.sha256(
                    (root / "tests/support_collation_diagnostics.py").read_bytes()
                ).hexdigest()
            if data is shared_errors:
                capture["shared_error_mirror_catalog_sha256"] = hashlib.sha256(
                    (
                        root / "tests/differential/shared_error_mirror_cases.py"
                    ).read_bytes()
                ).hexdigest()
                capture["shared_error_catalog_sha256"] = hashlib.sha256(
                    (root / "tests/differential/shared_error_cases.py").read_bytes()
                ).hexdigest()
            if data is find_modify:
                capture["find_modify_catalog_sha256"] = hashlib.sha256(
                    (root / "tests/differential/find_modify_cases.py").read_bytes()
                ).hexdigest()
            if data is identity:
                capture["identity_catalog_sha256"] = hashlib.sha256(
                    (root / "tests/differential/write_identity_cases.py").read_bytes()
                ).hexdigest()
                capture["raw_observations"] = identity_raw
                if args.check_dir is not None:
                    require(
                        identity_raw == expected["raw_observations"],
                        "native identity raw capture differs",
                    )
            if data is operations:
                capture["operation_catalog_sha256"] = hashlib.sha256(
                    (root / "tests/differential/write_operation_cases.py").read_bytes()
                ).hexdigest()
            if data is queries:
                capture["query_catalog_sha256"] = hashlib.sha256(
                    (root / "tests/differential/write_query_cases.py").read_bytes()
                ).hexdigest()
            (args.output_dir / filename).write_text(
                json.dumps(capture, indent=2) + "\n"
            )
        (args.histories_dir / f"native_{suffix}.json").write_text(
            json.dumps({**meta, "histories": histories}, indent=2) + "\n"
        )
        print(  # noqa: T201 - command line capture receipt
            f"MongoDB {build['version']} FCV {args.target}: "
            f"{len(observations)} writes, {len(raw)} commands, "
            f"{len(histories)} histories, {len(identity)} identity cases; "
            f"{len(identity_raw)} identity commands; "
            f"{len(queries)} query commands; "
            f"{len(operations)} operation payloads; "
            f"{len(find_modify)} findAndModify cases; "
            f"{len(shared_errors)} shared errors; "
            "native sync/async equal; cleanup verified"
        )


if __name__ == "__main__":
    main()
