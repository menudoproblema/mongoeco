"""Capture native wire behavior with source identity fixed before helper imports."""

import argparse
import asyncio
import importlib
import json
import os
import platform
import sys
import uuid

from pathlib import Path

import pymongo


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts._capture_identity import SourceIdentity  # noqa: E402


PATCHES = {"7.0": "7.0.43", "8.0": "8.0.32", "9.0": "9.0.2"}
SOURCES = (
    "scripts/capture_wire_read_parity.py",
    "scripts/_capture_identity.py",
    "tests/support_wire_reads.py",
    "tests/support_wire_envelopes.py",
    "tests/support_unacknowledged.py",
    "tests/support_write_parity.py",
)


def capture(uri, name, corpus):
    helper = importlib.import_module(f"tests.support_wire_{corpus}")
    case_ids = (
        [helper.case_id(*case) for case in helper.CASES]
        if corpus == "reads"
        else list(helper.CASES)
    )
    observations = {}
    for index, case in enumerate(helper.CASES):
        database = f"{name}_{index}"
        if corpus == "reads":
            row = asyncio.run(
                helper.observe(
                    uri, database, mirrored=case[0], more=case[1], filter_kind=case[2]
                )
            )
        else:
            row = helper.observe_sync(uri, database, case)
            # Fresh data with the same name: a different collection state must
            # not make sync/async appear equivalent.
            with pymongo.MongoClient(uri) as control:
                control.drop_database(database)
            asynchronous = asyncio.run(helper.observe_async(uri, database, case))
            if row != asynchronous:
                message = "native sync/async observations differ"
                raise RuntimeError(message)
        observations[case_ids[index]] = row
    return case_ids, observations


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", choices=PATCHES, required=True)
    parser.add_argument("--corpus", choices=("reads", "envelopes"), default="reads")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--check", type=Path)
    args = parser.parse_args()
    identity = SourceIdentity(ROOT, SOURCES)
    uri = os.environ["MONGOECO_REAL_MONGODB_URI"]
    name = f"mongoeco_wire_{uuid.uuid4().hex}"
    with pymongo.MongoClient(uri, serverSelectionTimeoutMS=3000) as control:
        build = control.server_info()
        fcv = control.admin.command(
            {"getParameter": 1, "featureCompatibilityVersion": 1}
        )["featureCompatibilityVersion"]
        if build["version"] != PATCHES[args.target] or fcv != {"version": args.target}:
            message = "unexpected native server version or FCV"
            raise RuntimeError(message)
        try:
            case_ids, observations = capture(uri, name, args.corpus)
        finally:
            for db in control.list_database_names():
                if db.startswith(name + "_"):
                    control.drop_database(db)
            if any(db.startswith(name) for db in control.list_database_names()):
                message = "native cleanup failed"
                raise RuntimeError(message)
    if any(n == "mongoeco" or n.startswith("mongoeco.") for n in sys.modules):
        message = "native observer imported the candidate"
        raise RuntimeError(message)
    # Compare the persisted JSON representation, including socket reply pairs.
    observations = json.loads(json.dumps(observations))
    result = {
        "target": args.target,
        "version": build["version"],
        "fcv": fcv,
        "sdk": pymongo.version,
        "python": platform.python_version(),
        "case_ids": case_ids,
        "observations": observations,
        "source_sha256": identity.verify(),
    }
    if (
        args.check
        and observations != json.loads(args.check.read_text())["observations"]
    ):
        message = "native observations differ"
        raise RuntimeError(message)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(  # noqa: T201 - capture receipt
        f"MongoDB {build['version']}: {len(observations)} {args.corpus}; "
        "identity and cleanup verified"
    )


if __name__ == "__main__":
    main()
