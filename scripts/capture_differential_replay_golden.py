from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import re
import sys
import uuid

from pathlib import Path

from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
from pymongo import MongoClient as PyMongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.run_mongodb_real_differential import inspect_server_runtime  # noqa: E402

from tests.differential.cases import REAL_PARITY_CASES  # noqa: E402
from tests.differential.index_guarantee_cases import INDEX_GUARANTEE_CASES  # noqa: E402
from tests.differential.review_improvement_cases import REVIEW_IMPROVEMENT_CASES  # noqa: E402
from tests.differential.semantic_guarantee_cases import SEMANTIC_GUARANTEE_CASES  # noqa: E402
from tests.differential.version_delta_cases import VERSION_DELTA_CASES  # noqa: E402


def _to_jsonable(value):
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    return value


def _capture_expectation(value):
    """Compare native errors without incidental IDs, time or type-list order."""
    if isinstance(value, list):
        return [_capture_expectation(item) for item in value]
    if isinstance(value, dict):
        if value.get("ok") is True:
            return value
        result = {key: _capture_expectation(item) for key, item in value.items()}
        if result.get("ok") is False and isinstance(result.get("message"), str):
            result["message"] = re.sub(
                r"mongoeco_replay_capture_[0-9a-f]{32}",
                "mongoeco_replay_capture_UUID",
                result["message"],
            )
            server_uuid = r"[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}"
            result["message"] = re.sub(
                rf"\AIndex build failed: {server_uuid}: "
                rf"(Collection mongoeco_replay_capture_UUID\.cases) "
                rf"\( {server_uuid} \) :: caused by :: ",
                r"Index build failed: INDEX_BUILD_UUID: \1 "
                r"( COLLECTION_UUID ) :: caused by :: ",
                result["message"],
            )
            result["message"] = re.sub(
                r"\A(Index build failed: INDEX_BUILD_UUID: "
                r"Collection mongoeco_replay_capture_UUID\.cases "
                r"\( COLLECTION_UUID \) :: caused by :: "
                r"collection scan stopped\. totalRecords: [0-9]+; durationMillis: )"
                r"[0-9]+(ms; phase: collection scan; )",
                r"\1DURATION\2",
                result["message"],
            )
            if (
                result.get("code") == {"$numberInt": "14"}
                and result.get("code_name") == "TypeMismatch"
            ):
                match = re.fullmatch(
                    r"BSON field '\$densify\.range\.step' is the wrong type "
                    r"'(?P<received>[^']+)', expected types "
                    r"'\[(?P<expected>[a-z]+(?:, [a-z]+)*)(?P<closing>'\]|\]')",
                    result["message"],
                )
                if match is not None:
                    # Native builds enumerate the same BSON types in different
                    # orders. Preserve membership and multiplicity, never sets.
                    result["message"] = {
                        "field": "$densify.range.step",
                        "received_type": match["received"],
                        "expected_types": sorted(match["expected"].split(", ")),
                        "type_list_closing": match["closing"],
                    }
        return result
    return value


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--target",
        choices=("7.0", "8.0", "9.0"),
        default=os.getenv("MONGOECO_REAL_MONGODB_TARGET", "8.0"),
    )
    parser.add_argument(
        "--case-set",
        choices=(
            "parity",
            "version-deltas",
            "review-improvements",
            "semantic-guarantees",
            "index-guarantees",
        ),
        default="parity",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
    )
    parser.add_argument(
        "--check",
        type=Path,
        help="Compare the fresh native capture with an immutable expectation.",
    )
    args = parser.parse_args()
    uri = os.getenv("MONGOECO_REAL_MONGODB_URI")
    if not uri:
        raise SystemExit("MONGOECO_REAL_MONGODB_URI is required")

    target = args.target
    if args.output is None and (target != "8.0" or args.case_set != "parity"):
        parser.error("--output is required outside the historical 8.0 parity capture")
    output_path = args.output or Path("tests/fixtures/differential_replay_golden.json")
    if args.check is not None and args.check.resolve() == output_path.resolve():
        parser.error("--check must remain distinct from the capture output")
    runtime = inspect_server_runtime(uri, target)

    corpora = {
        "parity": (REAL_PARITY_CASES, "cases"),
        "version-deltas": (VERSION_DELTA_CASES, "version_delta_cases"),
        "review-improvements": (REVIEW_IMPROVEMENT_CASES, "review_improvement_cases"),
        "semantic-guarantees": (SEMANTIC_GUARANTEE_CASES, "semantic_guarantee_cases"),
        "index-guarantees": (INDEX_GUARANTEE_CASES, "index_guarantee_cases"),
    }
    selected_cases, corpus_module = corpora[args.case_set]
    corpus_path = f"tests/differential/{corpus_module}.py"

    target_version = tuple(int(part) for part in target.split("."))
    selected_cases = tuple(
        case for case in selected_cases if case.supports(target_version)
    )
    if not selected_cases:
        parser.error("no applicable cases selected")
    cases: dict[str, object] = {}
    with PyMongoClient(uri, serverSelectionTimeoutMS=3000) as client:
        client.admin.command("ping")
        for case in selected_cases:
            database_name = f"mongoeco_replay_capture_{uuid.uuid4().hex}"
            collection = client[database_name]["cases"]
            try:
                for document in copy.deepcopy(case.seed_documents):
                    collection.insert_one(document)
                cases[case.name] = _to_jsonable(case.action(collection))
            finally:
                client.drop_database(database_name)

    payload = {
        "source": "real-mongodb",
        "targetDialect": target,
        "cases": dict(sorted(cases.items())),
        "runtime": runtime,
        "corpus": {
            "path": corpus_path,
            "sha256": hashlib.sha256(
                (PROJECT_ROOT / corpus_path).read_bytes()
            ).hexdigest(),
        },
        "case_manifests": [case.to_manifest() for case in selected_cases],
    }
    output_path.write_text(
        dumps(
            payload,
            indent=2,
            json_options=CANONICAL_JSON_OPTIONS,
        )
        + "\n",
        encoding="utf-8",
    )
    print(f"updated {output_path}")
    if args.check is not None:
        expected = json.loads(args.check.read_text(encoding="utf-8"))
        actual = json.loads(output_path.read_text(encoding="utf-8"))
        for field in ("source", "targetDialect", "corpus", "case_manifests", "cases"):
            observed = _capture_expectation(actual[field])
            if observed != _capture_expectation(expected[field]):
                message = f"native capture differs from {args.check}: {field}"
                raise SystemExit(message)
        sys.stdout.write(f"verified {len(cases)} native cases against {args.check}\n")


if __name__ == "__main__":
    main()
