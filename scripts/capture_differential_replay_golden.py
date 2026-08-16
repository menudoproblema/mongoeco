from __future__ import annotations

import copy
import os
import sys
from pathlib import Path
import uuid

from bson.json_util import CANONICAL_JSON_OPTIONS, dumps
from pymongo import MongoClient as PyMongoClient


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from tests.differential.cases import REAL_PARITY_CASES  # noqa: E402


def _to_jsonable(value):
    if isinstance(value, tuple):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, list):
        return [_to_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {key: _to_jsonable(item) for key, item in value.items()}
    return value


def main() -> None:
    uri = os.getenv("MONGOECO_REAL_MONGODB_URI")
    if not uri:
        raise SystemExit("MONGOECO_REAL_MONGODB_URI is required")

    target = os.getenv("MONGOECO_REAL_MONGODB_TARGET", "8.0")
    output_path = Path("tests/fixtures/differential_replay_golden.json")

    client = PyMongoClient(uri, serverSelectionTimeoutMS=3000)
    client.admin.command("ping")

    cases: dict[str, object] = {}
    for case in REAL_PARITY_CASES:
        database_name = f"mongoeco_replay_capture_{uuid.uuid4().hex}"
        collection_name = "cases"
        database = client[database_name]
        collection = database[collection_name]
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


if __name__ == "__main__":
    main()
