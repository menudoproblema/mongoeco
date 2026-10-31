"""Run with PYTHONPATH pointing to c835181 src to create the legacy witness."""

import argparse
import hashlib
import json

from pathlib import Path

from mongoeco import MongoClient
from mongoeco.core import _filtering_matching
from mongoeco.engines.sqlite import SQLiteEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("the witness must not overwrite an existing fixture")
    with MongoClient(SQLiteEngine(str(args.output))) as client:
        collection = client.db.records
        collection.insert_one({"_id": 1, "a": "z", "b": 1})
        client.db.command(
            {
                "createIndexes": "records",
                "indexes": [
                    {
                        "key": {"b": 1},
                        "name": "partial_b",
                        "unique": True,
                        "partialFilterExpression": {"a": {"$gt": 5}},
                    }
                ],
            }
        )
    source = Path(_filtering_matching.__file__)
    args.output.with_suffix(".json").write_text(
        json.dumps(
            {
                "base": "c835181",
                "fixture_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                "source_path": str(source),
                "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "scope": "Legacy partial membership under an unbracketed numeric query",
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
