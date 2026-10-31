"""Run with PYTHONPATH pointing to 23be20d src for the pre-traversal witness."""

import argparse
import hashlib
import json

from pathlib import Path

from mongoeco import MongoClient
from mongoeco.core import _filtering_support
from mongoeco.engines.sqlite import SQLiteEngine


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("the witness must not overwrite an existing fixture")
    with MongoClient(SQLiteEngine(str(args.output)), mongodb_dialect="9.0") as client:
        collection = client.db.records
        collection.insert_one({"_id": 1, "a": [{}, {"x": 7}], "b": 1})
        client.db.command(
            {
                "createIndexes": "records",
                "indexes": [
                    {
                        "key": {"b": 1},
                        "name": "partial_b",
                        "unique": True,
                        "partialFilterExpression": {"a.x": {"$gte": None}},
                    }
                ],
            }
        )
        assert list(collection.find({"b": 1, "a.x": {"$gte": None}})) == []
    source = Path(_filtering_support.__file__)
    args.output.with_suffix(".json").write_text(
        json.dumps(
            {
                "base": "23be20d",
                "fixture_sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
                "source_path": str(source),
                "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "scope": "Legacy partial membership omits a document with a missing branch in an array path",
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
