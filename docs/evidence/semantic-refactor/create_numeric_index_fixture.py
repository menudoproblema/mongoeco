"""Create the legacy witness with PYTHONPATH pointing at the recorded base src."""

import argparse
import hashlib
import json

from pathlib import Path

import mongoeco

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine

from tests.differential.semantic_numeric_predicate_cases import SEED


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("the witness must not overwrite an existing fixture")
    with MongoClient(SQLiteEngine(str(args.output))) as client:
        collection = client.semantic_numeric.records
        collection.insert_many(SEED)
        for field in ("v", "a", "d", "b", "plain", "plainArray"):
            collection.create_index(field)
    corpus = Path("tests/differential/semantic_numeric_predicate_cases.py")
    metadata = {
        "base": "b38ef96764ce4a976883005ade83f82f7b3ef547",
        "version": mongoeco.__version__,
        "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        "corpus": {
            "path": str(corpus),
            "sha256": hashlib.sha256(corpus.read_bytes()).hexdigest(),
        },
        "generator": str(Path(__file__).relative_to(Path.cwd())),
    }
    args.output.with_suffix(".json").write_text(json.dumps(metadata, indent=2) + "\n")


if __name__ == "__main__":
    main()
