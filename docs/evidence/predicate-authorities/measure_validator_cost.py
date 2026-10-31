"""Measure validator preparation and its existing consumers with the benchmark meter.

Run sequentially with the same interpreter/dependencies and a selected subject
on PYTHONPATH. Timings are trends; native corpus and preparation-count tests own
correctness. This evidence probe adds no benchmark framework or runtime policy.
"""

import argparse
import json

from importlib.metadata import version
from pathlib import Path

from benchmarks.runners.metrics import measure, summarize

import mongoeco

from mongoeco import MongoClient
from mongoeco.core.schema_validation import compile_collection_validator
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


OPTIONS = {
    "validator": {
        "$jsonSchema": {
            "required": ["a"],
            "properties": {"a": {"bsonType": "int", "minimum": 0, "enum": [1, 2]}},
        }
    }
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--rows", type=int, default=250)
    parser.add_argument("--repetitions", type=int, default=3)
    args = parser.parse_args()
    documents = [{"_id": i, "a": 1} for i in range(args.rows)]
    prepared = compile_collection_validator(OPTIONS)

    def prepare():
        for _ in documents:
            compile_collection_validator(OPTIONS)

    def validate():
        if not all(prepared.validate_document(doc).valid for doc in documents):
            message = "benchmark documents must satisfy the validator"
            raise RuntimeError(message)

    def batch(engine):
        def execute():
            with MongoClient(engine()) as client:
                collection = client.benchmark.create_collection("validated", **OPTIONS)
                result = collection.insert_many(documents)
                if len(result.inserted_ids) != args.rows:
                    message = "benchmark batch lost a document"
                    raise RuntimeError(message)

        return execute

    tasks = {
        "prepare": prepare,
        "validate_prepared": validate,
        "insert_many_memory": batch(MemoryEngine),
        "insert_many_sqlite": batch(SQLiteEngine),
    }
    results = {}
    for name, task in tasks.items():
        metrics = []
        for _ in range(args.repetitions):
            with measure(metrics):
                task()
        results[name] = summarize(metrics).to_dict()
    args.output.write_text(
        json.dumps(
            {
                "subject": mongoeco.__file__,
                "rows": args.rows,
                "dependencies": {
                    name: version(name) for name in ("pymongo", "msgspec", "rpds-py")
                },
                "results": results,
            },
            indent=2,
        )
        + "\n"
    )


if __name__ == "__main__":
    main()
