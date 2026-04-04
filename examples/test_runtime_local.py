from __future__ import annotations

from pathlib import Path

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from _demo_support import (
    TEST_RUNTIME_DOCUMENTS,
    build_content_search_index,
    create_demo_search_indexes,
    load_demo_documents,
)


def run_contract(engine, *, label: str) -> None:
    with MongoClient(engine) as client:
        collection = client.demo.runtime
        load_demo_documents(collection, TEST_RUNTIME_DOCUMENTS)
        create_demo_search_indexes(collection, [build_content_search_index(name="by_text")])

        note_ids = [document["_id"] for document in collection.find({"kind": "note"}).sort([("_id", 1)])]
        phrase_ids = [
            document["_id"]
            for document in collection.aggregate(
                [
                    {
                        "$search": {
                            "index": "by_text",
                            "phrase": {
                                "query": "Ada wrote the first algorithm",
                                "path": "body",
                                "slop": 1,
                            },
                        }
                    },
                    {"$project": {"_id": 1}},
                ]
            ).to_list()
        ]
        explain = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "by_text",
                        "phrase": {
                            "query": "Ada wrote the first algorithm",
                            "path": "body",
                            "slop": 1,
                        },
                    }
                }
            ]
        ).explain()
        details = explain["engine_plan"]["details"]
        print(f"\n[{label}]")
        print("note ids:", note_ids)
        print("phrase ids:", phrase_ids)
        print(
            "phrase slop/backend/post-validation:",
            details["slop"],
            details["backend"],
            details.get("postCandidateValidationRequired"),
        )


def main() -> None:
    run_contract(MemoryEngine(), label="MemoryEngine")
    run_contract(SQLiteEngine(Path("mongoeco-test-runtime.db")), label="SQLiteEngine")


if __name__ == "__main__":
    main()
