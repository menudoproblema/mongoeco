from __future__ import annotations

import time
from pathlib import Path

from mongoeco import MongoClient, SearchIndexModel
from mongoeco.engines.sqlite import SQLiteEngine


def wait_until_ready(collection, index_name: str, timeout_seconds: float = 2.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        indexes = collection.list_search_indexes(index_name).to_list()
        if indexes and indexes[0]["status"] == "READY":
            return
        time.sleep(0.02)
    raise RuntimeError(f"search index {index_name!r} did not become ready in time")


def print_vector_case(collection, label: str, stage: dict[str, object]) -> None:
    results = collection.aggregate([{"$vectorSearch": stage}, {"$project": {"_id": 1, "title": 1, "kind": 1, "score": 1}}]).to_list()
    explain = collection.aggregate([{"$vectorSearch": stage}]).explain()
    details = explain["engine_plan"]["details"]
    print(f"\n[{label}]")
    print("results:", results)
    print("similarity:", details["similarity"])
    print("mode:", details["mode"])
    print("numCandidates requested/evaluated:", details["candidatesRequested"], details["candidatesEvaluated"])
    print("minScore:", details["minScore"])
    print("fallback:", details["exactFallbackReason"])
    print("prefilter:", details["vectorFilterPrefilter"])
    print("residual:", details["vectorFilterResidual"])
    print("filteredByMinScore:", details["documentsFilteredByMinScore"])


def main() -> None:
    database_path = Path("mongoeco-vector-diagnostics.db")

    with MongoClient(SQLiteEngine(database_path)) as client:
        collection = client.demo.docs

        collection.delete_many({})
        collection.insert_many(
            [
                {"_id": 1, "title": "Ada search notes", "kind": "note", "score": 10, "embedding": [1.0, 0.0, 0.0]},
                {"_id": 2, "title": "Compiler reference", "kind": "reference", "score": 7, "embedding": [0.8, 0.2, 0.0]},
                {"_id": 3, "title": "Ada algorithms handbook", "kind": "note", "score": 9, "embedding": [0.9, 0.1, 0.0]},
                {"_id": 4, "title": "Vector ranking field guide", "kind": "reference", "score": 4, "embedding": [0.0, 0.0, 1.0]},
                {"_id": 5, "title": "Low-score reference", "kind": "reference", "score": 3, "embedding": [0.7, 0.1, 0.2]},
                {"_id": 6, "title": "Strong note candidate", "kind": "note", "score": 11, "embedding": [0.95, 0.05, 0.0]},
            ]
        )

        collection.create_search_indexes(
            [
                SearchIndexModel(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 3,
                                "similarity": "cosine",
                            }
                        ]
                    },
                    name="embedding_cosine",
                    type="vectorSearch",
                ),
                SearchIndexModel(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 3,
                                "similarity": "dotProduct",
                            }
                        ]
                    },
                    name="embedding_dot",
                    type="vectorSearch",
                ),
                SearchIndexModel(
                    {
                        "fields": [
                            {
                                "type": "vector",
                                "path": "embedding",
                                "numDimensions": 3,
                                "similarity": "euclidean",
                            }
                        ]
                    },
                    name="embedding_euclidean",
                    type="vectorSearch",
                ),
            ]
        )

        wait_until_ready(collection, "embedding_cosine")
        wait_until_ready(collection, "embedding_dot")
        wait_until_ready(collection, "embedding_euclidean")

        print_vector_case(
            collection,
            "cosine / low numCandidates",
            {
                "index": "embedding_cosine",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 3,
                "limit": 2,
            },
        )
        print_vector_case(
            collection,
            "cosine / higher numCandidates",
            {
                "index": "embedding_cosine",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 12,
                "limit": 2,
            },
        )
        print_vector_case(
            collection,
            "dotProduct / same query",
            {
                "index": "embedding_dot",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 12,
                "limit": 2,
            },
        )
        print_vector_case(
            collection,
            "euclidean / same query",
            {
                "index": "embedding_euclidean",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 12,
                "limit": 2,
            },
        )
        print_vector_case(
            collection,
            "cosine / boolean filter with residual explain",
            {
                "index": "embedding_cosine",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 12,
                "limit": 2,
                "filter": {
                    "$and": [
                        {"kind": {"$in": ["note", "reference"]}},
                        {"score": {"$gte": 9}},
                        {"title": {"$regex": "Ada"}},
                    ]
                },
            },
        )
        print_vector_case(
            collection,
            "cosine / restrictive exact prefilter",
            {
                "index": "embedding_cosine",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 2,
                "limit": 2,
                "filter": {
                    "$and": [
                        {"kind": "reference"},
                        {"score": {"$gte": 10}},
                    ]
                },
            },
        )
        print_vector_case(
            collection,
            "cosine / note filter + minScore",
            {
                "index": "embedding_cosine",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 12,
                "limit": 2,
                "filter": {"kind": "note"},
                "minScore": 0.999,
            },
        )


if __name__ == "__main__":
    main()
