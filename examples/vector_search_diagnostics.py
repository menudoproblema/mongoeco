from __future__ import annotations

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from _demo_support import (
    VECTOR_DIAGNOSTIC_DOCUMENTS,
    build_vector_index,
    create_demo_search_indexes,
    demo_database_path,
    load_demo_documents,
)


def print_vector_case(collection, label: str, stage: dict[str, object]) -> None:
    results = collection.aggregate(
        [
            {"$vectorSearch": stage},
            {"$project": {"_id": 1, "title": 1, "kind": 1, "score": 1, "vectorScore": {"$meta": "vectorSearchScore"}}},
            {"$sort": {"vectorScore": -1, "_id": 1}},
        ]
    ).to_list()
    explain = collection.aggregate([{"$vectorSearch": stage}]).explain()
    details = explain["engine_plan"]["details"]
    print(f"\n[{label}]")
    print("results:", results)
    print("top result score:", results[0]["vectorScore"] if results else None)
    print("similarity:", details["similarity"])
    print("mode:", details["mode"])
    print("numCandidates requested/evaluated:", details["candidatesRequested"], details["candidatesEvaluated"])
    print("minScore:", details["minScore"])
    print("fallback:", details["exactFallbackReason"])
    print("prefilter:", details["vectorFilterPrefilter"])
    print("residual:", details["vectorFilterResidual"])
    print("filteredByMinScore:", details["documentsFilteredByMinScore"])


def main() -> None:
    database_path = demo_database_path("mongoeco-vector-diagnostics.db")

    with MongoClient(SQLiteEngine(database_path)) as client:
        collection = client.demo.docs

        load_demo_documents(collection, VECTOR_DIAGNOSTIC_DOCUMENTS)
        create_demo_search_indexes(
            collection,
            [
                build_vector_index(name="embedding_cosine", similarity="cosine"),
                build_vector_index(name="embedding_dot", similarity="dotProduct"),
                build_vector_index(name="embedding_euclidean", similarity="euclidean"),
            ],
        )

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
