from __future__ import annotations

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from _demo_support import (
    VECTOR_DIAGNOSTIC_DOCUMENTS,
    build_vector_index,
    create_demo_search_indexes,
    demo_database_path,
    load_demo_documents,
)


def _print_vector_case(
    collection,
    label: str,
    stage: dict[str, object],
    *,
    post_match: dict[str, object] | None = None,
) -> None:
    pipeline: list[dict[str, object]] = [{"$vectorSearch": stage}]
    explain_pipeline: list[dict[str, object]] = [{"$vectorSearch": stage}]
    if post_match is not None:
        pipeline.append({"$match": post_match})
        explain_pipeline.append({"$match": post_match})
    pipeline.extend(
        [
            {
                "$project": {
                    "_id": 1,
                    "title": 1,
                    "kind": 1,
                    "score": 1,
                    "vectorScore": {"$meta": "vectorSearchScore"},
                }
            },
            {"$sort": {"vectorScore": -1, "_id": 1}},
        ]
    )
    results = collection.aggregate(pipeline).to_list()
    explain = collection.aggregate(explain_pipeline).explain()
    details = explain["engine_plan"]["details"]

    print(f"\n[{label}]")
    print("results:", results)
    print("top result score:", results[0]["vectorScore"] if results else None)
    print("similarity:", details["similarity"])
    print("score breakdown:", details["scoreBreakdown"])
    print("candidate plan:", details["candidatePlan"])
    print("hybrid retrieval:", details["hybridRetrieval"])
    print("pruning summary:", details["pruningSummary"])
    print("query filter prefilter:", details["vectorFilterPrefilter"])
    print("downstream filter prefilter:", details["downstreamFilterCandidatePrefilter"])
    print("query/downstream filter mode:", details["filterMode"], details["downstreamFilterMode"])
    print("vector backend:", details["vectorBackend"])


def _run_engine(engine_label: str, engine) -> None:
    print(f"\n=== {engine_label} ===")
    with MongoClient(engine) as client:
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

        _print_vector_case(
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
        _print_vector_case(
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
        _print_vector_case(
            collection,
            "cosine / hybrid boolean filter",
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
        _print_vector_case(
            collection,
            "cosine / downstream structured filter ($match)",
            {
                "index": "embedding_cosine",
                "path": "embedding",
                "queryVector": [1.0, 0.0, 0.0],
                "numCandidates": 12,
                "limit": 2,
            },
            post_match={"score": {"$gte": 15}},
        )
        _print_vector_case(
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
        _print_vector_case(
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


def main() -> None:
    _run_engine("MemoryEngine exact baseline", MemoryEngine())
    _run_engine(
        "SQLiteEngine ANN + exact baseline",
        SQLiteEngine(demo_database_path("mongoeco-vector-diagnostics.db")),
    )


if __name__ == "__main__":
    main()
