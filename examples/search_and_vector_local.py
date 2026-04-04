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


def main() -> None:
    database_path = Path("mongoeco-search-example.db")

    with MongoClient(SQLiteEngine(database_path)) as client:
        collection = client.demo.docs

        collection.delete_many({})
        collection.insert_many(
            [
                {
                    "_id": 1,
                    "title": "Ada Lovelace notes",
                    "kind": "note",
                    "score": 10,
                    "summary": "Local search summary",
                    "embedding": [1.0, 0.0, 0.0],
                },
                {
                    "_id": 2,
                    "title": "Compiler reference",
                    "kind": "reference",
                    "score": 7,
                    "embedding": [0.8, 0.2, 0.0],
                },
                {
                    "_id": 3,
                    "title": "Ada algorithms handbook",
                    "kind": "note",
                    "score": 9,
                    "summary": "Algorithm summary",
                    "embedding": [0.9, 0.1, 0.0],
                },
            ]
        )

        collection.create_search_indexes(
            [
                SearchIndexModel(
                    {
                        "mappings": {
                            "dynamic": False,
                            "fields": {
                                "title": {"type": "string"},
                                "summary": {"type": "string"},
                                "kind": {"type": "token"},
                                "score": {"type": "number"},
                            },
                        }
                    },
                    name="content_search",
                ),
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
                    name="embedding_search",
                    type="vectorSearch",
                ),
            ]
        )

        wait_until_ready(collection, "content_search")
        wait_until_ready(collection, "embedding_search")

        search_results = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "compound": {
                            "must": [{"text": {"query": "ada", "path": "title"}}],
                            "filter": [
                                {"exists": {"path": "summary"}},
                                {"in": {"path": "kind", "value": ["note", "reference"]}},
                                {"range": {"path": "score", "gte": 9}},
                            ],
                        },
                    }
                }
            ]
        ).to_list()
        print("$search results:", search_results)

        vector_results = collection.aggregate(
            [
                {
                    "$vectorSearch": {
                        "index": "embedding_search",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0, 0.0],
                        "numCandidates": 10,
                        "limit": 2,
                        "filter": {
                            "$and": [
                                {"kind": {"$in": ["note", "reference"]}},
                                {"score": {"$gte": 9}},
                                {"title": {"$regex": "Ada"}},
                            ]
                        },
                    }
                }
            ]
        ).to_list()
        print("$vectorSearch results:", vector_results)
        vector_explain = collection.aggregate(
            [
                {
                    "$vectorSearch": {
                        "index": "embedding_search",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0, 0.0],
                        "numCandidates": 10,
                        "limit": 2,
                        "filter": {
                            "$and": [
                                {"kind": {"$in": ["note", "reference"]}},
                                {"score": {"$gte": 9}},
                                {"title": {"$regex": "Ada"}},
                            ]
                        },
                    }
                }
            ]
        ).explain()
        details = vector_explain["engine_plan"]["details"]
        print("$vectorSearch prefilter:", details["vectorFilterPrefilter"])
        print("$vectorSearch residual:", details["vectorFilterResidual"])


if __name__ == "__main__":
    main()
