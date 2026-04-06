from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import time
import uuid

from mongoeco import SearchIndexModel
from mongoeco.types import ObjectId


EMBEDDED_APP_DOCUMENTS = [
    {"_id": 1, "kind": "task", "title": "Ship release notes", "done": False},
    {"_id": 2, "kind": "task", "title": "Benchmark search runtime", "done": True},
    {"_id": 3, "kind": "note", "title": "Keep docs aligned", "done": False},
]

LOCAL_SEARCH_DOCUMENTS = [
    {
        "_id": 1,
        "title": "Ada Lovelace notes",
        "kind": "note",
        "published": True,
        "contributors": [
            {"name": "Ada Lovelace", "role": "author", "verified": True},
            {"name": "Charles Babbage", "role": "editor", "verified": False},
        ],
        "ownerId": ObjectId("656565656565656565656561"),
        "externalUuid": uuid.UUID("11111111-1111-1111-1111-111111111111"),
        "score": 10,
        "summary": "Local search summary",
        "body": "Ada designed local search patterns for algorithm notes.",
        "embedding": [1.0, 0.0, 0.0],
    },
    {
        "_id": 2,
        "title": "Compiler reference",
        "kind": "reference",
        "published": False,
        "contributors": [
            {"name": "Grace Hopper", "role": "author", "verified": True},
        ],
        "ownerId": ObjectId("656565656565656565656562"),
        "externalUuid": uuid.UUID("22222222-2222-2222-2222-222222222222"),
        "score": 7,
        "body": "Compiler references rarely repeat the same phrase exactly.",
        "embedding": [0.8, 0.2, 0.0],
    },
    {
        "_id": 3,
        "title": "Ada algorithms handbook",
        "kind": "note",
        "published": True,
        "contributors": [
            {"name": "Ada Byron", "role": "author", "verified": True},
            {"name": "Compiler Team", "role": "reviewer", "verified": False},
        ],
        "ownerId": ObjectId("656565656565656565656563"),
        "externalUuid": uuid.UUID("33333333-3333-3333-3333-333333333333"),
        "score": 9,
        "summary": "Algorithm summary",
        "body": "Ada designed the practical local search patterns for algorithm ranking demos.",
        "embedding": [0.9, 0.1, 0.0],
    },
]

VECTOR_DIAGNOSTIC_DOCUMENTS = [
    {"_id": 1, "title": "Ada search notes", "kind": "note", "score": 10, "embedding": [1.0, 0.0, 0.0]},
    {"_id": 2, "title": "Compiler reference", "kind": "reference", "score": 7, "embedding": [0.8, 0.2, 0.0]},
    {"_id": 3, "title": "Ada algorithms handbook", "kind": "note", "score": 9, "embedding": [0.9, 0.1, 0.0]},
    {"_id": 4, "title": "Vector ranking field guide", "kind": "reference", "score": 4, "embedding": [0.0, 0.0, 1.0]},
    {"_id": 5, "title": "Low-score reference", "kind": "reference", "score": 3, "embedding": [0.7, 0.1, 0.2]},
    {"_id": 6, "title": "Strong note candidate", "kind": "note", "score": 11, "embedding": [0.95, 0.05, 0.0]},
]

TEST_RUNTIME_DOCUMENTS = [
    {"_id": 1, "kind": "note", "title": "Exact phrase", "body": "Ada wrote the first algorithm"},
    {"_id": 2, "kind": "note", "title": "Flexible phrase", "body": "Ada wrote the practical first algorithm"},
    {"_id": 3, "kind": "reference", "title": "Compiler guide", "body": "Grace documented the compiler pipeline"},
]


def wait_until_ready(collection, index_name: str, timeout_seconds: float = 2.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        indexes = collection.list_search_indexes(index_name).to_list()
        if indexes and indexes[0]["status"] == "READY":
            return
        time.sleep(0.02)
    raise RuntimeError(f"search index {index_name!r} did not become ready in time")


def reset_collection(collection) -> None:
    collection.delete_many({})


def load_demo_documents(collection, documents: list[dict[str, object]]) -> None:
    reset_collection(collection)
    collection.insert_many(deepcopy(documents))


def create_demo_search_indexes(
    collection,
    indexes: list[SearchIndexModel],
    *,
    timeout_seconds: float = 2.0,
) -> None:
    collection.create_search_indexes(indexes)
    for index in indexes:
        wait_until_ready(collection, index.name, timeout_seconds=timeout_seconds)


def build_content_search_index(name: str = "content_search") -> SearchIndexModel:
    return SearchIndexModel(
        {
            "mappings": {
                "dynamic": False,
                "fields": {
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "body": {"type": "string"},
                    "kind": {"type": "token"},
                    "published": {"type": "boolean"},
                    "contributors": {
                        "type": "embeddedDocuments",
                        "fields": {
                            "name": {"type": "string"},
                            "role": {"type": "token"},
                            "verified": {"type": "boolean"},
                        },
                    },
                    "ownerId": {"type": "objectId"},
                    "externalUuid": {"type": "uuid"},
                    "score": {"type": "number"},
                },
            }
        },
        name=name,
    )


def build_vector_index(
    *,
    name: str,
    path: str = "embedding",
    similarity: str = "cosine",
    dimensions: int = 3,
) -> SearchIndexModel:
    return SearchIndexModel(
        {
            "fields": [
                {
                    "type": "vector",
                    "path": path,
                    "numDimensions": dimensions,
                    "similarity": similarity,
                }
            ]
        },
        name=name,
        type="vectorSearch",
    )


def demo_database_path(filename: str) -> Path:
    return Path(filename)
