from __future__ import annotations

from mongoeco import MongoClient
from mongoeco.engines.sqlite import SQLiteEngine
from _demo_support import (
    LOCAL_SEARCH_DOCUMENTS,
    build_content_search_index,
    build_vector_index,
    create_demo_search_indexes,
    demo_database_path,
    load_demo_documents,
)


def main() -> None:
    database_path = demo_database_path("mongoeco-search-example.db")

    with MongoClient(SQLiteEngine(database_path)) as client:
        collection = client.demo.docs

        load_demo_documents(collection, LOCAL_SEARCH_DOCUMENTS)
        create_demo_search_indexes(
            collection,
            [
                build_content_search_index(name="content_search"),
                build_vector_index(name="embedding_search"),
            ],
        )

        exact_phrase_results = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "phrase": {
                            "query": "Ada designed local search patterns",
                            "path": "body",
                        },
                    }
                },
                {"$project": {"_id": 1, "title": 1, "body": 1}},
            ]
        ).to_list()
        print("$search phrase exact results:", exact_phrase_results)

        slop_phrase_results = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "phrase": {
                            "query": "Ada designed local search patterns",
                            "path": "body",
                            "slop": 2,
                        },
                    }
                },
                {"$project": {"_id": 1, "title": 1, "body": 1}},
            ]
        ).to_list()
        print("$search phrase with slop results:", slop_phrase_results)
        slop_explain = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "phrase": {
                            "query": "Ada designed local search patterns",
                            "path": "body",
                            "slop": 2,
                        },
                    }
                }
            ]
        ).explain()
        print(
            "$search phrase with slop explain:",
            slop_explain["engine_plan"]["details"]["slop"],
            slop_explain["engine_plan"]["details"]["backend"],
            slop_explain["engine_plan"]["details"].get("postCandidateValidationRequired"),
        )

        contributor_results = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "text": {
                            "query": "Ada",
                            "path": "contributors.name",
                        },
                    }
                },
                {"$project": {"_id": 1, "title": 1, "contributors": 1}},
            ]
        ).to_list()
        print(
            "$search embeddedDocuments contributor results:",
            contributor_results,
        )

        near_results = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "near": {
                            "path": "score",
                            "origin": 10,
                            "pivot": 2,
                        },
                    }
                },
                {"$project": {"_id": 1, "title": 1, "score": 1}},
            ]
        ).to_list()
        print("$search near results:", near_results)
        near_explain = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "near": {
                            "path": "score",
                            "origin": 10,
                            "pivot": 2,
                        },
                    }
                }
            ]
        ).explain()
        print(
            "$search near explain:",
            near_explain["engine_plan"]["details"]["originKind"],
            near_explain["engine_plan"]["details"]["ranking"]["scoreFormula"],
        )

        search_results = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "compound": {
                            "must": [
                                {
                                    "phrase": {
                                        "query": "Ada algorithms",
                                        "path": "title",
                                    }
                                }
                            ],
                            "filter": [
                                {"in": {"path": "kind", "value": ["note", "reference"]}},
                                {"equals": {"path": "published", "value": True}},
                                {"range": {"path": "score", "gte": 9}},
                            ],
                            "should": [
                                {
                                    "phrase": {
                                        "query": "Ada designed local search patterns",
                                        "path": "body",
                                        "slop": 2,
                                    }
                                },
                                {
                                    "near": {
                                        "path": "score",
                                        "origin": 10,
                                        "pivot": 2,
                                    }
                                },
                                {"exists": {"path": "summary"}},
                                {"regex": {"query": "Algorithm.*", "path": "summary"}},
                            ],
                        },
                    }
                },
                {
                    "$project": {
                        "_id": 1,
                        "title": 1,
                        "published": 1,
                        "score": 1,
                        "summary": 1,
                        "body": 1,
                    }
                },
            ]
        ).to_list()
        print(
            "$search compound phrase + equals + in + range + near + exists + regex results:",
            search_results,
        )
        search_explain = collection.aggregate(
            [
                {
                    "$search": {
                        "index": "content_search",
                        "compound": {
                            "must": [
                                {
                                    "phrase": {
                                        "query": "Ada algorithms",
                                        "path": "title",
                                    }
                                }
                            ],
                            "filter": [
                                {"in": {"path": "kind", "value": ["note", "reference"]}},
                                {"equals": {"path": "published", "value": True}},
                                {"range": {"path": "score", "gte": 9}},
                            ],
                            "should": [
                                {
                                    "phrase": {
                                        "query": "Ada designed local search patterns",
                                        "path": "body",
                                        "slop": 2,
                                    }
                                },
                                {
                                    "near": {
                                        "path": "score",
                                        "origin": 10,
                                        "pivot": 2,
                                    }
                                },
                                {"exists": {"path": "summary"}},
                                {"regex": {"query": "Algorithm.*", "path": "summary"}},
                            ],
                        },
                    }
                }
            ]
        ).explain()
        print("$search compound explain operator:", search_explain["engine_plan"]["details"]["queryOperator"])
        print("$search compound explain should operators:", search_explain["engine_plan"]["details"]["compound"]["shouldOperators"])
        print("$search compound explain ranking:", search_explain["engine_plan"]["details"]["ranking"])

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
                },
                {
                    "$project": {
                        "_id": 1,
                        "title": 1,
                        "score": 1,
                        "vectorScore": {"$meta": "vectorSearchScore"},
                    }
                },
                {"$sort": {"vectorScore": -1, "_id": 1}},
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
        print("$vectorSearch similarity:", details["similarity"])
        print("$vectorSearch mode:", details["mode"])
        print(
            "$vectorSearch numCandidates requested/evaluated:",
            details["candidatesRequested"],
            details["candidatesEvaluated"],
        )
        print("$vectorSearch fallback:", details["exactFallbackReason"])
        print("$vectorSearch prefilter:", details["vectorFilterPrefilter"])
        print("$vectorSearch residual:", details["vectorFilterResidual"])


if __name__ == "__main__":
    main()
