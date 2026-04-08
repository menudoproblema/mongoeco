import asyncio
import datetime
import decimal
import re
import threading
import unittest
import uuid
from bson import decode_all

from mongoeco import (
    AsyncMongoClient,
    CodecOptions,
    ClientSession,
    DeleteMany,
    DeleteOne,
    IndexModel,
    InsertOne,
    MongoClient,
    ObjectId,
    ReadConcern,
    ReadPreference,
    ReadPreferenceMode,
    ReplaceOne,
    ReturnDocument,
    SearchIndexModel,
    TransactionOptions,
    UpdateMany,
    UpdateOne,
    WriteConcern,
)
from mongoeco.api._async.aggregation_cursor import AsyncAggregationCursor
from mongoeco.api._async.cursor import AsyncCursor
from mongoeco.compat import MongoDialect80, PyMongoProfile413
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import (
    BulkWriteError,
    CollectionInvalid,
    DocumentValidationFailure,
    DuplicateKeyError,
    InvalidOperation,
    OperationFailure,
)
from tests.integration.api.search_vector_scenarios import (
    assert_search_advanced_option_explanation,
    assert_compound_candidateable_should_explanation,
    assert_compound_candidateable_should_limited_explanation,
    assert_compound_candidateable_should_matched_limited_explanation,
    assert_compound_candidateable_should_title_prefilter_explanation,
    assert_compound_should_near_explanation,
    assert_boolean_vector_residual_explanation,
    assert_in_equals_and_range_explanations,
    assert_filtered_vector_explanation,
    assert_phrase_slop_explanation,
    assert_phrase_in_range_compound_explanation,
    assert_regex_explanation,
    assert_ranged_vector_explanation,
    assert_vector_min_score_explanation,
    assert_vector_query_and_downstream_filter_explanation,
    assert_vector_downstream_filter_explanation,
    assert_vector_score_projection_results,
    assert_vector_similarity_explanation,
)
from tests.integration.api.admin_command_cases import (
    assert_build_info_command_shares_source_of_truth_with_server_info,
    assert_client_server_info_reflects_target_dialect,
    assert_database_command_count_supports_skip_limit_hint_and_comment,
    assert_database_command_distinct_supports_hint_comment_and_max_time,
    assert_database_command_supports_configure_fail_point,
    assert_database_command_index_commands_support_comment_and_max_time,
    assert_database_command_rejects_invalid_command_shapes,
    assert_database_command_rejects_unsupported_commands,
    assert_database_command_supports_db_hash_and_profile_status,
    assert_database_command_supports_coll_stats_and_db_stats,
    assert_database_command_supports_collection_index_count_and_distinct_commands,
    assert_database_command_supports_explain_for_find_and_aggregate,
    assert_database_command_supports_explain_for_count_distinct_and_find_and_modify,
    assert_database_command_supports_explain_for_update_and_delete,
    assert_database_command_supports_find_and_aggregate,
    assert_database_command_supports_find_and_modify,
    assert_database_command_supports_insert_update_and_delete,
    assert_database_command_supports_ping_list_collections_and_drop_database,
    assert_database_command_supports_rename_collection_within_current_database,
    assert_database_command_write_commands_surface_write_errors,
    assert_hello_and_is_master_commands_return_handshake_metadata,
    assert_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata,
    assert_list_collections_command_supports_name_only,
    assert_list_commands_and_connection_status_commands_return_local_admin_metadata,
    assert_server_status_opcounters_track_local_runtime_activity,
    assert_server_status_command_returns_local_runtime_metadata,
    assert_validate_collection_returns_metadata_and_rejects_missing_namespace,
)
from tests.integration.api.json_schema_cases import (
    assert_async_find_supports_richer_top_level_json_schema_filter,
)
from tests.support import ENGINE_FACTORIES, open_client


class AsyncApiIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_collection_search_index_lifecycle(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.search.get_collection("docs")

                    created = await collection.create_search_index({"mappings": {"dynamic": False}})
                    self.assertEqual(created, "default")

                    created_many = await collection.create_search_indexes(
                        [
                            SearchIndexModel({"mappings": {"dynamic": True}}, name="by_text"),
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
                                name="by_keyword",
                                type="vectorSearch",
                            ),
                        ]
                    )
                    self.assertEqual(created_many, ["by_text", "by_keyword"])

                    listed = await collection.list_search_indexes().to_list()
                    self.assertEqual(
                        [document["name"] for document in listed],
                        ["by_keyword", "by_text", "default"],
                    )
                    by_keyword = next(
                        document for document in listed if document["name"] == "by_keyword"
                    )
                    self.assertTrue(by_keyword["queryable"])
                    self.assertEqual(by_keyword["status"], "READY")
                    self.assertEqual(by_keyword["queryMode"], "vector")
                    self.assertTrue(by_keyword["experimental"])
                    self.assertEqual(by_keyword["capabilities"], ["vectorSearch"])

                    only_default = await collection.list_search_indexes("default").to_list()
                    self.assertEqual(len(only_default), 1)
                    self.assertEqual(only_default[0]["name"], "default")
                    self.assertEqual(only_default[0]["queryMode"], "text")
                    self.assertEqual(
                        only_default[0]["capabilities"],
                        ["text", "phrase", "autocomplete", "wildcard", "regex", "exists", "in", "equals", "range", "near", "compound"],
                    )

                    await collection.update_search_index("default", {"mappings": {"dynamic": True}})
                    updated = await collection.list_search_indexes("default").first()
                    self.assertEqual(updated["latestDefinition"], {"mappings": {"dynamic": True}})

                    await collection.drop_search_index("by_keyword")
                    remaining = await collection.list_search_indexes().to_list()
                    self.assertEqual([document["name"] for document in remaining], ["by_text", "default"])

    async def test_aggregate_search_executes_text_search_and_rejects_invalid_runtime(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.search_runtime.get_collection("docs")
                    await collection.insert_many(
                        [
                            {"_id": 1, "title": "Ada", "body": "Analytical engine notes", "kind": "reference", "score": 9, "publishedAt": datetime.datetime(2024, 1, 1, 12, 0, 0), "embedding": [1.0, 0.0, 0.0]},
                            {"_id": 2, "title": "Grace", "body": "Compiler pioneer", "kind": "note", "score": 15, "publishedAt": datetime.datetime(2024, 1, 1, 12, 5, 0), "embedding": [0.1, 0.9, 0.0]},
                            {"_id": 3, "title": "Notes", "body": "Ada wrote the first algorithm", "kind": "note", "score": 11, "publishedAt": datetime.datetime(2024, 1, 1, 12, 1, 0), "embedding": [0.9, 0.1, 0.0], "summary": "Algorithm summary"},
                        ]
                    )
                    await collection.create_search_indexes(
                        [
                            SearchIndexModel(
                                {
                                    "mappings": {
                                        "dynamic": False,
                                        "fields": {
                                            "title": {"type": "string"},
                                            "body": {"type": "string"},
                                            "summary": {"type": "string"},
                                            "score": {"type": "number"},
                                            "publishedAt": {"type": "date"},
                                        },
                                    }
                                },
                                name="by_text",
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
                                name="by_vector",
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
                                name="by_vector_dot",
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
                                name="by_vector_euclidean",
                                type="vectorSearch",
                            ),
                        ]
                    )

                    hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in hits], [1, 3])

                    await collection.update_one({"_id": 2}, {"$set": {"body": "Ada and Grace built compilers"}})
                    await collection.delete_one({"_id": 1})
                    updated_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in updated_hits], [2, 3])

                    explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "text": {"query": "ada", "path": ["title", "body"]}}}]
                    ).explain()
                    self.assertEqual(explanation["engine_plan"]["strategy"], "search")
                    self.assertEqual(explanation["pushdown"]["mode"], "search")
                    self.assertEqual(explanation["pushdown"]["pushedDownStages"], 1)
                    self.assertEqual(explanation["pushdown"]["totalStages"], 1)
                    self.assertTrue(explanation["engine_plan"]["details"]["backendAvailable"])
                    self.assertIn("readyAtEpoch", explanation["engine_plan"]["details"])
                    if engine_name == "sqlite":
                        self.assertEqual(explanation["engine_plan"]["details"]["backend"], "fts5")
                        self.assertEqual(explanation["engine_plan"]["details"]["fts5_match"], '"ada"')
                        self.assertTrue(explanation["engine_plan"]["details"]["backendMaterialized"])
                        self.assertIsNotNone(explanation["engine_plan"]["details"]["physicalName"])
                        self.assertIsNotNone(explanation["engine_plan"]["details"]["fts5Available"])
                    else:
                        self.assertEqual(explanation["engine_plan"]["details"]["backend"], "python")
                        self.assertFalse(explanation["engine_plan"]["details"]["backendMaterialized"])
                        self.assertIsNone(explanation["engine_plan"]["details"]["physicalName"])
                        self.assertIsNone(explanation["engine_plan"]["details"]["fts5Available"])

                    autocomplete_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "autocomplete": {"query": "ada", "path": ["title", "body"]}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in autocomplete_hits], [2, 3])
                    autocomplete_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "autocomplete": {"query": "ada", "path": ["title", "body"]}}}]
                    ).explain()
                    self.assertEqual(autocomplete_explanation["engine_plan"]["details"]["queryOperator"], "autocomplete")
                    if engine_name == "sqlite":
                        self.assertEqual(autocomplete_explanation["engine_plan"]["details"]["backend"], "fts5")
                        self.assertEqual(autocomplete_explanation["engine_plan"]["details"]["fts5_match"], '"ada"*')
                    else:
                        self.assertEqual(autocomplete_explanation["engine_plan"]["details"]["backend"], "python")

                    fuzzy_autocomplete_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "autocomplete": {
                                        "query": "algoritm",
                                        "path": "body",
                                        "fuzzy": {"maxEdits": 2, "prefixLength": 1},
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in fuzzy_autocomplete_hits], [3])
                    fuzzy_autocomplete_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "autocomplete": {
                                        "query": "algoritm",
                                        "path": "body",
                                        "fuzzy": {"maxEdits": 2, "prefixLength": 1},
                                    },
                                }
                            }
                        ]
                    ).explain()
                    self.assertEqual(
                        fuzzy_autocomplete_explanation["engine_plan"]["details"]["querySemantics"]["fuzzy"],
                        {"maxEdits": 2, "prefixLength": 1},
                    )
                    if engine_name == "sqlite":
                        self.assertEqual(
                            fuzzy_autocomplete_explanation["engine_plan"]["details"]["backend"],
                            "fts5-path",
                        )
                    else:
                        self.assertEqual(
                            fuzzy_autocomplete_explanation["engine_plan"]["details"]["backend"],
                            "python",
                        )

                    wildcard_hits = await collection.aggregate(
                        [{"$search": {"index": "by_text", "wildcard": {"query": "*algorithm*", "path": "body"}}}]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in wildcard_hits], [3])
                    wildcard_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "wildcard": {"query": "*algorithm*", "path": "body"}}}]
                    ).explain()
                    self.assertEqual(wildcard_explanation["engine_plan"]["details"]["queryOperator"], "wildcard")
                    if engine_name == "sqlite":
                        self.assertEqual(wildcard_explanation["engine_plan"]["details"]["backend"], "fts5-glob")
                        self.assertEqual(wildcard_explanation["engine_plan"]["details"]["candidateCount"], 1)
                    else:
                        self.assertEqual(wildcard_explanation["engine_plan"]["details"]["backend"], "python")

                    exists_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "exists": {"path": "title"}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in exists_hits], [2, 3])
                    exists_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "exists": {"path": "title"}}}]
                    ).explain()
                    self.assertEqual(exists_explanation["engine_plan"]["details"]["queryOperator"], "exists")
                    if engine_name == "sqlite":
                        self.assertEqual(exists_explanation["engine_plan"]["details"]["backend"], "fts5-path")
                        self.assertEqual(exists_explanation["engine_plan"]["details"]["candidateCount"], 2)
                    else:
                        self.assertEqual(exists_explanation["engine_plan"]["details"]["backend"], "python")

                    in_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "in": {"path": "kind", "value": ["note", "reference"]}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in in_hits], [2, 3])
                    equals_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "equals": {"path": "kind", "value": "note"}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in equals_hits], [2, 3])
                    range_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "range": {"path": "score", "gte": 9, "lte": 11}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in range_hits], [3])
                    in_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "in": {"path": "kind", "value": ["note", "reference"]}}}]
                    ).explain()
                    equals_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "equals": {"path": "kind", "value": "note"}}}]
                    ).explain()
                    range_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "range": {"path": "score", "gte": 9, "lte": 11}}}]
                    ).explain()
                    assert_in_equals_and_range_explanations(
                        self,
                        in_explanation,
                        equals_explanation,
                        range_explanation,
                        engine_name=engine_name,
                    )
                    regex_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "regex": {"query": "Ada.*algorithm", "path": "body"}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in regex_hits], [3])
                    assert_regex_explanation(
                        self,
                        await collection.aggregate(
                            [
                                {
                                    "$search": {
                                        "index": "by_text",
                                        "regex": {
                                            "query": "Ada.*algorithm",
                                            "path": "body",
                                            "flags": "i",
                                        },
                                    }
                                }
                            ]
                        ).explain(),
                        engine_name=engine_name,
                    )
                    sequential_autocomplete_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "autocomplete": {
                                        "query": "ada alg",
                                        "path": "body",
                                        "tokenOrder": "sequential",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in sequential_autocomplete_hits], [3])
                    advanced_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {"query": "ada", "path": ["title", "body"]},
                                    "count": {"type": "total"},
                                    "highlight": {
                                        "path": ["title", "body"],
                                        "maxChars": 40,
                                    },
                                    "facet": {"path": "kind", "numBuckets": 5},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in advanced_hits], [2, 3])
                    self.assertIn("searchHighlights", advanced_hits[0])
                    self.assertTrue(advanced_hits[0]["searchHighlights"])
                    advanced_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {"query": "ada", "path": ["title", "body"]},
                                    "count": {"type": "total"},
                                    "highlight": {
                                        "path": ["title", "body"],
                                        "maxChars": 40,
                                    },
                                    "facet": {"path": "kind", "numBuckets": 5},
                                }
                            }
                        ]
                    ).explain()
                    assert_search_advanced_option_explanation(
                        self,
                        advanced_explanation,
                    )
                    search_meta = await collection.aggregate(
                        [
                            {
                                "$searchMeta": {
                                    "index": "by_text",
                                    "text": {"query": "ada", "path": ["title", "body"]},
                                    "count": {"type": "total"},
                                    "facet": {"path": "kind", "numBuckets": 5},
                                }
                            }
                        ]
                    ).to_list()
                    self.assertEqual(
                        search_meta,
                        [
                            {
                                "count": {"total": 2},
                                "facet": {
                                    "path": "kind",
                                    "numBuckets": 5,
                                    "buckets": [{"value": "note", "count": 2}],
                                },
                            }
                        ],
                    )
                    search_meta_facets = await collection.aggregate(
                        [
                            {
                                "$searchMeta": {
                                    "index": "by_text",
                                    "text": {"query": "ada", "path": ["title", "body"]},
                                    "facet": {
                                        "facets": {
                                            "kindFacet": {"type": "string", "path": "kind", "numBuckets": 5},
                                            "titleFacet": {"path": "title", "numBuckets": 3},
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()
                    self.assertEqual(
                        search_meta_facets,
                        [
                            {
                                "facet": {
                                    "facets": {
                                        "kindFacet": {
                                            "path": "kind",
                                            "numBuckets": 5,
                                            "buckets": [{"value": "note", "count": 2}],
                                        },
                                        "titleFacet": {
                                            "path": "title",
                                            "numBuckets": 3,
                                            "buckets": [
                                                {"value": "Grace", "count": 1},
                                                {"value": "Notes", "count": 1},
                                            ],
                                        },
                                    }
                                }
                            }
                        ],
                    )
                    with self.assertRaisesRegex(OperationFailure, "\\$searchMeta does not support highlight"):
                        await collection.aggregate(
                            [
                                {
                                    "$searchMeta": {
                                        "index": "by_text",
                                        "text": {"query": "ada", "path": ["title", "body"]},
                                        "count": {"type": "total"},
                                        "highlight": {"path": "title"},
                                    }
                                }
                            ]
                        ).to_list()

                    compound_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "filter": [{"wildcard": {"query": "*algorithm*", "path": "body"}}],
                                    },
                                }
                            }
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_hits], [3])
                    compound_scalar_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
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
                    self.assertEqual([document["_id"] for document in compound_scalar_hits], [3])
                    compound_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "filter": [{"wildcard": {"query": "*algorithm*", "path": "body"}}],
                                    },
                                }
                            }
                        ]
                    ).explain()
                    self.assertEqual(compound_explanation["engine_plan"]["details"]["queryOperator"], "compound")
                    self.assertEqual(
                        (
                            await collection.aggregate(
                                [
                                    {
                                        "$search": {
                                            "index": "by_text",
                                            "compound": {
                                                "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                                "filter": [
                                                    {"exists": {"path": "summary"}},
                                                    {"in": {"path": "kind", "value": ["note", "reference"]}},
                                                    {"range": {"path": "score", "gte": 9}},
                                                ],
                                            },
                                        }
                                    }
                                ]
                            ).explain()
                        )["engine_plan"]["details"]["compound"]["filterOperators"],
                        ["exists", "in", "range"],
                    )
                    phrase_compound_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [
                                            {
                                                "phrase": {
                                                    "query": "Ada wrote the first algorithm",
                                                    "path": "body",
                                                }
                                            }
                                        ],
                                        "filter": [
                                            {"in": {"path": "kind", "value": ["note", "reference"]}},
                                            {"range": {"path": "score", "gte": 9}},
                                        ],
                                        "should": [
                                            {"exists": {"path": "summary"}},
                                            {"regex": {"query": "Algorithm.*", "path": "summary"}},
                                        ],
                                    },
                                }
                            }
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in phrase_compound_hits], [3])
                    assert_phrase_in_range_compound_explanation(
                        self,
                        await collection.aggregate(
                            [
                                {
                                    "$search": {
                                        "index": "by_text",
                                        "compound": {
                                            "must": [
                                                {
                                                    "phrase": {
                                                        "query": "Ada wrote the first algorithm",
                                                        "path": "body",
                                                    }
                                                }
                                            ],
                                            "filter": [
                                                {"in": {"path": "kind", "value": ["note", "reference"]}},
                                                {"range": {"path": "score", "gte": 9}},
                                            ],
                                            "should": [
                                                {"exists": {"path": "summary"}},
                                                {"regex": {"query": "Algorithm.*", "path": "summary"}},
                                            ],
                                        },
                                    }
                                }
                            ]
                        ).explain(),
                    )
                    if engine_name == "sqlite":
                        self.assertEqual(compound_explanation["engine_plan"]["details"]["backend"], "fts5-prefilter")
                        self.assertEqual(compound_explanation["engine_plan"]["details"]["candidateCount"], 1)
                    else:
                        self.assertEqual(compound_explanation["engine_plan"]["details"]["backend"], "python")
                    self.assertEqual(
                        compound_explanation["engine_plan"]["details"]["compound"],
                        {
                            "must": 1,
                            "should": 0,
                            "filter": 1,
                            "mustNot": 0,
                            "minimumShouldMatch": 0,
                            "mustOperators": ["text"],
                            "shouldOperators": [],
                            "filterOperators": ["wildcard"],
                            "mustNotOperators": [],
                        },
                    )
                    self.assertEqual(
                        compound_explanation["engine_plan"]["details"]["pathSummary"],
                        {
                            "must": ["body", "title"],
                            "should": [],
                            "filter": ["body"],
                            "mustNot": [],
                            "all": ["body", "title"],
                            "parentPaths": ["body", "title"],
                            "leafPaths": [],
                            "textualPaths": ["body", "title"],
                            "textualParentPaths": ["body", "title"],
                            "textualLeafPaths": [],
                            "scalarPaths": [],
                            "scalarParentPaths": [],
                            "scalarLeafPaths": [],
                            "embeddedPaths": [],
                            "embeddedTextualPaths": [],
                            "embeddedScalarPaths": [],
                            "embeddedPathSections": [],
                            "usesEmbeddedPaths": False,
                            "nestedCompoundCount": 0,
                            "maxClauseDepth": 1,
                            "resolvedLeafPaths": ["body", "title"],
                            "resolvedTextualLeafPaths": ["body", "title"],
                            "resolvedScalarLeafPaths": [],
                            "unresolvedPaths": [],
                        },
                    )

                    compound_should_near_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "filter": [{"wildcard": {"query": "*algorithm*", "path": "body"}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"near": {"path": "score", "origin": 10, "pivot": 2}},
                                        ],
                                        "minimumShouldMatch": 2,
                                    },
                                }
                            }
                        ]
                    ).explain()
                    assert_compound_should_near_explanation(
                        self,
                        compound_should_near_explanation,
                        engine_name=engine_name,
                    )

                    compound_should_near_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"near": {"path": "score", "origin": 10, "pivot": 2}},
                                        ],
                                        "minimumShouldMatch": 0,
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_should_near_hits], [3, 2])

                    compound_candidateable_should_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            }
                        ]
                    ).explain()
                    assert_compound_candidateable_should_explanation(
                        self,
                        compound_candidateable_should_explanation,
                        engine_name=engine_name,
                    )
                    compound_candidateable_should_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_candidateable_should_hits], [3, 2])

                    compound_candidateable_should_limited_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                            {"$limit": 1},
                        ]
                    ).explain()
                    assert_compound_candidateable_should_limited_explanation(
                        self,
                        compound_candidateable_should_limited_explanation,
                        engine_name=engine_name,
                    )

                    compound_candidateable_should_limited_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                            {"$limit": 1},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_candidateable_should_limited_hits], [3])

                    compound_candidateable_should_matched_limited_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            },
                            {"$match": {"_id": 3}},
                            {"$project": {"_id": 1}},
                            {"$limit": 1},
                        ]
                    ).explain()
                    assert_compound_candidateable_should_matched_limited_explanation(
                        self,
                        compound_candidateable_should_matched_limited_explanation,
                        engine_name=engine_name,
                    )

                    compound_candidateable_should_matched_limited_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            },
                            {"$match": {"_id": 3}},
                            {"$project": {"_id": 1}},
                            {"$limit": 1},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_candidateable_should_matched_limited_hits], [3])

                    compound_candidateable_should_title_prefilter_explanation = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [{"text": {"query": "ada", "path": ["title", "body"]}}],
                                        "should": [
                                            {"exists": {"path": "title"}},
                                            {"wildcard": {"query": "*algorithm*", "path": "body"}},
                                            {"autocomplete": {"query": "alg", "path": ["title", "body"]}},
                                        ],
                                        "minimumShouldMatch": 1,
                                    },
                                }
                            },
                            {"$match": {"title": "Notes"}},
                            {"$project": {"_id": 1}},
                            {"$limit": 1},
                        ]
                    ).explain()
                    assert_compound_candidateable_should_title_prefilter_explanation(
                        self,
                        compound_candidateable_should_title_prefilter_explanation,
                        engine_name=engine_name,
                    )

                    near_hits = await collection.aggregate(
                        [
                            {"$search": {"index": "by_text", "near": {"path": "score", "origin": 10, "pivot": 2}}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in near_hits], [3])
                    near_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "near": {"path": "score", "origin": 10, "pivot": 2}}}]
                    ).explain()
                    self.assertEqual(near_explanation["engine_plan"]["details"]["queryOperator"], "near")
                    self.assertEqual(near_explanation["engine_plan"]["details"]["path"], "score")
                    self.assertEqual(near_explanation["engine_plan"]["details"]["pivot"], 2.0)
                    self.assertEqual(
                        near_explanation["engine_plan"]["details"]["pathSummary"],
                        {
                            "all": ["score"],
                            "pathCount": 1,
                            "multiPath": False,
                            "parentPaths": ["score"],
                            "leafPaths": [],
                            "sections": ["near"],
                            "resolvedLeafPaths": ["score"],
                            "unresolvedPaths": [],
                            "usesEmbeddedPaths": False,
                            "embeddedPaths": [],
                        },
                    )
                    self.assertEqual(near_explanation["engine_plan"]["details"]["backend"], "python")

                    vector_hits = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                }
                            }
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in vector_hits], [3, 2])

                    vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                }
                            }
                        ]
                    ).explain()
                    if engine_name == "sqlite":
                        self.assertEqual(vector_explanation["engine_plan"]["details"]["backend"], "usearch")
                        self.assertEqual(vector_explanation["engine_plan"]["details"]["mode"], "ann")
                        self.assertTrue(vector_explanation["engine_plan"]["details"]["backendMaterialized"])
                        self.assertTrue(vector_explanation["engine_plan"]["details"]["annAvailable"])
                        self.assertIsNotNone(vector_explanation["engine_plan"]["details"]["vectorBackend"])
                        self.assertGreaterEqual(
                            vector_explanation["engine_plan"]["details"]["candidatesRequested"],
                            vector_explanation["engine_plan"]["details"]["candidatesEvaluated"],
                        )
                    else:
                        self.assertEqual(vector_explanation["engine_plan"]["details"]["backend"], "python")
                    self.assertEqual(vector_explanation["engine_plan"]["details"]["path"], "embedding")
                    assert_vector_similarity_explanation(
                        self,
                        vector_explanation,
                        expected_similarity="cosine",
                    )
                    assert_vector_similarity_explanation(
                        self,
                        await collection.aggregate(
                            [
                                {
                                    "$vectorSearch": {
                                        "index": "by_vector_dot",
                                        "path": "embedding",
                                        "queryVector": [1.0, 0.0, 0.0],
                                        "limit": 2,
                                        "numCandidates": 3,
                                    }
                                }
                            ]
                        ).explain(),
                        expected_similarity="dotProduct",
                    )
                    assert_vector_similarity_explanation(
                        self,
                        await collection.aggregate(
                            [
                                {
                                    "$vectorSearch": {
                                        "index": "by_vector_euclidean",
                                        "path": "embedding",
                                        "queryVector": [1.0, 0.0, 0.0],
                                        "limit": 2,
                                        "numCandidates": 3,
                                    }
                                }
                            ]
                        ).explain(),
                        expected_similarity="euclidean",
                    )

                    filtered_vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 1,
                                    "numCandidates": 1,
                                    "filter": {"kind": "reference"},
                                }
                            }
                        ]
                    ).explain()
                    assert_filtered_vector_explanation(
                        self,
                        filtered_vector_explanation,
                        engine_name=engine_name,
                    )
                    downstream_filtered_vector_hits = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                }
                            },
                            {"$match": {"score": {"$gte": 15}}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in downstream_filtered_vector_hits], [2])
                    downstream_filtered_vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                }
                            },
                            {"$match": {"score": {"$gte": 15}}},
                        ]
                    ).explain()
                    assert_vector_downstream_filter_explanation(
                        self,
                        downstream_filtered_vector_explanation,
                        engine_name=engine_name,
                    )
                    query_and_downstream_filtered_vector_hits = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                    "filter": {"kind": "note"},
                                }
                            },
                            {"$match": {"score": {"$gte": 15}}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in query_and_downstream_filtered_vector_hits], [2])
                    query_and_downstream_filtered_vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                    "filter": {"kind": "note"},
                                }
                            },
                            {"$match": {"score": {"$gte": 15}}},
                        ]
                    ).explain()
                    assert_vector_query_and_downstream_filter_explanation(
                        self,
                        query_and_downstream_filtered_vector_explanation,
                        engine_name=engine_name,
                    )

                    ranged_vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 2,
                                    "filter": {"score": {"$gte": 11, "$lt": 16}},
                                }
                            }
                        ]
                    ).explain()
                    assert_ranged_vector_explanation(
                        self,
                        ranged_vector_explanation,
                        engine_name=engine_name,
                    )
                    boolean_vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
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
                    assert_boolean_vector_residual_explanation(
                        self,
                        boolean_vector_explanation,
                        engine_name=engine_name,
                    )
                    min_score_vector_hits = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                    "filter": {"kind": "note"},
                                    "minScore": 0.95,
                                }
                            }
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in min_score_vector_hits], [3])
                    min_score_vector_explanation = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                    "filter": {"kind": "note"},
                                    "minScore": 0.95,
                                }
                            }
                        ]
                    ).explain()
                    assert_vector_min_score_explanation(
                        self,
                        min_score_vector_explanation,
                        expected_min_score=0.95,
                        engine_name=engine_name,
                    )
                    vector_score_hits = await collection.aggregate(
                        [
                            {
                                "$vectorSearch": {
                                    "index": "by_vector",
                                    "path": "embedding",
                                    "queryVector": [1.0, 0.0, 0.0],
                                    "limit": 2,
                                    "numCandidates": 3,
                                }
                            },
                            {"$project": {"_id": 1, "title": 1, "vectorScore": {"$meta": "vectorSearchScore"}}},
                        ]
                    ).to_list()
                    assert_vector_score_projection_results(self, vector_score_hits, expected_ids=[3, 2])

                    with self.assertRaises(OperationFailure):
                        collection.aggregate(
                            [
                                {"$match": {"_id": {"$gt": 0}}},
                                {"$search": {"index": "by_text", "text": {"query": "ada"}}},
                            ]
                        )
                    with self.assertRaises(OperationFailure):
                        await collection.aggregate([{"$vectorSearch": {"index": "vec"}}]).to_list()
                    phrase_hits = await collection.aggregate(
                        [{"$search": {"index": "by_text", "phrase": {"query": "Ada wrote the first algorithm", "path": "body"}}}]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in phrase_hits], [3])
                    phrase_explanation = await collection.aggregate(
                        [{"$search": {"index": "by_text", "phrase": {"query": "Ada wrote the first algorithm", "path": "body"}}}]
                    ).explain()
                    assert_phrase_slop_explanation(
                        self,
                        phrase_explanation,
                        engine_name=engine_name,
                        expected_slop=0,
                        expected_match='"Ada wrote the first algorithm"',
                    )
                    with self.assertRaises(OperationFailure):
                        await collection.create_search_index({"mappings": {"fields": {"title": {"type": "decimal"}}}})

    async def test_search_phrase_slop_keeps_parity_between_memory_and_sqlite(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.docs.get_collection("phrases")
                    await collection.insert_many(
                        [
                            {
                                "_id": 1,
                                "kind": "note",
                                "title": "Exact phrase",
                                "body": "Ada wrote the first algorithm for the engine.",
                            },
                            {
                                "_id": 2,
                                "kind": "note",
                                "title": "Flexible phrase",
                                "body": "Ada wrote the practical first algorithm for the engine.",
                            },
                            {
                                "_id": 3,
                                "kind": "reference",
                                "title": "Different topic",
                                "body": "Grace documented the compiler pipeline.",
                            },
                        ]
                    )
                    await collection.create_search_indexes(
                        [
                            SearchIndexModel(
                                {
                                    "mappings": {
                                        "dynamic": False,
                                        "fields": {
                                            "title": {"type": "string"},
                                            "body": {"type": "string"},
                                            "kind": {"type": "token"},
                                        },
                                    }
                                },
                                name="by_text",
                            )
                        ]
                    )

                    exact_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "phrase": {
                                        "query": "Ada wrote the first algorithm",
                                        "path": "body",
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in exact_hits], [1])

                    slop_hits = await collection.aggregate(
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
                    self.assertEqual([document["_id"] for document in slop_hits], [1, 2])

                    compound_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [
                                            {
                                                "phrase": {
                                                    "query": "Ada wrote the first algorithm",
                                                    "path": "body",
                                                    "slop": 1,
                                                }
                                            }
                                        ],
                                        "filter": [{"equals": {"path": "kind", "value": "note"}}],
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_hits], [1, 2])

                    slop_explanation = await collection.aggregate(
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
                    assert_phrase_slop_explanation(
                        self,
                        slop_explanation,
                        engine_name=engine_name,
                        expected_slop=1,
                        expected_match='"ada" AND "wrote" AND "the" AND "first" AND "algorithm"',
                    )

    async def test_search_embedded_documents_mapping_keeps_parity_between_memory_and_sqlite(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.docs.get_collection("embedded_search")
                    await collection.insert_many(
                        [
                            {
                                "_id": 1,
                                "title": "Local search handbook",
                                "contributors": [
                                    {"name": "Ada Lovelace", "role": "author", "verified": True, "impact": 10},
                                    {"name": "Charles Babbage", "role": "editor", "verified": False, "impact": 2},
                                ],
                            },
                            {
                                "_id": 2,
                                "title": "Compiler reference",
                                "contributors": [
                                    {"name": "Grace Hopper", "role": "author", "verified": False, "impact": 4},
                                ],
                            },
                            {
                                "_id": 3,
                                "title": "Ada algorithms handbook",
                                "contributors": [
                                    {"name": "Ada Byron", "role": "author", "verified": True, "impact": 7},
                                ],
                            },
                        ]
                    )
                    await collection.create_search_index(
                        SearchIndexModel(
                            {
                                "mappings": {
                                    "dynamic": False,
                                    "fields": {
                                        "title": {"type": "string"},
                                        "contributors": {
                                            "type": "embeddedDocuments",
                                            "fields": {
                                                "name": {"type": "string"},
                                                "role": {"type": "token"},
                                                "verified": {"type": "boolean"},
                                                "impact": {"type": "number"},
                                            },
                                        },
                                    },
                                }
                            },
                            name="by_text",
                        )
                    )

                    hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {
                                        "query": "Ada",
                                        "path": "contributors.name",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in hits], [1, 3])

                    parent_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {
                                        "query": "Ada",
                                        "path": "contributors",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in parent_hits], [1, 3])

                    parent_autocomplete_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "autocomplete": {
                                        "query": "Ada Lo",
                                        "path": "contributors",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in parent_autocomplete_hits], [1])

                    parent_regex_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "regex": {
                                        "query": "Charles.*",
                                        "path": "contributors",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in parent_regex_hits], [1])

                    parent_exists_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "exists": {
                                        "path": "contributors",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual(
                        [document["_id"] for document in parent_exists_hits],
                        [1, 2, 3],
                    )

                    equals_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "equals": {
                                        "path": "contributors.verified",
                                        "value": True,
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in equals_hits], [1, 3])

                    near_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "near": {
                                        "path": "contributors.impact",
                                        "origin": 8,
                                        "pivot": 3,
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in near_hits], [3, 1])

                    compound_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [
                                            {
                                                "text": {
                                                    "query": "Ada",
                                                    "path": ["title", "contributors.name"],
                                                }
                                            }
                                        ],
                                        "filter": [
                                            {
                                                "equals": {
                                                    "path": "contributors.verified",
                                                    "value": True,
                                                }
                                            }
                                        ],
                                        "should": [
                                            {
                                                "near": {
                                                    "path": "contributors.impact",
                                                    "origin": 8,
                                                    "pivot": 3,
                                                }
                                            }
                                        ],
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in compound_hits], [3, 1])

                    compound_explain = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "compound": {
                                        "must": [
                                            {
                                                "text": {
                                                    "query": "Ada",
                                                    "path": ["title", "contributors.name"],
                                                }
                                            }
                                        ],
                                        "filter": [
                                            {
                                                "equals": {
                                                    "path": "contributors.verified",
                                                    "value": True,
                                                }
                                            }
                                        ],
                                        "should": [
                                            {
                                                "near": {
                                                    "path": "contributors.impact",
                                                    "origin": 8,
                                                    "pivot": 3,
                                                }
                                            }
                                        ],
                                    },
                                }
                            }
                        ]
                    ).explain()
                    self.assertEqual(
                        compound_explain["engine_plan"]["details"]["pathSummary"][
                            "embeddedPathSections"
                        ],
                        ["must", "should", "filter"],
                    )
                    self.assertEqual(
                        compound_explain["engine_plan"]["details"]["pathSummary"][
                            "embeddedScalarPaths"
                        ],
                        ["contributors.impact", "contributors.verified"],
                    )
                    self.assertEqual(
                        compound_explain["engine_plan"]["details"]["pathSummary"][
                            "resolvedTextualLeafPaths"
                        ],
                        ["contributors.name", "title"],
                    )
                    self.assertEqual(
                        compound_explain["engine_plan"]["details"]["pathSummary"][
                            "resolvedScalarLeafPaths"
                        ],
                        ["contributors.impact", "contributors.verified"],
                    )

    async def test_search_document_mapping_and_date_near_keep_parity_between_memory_and_sqlite(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.docs.get_collection("document_search")
                    await collection.insert_many(
                        [
                            {
                                "_id": 1,
                                "title": "Local search handbook",
                                "metadata": {
                                    "topic": "Local search",
                                    "series": "analysis",
                                    "publishedAt": datetime.datetime(2024, 1, 10, 0, 0, 0),
                                },
                            },
                            {
                                "_id": 2,
                                "title": "Compiler reference",
                                "metadata": {
                                    "topic": "Compiler internals",
                                    "series": "reference",
                                    "publishedAt": datetime.datetime(2024, 2, 20, 0, 0, 0),
                                },
                            },
                            {
                                "_id": 3,
                                "title": "Ada algorithms handbook",
                                "metadata": {
                                    "topic": "Algorithm ranking",
                                    "series": "analysis",
                                    "publishedAt": datetime.datetime(2024, 1, 18, 0, 0, 0),
                                },
                            },
                        ]
                    )
                    await collection.create_search_index(
                        SearchIndexModel(
                            {
                                "mappings": {
                                    "dynamic": False,
                                    "fields": {
                                        "metadata": {
                                            "type": "document",
                                            "fields": {
                                                "topic": {"type": "string"},
                                                "series": {"type": "token"},
                                                "publishedAt": {"type": "date"},
                                            },
                                        },
                                    },
                                }
                            },
                            name="by_text",
                        )
                    )

                    topic_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {
                                        "query": "Local",
                                        "path": "metadata.topic",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in topic_hits], [1])

                    parent_topic_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "text": {
                                        "query": "Local",
                                        "path": "metadata",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in parent_topic_hits], [1])

                    parent_wildcard_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "wildcard": {
                                        "query": "*search*",
                                        "path": "metadata",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in parent_wildcard_hits], [1])

                    parent_exists_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "exists": {
                                        "path": "metadata",
                                    },
                                }
                            },
                            {"$sort": {"_id": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual(
                        [document["_id"] for document in parent_exists_hits],
                        [1, 2, 3],
                    )

                    parent_exists_explain = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "exists": {
                                        "path": "metadata",
                                    },
                                }
                            }
                        ]
                    ).explain()
                    self.assertEqual(
                        parent_exists_explain["engine_plan"]["details"]["pathSummary"][
                            "resolvedLeafPaths"
                        ],
                        ["metadata.publishedAt", "metadata.series", "metadata.topic"],
                    )

                    near_hits = await collection.aggregate(
                        [
                            {
                                "$search": {
                                    "index": "by_text",
                                    "near": {
                                        "path": "metadata.publishedAt",
                                        "origin": datetime.date(2024, 1, 15),
                                        "pivot": 10 * 86400,
                                    },
                                }
                            },
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()
                    self.assertEqual([document["_id"] for document in near_hits], [3, 1])

    async def test_aggregate_explain_reports_pipeline_pushdown_summary(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.get_collection("events")
                    await collection.insert_many(
                        [
                            {"_id": 1, "kind": "view", "rank": 3},
                            {"_id": 2, "kind": "view", "rank": 1},
                            {"_id": 3, "kind": "click", "rank": 2},
                        ]
                    )

                    explanation = await collection.aggregate(
                        [
                            {"$match": {"kind": "view"}},
                            {"$sort": {"rank": 1}},
                            {"$limit": 1},
                            {"$project": {"_id": 0, "rank": 1}},
                        ]
                    ).explain()

                    self.assertEqual(explanation["pushdown"]["mode"], "pipeline-prefix")
                    self.assertEqual(explanation["pushdown"]["totalStages"], 4)
                    self.assertEqual(explanation["pushdown"]["pushedDownStages"], 4)
                    self.assertEqual(explanation["pushdown"]["remainingStages"], 0)
                    self.assertEqual(explanation["remaining_pipeline"], [])

    async def test_search_index_latency_can_surface_pending_state(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                engine = ENGINE_FACTORIES[engine_name](simulate_search_index_latency=0.02)
                await engine.connect()
                try:
                    async with AsyncMongoClient(engine) as client:
                        collection = client.search_latency.get_collection("docs")
                        await collection.create_search_index(
                            SearchIndexModel(
                                {
                                    "mappings": {
                                        "dynamic": False,
                                        "fields": {"title": {"type": "string"}},
                                    }
                                },
                                name="by_text",
                            )
                        )
                        listed = await collection.list_search_indexes("by_text").to_list()
                        self.assertEqual(listed[0]["status"], "PENDING")
                        self.assertEqual(listed[0]["statusDetail"], "pending-build")
                        self.assertIsNotNone(listed[0]["readyAtEpoch"])
                        with self.assertRaises(OperationFailure):
                            await collection.aggregate(
                                [{"$search": {"index": "by_text", "text": {"query": "ada", "path": "title"}}}]
                            ).to_list()
                        await asyncio.sleep(0.03)
                        ready = await collection.list_search_indexes("by_text").to_list()
                        self.assertEqual(ready[0]["status"], "READY")
                        self.assertEqual(ready[0]["statusDetail"], "ready")
                finally:
                    await engine.disconnect()

    async def test_watch_surfaces_client_database_and_collection_scopes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.observe.get_collection("items")
                    other = client.observe.get_collection("other")
                    client_stream = client.watch(max_await_time_ms=100)
                    lookup_stream = client.watch(max_await_time_ms=100, full_document="updateLookup")
                    database_stream = client.observe.watch(max_await_time_ms=100)
                    collection_stream = collection.watch(
                        [{"$match": {"operationType": "insert"}}],
                        max_await_time_ms=100,
                    )

                    await collection.insert_one({"_id": 1, "name": "Ada"})
                    await other.insert_one({"_id": 2, "name": "Bob"})

                    collection_event = await collection_stream.try_next()
                    self.assertIsNotNone(collection_event)
                    self.assertEqual(collection_event["operationType"], "insert")
                    self.assertEqual(collection_event["ns"], {"db": "observe", "coll": "items"})

                    database_event = await database_stream.try_next()
                    self.assertIsNotNone(database_event)
                    self.assertEqual(database_event["ns"]["db"], "observe")

                    client_event = await client_stream.try_next()
                    self.assertIsNotNone(client_event)
                    self.assertIn(client_event["ns"]["coll"], {"items", "other"})
                    second_insert = await client_stream.try_next()
                    self.assertIsNotNone(second_insert)
                    self.assertEqual(second_insert["operationType"], "insert")

                    lookup_stream = client.observe.watch(max_await_time_ms=100, full_document="updateLookup")
                    await collection.update_one({"_id": 1}, {"$set": {"name": "Ada Lovelace"}})
                    await collection.delete_one({"_id": 1})
                    update_event = await client_stream.try_next()
                    update_lookup_event = await lookup_stream.try_next()
                    delete_event = await client_stream.try_next()
                    self.assertEqual(update_event["operationType"], "update")
                    self.assertNotIn("fullDocument", update_event)
                    self.assertEqual(update_lookup_event["fullDocument"]["name"], "Ada Lovelace")
                    self.assertEqual(delete_event["operationType"], "delete")

                    invalidate_stream = collection.watch(max_await_time_ms=100)
                    await other.drop()
                    self.assertIsNone(await invalidate_stream.try_next())
                    await collection.drop()
                    invalidate_event = await invalidate_stream.try_next()
                    self.assertEqual(invalidate_event["operationType"], "invalidate")
                    self.assertFalse(invalidate_stream.alive)

    async def test_collection_json_schema_validator_rejects_invalid_inserts_and_updates(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    database = client.get_database("validation")
                    collection = await database.create_collection(
                        "users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        },
                    )

                    with self.assertRaises(DocumentValidationFailure) as insert_error:
                        await collection.insert_one({"_id": "1", "age": 10})
                    self.assertEqual(insert_error.exception.code, 121)

                    await collection.insert_one({"_id": "1", "name": "Ada", "age": 10})
                    with self.assertRaises(DocumentValidationFailure):
                        await collection.update_one({"_id": "1"}, {"$set": {"age": "old"}})

    async def test_collection_json_schema_warn_mode_allows_write(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.validation.create_collection(
                        "warn_users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {"name": {"bsonType": "string"}},
                            }
                        },
                        validationAction="warn",
                    )

                    await collection.insert_one({"_id": "1"})
                    self.assertEqual(await collection.find_one({"_id": "1"}), {"_id": "1"})

    async def test_find_supports_top_level_json_schema_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.validation.get_collection("query_users")
                    await collection.insert_many(
                        [
                            {"_id": "1", "name": "Ada", "age": 10},
                            {"_id": "2", "name": "Bob", "age": "old"},
                            {"_id": "3", "age": 11},
                        ]
                    )

                    result = await collection.find(
                        {
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        }
                    ).to_list()

                    self.assertEqual(result, [{"_id": "1", "name": "Ada", "age": 10}])

    async def test_find_supports_top_level_json_schema_inside_logical_clauses(self):
        schema_clause = {
            "$jsonSchema": {
                "required": ["name", "age"],
                "properties": {
                    "name": {"bsonType": "string"},
                    "age": {"bsonType": "int"},
                },
            }
        }

        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.validation.get_collection("logical_query_users")
                    await collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "name": "Ada", "age": 10},
                            {"_id": "2", "tenant": "a", "name": "Bob", "age": "old"},
                            {"_id": "3", "tenant": "b", "age": 11},
                            {"_id": "4", "tenant": "c", "name": "Cora", "age": 12},
                        ]
                    )

                    and_result = await collection.find(
                        {"$and": [schema_clause, {"tenant": "a"}]},
                        sort=[("_id", 1)],
                    ).to_list()
                    or_result = await collection.find(
                        {"$or": [schema_clause, {"tenant": "b"}]},
                        sort=[("_id", 1)],
                    ).to_list()
                    nor_result = await collection.find(
                        {"$nor": [schema_clause]},
                        sort=[("_id", 1)],
                    ).to_list()

                    self.assertEqual(and_result, [{"_id": "1", "tenant": "a", "name": "Ada", "age": 10}])
                    self.assertEqual(
                        or_result,
                        [
                            {"_id": "1", "tenant": "a", "name": "Ada", "age": 10},
                            {"_id": "3", "tenant": "b", "age": 11},
                            {"_id": "4", "tenant": "c", "name": "Cora", "age": 12},
                        ],
                    )
                    self.assertEqual(
                        nor_result,
                        [
                            {"_id": "2", "tenant": "a", "name": "Bob", "age": "old"},
                            {"_id": "3", "tenant": "b", "age": 11},
                        ],
                    )

    async def test_find_supports_richer_top_level_json_schema_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await assert_async_find_supports_richer_top_level_json_schema_filter(client)

    async def test_bypass_document_validation_allows_invalid_writes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.validation.create_collection(
                        "bypass_users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {
                                    "name": {"bsonType": "string"},
                                    "age": {"bsonType": "int"},
                                },
                            }
                        },
                    )

                    await collection.insert_one(
                        {"_id": "1", "age": 10},
                        bypass_document_validation=True,
                    )
                    await collection.update_one(
                        {"_id": "1"},
                        {"$set": {"age": "old"}},
                        bypass_document_validation=True,
                    )
                    await collection.replace_one(
                        {"_id": "1"},
                        {"_id": "1", "age": "stale"},
                        bypass_document_validation=True,
                    )
                    await collection.bulk_write(
                        [
                            InsertOne({"_id": "2"}),
                            UpdateOne({"_id": "2"}, {"$set": {"age": "legacy"}}),
                        ],
                        bypass_document_validation=True,
                    )

                    self.assertEqual(await collection.find_one({"_id": "1"}), {"_id": "1", "age": "stale"})
                    self.assertEqual(await collection.find_one({"_id": "2"}), {"_id": "2", "age": "legacy"})

    async def test_collation_applies_to_query_sort_update_delete_and_distinct(self):
        collation = {"locale": "en", "strength": 2, "numericOrdering": True}
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.alpha.names
                    await collection.insert_many(
                        [
                            {"_id": 1, "name": "Alice", "code": "10"},
                            {"_id": 2, "name": "alice", "code": "2"},
                            {"_id": 3, "name": "Bob", "code": "3"},
                        ]
                    )

                    found = await collection.find_one({"name": "ALICE"}, collation=collation)
                    self.assertEqual(found["_id"], 1)

                    ordered = await collection.find(
                        {},
                        {"_id": 0, "code": 1},
                        sort=[("code", 1)],
                        collation=collation,
                    ).to_list()
                    self.assertEqual(ordered, [{"code": "2"}, {"code": "3"}, {"code": "10"}])

                    update_result = await collection.update_one(
                        {"name": "ALICE"},
                        {"$set": {"matched": True}},
                        collation=collation,
                    )
                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(await collection.find_one({"_id": 1}), {"_id": 1, "name": "Alice", "code": "10", "matched": True})

                    distinct_names = await collection.distinct("name", collation=collation)
                    self.assertEqual(distinct_names, ["Alice", "Bob"])

                    delete_result = await collection.delete_one({"name": "bob"}, collation=collation)
                    self.assertEqual(delete_result.deleted_count, 1)
                    self.assertIsNone(await collection.find_one({"_id": 3}))

    async def test_collation_applies_to_aggregate_and_aggregate_command(self):
        collation = {"locale": "en", "strength": 2, "numericOrdering": True}
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.alpha.events
                    await collection.insert_many(
                        [
                            {"_id": 1, "kind": "View", "rank": "10"},
                            {"_id": 2, "kind": "view", "rank": "2"},
                            {"_id": 3, "kind": "click", "rank": "3"},
                        ]
                    )

                    aggregated = await collection.aggregate(
                        [
                            {"$match": {"kind": "VIEW"}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"_id": 0, "rank": 1}},
                        ],
                        collation=collation,
                    ).to_list()
                    self.assertEqual(aggregated, [{"rank": "2"}, {"rank": "10"}])

                    command_result = await client.alpha.command(
                        {
                            "aggregate": "events",
                            "pipeline": [
                                {"$match": {"kind": "VIEW"}},
                                {"$sort": {"rank": 1}},
                                {"$project": {"_id": 0, "rank": 1}},
                            ],
                            "collation": collation,
                            "cursor": {"batchSize": 1},
                        }
                    )
                    self.assertEqual(
                        command_result,
                        {
                            "cursor": {"id": 0, "ns": "alpha.events", "firstBatch": [{"rank": "2"}, {"rank": "10"}]},
                            "ok": 1.0,
                        },
                    )

    async def test_collation_applies_to_array_update_operators_and_array_filters(self):
        collation = {"locale": "en", "strength": 2}
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.alpha.tags
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "tags": ["Ada", "Grace"],
                            "items": [{"kind": "Ada"}, {"kind": "Grace"}],
                        }
                    )

                    add_result = await collection.update_one(
                        {"_id": "1"},
                        {"$addToSet": {"tags": "ada"}},
                        collation=collation,
                    )
                    pull_result = await collection.update_one(
                        {"_id": "1"},
                        {"$pull": {"tags": "ada"}},
                        collation=collation,
                    )
                    filter_result = await collection.update_one(
                        {"_id": "1"},
                        {"$set": {"items.$[item].matched": True}},
                        array_filters=[{"item.kind": "ada"}],
                        collation=collation,
                    )
                    pull_all_result = await collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"tags": ["grace"]}},
                        collation=collation,
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(add_result.modified_count, 0)
                    self.assertEqual(pull_result.modified_count, 1)
                    self.assertEqual(filter_result.modified_count, 1)
                    self.assertEqual(pull_all_result.modified_count, 1)
                    self.assertEqual(
                        updated,
                        {
                            "_id": "1",
                            "tags": [],
                            "items": [{"kind": "Ada", "matched": True}, {"kind": "Grace"}],
                        },
                    )

    async def test_database_command_supports_bypass_document_validation_and_collation(self):
        collation = {"locale": "en", "strength": 2}
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    database = client.validation
                    collection = await database.create_collection(
                        "cmd_users",
                        validator={
                            "$jsonSchema": {
                                "required": ["name"],
                                "properties": {"name": {"bsonType": "string"}},
                            }
                        },
                    )
                    await database.command(
                        {
                            "insert": "cmd_users",
                            "documents": [{"_id": "1"}],
                            "bypassDocumentValidation": True,
                        }
                    )
                    await collection.insert_many(
                        [
                            {"_id": "2", "name": "Alice"},
                            {"_id": "3", "name": "alice"},
                            {"_id": "4", "name": "Bob"},
                        ]
                    )

                    count_result = await database.command(
                        {"count": "cmd_users", "query": {"name": "ALICE"}, "collation": collation}
                    )
                    self.assertEqual(count_result["n"], 2)

                    await database.command(
                        {
                            "update": "cmd_users",
                            "updates": [
                                {
                                    "q": {"name": "ALICE"},
                                    "u": {"$set": {"matched": True}},
                                    "collation": collation,
                                }
                            ],
                        }
                    )
                    self.assertTrue((await collection.find_one({"_id": "2"}))["matched"])

                    distinct_result = await database.command(
                        {
                            "distinct": "cmd_users",
                            "key": "name",
                            "query": {"name": {"$exists": True}},
                            "collation": collation,
                        }
                    )
                    self.assertEqual(distinct_result["values"], ["Alice", "Bob"])

                    await database.command(
                        {
                            "delete": "cmd_users",
                            "deletes": [{"q": {"name": "bob"}, "limit": 1, "collation": collation}],
                        }
                    )
                    self.assertIsNone(await collection.find_one({"_id": "4"}))

    async def test_create_collection_rejects_invalid_json_schema_options(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            with self.assertRaises(OperationFailure):
                await client.validation.create_collection(
                    "broken",
                    validator={"$jsonSchema": {"required": "name"}},
                )

    async def test_profile_command_and_system_profile_capture_operations(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    database = client.get_database("profiled")
                    profile_enabled = await database.command({"profile": 2, "slowms": 0})
                    self.assertEqual(profile_enabled["was"], 0)
                    self.assertEqual(profile_enabled["level"], 2)
                    self.assertEqual(profile_enabled["entryCount"], 0)

                    collection = database.get_collection("users")
                    await collection.insert_one({"_id": "1", "name": "Ada"})
                    await collection.find_one({"_id": "1"})
                    await collection.update_one({"_id": "1"}, {"$set": {"active": True}})

                    profile_status = await database.command({"profile": -1})
                    self.assertEqual(profile_status["was"], 2)
                    self.assertEqual(profile_status["level"], 2)
                    self.assertGreaterEqual(profile_status["entryCount"], 3)
                    profile_entries = await database.get_collection("system.profile").find().to_list()

                    self.assertTrue(any(entry["op"] == "insert" for entry in profile_entries))
                    self.assertTrue(any(entry["op"] == "query" for entry in profile_entries))
                    self.assertTrue(any(entry["op"] == "update" for entry in profile_entries))

    async def test_memory_engine_transactions_use_isolated_mvcc_snapshots(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            session = client.start_session()
            session.start_transaction()

            await client.alpha.users.insert_one({"_id": "1", "name": "Ada"}, session=session)

            self.assertIsNone(await client.alpha.users.find_one({"_id": "1"}))
            self.assertEqual(
                await client.alpha.users.find_one({"_id": "1"}, session=session),
                {"_id": "1", "name": "Ada"},
            )

            session.commit_transaction()

            self.assertEqual(await client.alpha.users.find_one({"_id": "1"}), {"_id": "1", "name": "Ada"})

    async def test_memory_engine_transactions_detect_write_conflicts_on_commit(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            first = client.start_session()
            second = client.start_session()
            first.start_transaction()
            second.start_transaction()

            await client.alpha.users.insert_one({"_id": "1", "name": "Ada"}, session=first)
            await client.alpha.users.insert_one({"_id": "1", "name": "Grace"}, session=second)

            first.commit_transaction()
            with self.assertRaisesRegex(OperationFailure, "Write conflict"):
                second.commit_transaction()

            self.assertEqual(await client.alpha.users.find_one({"_id": "1"}), {"_id": "1", "name": "Ada"})

    async def test_session_observes_operation_times_after_successful_collection_ops(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            session = client.start_session()

            self.assertIsNone(session.operation_time)
            self.assertIsNone(session.cluster_time)

            await client.alpha.users.insert_one({"_id": "1", "name": "Ada"}, session=session)

            self.assertIsNotNone(session.operation_time)
            self.assertIsNotNone(session.cluster_time)
            self.assertGreaterEqual(session.cluster_time, session.operation_time)

    async def test_client_propagates_dialect_and_profile_to_database_and_collection(self):
        engine = MemoryEngine()

        async with AsyncMongoClient(
            engine,
            mongodb_dialect='8.0',
            pymongo_profile='4.13',
        ) as client:
            database = client.get_database('alpha')
            collection = database.get_collection('users')

            self.assertEqual(client.mongodb_dialect, MongoDialect80())
            self.assertEqual(client.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(client.pymongo_profile, PyMongoProfile413())
            self.assertEqual(client.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(database.mongodb_dialect, MongoDialect80())
            self.assertEqual(database.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(database.pymongo_profile, PyMongoProfile413())
            self.assertEqual(database.pymongo_profile_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(collection.mongodb_dialect, MongoDialect80())
            self.assertEqual(collection.mongodb_dialect_resolution.resolution_mode, 'explicit-alias')
            self.assertEqual(collection.pymongo_profile, PyMongoProfile413())
            self.assertEqual(collection.pymongo_profile_resolution.resolution_mode, 'explicit-alias')

    async def test_client_supports_attribute_and_item_access(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client["alpha"]["users"].insert_one({"_id": "1", "name": "Ada"})

                    found = await client.alpha.users.find_one({"_id": "1"})
                    collections = await client["alpha"].list_collection_names()

                    self.assertEqual(found, {"_id": "1", "name": "Ada"})
                    self.assertEqual(collections, ["users"])

    async def test_client_server_info_reflects_target_dialect(self):
        await assert_client_server_info_reflects_target_dialect(self, open_client)

    async def test_build_info_command_shares_source_of_truth_with_server_info(self):
        await assert_build_info_command_shares_source_of_truth_with_server_info(self, open_client)

    async def test_hello_and_is_master_commands_return_handshake_metadata(self):
        await assert_hello_and_is_master_commands_return_handshake_metadata(self, open_client)

    async def test_list_commands_and_connection_status_commands_return_local_admin_metadata(self):
        await assert_list_commands_and_connection_status_commands_return_local_admin_metadata(self, open_client)

    async def test_server_status_command_returns_local_runtime_metadata(self):
        await assert_server_status_command_returns_local_runtime_metadata(self, ENGINE_FACTORIES, open_client)

    async def test_server_status_opcounters_track_local_runtime_activity(self):
        await assert_server_status_opcounters_track_local_runtime_activity(self, ENGINE_FACTORIES, open_client)

    async def test_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata(self):
        await assert_host_info_whats_my_uri_and_cmd_line_opts_commands_return_local_metadata(self, open_client)

    async def test_list_collections_command_supports_name_only(self):
        await assert_list_collections_command_supports_name_only(self, ENGINE_FACTORIES, open_client)

    async def test_create_collection_registers_empty_namespace_and_rejects_duplicates(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    created = await client.alpha.create_collection("events")

                    self.assertEqual(created.name, "events")
                    self.assertEqual(await client.alpha.list_collection_names(), ["events"])
                    self.assertIn("alpha", await client.list_database_names())

                    with self.assertRaises(CollectionInvalid):
                        await client.alpha.create_collection("events")

    async def test_list_collections_returns_cursor_documents_and_supports_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.create_collection("events")
                    await client.alpha.create_collection("logs")

                    cursor = client.alpha.list_collections({"name": "events"})

                    self.assertEqual(
                        await cursor.to_list(),
                        [
                            {
                                "name": "events",
                                "type": "collection",
                                "options": {},
                                "info": {"readOnly": False},
                            }
                        ],
                    )
                    self.assertEqual(
                        await client.alpha.list_collection_names({"name": "logs"}),
                        ["logs"],
                    )

    async def test_collection_options_round_trip_through_admin_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=1024,
                    )

                    self.assertEqual(
                        await collection.options(),
                        {"capped": True, "size": 1024},
                    )
                    self.assertEqual(
                        await client.alpha.list_collections({"name": "events"}).to_list(),
                        [
                            {
                                "name": "events",
                                "type": "collection",
                                "options": {"capped": True, "size": 1024},
                                "info": {"readOnly": False},
                            }
                        ],
                    )

    async def test_delete_one_is_allowed_on_capped_collections(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=1024,
                    )
                    await collection.insert_many([{"_id": "1"}, {"_id": "2"}])

                    result = await collection.delete_one({"_id": "1"})

                    self.assertEqual(result.deleted_count, 1)
                    self.assertEqual(await collection.find({}, sort=[("_id", 1)]).to_list(), [{"_id": "2"}])

    async def test_client_database_and_collection_expose_configured_pymongo_options(self):
        write_concern = WriteConcern("majority", j=True)
        read_concern = ReadConcern("majority")
        read_preference = ReadPreference(ReadPreferenceMode.SECONDARY)
        codec_options = CodecOptions(dict, tz_aware=True)
        transaction_options = TransactionOptions(
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            max_commit_time_ms=250,
        )

        async with AsyncMongoClient(
            MemoryEngine(),
            write_concern=write_concern,
            read_concern=read_concern,
            read_preference=read_preference,
            codec_options=codec_options,
            transaction_options=transaction_options,
        ) as client:
            database = client.get_database("alpha")
            collection = database.get_collection("events")

            self.assertIs(client.write_concern, write_concern)
            self.assertIs(client.read_concern, read_concern)
            self.assertIs(client.read_preference, read_preference)
            self.assertIs(client.codec_options, codec_options)
            self.assertIs(client.transaction_options, transaction_options)
            self.assertIs(database.write_concern, write_concern)
            self.assertIs(database.read_concern, read_concern)
            self.assertIs(database.read_preference, read_preference)
            self.assertIs(database.codec_options, codec_options)
            self.assertIs(collection.write_concern, write_concern)
            self.assertIs(collection.read_concern, read_concern)
            self.assertIs(collection.read_preference, read_preference)
            self.assertIs(collection.codec_options, codec_options)

    async def test_with_options_clones_database_and_collection_without_mutating_parent(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            base_database = client.get_database("alpha")
            tuned_database = base_database.with_options(
                write_concern=WriteConcern(1),
                read_concern=ReadConcern("local"),
            )
            base_collection = tuned_database.get_collection("events")
            tuned_collection = base_collection.with_options(
                read_preference=ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
                codec_options=CodecOptions(dict, tz_aware=True),
            )

            self.assertEqual(base_database.write_concern, WriteConcern())
            self.assertEqual(base_database.read_concern, ReadConcern())
            self.assertEqual(tuned_database.write_concern, WriteConcern(1))
            self.assertEqual(tuned_database.read_concern, ReadConcern("local"))
            self.assertEqual(base_collection.read_preference, ReadPreference())
            self.assertEqual(
                tuned_collection.read_preference,
                ReadPreference(ReadPreferenceMode.SECONDARY_PREFERRED),
            )
            self.assertEqual(
                tuned_collection.codec_options,
                CodecOptions(dict, tz_aware=True),
            )

    async def test_client_with_options_clones_client_without_mutating_parent(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            tuned = client.with_options(
                write_concern=WriteConcern(1),
                read_concern=ReadConcern("local"),
            )

            self.assertEqual(client.write_concern, WriteConcern())
            self.assertEqual(client.read_concern, ReadConcern())
            self.assertEqual(tuned.write_concern, WriteConcern(1))
            self.assertEqual(tuned.read_concern, ReadConcern("local"))

    async def test_get_default_database_uses_uri_name_or_explicit_default_and_preserves_options(self):
        read_preference = ReadPreference(ReadPreferenceMode.SECONDARY)
        codec_options = CodecOptions(dict, tz_aware=True)

        async with AsyncMongoClient(
            MemoryEngine(),
            uri="mongodb://localhost/observe",
            read_preference=read_preference,
            codec_options=codec_options,
        ) as client:
            from_uri = client.get_default_database()
            overridden = client.get_default_database(
                "ignored",
                write_concern=WriteConcern(1),
            )

            self.assertEqual(from_uri._db_name, "observe")
            self.assertEqual(overridden._db_name, "observe")
            self.assertIs(from_uri.read_preference, read_preference)
            self.assertIs(from_uri.codec_options, codec_options)
            self.assertEqual(overridden.write_concern, WriteConcern(1))

        async with AsyncMongoClient(MemoryEngine(), uri="mongodb://localhost") as client:
            explicit = client.get_default_database("fallback")
            self.assertEqual(explicit._db_name, "fallback")
            with self.assertRaises(InvalidOperation):
                client.get_default_database()

    async def test_client_context_manager_and_server_info_are_stable(self):
        client = AsyncMongoClient(MemoryEngine())
        async with client as managed:
            self.assertIs(managed, client)
            first = await client.server_info()
            second = await client.server_info()

            self.assertTrue(first["version"].startswith(f"{client.mongodb_dialect.server_version}."))
            self.assertTrue(second["version"].startswith(f"{client.mongodb_dialect.server_version}."))
            self.assertEqual(first["versionArray"], second["versionArray"])

    async def test_collection_namespace_helpers_and_cursor_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.alpha.get_collection(
                        "events",
                        read_concern=ReadConcern("majority"),
                    )
                    child = collection.daily
                    child_underscore = collection["daily_logs"]
                    sibling = collection.database.get_collection(
                        "logs",
                        read_concern=ReadConcern("local"),
                    )

                    self.assertEqual(collection.name, "events")
                    self.assertEqual(collection.full_name, "alpha.events")
                    self.assertEqual(collection.database.name, "alpha")
                    self.assertEqual(child.name, "events.daily")
                    self.assertEqual(child.full_name, "alpha.events.daily")
                    self.assertEqual(child_underscore.name, "events.daily_logs")
                    self.assertEqual(child_underscore.full_name, "alpha.events.daily_logs")
                    self.assertEqual(sibling.name, "logs")
                    self.assertEqual(sibling.full_name, "alpha.logs")
                    self.assertEqual(sibling.read_concern, ReadConcern("local"))
                    self.assertEqual(collection.read_concern, ReadConcern("majority"))

                    await collection.insert_many(
                        [
                            {"_id": "1", "kind": "view"},
                            {"_id": "2", "kind": "click"},
                        ]
                    )
                    await collection.create_index([("kind", 1)], name="kind_idx")

                    cursor = collection.find({"kind": "view"}).hint("kind_idx")
                    clone = cursor.clone().limit(1)

                    self.assertEqual(cursor.collection.full_name, "alpha.events")
                    self.assertEqual(clone.collection.full_name, "alpha.events")
                    self.assertTrue(cursor.alive)
                    self.assertEqual(await clone.to_list(), [{"_id": "1", "kind": "view"}])
                    self.assertEqual(await cursor.to_list(), [{"_id": "1", "kind": "view"}])

    async def test_start_session_inherits_default_transaction_options_from_client(self):
        transaction_options = TransactionOptions(
            write_concern=WriteConcern("majority"),
            max_commit_time_ms=200,
        )

        async with AsyncMongoClient(
            MemoryEngine(),
            transaction_options=transaction_options,
        ) as client:
            session = client.start_session()
            session.start_transaction()

            self.assertEqual(session.default_transaction_options, transaction_options)
            self.assertEqual(session.transaction_options, transaction_options)

    async def test_list_collections_rejects_invalid_filter(self):
        async with open_client("memory") as client:
            with self.assertRaises(TypeError):
                client.alpha.list_collections(["bad"])  # type: ignore[arg-type]
            with self.assertRaises(TypeError):
                await client.alpha.list_collection_names(["bad"])  # type: ignore[arg-type]

    async def test_database_admin_methods_respect_session_bound_sqlite_transactions(self):
        async with AsyncMongoClient(SQLiteEngine()) as client:
            session = client.start_session()
            session.start_transaction()

            await client.alpha.create_collection("events", session=session)

            self.assertEqual(await client.list_database_names(session=session), ["alpha"])
            self.assertEqual(await client.alpha.list_collection_names(session=session), ["events"])

            session.abort_transaction()

            self.assertEqual(await client.list_database_names(), [])
            self.assertEqual(await client.alpha.list_collection_names(), [])

    async def test_database_command_supports_ping_list_collections_and_drop_database(self):
        await assert_database_command_supports_ping_list_collections_and_drop_database(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_collection_index_count_and_distinct_commands(self):
        await assert_database_command_supports_collection_index_count_and_distinct_commands(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_index_commands_support_comment_and_max_time(self):
        await assert_database_command_index_commands_support_comment_and_max_time(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_count_supports_skip_limit_hint_and_comment(self):
        await assert_database_command_count_supports_skip_limit_hint_and_comment(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_distinct_supports_hint_comment_and_max_time(self):
        await assert_database_command_distinct_supports_hint_comment_and_max_time(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_rename_collection_within_current_database(self):
        await assert_database_command_supports_rename_collection_within_current_database(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_find_and_aggregate(self):
        await assert_database_command_supports_find_and_aggregate(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_explain_for_find_and_aggregate(self):
        await assert_database_command_supports_explain_for_find_and_aggregate(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_explain_for_update_and_delete(self):
        await assert_database_command_supports_explain_for_update_and_delete(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_explain_for_count_distinct_and_find_and_modify(self):
        await assert_database_command_supports_explain_for_count_distinct_and_find_and_modify(
            self,
            ENGINE_FACTORIES,
            open_client,
        )

    async def test_database_command_supports_db_hash_and_profile_status(self):
        await assert_database_command_supports_db_hash_and_profile_status(
            self,
            ENGINE_FACTORIES,
            open_client,
        )

    async def test_database_command_supports_insert_update_and_delete(self):
        await assert_database_command_supports_insert_update_and_delete(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_write_commands_surface_write_errors(self):
        await assert_database_command_write_commands_surface_write_errors(self, open_client)

    async def test_database_command_supports_find_and_modify(self):
        await assert_database_command_supports_find_and_modify(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_coll_stats_and_db_stats(self):
        await assert_database_command_supports_coll_stats_and_db_stats(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_supports_configure_fail_point(self):
        await assert_database_command_supports_configure_fail_point(self, ENGINE_FACTORIES, open_client)

    async def test_database_command_rejects_unsupported_commands(self):
        await assert_database_command_rejects_unsupported_commands(self, open_client)

    async def test_database_command_rejects_invalid_command_shapes(self):
        await assert_database_command_rejects_invalid_command_shapes(self, open_client)

    async def test_validate_collection_returns_metadata_and_rejects_missing_namespace(self):
        await assert_validate_collection_returns_metadata_and_rejects_missing_namespace(self, ENGINE_FACTORIES, open_client)

    async def test_collection_rename_moves_documents_and_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.alpha.events
                    await collection.insert_one({"_id": "1", "kind": "view"})
                    await collection.create_index([("kind", 1)], name="kind_idx")

                    renamed = await collection.rename("archived")

                    self.assertEqual(renamed.name, "archived")
                    self.assertEqual(await client.alpha.list_collection_names(), ["archived"])
                    self.assertEqual(await renamed.find_one({"_id": "1"}), {"_id": "1", "kind": "view"})
                    self.assertIn("kind_idx", await renamed.index_information())
                    self.assertEqual(await renamed.options(), {})

    async def test_collection_rename_preserves_options_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = await client.alpha.create_collection(
                        "events",
                        capped=True,
                        size=512,
                    )

                    renamed = await collection.rename("archived")

                    self.assertEqual(
                        await renamed.options(),
                        {"capped": True, "size": 512},
                    )

    async def test_collection_rename_rejects_conflicting_or_identical_names(self):
        async with open_client("memory") as client:
            await client.alpha.events.insert_one({"_id": "1"})
            await client.alpha.logs.insert_one({"_id": "2"})

            with self.assertRaises(CollectionInvalid):
                await client.alpha.events.rename("logs")
            with self.assertRaises(CollectionInvalid):
                await client.alpha.events.rename("events")

    async def test_find_one_without_filter_returns_first_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    found = await collection.find_one()

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_find_supports_implicit_regex_literals_and_in_regex(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.users.insert_many(
                        [
                            {"_id": "1", "name": "MongoDB", "tags": ["beta", "stable"]},
                            {"_id": "2", "name": "Postgres", "tags": ["alpha", "stable"]},
                        ]
                    )

                    implicit = await client.alpha.users.find(
                        {"name": re.compile("^mongo", re.IGNORECASE)},
                        sort=[("_id", 1)],
                    ).to_list()
                    eq_regex = await client.alpha.users.find(
                        {"name": {"$eq": re.compile("^mongo", re.IGNORECASE)}},
                        sort=[("_id", 1)],
                    ).to_list()
                    in_regex = await client.alpha.users.find(
                        {"tags": {"$in": [re.compile("^be"), re.compile("^zz")]}},
                        sort=[("_id", 1)],
                    ).to_list()

                    self.assertEqual([document["_id"] for document in implicit], ["1"])
                    self.assertEqual([document["_id"] for document in eq_regex], ["1"])
                    self.assertEqual([document["_id"] for document in in_regex], ["1"])

    async def test_update_one_sort_is_profile_gated(self):
        engine = MemoryEngine()

        async with AsyncMongoClient(engine, pymongo_profile='4.9') as client:
            collection = client.test.users
            await collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
            await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})

            with self.assertRaises(TypeError):
                await collection.update_one(
                    {"kind": "view"},
                    {"$set": {"done": True}},
                    sort=[("rank", 1)],
                )

    async def test_update_one_sort_updates_first_sorted_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                engine = ENGINE_FACTORIES[engine_name]()
                async with AsyncMongoClient(engine, pymongo_profile='4.11') as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2, "done": False})
                    await collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "done": False})

                    result = await collection.update_one(
                        {"kind": "view"},
                        {"$set": {"done": True}},
                        sort=[("rank", 1)],
                    )
                    first = await collection.find_one({"_id": "1"})
                    second = await collection.find_one({"_id": "2"})

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(result.modified_count, 1)
                    self.assertFalse(first["done"])
                    self.assertTrue(second["done"])

    async def test_insert_one_generates_id_and_persists_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    payload = {
                        "name": "Ada",
                        "created_at": datetime.datetime(2026, 3, 23, 10, 0, 0),
                        "owner_id": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                    }

                    result = await collection.insert_one(payload)
                    found = await collection.find_one({"_id": result.inserted_id})

                    self.assertTrue(result.inserted_id)
                    self.assertEqual(found["_id"], result.inserted_id)
                    self.assertEqual(found["name"], "Ada")
                    self.assertEqual(found["created_at"], payload["created_at"])
                    self.assertEqual(found["owner_id"], payload["owner_id"])
                    self.assertEqual(payload["_id"], result.inserted_id)

    async def test_insert_one_duplicate_id_raises_duplicate_key_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "same", "name": "Ada"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "same", "name": "Grace"})

    async def test_insert_many_returns_ids_and_persists_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    result = await collection.insert_many(
                        [{"name": "Ada"}, {"_id": "grace", "name": "Grace"}]
                    )
                    documents = await collection.find({}, sort=[("name", 1)]).to_list()

                    self.assertEqual(len(result.inserted_ids), 2)
                    self.assertEqual(result.inserted_ids[1], "grace")
                    self.assertEqual([document["name"] for document in documents], ["Ada", "Grace"])
                    self.assertTrue(result.inserted_ids[0])

    async def test_bulk_write_supports_ordered_and_unordered_execution(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name](), pymongo_profile="4.11") as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "seed", "kind": "view", "rank": 2, "done": False})
                    await collection.insert_one({"_id": "other", "kind": "view", "rank": 1, "done": False})

                    success = await collection.bulk_write(
                        [
                            InsertOne({"_id": "new", "kind": "click"}),
                            UpdateOne({"kind": "view"}, {"$set": {"done": True}}, sort=[("rank", 1)]),
                            UpdateMany({"kind": "view"}, {"$set": {"tag": "seen"}}),
                            ReplaceOne({"_id": "new"}, {"kind": "click", "done": True}),
                            DeleteOne({"_id": "seed"}),
                            DeleteMany({"kind": "view"}),
                        ]
                    )

                    self.assertEqual(success.inserted_count, 1)
                    self.assertEqual(success.matched_count, 4)
                    self.assertEqual(success.modified_count, 4)
                    self.assertEqual(success.deleted_count, 2)
                    self.assertEqual(success.upserted_count, 0)
                    self.assertEqual(
                        await collection.find({}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "new", "kind": "click", "done": True}],
                    )

                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name]()) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "dup", "done": False})

                    with self.assertRaises(BulkWriteError) as ctx:
                        await collection.bulk_write(
                            [
                                InsertOne({"_id": "dup"}),
                                UpdateOne({"_id": "dup"}, {"$set": {"done": True}}),
                                DeleteOne({"_id": "dup"}),
                            ],
                            ordered=False,
                        )

                    self.assertEqual(ctx.exception.details["writeErrors"][0]["index"], 0)
                    self.assertEqual(ctx.exception.details["nModified"], 1)
                    self.assertEqual(ctx.exception.details["nRemoved"], 1)
                    self.assertEqual(await collection.find({}).to_list(), [])

    async def test_update_many_updates_all_matching_documents_and_supports_upsert(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view", "done": False})
                    await collection.insert_one({"_id": "2", "kind": "view", "done": False})
                    await collection.insert_one({"_id": "3", "kind": "click", "done": False})

                    result = await collection.update_many(
                        {"kind": "view"},
                        {"$set": {"done": True}},
                    )
                    upserted = await collection.update_many(
                        {"kind": "missing", "tenant": "a"},
                        {"$set": {"done": True}},
                        upsert=True,
                    )
                    documents = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(result.matched_count, 2)
                    self.assertEqual(result.modified_count, 2)
                    self.assertEqual(upserted.matched_count, 0)
                    self.assertEqual(upserted.modified_count, 0)
                    self.assertTrue(upserted.upserted_id)
                    self.assertEqual(
                        [(doc["_id"], doc["done"]) for doc in documents if doc["kind"] != "missing"],
                        [("1", True), ("2", True), ("3", False)],
                    )
                    upserted_document = await collection.find_one({"_id": upserted.upserted_id})
                    self.assertEqual(
                        upserted_document,
                        {"_id": upserted.upserted_id, "kind": "missing", "tenant": "a", "done": True},
                    )

    async def test_update_operators_min_max_mul_and_rename_are_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "score": 10, "rank": 2, "profile": {"name": "Ada"}},
                            {"_id": "2", "score": 4, "rank": 1},
                        ]
                    )

                    min_result = await collection.update_one({"_id": "1"}, {"$min": {"score": 7}})
                    max_result = await collection.update_one({"_id": "2"}, {"$max": {"score": 8}})
                    mul_result = await collection.update_many({}, {"$mul": {"score": 2}})
                    renamed = await collection.find_one_and_update(
                        {"_id": "1"},
                        {"$rename": {"profile.name": "profile.alias"}},
                        return_document=ReturnDocument.AFTER,
                    )
                    bulk = await collection.bulk_write(
                        [
                            UpdateOne({"_id": "1"}, {"$max": {"rank": 5}}),
                            UpdateOne({"_id": "2"}, {"$rename": {"score": "points"}}),
                        ]
                    )
                    first = await collection.find_one({"_id": "1"})
                    second = await collection.find_one({"_id": "2"})

                    self.assertEqual(min_result.modified_count, 1)
                    self.assertEqual(max_result.modified_count, 1)
                    self.assertEqual(mul_result.modified_count, 2)
                    self.assertEqual(renamed["profile"], {"alias": "Ada"})
                    self.assertEqual(bulk.modified_count, 2)
                    self.assertEqual(first["score"], 14)
                    self.assertEqual(first["rank"], 5)
                    self.assertEqual(second, {"_id": "2", "rank": 1, "points": 16})

    async def test_current_date_and_set_on_insert_are_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "name": "Ada"})

                    updated = await collection.update_one(
                        {"_id": "1"},
                        {"$currentDate": {"updated_at": True}},
                    )
                    inserted = await collection.update_one(
                        {"_id": "2"},
                        {
                            "$set": {"name": "Grace"},
                            "$setOnInsert": {"created_at": "seeded"},
                        },
                        upsert=True,
                    )
                    existing = await collection.update_one(
                        {"_id": "1"},
                        {"$setOnInsert": {"created_at": "ignored"}},
                    )
                    first = await collection.find_one({"_id": "1"})
                    second = await collection.find_one({"_id": "2"})

                    self.assertEqual(updated.modified_count, 1)
                    self.assertEqual(inserted.upserted_id, "2")
                    self.assertEqual(existing.modified_count, 0)
                    self.assertIsInstance(first["updated_at"], datetime.datetime)
                    self.assertNotIn("created_at", first)
                    self.assertEqual(second["created_at"], "seeded")

    async def test_array_filters_and_all_positional_updates_are_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "items": [{"qty": 1}, {"qty": 3}]})

                    update_one_result = await collection.update_one(
                        {"_id": "1"},
                        {"$inc": {"items.$[high].qty": 2}},
                        array_filters=[{"high.qty": {"$gte": 2}}],
                    )
                    update_many_result = await collection.update_many(
                        {"_id": "1"},
                        {"$set": {"items.$[].flag": True}},
                    )
                    bulk_result = await collection.bulk_write(
                        [
                            UpdateOne(
                                {"_id": "1"},
                                {"$max": {"items.$[high].qty": 10}},
                                array_filters=[{"high.qty": {"$gte": 5}}],
                            )
                        ]
                    )
                    updated = await collection.find_one_and_update(
                        {"_id": "1"},
                        {"$min": {"items.$[flagged].qty": 8}},
                        array_filters=[{"flagged.flag": True}],
                        return_document=ReturnDocument.AFTER,
                    )

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(update_many_result.modified_count, 1)
                    self.assertEqual(bulk_result.modified_count, 1)
                    self.assertEqual(
                        updated["items"],
                        [{"qty": 1, "flag": True}, {"qty": 8, "flag": True}],
                    )

    async def test_legacy_positional_operator_is_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "items": [{"qty": 1}, {"qty": 3}, {"qty": 4}]})

                    result = await collection.update_one(
                        {"items.qty": {"$gte": 3}},
                        {"$inc": {"items.$.qty": 2}},
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(result.modified_count, 1)
                    self.assertEqual(updated["items"], [{"qty": 1}, {"qty": 5}, {"qty": 4}])

    async def test_bit_update_operator_is_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "score": 13, "flags": [1, 3, 4]})

                    update_one_result = await collection.update_one(
                        {"_id": "1"},
                        {"$bit": {"score": {"and": 10}}},
                    )
                    update_many_result = await collection.update_many(
                        {"_id": "1"},
                        {"$bit": {"flags.$[flag]": {"xor": 2}}},
                        array_filters=[{"flag": {"$gte": 3}}],
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(update_many_result.modified_count, 1)
                    self.assertEqual(updated, {"_id": "1", "score": 8, "flags": [1, 1, 6]})

    async def test_pull_all_update_operator_is_observable_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "tags": ["python", "async", "python"], "nums": [1, 2, 3, 2]})

                    update_one_result = await collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"tags": ["python"], "nums": [2, 4]}},
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(update_one_result.modified_count, 1)
                    self.assertEqual(updated, {"_id": "1", "tags": ["async"], "nums": [1, 3]})

    async def test_array_mutation_operators_support_positional_paths_via_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "groups": [
                                {"name": "alpha", "tags": ["a"], "scores": [1, 2]},
                                {"name": "beta", "tags": ["b"], "scores": [2, 3]},
                            ],
                        }
                    )

                    push_result = await collection.update_one(
                        {"_id": "1"},
                        {"$push": {"groups.$[group].tags": {"$each": ["x"], "$position": 0}}},
                        array_filters=[{"group.name": "beta"}],
                    )
                    add_to_set_result = await collection.update_one(
                        {"_id": "1"},
                        {"$addToSet": {"groups.$[].tags": "common"}},
                    )
                    pull_result = await collection.update_one(
                        {"_id": "1"},
                        {"$pull": {"groups.$[group].tags": "x"}},
                        array_filters=[{"group.name": "beta"}],
                    )
                    pull_all_result = await collection.update_one(
                        {"_id": "1"},
                        {"$pullAll": {"groups.$[].scores": [2]}},
                    )
                    pop_result = await collection.update_one(
                        {"groups.name": "beta"},
                        {"$pop": {"groups.$.scores": 1}},
                    )
                    updated = await collection.find_one({"_id": "1"})

                    self.assertEqual(push_result.modified_count, 1)
                    self.assertEqual(add_to_set_result.modified_count, 1)
                    self.assertEqual(pull_result.modified_count, 1)
                    self.assertEqual(pull_all_result.modified_count, 1)
                    self.assertEqual(pop_result.modified_count, 1)
                    self.assertEqual(
                        updated["groups"],
                        [
                            {"name": "alpha", "tags": ["a", "common"], "scores": [1]},
                            {"name": "beta", "tags": ["b", "common"], "scores": []},
                        ],
                    )

    async def test_delete_many_deletes_all_matching_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view"})
                    await collection.insert_one({"_id": "2", "kind": "view"})
                    await collection.insert_one({"_id": "3", "kind": "click"})

                    result = await collection.delete_many({"kind": "view"})
                    remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(result.deleted_count, 2)
                    self.assertEqual(remaining, [{"_id": "3", "kind": "click"}])

    async def test_replace_one_and_find_one_and_family(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name](), pymongo_profile='4.11') as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2, "done": False})
                    await collection.insert_one({"_id": "2", "kind": "view", "rank": 1, "done": False})

                    replaced = await collection.replace_one(
                        {"kind": "view"},
                        {"kind": "view", "rank": 1, "done": True, "tag": "replaced"},
                        sort=[("rank", 1)],
                    )
                    before = await collection.find_one_and_update(
                        {"kind": "view"},
                        {"$set": {"done": False}},
                        sort=[("rank", 1)],
                        projection={"done": 1, "_id": 0},
                    )
                    after = await collection.find_one_and_replace(
                        {"kind": "view"},
                        {"kind": "view", "rank": 1, "done": True, "tag": "after"},
                        sort=[("rank", 1)],
                        return_document=ReturnDocument.AFTER,
                        projection={"done": 1, "tag": 1, "_id": 0},
                    )
                    deleted = await collection.find_one_and_delete(
                        {"kind": "view"},
                        sort=[("rank", -1)],
                        projection={"rank": 1, "_id": 0},
                    )
                    remaining = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(replaced.matched_count, 1)
                    self.assertEqual(replaced.modified_count, 1)
                    self.assertEqual(before, {"done": True})
                    self.assertEqual(after, {"done": True, "tag": "after"})
                    self.assertEqual(deleted, {"rank": 2})
                    self.assertEqual(
                        remaining,
                        [{"_id": "2", "kind": "view", "rank": 1, "done": True, "tag": "after"}],
                    )

    async def test_find_one_and_update_and_delete_support_positional_projection(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with AsyncMongoClient(ENGINE_FACTORIES[engine_name](), pymongo_profile='4.11') as client:
                    collection = client.test.users
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "students": [
                                {"school": 100, "age": 7},
                                {"school": 102, "age": 10},
                                {"school": 102, "age": 11},
                            ],
                        }
                    )

                    before = await collection.find_one_and_update(
                        {"_id": "1", "students.school": 102, "students.age": {"$gt": 10}},
                        {"$set": {"flag": True}},
                        return_document=ReturnDocument.BEFORE,
                        projection={"students.$": 1, "_id": 0},
                    )
                    deleted = await collection.find_one_and_delete(
                        {"_id": "1", "students.school": 102, "students.age": {"$gt": 10}},
                        projection={"students.$": 1, "_id": 0},
                    )

                    self.assertEqual(before, {"students": [{"school": 102, "age": 11}]})
                    self.assertEqual(deleted, {"students": [{"school": 102, "age": 11}]})

    async def test_find_one_and_update_upsert_returns_none_or_after_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    before = await collection.find_one_and_update(
                        {"kind": "missing", "tenant": "a"},
                        {"$set": {"done": True}},
                        upsert=True,
                    )
                    after = await collection.find_one_and_update(
                        {"kind": "another", "tenant": "b"},
                        {"$set": {"done": True}},
                        upsert=True,
                        return_document=ReturnDocument.AFTER,
                        projection={"kind": 1, "tenant": 1, "done": 1, "_id": 0},
                    )

                    self.assertIsNone(before)
                    self.assertEqual(after, {"kind": "another", "tenant": "b", "done": True})

    async def test_distinct_supports_scalars_arrays_nested_paths_and_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one(
                        {"_id": "1", "kind": "view", "tags": ["a", "b"], "profile": {"city": "Madrid"}}
                    )
                    await collection.insert_one(
                        {"_id": "2", "kind": "view", "tags": ["b", "c"], "profile": {"city": "Sevilla"}}
                    )
                    await collection.insert_one(
                        {"_id": "3", "kind": "click", "tags": [], "profile": {"city": "Madrid"}}
                    )

                    tags = await collection.distinct("tags")
                    cities = await collection.distinct("profile.city", {"kind": "view"})

                    self.assertEqual(tags, ["a", "b", "c"])
                    self.assertEqual(cities, ["Madrid", "Sevilla"])

    async def test_distinct_supports_document_values_and_arrays_of_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {
                                "_id": "1",
                                "profile": {"city": "Madrid"},
                                "items": [{"code": "a"}, {"code": "b"}],
                                "nested": {"items": [{"code": "a"}, {"code": "b"}]},
                            },
                            {
                                "_id": "2",
                                "profile": {"city": "Sevilla"},
                                "items": [{"code": "b"}, {"code": "c"}],
                                "nested": {"items": [{"code": "b"}, {"code": "c"}]},
                            },
                        ]
                    )

                    profiles = await collection.distinct("profile")
                    item_documents = await collection.distinct("items")
                    nested_codes = await collection.distinct("nested.items.code")

                    self.assertEqual(profiles, [{"city": "Madrid"}, {"city": "Sevilla"}])
                    self.assertEqual(
                        item_documents,
                        [{"code": "a"}, {"code": "b"}, {"code": "c"}],
                    )
                    self.assertEqual(nested_codes, ["a", "b", "c"])

    async def test_distinct_supports_nested_scalar_arrays(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "matrix": [[1, 2, 3], [3, 4]]},
                            {"_id": "2", "matrix": [[1, 2, 3], [5, 6]]},
                        ]
                    )

                    values = await collection.distinct("matrix")

                    self.assertEqual(values, [[1, 2, 3], [3, 4], [5, 6]])

    async def test_distinct_supports_hint_comment_and_max_time(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "kind": "view", "tag": "python"},
                            {"_id": "2", "kind": "view", "tag": "mongodb"},
                            {"_id": "3", "kind": "click", "tag": "python"},
                        ],
                        session=session,
                    )
                    await collection.create_index([("kind", 1)], name="kind_idx", session=session)

                    values = await collection.distinct(
                        "tag",
                        {"kind": "view"},
                        hint="kind_idx",
                        comment="trace-distinct",
                        max_time_ms=25,
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(values, ["python", "mongodb"])
                    self.assertEqual(state["last_operation"]["comment"], "trace-distinct")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    async def test_find_supports_type_query_operator(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "value": 7},
                            {"_id": "2", "value": "7"},
                            {"_id": "3", "value": [1, "x"]},
                        ]
                    )

                    numbers = [doc["_id"] async for doc in collection.find({"value": {"$type": "number"}})]
                    arrays = [doc["_id"] async for doc in collection.find({"value": {"$type": "array"}})]
                    strings_in_arrays = [doc["_id"] async for doc in collection.find({"value": {"$type": "string"}})]

                    self.assertEqual(numbers, ["1", "3"])
                    self.assertEqual(arrays, ["3"])
                    self.assertEqual(strings_in_arrays, ["2", "3"])

    async def test_find_supports_bitwise_query_operators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "mask": 0b1010},
                            {"_id": "2", "mask": 0b0101},
                            {"_id": "3", "mask": bytes([0b00000101])},
                        ]
                    )

                    all_set = [doc["_id"] async for doc in collection.find({"mask": {"$bitsAllSet": 0b1000}})]
                    any_clear = [doc["_id"] async for doc in collection.find({"mask": {"$bitsAnyClear": [1, 3]}})]
                    binary = [doc["_id"] async for doc in collection.find({"mask": {"$bitsAllSet": bytes([0b00000101])}})]

                    self.assertEqual(all_set, ["1"])
                    self.assertEqual(any_clear, ["2", "3"])
                    self.assertEqual(binary, ["2", "3"])

    async def test_find_one_projection_supports_inclusion_and_id_exclusion(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": "user-1"}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    async def test_find_one_projection_supports_exclusion_with_id_included(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": "user-1"}, {"role": 0, "_id": 1})

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_find_projection_supports_slice_elem_match_and_positional_operators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one(
                        {
                            "_id": "user-1",
                            "name": "Ada",
                            "tags": ["a", "b", "c", "d"],
                            "students": [
                                {"school": 100, "age": 7},
                                {"school": 102, "age": 10},
                                {"school": 102, "age": 11},
                            ],
                            "role": "admin",
                        }
                    )

                    sliced = await collection.find_one({"_id": "user-1"}, {"tags": {"$slice": [1, 2]}, "role": 0})
                    matched = await collection.find_one(
                        {"_id": "user-1"},
                        {"name": 1, "students": {"$elemMatch": {"school": 102, "age": {"$gt": 10}}}, "_id": 0},
                    )
                    positional = await collection.find_one(
                        {"_id": "user-1", "students.school": 102, "students.age": {"$gt": 10}},
                        {"students.$": 1, "_id": 0},
                    )

                    self.assertEqual(
                        sliced,
                        {
                            "_id": "user-1",
                            "name": "Ada",
                            "tags": ["b", "c"],
                            "students": [
                                {"school": 100, "age": 7},
                                {"school": 102, "age": 10},
                                {"school": 102, "age": 11},
                            ],
                        },
                    )
                    self.assertEqual(matched, {"name": "Ada", "students": [{"school": 102, "age": 11}]})
                    self.assertEqual(positional, {"students": [{"school": 102, "age": 11}]})

    async def test_find_one_supports_id_operator_filter_without_direct_lookup_crash(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    found = await collection.find_one({"_id": {"$eq": "user-1"}})

                    self.assertEqual(found, {"_id": "user-1", "name": "Ada"})

    async def test_update_and_delete_support_id_operator_filter(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "name": "Ada"})

                    update_result = await collection.update_one(
                        {"_id": {"$eq": "user-1"}},
                        {"$set": {"active": True}},
                    )
                    delete_result = await collection.delete_one({"_id": {"$eq": "user-1"}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    async def test_insert_one_supports_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}

                    result = await collection.insert_one({"_id": doc_id, "name": "Ada"})
                    found = await collection.find_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(result.inserted_id, doc_id)
                    self.assertEqual(found, {"_id": doc_id, "name": "Ada"})

    async def test_find_one_supports_list_id_via_direct_lookup(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = [1, 2, 3]
                    await collection.insert_one({"_id": doc_id, "name": "Ada"})

                    found = await collection.find_one({"_id": [1, 2, 3]})

                    self.assertEqual(found, {"_id": doc_id, "name": "Ada"})

    async def test_count_documents_uses_core_filtering_rules(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "payload": {"kind": "view"}})
                    await collection.insert_one({"_id": "2", "payload": {"kind": "click"}})
                    await collection.insert_one({"_id": "3", "payload": {"kind": "view"}})

                    count = await collection.count_documents({"payload.kind": "view"})

                    self.assertEqual(count, 2)

    async def test_count_documents_supports_skip_limit_hint_comment_and_max_time(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "kind": "view"},
                            {"_id": "2", "kind": "view"},
                            {"_id": "3", "kind": "view"},
                        ],
                        session=session,
                    )
                    await collection.create_index([("kind", 1)], name="kind_idx", session=session)

                    count = await collection.count_documents(
                        {"kind": "view"},
                        skip=1,
                        limit=1,
                        hint="kind_idx",
                        comment="trace-count-documents",
                        max_time_ms=25,
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(count, 1)
                    self.assertEqual(state["last_operation"]["comment"], "trace-count-documents")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)

    async def test_estimated_document_count_and_drop_collection_and_database(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    users = client.alpha.users
                    logs = client.alpha.logs
                    beta = client.beta.events
                    await users.insert_one({"_id": "1", "name": "Ada"})
                    await logs.insert_one({"_id": "1", "kind": "view"})
                    await beta.insert_one({"_id": "1", "kind": "beta"})

                    self.assertEqual(await users.estimated_document_count(), 1)
                    await client.alpha.drop_collection("logs")
                    self.assertEqual(await client.alpha.list_collection_names(), ["users"])

                    await users.drop()
                    self.assertEqual(await client.alpha.list_collection_names(), [])

                    await client.drop_database("beta")
                    self.assertNotIn("beta", await client.list_database_names())

    async def test_estimated_document_count_supports_comment_and_max_time(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "kind": "view"},
                            {"_id": "2", "kind": "click"},
                        ],
                        session=session,
                    )

                    count = await collection.estimated_document_count(
                        comment="trace-estimated-count",
                        max_time_ms=25,
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(count, 2)
                    self.assertEqual(state["last_operation"]["comment"], "trace-estimated-count")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)

                    await client.drop_database("alpha")
                    self.assertNotIn("alpha", await client.list_database_names())

    async def test_drop_operations_on_missing_targets_are_noops(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.drop_collection("missing")
                    await client.alpha.users.drop()
                    await client.drop_database("missing")
                    self.assertEqual(await client.list_database_names(), [])

    async def test_delete_last_document_keeps_collection_visible_until_drop(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.events.insert_one({"_id": "1"})
                    await client.alpha.events.delete_one({"_id": "1"})

                    self.assertEqual(await client.alpha.list_collection_names(), ["events"])

    async def test_drop_database_removes_sqlite_database_with_only_index_metadata(self):
        async with AsyncMongoClient(SQLiteEngine()) as client:
            await client.alpha.users.create_index(["email"], unique=False)

            self.assertIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), ["users"])

            await client.drop_database("alpha")

            self.assertNotIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), [])

    async def test_drop_database_removes_memory_database_with_only_index_metadata(self):
        async with AsyncMongoClient(MemoryEngine()) as client:
            await client.alpha.users.create_index(["email"], unique=False)

            self.assertIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), ["users"])

            await client.drop_database("alpha")

            self.assertNotIn("alpha", await client.list_database_names())
            self.assertEqual(await client.alpha.list_collection_names(), [])

    async def test_find_supports_iteration_with_sort_skip_and_limit(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 3})
                    await collection.insert_one({"_id": "2", "rank": 1})
                    await collection.insert_one({"_id": "3", "rank": 2})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {},
                            sort=[("rank", 1)],
                            skip=1,
                            limit=1,
                        )
                    ]

                    self.assertEqual(documents, [{"_id": "3", "rank": 2}])

    async def test_find_returns_async_cursor_with_to_list_and_first(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 3})
                    await collection.insert_one({"_id": "2", "rank": 1})
                    await collection.insert_one({"_id": "3", "rank": 2})

                    cursor = collection.find({})

                    self.assertIsInstance(cursor, AsyncCursor)
                    self.assertEqual(
                        await cursor.sort([("rank", 1)]).skip(1).limit(1).to_list(),
                        [{"_id": "3", "rank": 2}],
                    )
                    self.assertEqual(
                        await collection.find({}).sort([("rank", 1)]).first(),
                        {"_id": "2", "rank": 1},
                    )

    async def test_find_and_aggregate_raw_batches_return_bson_batches(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 3})
                    await collection.insert_one({"_id": "2", "rank": 1})
                    await collection.insert_one({"_id": "3", "rank": 2})

                    find_batches = await collection.find_raw_batches(
                        {},
                        sort=[("rank", 1)],
                        batch_size=2,
                    ).to_list()
                    aggregate_batches = await collection.aggregate_raw_batches(
                        [{"$sort": {"rank": 1}}, {"$project": {"_id": 1, "rank": 1}}],
                        batch_size=2,
                    ).to_list()

                    self.assertEqual(len(find_batches), 2)
                    self.assertEqual(decode_all(find_batches[0]), [{"_id": "2", "rank": 1}, {"_id": "3", "rank": 2}])
                    self.assertEqual(decode_all(find_batches[1]), [{"_id": "1", "rank": 3}])
                    self.assertEqual(len(aggregate_batches), 2)
                    self.assertEqual(decode_all(aggregate_batches[0]), [{"_id": "2", "rank": 1}, {"_id": "3", "rank": 2}])
                    self.assertEqual(decode_all(aggregate_batches[1]), [{"_id": "1", "rank": 3}])

    async def test_find_cursor_rejects_mutation_after_iteration_starts(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": 1})
                    await collection.insert_one({"_id": "2", "rank": 2})

                    cursor = collection.find({})
                    iterator = cursor.__aiter__()

                    self.assertEqual(await iterator.__anext__(), {"_id": "1", "rank": 1})

                    with self.assertRaises(InvalidOperation):
                        cursor.limit(1)
                    with self.assertRaises(InvalidOperation):
                        cursor.skip(1)
                    with self.assertRaises(InvalidOperation):
                        cursor.sort([("rank", 1)])

    async def test_aggregate_supports_minimal_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})

                    cursor = collection.aggregate(
                        [
                            {"$match": {"kind": "view"}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"payload.city": 1, "_id": 0}},
                        ]
                    )

                    self.assertIsInstance(cursor, AsyncAggregationCursor)
                    self.assertEqual(
                        await cursor.to_list(),
                        [{"payload": {"city": "Bilbao"}}, {"payload": {"city": "Sevilla"}}],
                    )
                    self.assertEqual(
                        await collection.aggregate([{"$sort": {"rank": 1}}]).first(),
                        {"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}},
                    )

    async def test_aggregate_supports_is_number_and_type_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": 7, "text": "Ada"})
                    await collection.insert_one({"_id": "2", "value": "7"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "value_type": {"$type": "$value"},
                                    "value_is_number": {"$isNumber": "$value"},
                                    "missing_type": {"$type": "$missing"},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "value_type": "int", "value_is_number": True, "missing_type": "missing"},
                            {"_id": "2", "value_type": "string", "value_is_number": False, "missing_type": "missing"},
                        ],
                    )

    async def test_aggregate_supports_scalar_coercion_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "int_text": "42", "float_text": "3.5", "truthy_text": "false", "zero": 0})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "as_int": {"$toInt": "$int_text"},
                                    "as_double": {"$toDouble": "$float_text"},
                                    "as_bool": {"$toBool": "$truthy_text"},
                                    "zero_bool": {"$toBool": "$zero"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "as_int": 42, "as_double": 3.5, "as_bool": True, "zero_bool": False}],
                    )

    async def test_aggregate_supports_date_math_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "start": datetime.datetime(2026, 3, 25, 10, 0, 0),
                            "end": datetime.datetime(2026, 3, 27, 9, 0, 0),
                        }
                    )

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "added": {"$dateAdd": {"startDate": "$start", "unit": "day", "amount": 2}},
                                    "subtracted": {"$dateSubtract": {"startDate": "$start", "unit": "hour", "amount": 3}},
                                    "diff_days": {"$dateDiff": {"startDate": "$start", "endDate": "$end", "unit": "day"}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "added": datetime.datetime(2026, 3, 27, 10, 0, 0),
                                "subtracted": datetime.datetime(2026, 3, 25, 7, 0, 0),
                                "diff_days": 2,
                            }
                        ],
                    )

    async def test_aggregate_supports_substr_and_strlen_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "text": "é寿司A"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "byte_substr": {"$substrBytes": ["$text", 0, 2]},
                                    "cp_substr": {"$substrCP": ["$text", 1, 2]},
                                    "byte_len": {"$strLenBytes": "$text"},
                                    "cp_len": {"$strLenCP": "$text"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "byte_substr": "é", "cp_substr": "寿司", "byte_len": 9, "cp_len": 4}],
                    )

    async def test_aggregate_supports_string_index_and_binary_size_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "text": "é寿司A", "blob": b"abcd"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "byte_index": {"$indexOfBytes": ["$text", "寿", 0, 9]},
                                    "cp_index": {"$indexOfCP": ["$text", "司A", 0, 4]},
                                    "blob_size": {"$binarySize": "$blob"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "byte_index": 2, "cp_index": 2, "blob_size": 4}],
                    )

    async def test_aggregate_supports_regex_match_find_and_find_all(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "text": "Ada and ada"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "matched": {"$regexMatch": {"input": "$text", "regex": "^ada", "options": "i"}},
                                    "found": {"$regexFind": {"input": "$text", "regex": "(a)(d)a", "options": "i"}},
                                    "found_all": {"$regexFindAll": {"input": "$text", "regex": "a", "options": "i"}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "matched": True,
                                "found": {"match": "Ada", "idx": 0, "captures": ["A", "d"]},
                                "found_all": [
                                    {"match": "A", "idx": 0, "captures": []},
                                    {"match": "a", "idx": 2, "captures": []},
                                    {"match": "a", "idx": 4, "captures": []},
                                    {"match": "a", "idx": 8, "captures": []},
                                    {"match": "a", "idx": 10, "captures": []},
                                ],
                            }
                        ],
                    )

    async def test_aggregate_supports_numeric_math_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": 19.25, "base": 100})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "absolute": {"$abs": -4},
                                    "natural_log": {"$ln": {"$exp": 1}},
                                    "base_log": {"$log": ["$base", 10]},
                                    "base10_log": {"$log10": "$base"},
                                    "power": {"$pow": [2, 3]},
                                    "rounded": {"$round": ["$value", 1]},
                                    "truncated": {"$trunc": ["$value", -1]},
                                    "root": {"$sqrt": 25},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["absolute"], 4)
                    self.assertAlmostEqual(documents[0]["natural_log"], 1.0)
                    self.assertAlmostEqual(documents[0]["base_log"], 2.0)
                    self.assertAlmostEqual(documents[0]["base10_log"], 2.0)
                    self.assertEqual(documents[0]["power"], 8.0)
                    self.assertEqual(documents[0]["rounded"], 19.2)
                    self.assertEqual(documents[0]["truncated"], 10)
                    self.assertEqual(documents[0]["root"], 5.0)

    async def test_aggregate_supports_week_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "created_at": datetime.datetime(2026, 1, 1, 23, 30, 0)})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "week": {"$week": "$created_at"},
                                    "iso_week": {"$isoWeek": "$created_at"},
                                    "iso_week_year": {"$isoWeekYear": "$created_at"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "week": 0, "iso_week": 1, "iso_week_year": 2026}],
                    )

    async def test_aggregate_supports_date_string_parts_and_parsing_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "created_at": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000)})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "formatted": {
                                        "$dateToString": {
                                            "date": "$created_at",
                                            "format": "%Y-%m-%d %H:%M:%S.%L",
                                            "timezone": "UTC",
                                        }
                                    },
                                    "parts": {"$dateToParts": {"date": "$created_at", "timezone": "UTC"}},
                                    "parsed": {
                                        "$dateFromString": {
                                            "dateString": "2026-03-25 12:05:06.789",
                                            "format": "%Y-%m-%d %H:%M:%S.%L",
                                            "timezone": "+02:00",
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["formatted"], "2026-03-25 10:05:06.789")
                    self.assertEqual(
                        documents[0]["parts"],
                        {
                            "year": 2026,
                            "month": 3,
                            "day": 25,
                            "hour": 10,
                            "minute": 5,
                            "second": 6,
                            "millisecond": 789,
                        },
                    )
                    self.assertEqual(documents[0]["parsed"], datetime.datetime(2026, 3, 25, 10, 5, 6, 789000))

    async def test_aggregate_supports_object_to_array_and_zip(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "doc": {"a": 1, "b": 2}, "left": ["a", "b"], "right": [1], "defaults": ["x", 0]})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "pairs": {"$objectToArray": "$doc"},
                                    "zipped": {
                                        "$zip": {
                                            "inputs": ["$left", "$right"],
                                            "useLongestLength": True,
                                            "defaults": "$defaults",
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "pairs": [{"k": "a", "v": 1}, {"k": "b", "v": 2}],
                                "zipped": [["a", 1], ["b", 0]],
                            }
                        ],
                    )

    async def test_aggregate_supports_to_date_and_date_from_parts(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "millis": 1_711_361_506_789, "text": "2026-03-25T10:05:06.789Z"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "as_date": {"$toDate": "$text"},
                                    "from_parts": {
                                        "$dateFromParts": {
                                            "year": 2026,
                                            "month": 3,
                                            "day": 25,
                                            "hour": 12,
                                            "timezone": "+02:00",
                                        }
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "as_date": datetime.datetime(2026, 3, 25, 10, 5, 6, 789000),
                                "from_parts": datetime.datetime(2026, 3, 25, 10, 0, 0),
                            }
                        ],
                    )

    async def test_aggregate_supports_to_object_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "oid_text": "65f0a1000000000000000000"})

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "oid": {"$toObjectId": "$oid_text"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "oid": ObjectId("65f0a1000000000000000000")}],
                    )

    async def test_aggregate_supports_to_decimal(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": "10.25"})

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "decimal": {"$toDecimal": "$value"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "decimal": decimal.Decimal("10.25")}],
                    )

    async def test_aggregate_supports_to_uuid(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "uuid_text": "12345678-1234-5678-1234-567812345678",
                        }
                    )

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "uuid": {"$toUUID": "$uuid_text"}}}]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "uuid": uuid.UUID("12345678-1234-5678-1234-567812345678"),
                            }
                        ],
                    )

    async def test_aggregate_supports_documents_stage(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "persisted", "score": 1})

                    documents = await collection.aggregate(
                        [
                            {
                                "$documents": [
                                    {"_id": "1", "score": 10},
                                    {"_id": "2", "score": 20},
                                ]
                            },
                            {"$match": {"score": {"$gte": 15}}},
                        ]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "2", "score": 20}])

    async def test_aggregate_supports_date_part_extractors(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "created_at": datetime.datetime(2026, 3, 29, 22, 5, 6, 789000),
                        }
                    )

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "year": {"$year": "$created_at"},
                                    "month": {"$month": "$created_at"},
                                    "day_of_month": {
                                        "$dayOfMonth": {
                                            "date": "$created_at",
                                            "timezone": "+02:00",
                                        }
                                    },
                                    "day_of_week": {
                                        "$dayOfWeek": {
                                            "date": "$created_at",
                                            "timezone": "+02:00",
                                        }
                                    },
                                    "day_of_year": {"$dayOfYear": "$created_at"},
                                    "hour": {
                                        "$hour": {
                                            "date": "$created_at",
                                            "timezone": "+02:00",
                                        }
                                    },
                                    "minute": {"$minute": "$created_at"},
                                    "second": {"$second": "$created_at"},
                                    "millisecond": {"$millisecond": "$created_at"},
                                    "iso_day_of_week": {"$isoDayOfWeek": "$created_at"},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "year": 2026,
                                "month": 3,
                                "day_of_month": 30,
                                "day_of_week": 2,
                                "day_of_year": 88,
                                "hour": 0,
                                "minute": 5,
                                "second": 6,
                                "millisecond": 789,
                                "iso_day_of_week": 7,
                            }
                        ],
                    )

    async def test_aggregate_supports_slice_is_array_and_cmp(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "values": [1, 2, 3, 4],
                            "nested": {"a": 1},
                        }
                    )

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "head": {"$slice": ["$values", 2]},
                                    "tail": {"$slice": ["$values", -2]},
                                    "middle": {"$slice": ["$values", 1, 2]},
                                    "is_array": {"$isArray": "$values"},
                                    "cmp": {"$cmp": [3, 2]},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "head": [1, 2],
                                "tail": [3, 4],
                                "middle": [2, 3],
                                "is_array": True,
                                "cmp": 1,
                            }
                        ],
                    )

    async def test_aggregate_supports_convert_set_field_and_unset_field(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": "10", "nested": {"a": 1}})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "converted": {"$convert": {"input": "$value", "to": "int"}},
                                    "updated": {"$setField": {"field": "name", "input": "$nested", "value": "Ada"}},
                                    "trimmed": {"$unsetField": {"field": "a", "input": "$nested"}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "converted": 10, "updated": {"a": 1, "name": "Ada"}, "trimmed": {}}],
                    )

    async def test_aggregate_supports_bson_size_and_rand(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "doc": {"a": 1, "name": "Ada"}})

                    documents = await collection.aggregate(
                        [{"$project": {"_id": 1, "size": {"$bsonSize": "$doc"}, "random": {"$rand": {}}}}]
                    ).to_list()

                    self.assertEqual(documents[0]["_id"], "1")
                    self.assertEqual(documents[0]["size"], 26)
                    self.assertGreaterEqual(documents[0]["random"], 0.0)
                    self.assertLess(documents[0]["random"], 1.0)

    async def test_aggregate_supports_switch_and_bitwise_variants(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "value": 5})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 1,
                                    "size": {
                                        "$switch": {
                                            "branches": [{"case": {"$gt": ["$value", 10]}, "then": "big"}],
                                            "default": "small",
                                        }
                                    },
                                    "anded": {"$bitAnd": [7, 3]},
                                    "notted": {"$bitNot": 7},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "size": "small", "anded": 3, "notted": -8}],
                    )

    async def test_aggregate_supports_async_iteration_and_validates_pipeline_type(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1})

                    documents = [
                        document
                        async for document in collection.aggregate([{"$sort": {"rank": 1}}])
                    ]

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "2", "kind": "click", "rank": 1},
                            {"_id": "1", "kind": "view", "rank": 2},
                        ],
                    )
                    with self.assertRaises(TypeError):
                        collection.aggregate({})

    async def test_aggregate_supports_unwind(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tags": ["python", "mongodb"]})
                    await collection.insert_one({"_id": "2", "tags": ["sqlite"]})

                    documents = await collection.aggregate(
                        [
                            {"$unwind": "$tags"},
                            {"$sort": {"tags": 1}},
                            {"$project": {"tags": 1, "_id": 0}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"tags": "mongodb"},
                            {"tags": "python"},
                            {"tags": "sqlite"},
                        ],
                    )

    async def test_aggregate_supports_group_and_computed_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "amount": 10, "user": "ada"})
                    await collection.insert_one({"_id": "2", "kind": "view", "amount": 7, "bonus": 1, "user": "grace"})
                    await collection.insert_one({"_id": "3", "kind": "click", "amount": 3, "bonus": 2, "user": "alan"})

                    documents = await collection.aggregate(
                        [
                            {"$addFields": {"effective_amount": {"$add": ["$amount", {"$ifNull": ["$bonus", 0]}]}}},
                            {"$match": {"$expr": {"$gte": ["$effective_amount", 5]}}},
                            {"$group": {"_id": "$kind", "total": {"$sum": "$effective_amount"}, "first_user": {"$first": "$user"}}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "click", "total": 5, "first_user": "alan"},
                            {"_id": "view", "total": 18, "first_user": "ada"},
                        ],
                    )

    async def test_aggregate_supports_lookup_and_replace_root(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "name": "Ada", "city": "Sevilla"})
                    await users.insert_one({"_id": "u2", "name": "Grace", "city": "Madrid"})
                    await events.insert_one({"_id": "1", "kind": "view", "user_id": "u1"})
                    await events.insert_one({"_id": "2", "kind": "click", "user_id": "u2"})

                    documents = await events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "user_id", "foreignField": "_id", "as": "user"}},
                            {"$addFields": {"user": {"$arrayElemAt": ["$user", 0]}}},
                            {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", "$user"]}}},
                            {"$project": {"user": 0, "user_id": 0}},
                            {"$sort": {"kind": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "u2", "kind": "click", "name": "Grace", "city": "Madrid"},
                            {"_id": "u1", "kind": "view", "name": "Ada", "city": "Sevilla"},
                        ],
                    )

    async def test_aggregate_supports_merge_objects_over_lookup_result_array(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    deliveries = client.analytics.deliveries
                    units = client.analytics.units
                    await deliveries.insert_one({"_id": "d1", "tenant": "a"})
                    await units.insert_one({"_id": "u1", "fulfillment_id": "d1", "name": "Box"})
                    await units.insert_one({"_id": "u2", "fulfillment_id": "d1", "status": "packed"})

                    documents = await deliveries.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "units",
                                    "as": "unit",
                                    "let": {"ref_key": "$_id"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$eq": ["$fulfillment_id", "$$ref_key"]},
                                                    ]
                                                }
                                            }
                                        }
                                    ],
                                }
                            },
                            {"$match": {"$expr": {"$gt": [{"$size": "$unit"}, 0]}}},
                            {"$addFields": {"unit_list": "$unit", "unit": {"$mergeObjects": "$unit"}}},
                            {"$project": {"_id": 0, "unit_list": 1, "unit": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(len(documents), 1)
                    self.assertIsInstance(documents[0]["unit_list"], list)
                    self.assertEqual(len(documents[0]["unit_list"]), 2)
                    self.assertEqual(documents[0]["unit"]["name"], "Box")
                    self.assertEqual(documents[0]["unit"]["status"], "packed")

    async def test_aggregate_supports_array_transform_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {"_id": "1", "tags": ["a", "b", "c"], "other_tags": ["b", "d"], "numbers": [1, 2, 3, 4]}
                    )

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "mapped": {"$map": {"input": "$tags", "as": "tag", "in": {"$toString": "$$tag"}}},
                                    "filtered": {"$filter": {"input": "$numbers", "as": "n", "cond": {"$gt": ["$$n", 2]}}},
                                    "reduced": {"$reduce": {"input": "$numbers", "initialValue": 0, "in": {"$add": ["$$value", "$$this"]}}},
                                    "concatenated": {"$concatArrays": ["$tags", "$other_tags"]},
                                    "unioned": {"$setUnion": ["$tags", "$other_tags"]},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "mapped": ["a", "b", "c"],
                                "filtered": [3, 4],
                                "reduced": 10,
                                "concatenated": ["a", "b", "c", "b", "d"],
                                "unioned": ["a", "b", "c", "d"],
                            }
                        ],
                    )

    async def test_aggregate_supports_lookup_with_multiple_and_missing_matches(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a"})
                    await users.insert_one({"_id": "u2", "tenant": "a"})
                    await users.insert_one({"_id": "u3"})
                    await events.insert_one({"_id": "1", "tenant": "a"})
                    await events.insert_one({"_id": "2", "tenant": "missing"})
                    await events.insert_one({"_id": "3"})

                    documents = await events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
                            {"$project": {"_id": 1, "users": 1}},
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "users": [{"_id": "u1", "tenant": "a"}, {"_id": "u2", "tenant": "a"}]},
                            {"_id": "2", "users": []},
                            {"_id": "3", "users": [{"_id": "u3"}]},
                        ],
                    )

    async def test_aggregate_supports_lookup_with_let_and_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                    await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                    await events.insert_one({"_id": "1", "tenant": "a"})
                    await events.insert_one({"_id": "2", "tenant": "b"})

                    documents = await events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                                        {"$project": {"_id": 0, "name": 1}},
                                        {"$sort": {"name": 1}},
                                    ],
                                    "as": "users",
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                            {"_id": "2", "tenant": "b", "users": [{"name": "Linus"}]},
                        ],
                    )

    async def test_aggregate_supports_lookup_with_local_foreign_and_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace"})
                    await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus"})
                    await users.insert_one({"_id": "u4", "tenant": "c", "name": "Nope"})
                    await events.insert_one({"_id": "1", "tenant": "a"})
                    await events.insert_one({"_id": "2", "tenant": "b"})

                    documents = await events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "localField": "tenant",
                                    "foreignField": "tenant",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                                        {"$project": {"_id": 0, "name": 1}},
                                        {"$sort": {"name": 1}},
                                    ],
                                    "as": "users",
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                            {"_id": "2", "tenant": "b", "users": [{"name": "Linus"}]},
                        ],
                    )

    async def test_aggregate_supports_field_bound_logical_expr_conditions_inside_lookup(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "role": "admin", "score": 3})
                    await users.insert_one({"_id": "u2", "tenant": "a", "role": "staff", "score": 4})
                    await users.insert_one({"_id": "u3", "tenant": "a", "role": "guest", "score": 4})
                    await users.insert_one({"_id": "u4", "tenant": "a", "role": "admin", "score": 7})
                    await events.insert_one({"_id": "1", "tenant": "a"})

                    documents = await events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$eq": ["$tenant", "$$tenantId"]},
                                                        {"$or": ["$role", [{"$eq": "admin"}, {"$eq": "staff"}]]},
                                                        {"$and": ["$score", [{"$gte": 3}, {"$lt": 5}]]},
                                                    ]
                                                }
                                            }
                                        },
                                        {"$project": {"_id": 0, "role": 1, "score": 1}},
                                        {"$sort": {"role": 1}},
                                    ],
                                    "as": "users",
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "tenant": "a",
                                "users": [
                                    {"role": "admin", "score": 3},
                                    {"role": "staff", "score": 4},
                                ],
                            }
                        ],
                    )

    async def test_aggregate_supports_field_bound_query_filter_expr_conditions_inside_lookup(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one(
                        {
                            "_id": "u1",
                            "tenant": "a",
                            "tags": ["a", "b"],
                            "status": "active",
                            "items": [{"kind": "x", "qty": 1}, {"kind": "y", "qty": 3}],
                        }
                    )
                    await users.insert_one(
                        {
                            "_id": "u2",
                            "tenant": "a",
                            "tags": ["a"],
                            "status": "archived",
                            "items": [{"kind": "y", "qty": 1}],
                        }
                    )
                    await users.insert_one({"_id": "u3", "tenant": "a", "status": "active"})
                    await events.insert_one({"_id": "1", "tenant": "a"})

                    documents = await events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$eq": ["$tenant", "$$tenantId"]},
                                                        {"$exists": ["$tags", True]},
                                                        {"$all": ["$tags", ["a", "b"]]},
                                                        {"$nin": ["$status", ["archived"]]},
                                                        {"$elemMatch": ["$items", {"kind": "y", "qty": {"$gte": 2}}]},
                                                    ]
                                                }
                                            }
                                        },
                                        {"$project": {"_id": 1}},
                                    ],
                                    "as": "users",
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [{"_id": "1", "tenant": "a", "users": [{"_id": "u1"}]}],
                    )

    async def test_aggregate_supports_correlated_list_lookup_with_in_and_dotted_variable_path(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "name": "Grace"})
                    await users.insert_one({"_id": "u3", "name": "Linus"})
                    await events.insert_one({"_id": "1", "links": [{"id": "u1"}, {"id": "u3"}]})
                    await events.insert_one({"_id": "2", "links": []})
                    await events.insert_one({"_id": "3"})

                    documents = await events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"ref_key": "$links"},
                                    "pipeline": [
                                        {
                                            "$match": {
                                                "$expr": {
                                                    "$and": [
                                                        {"$gt": [{"$size": {"$ifNull": ["$$ref_key.id", []]}}, 0]},
                                                        {"$in": ["$_id", "$$ref_key.id"]},
                                                    ]
                                                }
                                            }
                                        },
                                        {"$project": {"_id": 0, "name": 1}},
                                        {"$sort": {"name": 1}},
                                    ],
                                    "as": "users",
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "links": [{"id": "u1"}, {"id": "u3"}], "users": [{"name": "Ada"}, {"name": "Linus"}]},
                            {"_id": "2", "links": [], "users": []},
                            {"_id": "3", "users": []},
                        ],
                    )

    async def test_aggregate_supports_lookup_with_missing_foreign_collection(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    await events.insert_one({"_id": "1", "tenant": "a"})

                    documents = await events.aggregate(
                        [
                            {"$lookup": {"from": "users", "localField": "tenant", "foreignField": "tenant", "as": "users"}},
                            {"$project": {"_id": 1, "users": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "1", "users": []}])

    async def test_aggregate_supports_nested_lookup_inside_lookup_pipeline(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    roles = client.analytics.roles
                    await roles.insert_one({"_id": "r1", "label": "admin"})
                    await roles.insert_one({"_id": "r2", "label": "staff"})
                    await users.insert_one({"_id": "u1", "tenant": "a", "role_id": "r1", "name": "Ada"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "role_id": "r2", "name": "Grace"})
                    await events.insert_one({"_id": "1", "tenant": "a"})

                    documents = await events.aggregate(
                        [
                            {
                                "$lookup": {
                                    "from": "users",
                                    "let": {"tenantId": "$tenant"},
                                    "pipeline": [
                                        {"$match": {"$expr": {"$eq": ["$tenant", "$$tenantId"]}}},
                                        {"$lookup": {"from": "roles", "localField": "role_id", "foreignField": "_id", "as": "roles"}},
                                        {"$project": {"_id": 0, "name": 1, "roles": 1}},
                                        {"$sort": {"name": 1}},
                                    ],
                                    "as": "users",
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "tenant": "a",
                                "users": [
                                    {"name": "Ada", "roles": [{"_id": "r1", "label": "admin"}]},
                                    {"name": "Grace", "roles": [{"_id": "r2", "label": "staff"}]},
                                ],
                            }
                        ],
                    )

    async def test_aggregate_supports_join_operator_combinations(self):
        pipelines = {
            "no_join": [
                {"$match": {"$expr": {"$eq": ["$tenant", "a"]}}},
                {"$project": {"_id": 1, "tenant": 1}},
            ],
            "inner_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$match": {"$expr": {"$gt": [{"$size": "$users"}, 0]}}},
                {"$project": {"_id": 1, "tenant": 1, "users": 1}},
                {"$sort": {"_id": 1}},
            ],
            "document_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$addFields": {"joined_user": {"$mergeObjects": [{"tenant": "$tenant"}, {"$first": "$users"}]}}},
                {"$project": {"_id": 1, "joined_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            "left_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {
                    "$addFields": {
                        "joined_user": {
                            "$cond": [
                                {"$gt": [{"$size": "$users"}, 0]},
                                {"$arrayElemAt": ["$users", 0]},
                                {"name": "unknown"},
                            ]
                        }
                    }
                },
                {"$project": {"_id": 1, "joined_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            "count_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$set": {"user_count": {"$size": "$users"}}},
                {"$project": {"_id": 1, "user_count": 1}},
                {"$sort": {"_id": 1}},
            ],
            "aggregated_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$tenant"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$tenant", "$$ref_key"]}}},
                            {"$project": {"_id": 0, "name": 1}},
                            {"$sort": {"name": 1}},
                        ],
                        "as": "users",
                    }
                },
                {"$set": {"primary_user": {"$ifNull": [{"$first": "$users"}, {"name": "unknown"}]}}},
                {"$project": {"_id": 1, "primary_user": 1}},
                {"$sort": {"_id": 1}},
            ],
            "merge_join": [
                {
                    "$lookup": {
                        "from": "users",
                        "let": {"ref_key": "$user_id"},
                        "pipeline": [
                            {"$match": {"$expr": {"$eq": ["$_id", "$$ref_key"]}}},
                            {"$project": {"name": 1, "role": 1}},
                        ],
                        "as": "user_doc",
                    }
                },
                {"$replaceRoot": {"newRoot": {"$mergeObjects": ["$$ROOT", {"$arrayElemAt": ["$user_doc", 0]}]}}},
                {"$project": {"user_doc": 0}},
                {"$sort": {"_id": 1}},
            ],
        }
        expected = {
            "no_join": [{"_id": "e1", "tenant": "a"}],
            "inner_join": [
                {"_id": "e1", "tenant": "a", "users": [{"name": "Ada"}, {"name": "Grace"}]},
                {"_id": "e2", "tenant": "b", "users": [{"name": "Linus"}]},
            ],
            "document_join": [
                {"_id": "e1", "joined_user": {"tenant": "a", "name": "Ada"}},
                {"_id": "e2", "joined_user": {"tenant": "b", "name": "Linus"}},
                {"_id": "e3", "joined_user": {"tenant": "missing"}},
            ],
            "left_join": [
                {"_id": "e1", "joined_user": {"name": "Ada"}},
                {"_id": "e2", "joined_user": {"name": "Linus"}},
                {"_id": "e3", "joined_user": {"name": "unknown"}},
            ],
            "count_join": [
                {"_id": "e1", "user_count": 2},
                {"_id": "e2", "user_count": 1},
                {"_id": "e3", "user_count": 0},
            ],
            "aggregated_join": [
                {"_id": "e1", "primary_user": {"name": "Ada"}},
                {"_id": "e2", "primary_user": {"name": "Linus"}},
                {"_id": "e3", "primary_user": {"name": "unknown"}},
            ],
            "merge_join": [
                {"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"},
                {"_id": "u1", "tenant": "a", "user_id": "u1", "kind": "view", "name": "Ada", "role": "admin"},
                {"_id": "u3", "tenant": "b", "user_id": "u3", "kind": "click", "name": "Linus", "role": "owner"},
            ],
        }

        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    users = client.analytics.users
                    await users.insert_one({"_id": "u1", "tenant": "a", "name": "Ada", "role": "admin"})
                    await users.insert_one({"_id": "u2", "tenant": "a", "name": "Grace", "role": "staff"})
                    await users.insert_one({"_id": "u3", "tenant": "b", "name": "Linus", "role": "owner"})
                    await events.insert_one({"_id": "e1", "tenant": "a", "user_id": "u1", "kind": "view"})
                    await events.insert_one({"_id": "e2", "tenant": "b", "user_id": "u3", "kind": "click"})
                    await events.insert_one({"_id": "e3", "tenant": "missing", "user_id": "ux", "kind": "open"})

                    for name, pipeline in pipelines.items():
                        with self.subTest(engine=engine_name, pipeline=name):
                            documents = await events.aggregate(pipeline).to_list()
                            self.assertEqual(documents, expected[name])

    async def test_aggregate_match_supports_nested_expr_inside_and_or(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "a": 5, "b": 11})
                    await collection.insert_one({"_id": "2", "a": 5, "b": 9})
                    await collection.insert_one({"_id": "3", "a": 4, "b": 20})

                    documents = await collection.aggregate(
                        [{"$match": {"$and": [{"a": 5}, {"$expr": {"$gt": ["$b", 10]}}]}}]
                    ).to_list()

                    self.assertEqual(documents, [{"_id": "1", "a": 5, "b": 11}])

    async def test_aggregate_supports_replace_with(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "profile": {"name": "Ada"}})

                    documents = await collection.aggregate([{"$replaceWith": "$profile"}]).to_list()

                    self.assertEqual(documents, [{"name": "Ada"}])

    async def test_aggregate_let_bindings_are_visible_to_pipeline_expressions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "name": "Ada"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "tenant_match": {"$eq": ["$tenant", "$$tenant"]},
                                    "label": {"$concat": ["$$prefix", "$name"]},
                                }
                            }
                        ],
                        let={"tenant": "a", "prefix": "user:"},
                    ).to_list()

                    self.assertEqual(documents, [{"tenant_match": True, "label": "user:Ada"}])

    async def test_write_operations_support_let_through_expr_filters(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "matched": False},
                            {"_id": "2", "tenant": "b", "matched": False},
                        ]
                    )

                    result = await collection.update_many(
                        {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
                        {"$set": {"matched": True}},
                        let={"tenant": "a"},
                    )

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(
                        await collection.find({}, {"_id": 1, "matched": 1}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "1", "matched": True}, {"_id": "2", "matched": False}],
                    )

    async def test_bulk_write_inherits_let_for_expr_filters(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "flag": False},
                            {"_id": "2", "tenant": "b", "flag": False},
                        ]
                    )

                    result = await collection.bulk_write(
                        [
                            UpdateOne(
                                {"$expr": {"$eq": ["$tenant", "$$tenant"]}},
                                {"$set": {"flag": True}},
                            )
                        ],
                        let={"tenant": "b"},
                    )

                    self.assertEqual(result.matched_count, 1)
                    self.assertEqual(
                        await collection.find({}, {"_id": 1, "flag": 1}, sort=[("_id", 1)]).to_list(),
                        [{"_id": "1", "flag": False}, {"_id": "2", "flag": True}],
                    )

    async def test_aggregate_supports_array_to_object_index_of_array_and_sort_array(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {
                            "_id": "1",
                            "pairs": [["a", 1], ["b", 2]],
                            "numbers": [4, 1, 3, 2],
                            "items": [
                                {"rank": 3, "name": "c"},
                                {"rank": 1, "name": "a"},
                                {"rank": 2, "name": "b"},
                            ],
                        }
                    )

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "mapped": {"$arrayToObject": "$pairs"},
                                    "position": {"$indexOfArray": ["$numbers", 3]},
                                    "sorted_numbers": {"$sortArray": {"input": "$numbers", "sortBy": 1}},
                                    "sorted_items": {"$sortArray": {"input": "$items", "sortBy": {"rank": 1}}},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "mapped": {"a": 1, "b": 2},
                                "position": 2,
                                "sorted_numbers": [1, 2, 3, 4],
                                "sorted_items": [
                                    {"rank": 1, "name": "a"},
                                    {"rank": 2, "name": "b"},
                                    {"rank": 3, "name": "c"},
                                ],
                            }
                        ],
                    )

    async def test_aggregate_supports_facet_and_date_trunc(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one(
                        {"_id": "1", "kind": "view", "score": 5, "created_at": datetime.datetime(2026, 3, 24, 10, 37, 52)}
                    )
                    await collection.insert_one(
                        {"_id": "2", "kind": "view", "score": 7, "created_at": datetime.datetime(2026, 3, 24, 10, 5, 0)}
                    )
                    await collection.insert_one(
                        {"_id": "3", "kind": "click", "score": 3, "created_at": datetime.datetime(2026, 3, 24, 11, 10, 0)}
                    )

                    documents = await collection.aggregate(
                        [
                            {"$addFields": {"bucket": {"$dateTrunc": {"date": "$created_at", "unit": "hour"}}}},
                            {
                                "$facet": {
                                    "views": [
                                        {"$match": {"kind": "view"}},
                                        {"$project": {"_id": 0, "bucket": 1}},
                                        {"$sort": {"bucket": 1}},
                                    ],
                                    "scores": [
                                        {"$group": {"_id": "$kind", "total": {"$sum": "$score"}}},
                                        {"$sort": {"_id": 1}},
                                    ],
                                }
                            },
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {
                                "views": [
                                    {"bucket": datetime.datetime(2026, 3, 24, 10, 0, 0)},
                                    {"bucket": datetime.datetime(2026, 3, 24, 10, 0, 0)},
                                ],
                                "scores": [{"_id": "click", "total": 3}, {"_id": "view", "total": 12}],
                            }
                        ],
                    )

    async def test_aggregate_supports_union_with_and_union_with_inside_facet(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    events = client.analytics.events
                    archived = client.analytics.archived_events
                    await events.insert_many(
                        [
                            {"_id": "e1", "kind": "event", "rank": 2},
                            {"_id": "e2", "kind": "event", "rank": 1},
                        ]
                    )
                    await archived.insert_many(
                        [
                            {"_id": "a1", "kind": "archive", "rank": 3},
                            {"_id": "a2", "kind": "archive", "rank": 0},
                        ]
                    )

                    unioned = await events.aggregate(
                        [
                            {"$unionWith": {"coll": "archived_events", "pipeline": [{"$sort": {"rank": 1}}]}},
                            {"$project": {"_id": 1, "kind": 1, "rank": 1}},
                        ]
                    ).to_list()
                    faceted = await events.aggregate(
                        [
                            {
                                "$facet": {
                                    "combined": [
                                        {"$unionWith": "archived_events"},
                                        {"$sort": {"rank": 1}},
                                        {"$project": {"_id": 1}},
                                    ]
                                }
                            }
                        ]
                    ).to_list()
                    await events.insert_one({"_id": "e3", "kind": "archive", "rank": 0})
                    current_only = await events.aggregate(
                        [
                            {"$match": {"kind": "event"}},
                            {"$unionWith": {"pipeline": [{"$match": {"kind": "archive"}}]}},
                            {"$sort": {"rank": 1}},
                            {"$project": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        unioned,
                        [
                            {"_id": "e1", "kind": "event", "rank": 2},
                            {"_id": "e2", "kind": "event", "rank": 1},
                            {"_id": "a2", "kind": "archive", "rank": 0},
                            {"_id": "a1", "kind": "archive", "rank": 3},
                        ],
                    )
                    self.assertEqual(
                        faceted,
                        [{"combined": [{"_id": "a2"}, {"_id": "e2"}, {"_id": "e1"}, {"_id": "a1"}]}],
                    )
                    self.assertEqual(
                        current_only,
                        [{"_id": "e3"}, {"_id": "e2"}, {"_id": "e1"}],
                    )

    async def test_aggregate_supports_bucket(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "score": 5, "kind": "view"})
                    await collection.insert_one({"_id": "2", "score": 12, "kind": "view"})
                    await collection.insert_one({"_id": "3", "score": 17, "kind": "click"})
                    await collection.insert_one({"_id": "4", "score": 25, "kind": "view"})

                    documents = await collection.aggregate(
                        [
                            {
                                "$bucket": {
                                    "groupBy": "$score",
                                    "boundaries": [0, 10, 20],
                                    "default": "other",
                                    "output": {
                                        "count": {"$sum": 1},
                                        "kinds": {"$push": "$kind"},
                                    },
                                }
                            }
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": 0, "count": 1, "kinds": ["view"]},
                            {"_id": 10, "count": 2, "kinds": ["view", "click"]},
                            {"_id": "other", "count": 1, "kinds": ["view"]},
                        ],
                    )

    async def test_aggregate_supports_bucket_auto_and_set_window_fields(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "score": 5, "kind": "view"})
                    await collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "score": 7, "kind": "view"})
                    await collection.insert_one({"_id": "3", "tenant": "b", "rank": 1, "score": 3, "kind": "click"})
                    await collection.insert_one({"_id": "4", "tenant": "b", "rank": 2, "score": 9, "kind": "click"})

                    bucketed = await collection.aggregate(
                        [
                            {
                                "$bucketAuto": {
                                    "groupBy": "$score",
                                    "buckets": 2,
                                    "output": {"count": {"$sum": 1}},
                                }
                            }
                        ]
                    ).to_list()
                    windowed = await collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "partitionBy": "$tenant",
                                    "sortBy": {"rank": 1},
                                    "output": {
                                        "runningTotal": {
                                            "$sum": "$score",
                                            "window": {"documents": ["unbounded", "current"]},
                                        }
                                    },
                                }
                            },
                            {"$project": {"_id": 0, "tenant": 1, "rank": 1, "runningTotal": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        bucketed,
                        [
                            {"_id": {"min": 3, "max": 7}, "count": 2},
                            {"_id": {"min": 7, "max": 9}, "count": 2},
                        ],
                    )
                    self.assertEqual(
                        windowed,
                        [
                            {"tenant": "a", "rank": 1, "runningTotal": 5},
                            {"tenant": "a", "rank": 2, "runningTotal": 12},
                            {"tenant": "b", "rank": 1, "runningTotal": 3},
                            {"tenant": "b", "rank": 2, "runningTotal": 12},
                        ],
                    )

    async def test_aggregate_supports_stddev_accumulators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4},
                            {"_id": "3", "group": "a", "rank": 3, "score": 4},
                            {"_id": "4", "group": "b", "rank": 1, "score": 10},
                        ]
                    )

                    grouped = await collection.aggregate(
                        [{"$group": {"_id": "$group", "pop": {"$stdDevPop": "$score"}, "samp": {"$stdDevSamp": "$score"}}}]
                    ).to_list()
                    windowed = await collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "partitionBy": "$group",
                                    "sortBy": {"rank": 1},
                                    "output": {
                                        "runningPop": {"$stdDevPop": "$score", "window": {"documents": ["unbounded", "current"]}},
                                    },
                                }
                            },
                            {"$project": {"_id": 0, "group": 1, "rank": 1, "runningPop": 1}},
                        ]
                    ).to_list()

                    self.assertAlmostEqual(grouped[0]["pop"], 0.94280904158, places=10)
                    self.assertAlmostEqual(grouped[0]["samp"], 1.15470053838, places=10)
                    self.assertEqual(grouped[1], {"_id": "b", "pop": 0.0, "samp": None})
                    self.assertEqual(windowed[0]["runningPop"], 0.0)
                    self.assertAlmostEqual(windowed[1]["runningPop"], 1.0, places=10)
                    self.assertAlmostEqual(windowed[2]["runningPop"], 0.94280904158, places=10)

    async def test_aggregate_supports_stddev_expression_forms(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "scores": [2, 4, 4, "x"], "a": 2, "b": 4, "c": 4})

                    documents = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "popArray": {"$stdDevPop": "$scores"},
                                    "sampArray": {"$stdDevSamp": "$scores"},
                                    "popList": {"$stdDevPop": ["$a", "$b", "$c"]},
                                }
                            }
                        ]
                    ).to_list()

                    self.assertAlmostEqual(documents[0]["popArray"], 0.94280904158, places=10)
                    self.assertAlmostEqual(documents[0]["sampArray"], 1.15470053838, places=10)
                    self.assertAlmostEqual(documents[0]["popList"], 0.94280904158, places=10)

    async def test_aggregate_supports_first_n_and_last_n(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4},
                            {"_id": "3", "group": "a", "rank": 3, "score": 6},
                            {"_id": "4", "group": "b", "rank": 1, "score": 9},
                            {"_id": "5", "group": "b", "rank": 2},
                        ]
                    )

                    grouped = await collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$group",
                                    "firstTwo": {"$firstN": {"input": "$score", "n": 2}},
                                    "lastTwo": {"$lastN": {"input": "$score", "n": 2}},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()
                    projected = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "firstScores": {"$firstN": {"input": [10, 20, 30, 40], "n": 3}},
                                    "lastScores": {"$lastN": {"input": [10, 20, 30, 40], "n": 2}},
                                    "topScores": {"$maxN": {"input": [1, None, 3, 2], "n": 2}},
                                    "bottomScores": {"$minN": {"input": [1, None, 3, 2], "n": 2}},
                                }
                            },
                            {"$limit": 1},
                        ]
                    ).to_list()

                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "firstTwo": [2, 4], "lastTwo": [4, 6]},
                            {"_id": "b", "firstTwo": [9, None], "lastTwo": [9, None]},
                        ],
                    )
                    self.assertEqual(
                        projected,
                        [
                            {
                                "firstScores": [10, 20, 30],
                                "lastScores": [30, 40],
                                "topScores": [3, 2],
                                "bottomScores": [1, 2],
                            }
                        ],
                    )

    async def test_aggregate_supports_top_and_bottom_accumulators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 2, "label": "a1"},
                            {"_id": "2", "group": "a", "rank": 2, "score": 4, "label": "a2"},
                            {"_id": "3", "group": "a", "rank": 3, "score": 4, "label": "a3"},
                            {"_id": "4", "group": "b", "rank": 1, "score": None, "label": "b1"},
                            {"_id": "5", "group": "b", "rank": 2, "label": "b2"},
                        ]
                    )

                    grouped = await collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$group",
                                    "topLabel": {"$top": {"sortBy": {"score": -1}, "output": "$label"}},
                                    "bottomLabel": {"$bottom": {"sortBy": {"score": -1}, "output": "$label"}},
                                    "topTwo": {"$topN": {"sortBy": {"score": -1}, "output": "$label", "n": 2}},
                                    "bottomTwo": {"$bottomN": {"sortBy": {"score": -1}, "output": "$label", "n": 2}},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "topLabel": "a2", "bottomLabel": "a1", "topTwo": ["a2", "a3"], "bottomTwo": ["a2", "a1"]},
                            {"_id": "b", "topLabel": "b1", "bottomLabel": "b1", "topTwo": ["b1", "b2"], "bottomTwo": ["b1", "b2"]},
                        ],
                    )

    async def test_aggregate_supports_percentile_and_median(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "rank": 1, "score": 1, "scores": [1, 2, 3, 4]},
                            {"_id": "2", "group": "a", "rank": 2, "score": 5, "scores": [10, 20, 30, 40]},
                            {"_id": "3", "group": "a", "rank": 3, "score": "x", "scores": [7, "x", 9]},
                            {"_id": "4", "group": "b", "rank": 1, "score": 2},
                        ]
                    )

                    grouped = await collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$group",
                                    "medianScore": {"$median": {"input": "$score", "method": "approximate"}},
                                    "percentiles": {"$percentile": {"input": "$score", "p": [0.0, 0.5, 1.0], "method": "approximate"}},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "medianScore": 1, "percentiles": [1, 1, 5]},
                            {"_id": "b", "medianScore": 2, "percentiles": [2, 2, 2]},
                        ],
                    )

    async def test_aggregate_supports_rank_family_window_operators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "group": "a", "score": 10},
                            {"_id": "2", "group": "a", "score": 20},
                            {"_id": "3", "group": "a", "score": 20},
                            {"_id": "4", "group": "b", "score": 5},
                        ]
                    )

                    documents = await collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "partitionBy": "$group",
                                    "sortBy": {"score": 1},
                                    "output": {
                                        "rank": {"$rank": {}},
                                        "dense": {"$denseRank": {}},
                                        "docnum": {"$documentNumber": {}},
                                    },
                                }
                            },
                            {"$project": {"_id": 1, "group": 1, "score": 1, "rank": 1, "dense": 1, "docnum": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "group": "a", "score": 10, "rank": 1, "dense": 1, "docnum": 1},
                            {"_id": "2", "group": "a", "score": 20, "rank": 2, "dense": 2, "docnum": 2},
                            {"_id": "3", "group": "a", "score": 20, "rank": 2, "dense": 2, "docnum": 3},
                            {"_id": "4", "group": "b", "score": 5, "rank": 1, "dense": 1, "docnum": 1},
                        ],
                    )

    async def test_aggregate_supports_string_expressions_last_and_add_to_set(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "rank": 1, "kind": "view", "label": "Ada"})
                    await collection.insert_one({"_id": "2", "tenant": "a", "rank": 2, "kind": "view", "label": "Lovelace"})
                    await collection.insert_one({"_id": "3", "tenant": "a", "rank": 3, "kind": "click", "label": "Analytical"})

                    projected = await collection.aggregate(
                        [
                            {
                                "$project": {
                                    "_id": 0,
                                    "full": {"$concat": ["$label", "-", "$kind"]},
                                    "lower": {"$toLower": "$label"},
                                    "upper": {"$toUpper": "$kind"},
                                    "prefix": {"$substr": ["$label", 0, 3]},
                                    "compare": {"$strcasecmp": ["$kind", "VIEW"]},
                                }
                            },
                            {"$sort": {"full": 1}},
                        ]
                    ).to_list()
                    grouped = await collection.aggregate(
                        [
                            {"$sort": {"rank": 1}},
                            {
                                "$group": {
                                    "_id": "$tenant",
                                    "lastKind": {"$last": "$kind"},
                                    "kinds": {"$addToSet": "$kind"},
                                }
                            },
                        ]
                    ).to_list()

                    self.assertEqual(
                        projected,
                        [
                            {"full": "Ada-view", "lower": "ada", "upper": "VIEW", "prefix": "Ada", "compare": 0},
                            {"full": "Analytical-click", "lower": "analytical", "upper": "CLICK", "prefix": "Ana", "compare": -1},
                            {"full": "Lovelace-view", "lower": "lovelace", "upper": "VIEW", "prefix": "Lov", "compare": 0},
                        ],
                    )
                    self.assertEqual(
                        grouped,
                        [{"_id": "a", "lastKind": "click", "kinds": ["view", "click"]}],
                    )

    async def test_aggregate_supports_split_count_and_merge_objects(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "tenant": "a", "text": "Ada Lovelace", "meta": {"x": 1}})
                    await collection.insert_one({"_id": "2", "tenant": "a", "text": "Grace Hopper", "meta": {"y": 2}})
                    await collection.insert_one({"_id": "3", "tenant": "b", "text": "Alan Turing", "meta": None})

                    projected = await collection.aggregate(
                        [
                            {"$project": {"_id": 0, "parts": {"$split": ["$text", " "]}}},
                            {"$limit": 1},
                        ]
                    ).to_list()
                    grouped = await collection.aggregate(
                        [
                            {
                                "$group": {
                                    "_id": "$tenant",
                                    "count": {"$count": {}},
                                    "merged": {"$mergeObjects": "$meta"},
                                }
                            },
                            {"$sort": {"_id": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(projected, [{"parts": ["Ada", "Lovelace"]}])
                    self.assertEqual(
                        grouped,
                        [
                            {"_id": "a", "count": 2, "merged": {"x": 1, "y": 2}},
                            {"_id": "b", "count": 1, "merged": {}},
                        ],
                    )

    async def test_aggregate_supports_count_and_sort_by_count(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view"})
                    await collection.insert_one({"_id": "2", "kind": "view"})
                    await collection.insert_one({"_id": "3", "kind": "click"})

                    counted = await collection.aggregate([{"$count": "total"}]).to_list()
                    sorted_counts = await collection.aggregate([{"$sortByCount": "$kind"}]).to_list()

                    self.assertEqual(counted, [{"total": 3}])
                    self.assertEqual(sorted_counts, [{"_id": "view", "count": 2}, {"_id": "click", "count": 1}])

    async def test_aggregate_supports_set_window_fields_numeric_range(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "score": 5})
                    await collection.insert_one({"_id": "2", "score": 7})
                    await collection.insert_one({"_id": "3", "score": 12})

                    documents = await collection.aggregate(
                        [
                            {
                                "$setWindowFields": {
                                    "sortBy": {"score": 1},
                                    "output": {
                                        "nearbyTotal": {
                                            "$sum": "$score",
                                            "window": {"range": [-2, 2]},
                                        }
                                    },
                                }
                            },
                            {"$project": {"_id": 1, "nearbyTotal": 1}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "1", "nearbyTotal": 12},
                            {"_id": "2", "nearbyTotal": 12},
                            {"_id": "3", "nearbyTotal": 12},
                        ],
                    )

    async def test_aggregate_propagates_operation_failure_for_invalid_stage(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1"})

                    with self.assertRaises(OperationFailure):
                        await collection.aggregate([{"$densify": {}}]).to_list()

    async def test_aggregate_supports_densify_fill_and_merge(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    source = client.analytics.series
                    sink = client.analytics.series_filled
                    await source.insert_many(
                        [
                            {"_id": "1", "bucket": "a", "x": 1, "qty": 10},
                            {"_id": "2", "bucket": "a", "x": 3, "qty": None},
                            {"_id": "3", "bucket": "a", "x": 4, "qty": 40},
                        ]
                    )

                    densified = await source.aggregate(
                        [
                            {"$project": {"_id": 0, "bucket": 1, "x": 1, "qty": 1}},
                            {"$densify": {"field": "x", "partitionByFields": ["bucket"], "range": {"step": 1, "bounds": "full"}}},
                            {"$fill": {"sortBy": {"x": 1}, "partitionByFields": ["bucket"], "output": {"qty": {"method": "linear"}}}},
                        ]
                    ).to_list()

                    self.assertEqual(
                        densified,
                        [
                            {"bucket": "a", "x": 1, "qty": 10},
                            {"bucket": "a", "x": 2, "qty": 20.0},
                            {"bucket": "a", "x": 3, "qty": 30.0},
                            {"bucket": "a", "x": 4, "qty": 40},
                        ],
                    )

                    merged = await source.aggregate(
                        [
                            {"$project": {"_id": 1, "bucket": 1}},
                            {"$merge": {"into": "series_filled", "whenMatched": "merge", "whenNotMatched": "insert"}},
                        ]
                    ).to_list()

                    self.assertEqual(merged, [])
                    stored = await sink.find({}, sort=[("_id", 1)]).to_list()
                    self.assertEqual(stored, [{"_id": "1", "bucket": "a"}, {"_id": "2", "bucket": "a"}, {"_id": "3", "bucket": "a"}])

    async def test_sqlite_cursor_early_break_does_not_block_follow_up_writes(self):
        async with open_client("sqlite") as client:
            collection = client.analytics.events
            for i in range(250):
                await collection.insert_one({"_id": str(i), "kind": "view", "rank": i})

            seen: list[str] = []
            async for document in collection.find({"kind": "view"}, sort=[("rank", 1)]):
                seen.append(document["_id"])
                if len(seen) == 5:
                    break

            result = await collection.insert_one({"_id": "after-break", "kind": "view", "rank": 999})
            found = await collection.find_one({"_id": result.inserted_id})

            self.assertEqual(seen, ["0", "1", "2", "3", "4"])
            self.assertEqual(found, {"_id": "after-break", "kind": "view", "rank": 999})

    async def test_shared_memory_engine_is_safe_across_async_and_sync_clients(self):
        engine = MemoryEngine()
        errors: list[object] = []

        def sync_worker() -> None:
            try:
                with MongoClient(engine) as client:
                    for i in range(20):
                        doc_id = f"sync-{i}"
                        client.test.users.insert_one({"_id": doc_id, "origin": "sync", "n": i})
                        found = client.test.users.find_one({"_id": doc_id})
                        if found != {"_id": doc_id, "origin": "sync", "n": i}:
                            errors.append(("sync-mismatch", found))
            except Exception as exc:  # pragma: no cover
                errors.append(exc)

        thread = threading.Thread(target=sync_worker)

        async with AsyncMongoClient(engine) as client:
            thread.start()
            for i in range(20):
                doc_id = f"async-{i}"
                await client.test.users.insert_one({"_id": doc_id, "origin": "async", "n": i})
                found = await client.test.users.find_one({"_id": doc_id})
                if found != {"_id": doc_id, "origin": "async", "n": i}:
                    errors.append(("async-mismatch", found))
            await asyncio.to_thread(thread.join)

            self.assertEqual(await client.test.users.count_documents({"origin": "sync"}), 20)
            self.assertEqual(await client.test.users.count_documents({"origin": "async"}), 20)

        self.assertEqual(errors, [])

    async def test_find_one_and_cursor_first_do_not_hang_with_multiple_matches(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 2})
                    await collection.insert_one({"_id": "2", "kind": "view", "rank": 1})
                    await collection.insert_one({"_id": "3", "kind": "click", "rank": 3})

                    found = await asyncio.wait_for(collection.find_one({"kind": "view"}), timeout=1)
                    first = await asyncio.wait_for(
                        collection.find({"kind": "view"}).sort([("rank", 1)]).first(),
                        timeout=1,
                    )

                    self.assertEqual(found["kind"], "view")
                    self.assertEqual(first, {"_id": "2", "kind": "view", "rank": 1})

    async def test_find_supports_filter_sort_skip_limit_and_projection_together(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view", "rank": 3, "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "kind": "click", "rank": 1, "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "kind": "view", "rank": 2, "payload": {"city": "Bilbao"}})
                    await collection.insert_one({"_id": "4", "kind": "view", "rank": 4, "payload": {"city": "Valencia"}})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {"kind": "view"},
                            {"payload.city": 1, "_id": 0},
                            sort=[("rank", 1)],
                            skip=1,
                            limit=1,
                        )
                    ]

                    self.assertEqual(documents, [{"payload": {"city": "Sevilla"}}])

    async def test_find_supports_array_sort_and_projection_together(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "rank": [3, 8], "payload": {"city": "Sevilla"}})
                    await collection.insert_one({"_id": "2", "rank": [1, 9], "payload": {"city": "Madrid"}})
                    await collection.insert_one({"_id": "3", "rank": [2, 4], "payload": {"city": "Bilbao"}})

                    documents = [
                        doc
                        async for doc in collection.find(
                            {},
                            {"payload.city": 1, "_id": 0},
                            sort=[("rank", 1)],
                        )
                    ]

                    self.assertEqual(
                        documents,
                        [
                            {"payload": {"city": "Madrid"}},
                            {"payload": {"city": "Bilbao"}},
                            {"payload": {"city": "Sevilla"}},
                        ],
                    )

    async def test_async_client_exposes_client_session_and_accepts_it(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    self.assertIsInstance(session, ClientSession)
                    async with session:
                        result = await client.test.users.insert_one({"name": "Ada"}, session=session)
                        found = await client.test.users.find_one({"_id": result.inserted_id}, session=session)

                    self.assertFalse(session.active)
                    self.assertEqual(found["name"], "Ada")

    async def test_async_session_with_transaction_commits_and_returns_result(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()

                    async def _run(active: ClientSession) -> str:
                        await client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=active)
                        return "ok"

                    result = await session.with_transaction(_run)

                    self.assertEqual(result, "ok")
                    self.assertFalse(session.in_transaction)
                    self.assertEqual(await client.test.users.count_documents({}), 1)

    async def test_async_client_with_transaction_retries_transient_callback_errors(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    attempts = {"count": 0}

                    async def _run(active: ClientSession) -> str:
                        attempts["count"] += 1
                        if attempts["count"] == 1:
                            raise OperationFailure(
                                "transient callback failure",
                                error_labels=("TransientTransactionError",),
                            )
                        await client.test.users.insert_one(
                            {"_id": "tx-callback", "name": "Ada"},
                            session=active,
                        )
                        return "ok"

                    result = await client.with_transaction(_run)

                    self.assertEqual(result, "ok")
                    self.assertEqual(attempts["count"], 2)
                    self.assertEqual(await client.test.users.count_documents({}), 1)

    async def test_async_sqlite_session_transaction_is_isolated_and_abortable(self):
        async with AsyncMongoClient(SQLiteEngine()) as client:
            session = client.start_session()
            self.assertTrue(session.get_engine_state(next(iter(session.engine_state)))["supports_transactions"])

            session.start_transaction()
            await client.test.users.insert_one({"_id": "1", "name": "Ada"}, session=session)

            self.assertEqual(await client.test.users.count_documents({}, session=session), 1)
            with self.assertRaises(InvalidOperation):
                await client.test.users.count_documents({})

            session.abort_transaction()

            self.assertEqual(await client.test.users.count_documents({}), 0)

    async def test_async_session_state_reflects_connected_engine(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    self.assertEqual(len(session.engine_state), 1)
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(
                        state["connected"],
                        True,
                    )

    async def test_find_with_comment_and_max_time_updates_session_state_and_explain(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view"}, session=session)

                    documents = await collection.find(
                        {"kind": "view"},
                        comment="trace-find",
                        max_time_ms=25,
                        session=session,
                    ).to_list()
                    explanation = await collection.find(
                        {"kind": "view"},
                        comment="trace-find",
                        max_time_ms=25,
                        session=session,
                    ).explain()
                    state = next(iter(session.engine_state.values()))

                    self.assertEqual(documents, [{"_id": "1", "kind": "view"}])
                    self.assertEqual(state["last_operation"]["comment"], "trace-find")
                    self.assertEqual(state["last_operation"]["max_time_ms"], 25)
                    self.assertEqual(explanation["comment"], "trace-find")
                    self.assertEqual(explanation["max_time_ms"], 25)

    async def test_write_and_index_comments_update_session_state(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    session = client.start_session()
                    collection = client.analytics.events
                    await collection.insert_one({"_id": "1", "kind": "view"}, session=session)

                    await collection.update_one(
                        {"_id": "1"},
                        {"$set": {"done": True}},
                        comment="trace-update",
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))
                    self.assertEqual(state["last_operation"]["operation"], "update_one")
                    self.assertEqual(state["last_operation"]["comment"], "trace-update")

                    await collection.create_index(
                        [("kind", 1)],
                        comment="trace-index",
                        session=session,
                    )
                    state = next(iter(session.engine_state.values()))
                    self.assertEqual(state["last_operation"]["operation"], "create_index")
                    self.assertEqual(state["last_operation"]["comment"], "trace-index")

    async def test_collection_can_manage_index_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    name = await collection.create_index([("payload.kind", -1)], unique=False)
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()

                    self.assertEqual(name, "payload.kind_-1")
                    self.assertEqual(
                        indexes,
                        [
                            {"name": "_id_", "key": {"_id": 1}, "unique": True},
                            {
                                "name": "payload.kind_-1",
                                "key": {"payload.kind": -1},
                                "unique": False,
                            },
                        ],
                    )
                    self.assertEqual(
                        info,
                        {
                            "_id_": {"key": [("_id", 1)], "unique": True},
                            "payload.kind_-1": {"key": [("payload.kind", -1)]},
                        },
                    )

    async def test_collection_accepts_mapping_key_spec_for_create_index(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    name = await collection.create_index({"email": 1, "created_at": -1})
                    info = await collection.index_information()

                    self.assertEqual(name, "email_1_created_at_-1")
                    self.assertEqual(
                        info["email_1_created_at_-1"],
                        {"key": [("email", 1), ("created_at", -1)]},
                    )

    async def test_collection_supports_text_and_geo_special_index_key_types(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "content": "Ada Lovelace wrote the first algorithm", "location": [40.0, -3.0]},
                            {"_id": "2", "content": "Grace Hopper built compilers", "location": [41.0, -3.5]},
                        ]
                    )

                    text_name = await collection.create_index({"content": "text"})
                    geo_name = await collection.create_index([("location", "2dsphere")])
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()

                    self.assertEqual(text_name, "content_text")
                    self.assertEqual(geo_name, "location_2dsphere")
                    self.assertIn(
                        {"name": "content_text", "key": {"content": "text"}, "unique": False},
                        indexes,
                    )
                    self.assertIn(
                        {"name": "location_2dsphere", "key": {"location": "2dsphere"}, "unique": False},
                        indexes,
                    )
                    self.assertEqual(info["content_text"], {"key": [("content", "text")]})
                    self.assertEqual(info["location_2dsphere"], {"key": [("location", "2dsphere")]})
                    self.assertEqual(
                        await collection.find(
                            {"$text": {"$search": "algorithm"}},
                            {"_id": 1, "score": {"$meta": "textScore"}},
                            sort={"score": {"$meta": "textScore"}},
                        ).to_list(),
                        [{"_id": "1", "score": 1.0}],
                    )
                    command_result = await client.analytics.command(
                        {
                            "find": "events",
                            "filter": {"$text": {"$search": "algorithm"}},
                            "projection": {"_id": 1, "score": {"$meta": "textScore"}},
                            "sort": {"score": {"$meta": "textScore"}},
                        }
                    )
                    self.assertEqual(
                        command_result["cursor"]["firstBatch"],
                        [{"_id": "1", "score": 1.0}],
                    )
                    explain = await collection.find(
                        {"$text": {"$search": "algorithm"}},
                    ).explain()
                    self.assertIn("textQuery", explain["details"])

                    with self.assertRaises(OperationFailure):
                        await collection.find({"content": "Ada"}, hint="content_text").to_list()

    async def test_collection_rejects_unsupported_special_index_shapes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    index_name = await collection.create_index([("tenant", 1), ("content", "text")])
                    with self.assertRaises(OperationFailure):
                        await collection.create_index([("content", "hashed")], unique=True)

                    indexes = await collection.list_indexes().to_list()
                    self.assertIn(
                        {
                            "name": index_name,
                            "key": {"tenant": 1, "content": "text"},
                            "unique": False,
                        },
                        indexes,
                    )

    async def test_collection_supports_multi_field_local_text_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.articles
                    await collection.insert_many(
                        [
                            {"_id": "1", "title": "Ada", "content": "First algorithm"},
                            {"_id": "2", "title": "Grace", "content": "Compiler pioneer"},
                            {"_id": "3", "title": "Ada", "content": "Algorithm notes"},
                        ]
                    )

                    index_name = await collection.create_index([("title", "text"), ("content", "text")])
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()
                    results = await collection.find(
                        {"$text": {"$search": "Ada algorithm"}},
                        {"_id": 1, "score": {"$meta": "textScore"}},
                        sort={"score": {"$meta": "textScore"}},
                    ).to_list()
                    explain = await collection.find({"$text": {"$search": "Ada algorithm"}}).explain()

                    self.assertEqual(index_name, "title_text_content_text")
                    self.assertIn(
                        {
                            "name": "title_text_content_text",
                            "key": {"title": "text", "content": "text"},
                            "unique": False,
                        },
                        indexes,
                    )
                    self.assertEqual(
                        info["title_text_content_text"],
                        {"key": [("title", "text"), ("content", "text")]},
                    )
                    self.assertEqual(
                        results,
                        [
                            {"_id": "1", "score": 2.0},
                            {"_id": "3", "score": 2.0},
                        ],
                    )
                    self.assertEqual(
                        explain["details"]["textQuery"]["fields"],
                        ["title", "content"],
                    )

    async def test_collection_text_index_supports_weights_and_language_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.articles
                    await collection.insert_many(
                        [
                            {"_id": "1", "title": "Ada", "content": "none"},
                            {"_id": "2", "title": "none", "content": "Ada"},
                        ]
                    )

                    index_name = await collection.create_index(
                        [("title", "text"), ("content", "text")],
                        name="title_text_content_text",
                        weights={"title": 5, "content": 1},
                        default_language="english",
                        language_override="lang",
                    )
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()
                    results = await collection.find(
                        {"$text": {"$search": "Ada"}},
                        {"_id": 1, "score": {"$meta": "textScore"}},
                        sort={"score": {"$meta": "textScore"}},
                    ).to_list()
                    explain = await collection.find({"$text": {"$search": "Ada"}}).explain()

                    self.assertEqual(index_name, "title_text_content_text")
                    self.assertIn(
                        {
                            "name": "title_text_content_text",
                            "key": {"title": "text", "content": "text"},
                            "unique": False,
                            "weights": {"title": 5, "content": 1},
                            "default_language": "english",
                            "language_override": "lang",
                        },
                        indexes,
                    )
                    self.assertEqual(
                        info["title_text_content_text"],
                        {
                            "key": [("title", "text"), ("content", "text")],
                            "weights": {"title": 5, "content": 1},
                            "default_language": "english",
                            "language_override": "lang",
                        },
                    )
                    self.assertEqual(
                        results,
                        [{"_id": "1", "score": 5.0}, {"_id": "2", "score": 1.0}],
                    )
                    self.assertEqual(
                        explain["details"]["textQuery"]["weights"],
                        {"title": 5, "content": 1},
                    )
                    self.assertEqual(explain["details"]["textQuery"]["defaultLanguage"], "english")
                    self.assertEqual(explain["details"]["textQuery"]["languageOverride"], "lang")

    async def test_collection_text_query_supports_phrase_and_exclusion_semantics(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.text_semantics
                    await collection.insert_many(
                        [
                            {"_id": "1", "content": "Ada Lovelace algorithm notes"},
                            {"_id": "2", "content": "Ada Lovelace compiler notes"},
                            {"_id": "3", "content": "Ada debug note"},
                        ]
                    )
                    await collection.create_index({"content": "text"})

                    phrase_results = await collection.find(
                        {"$text": {"$search": '"Ada Lovelace" -compiler'}},
                        {"_id": 1, "score": {"$meta": "textScore"}},
                        sort={"score": {"$meta": "textScore"}},
                    ).to_list()
                    exclusion_results = await collection.find(
                        {"$text": {"$search": 'ada -"debug note"'}},
                        {"_id": 1},
                        sort={"_id": 1},
                    ).to_list()
                    explain = await collection.find(
                        {"$text": {"$search": '"Ada Lovelace" -compiler -"debug note"'}},
                    ).explain()

                    self.assertEqual(phrase_results, [{"_id": "1", "score": 2.0}])
                    self.assertEqual(exclusion_results, [{"_id": "1"}, {"_id": "2"}])
                    self.assertEqual(explain["details"]["textQuery"]["requiredPhrases"], ["ada lovelace"])
                    self.assertEqual(explain["details"]["textQuery"]["excludedTerms"], ["compiler"])
                    self.assertEqual(explain["details"]["textQuery"]["excludedPhrases"], ["debug note"])

    async def test_collection_text_query_hint_selects_text_index_and_rejects_non_text_hint(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.text_hint
                    await collection.insert_many(
                        [
                            {"_id": "1", "title": "Ada", "body": "none", "rank": 1},
                            {"_id": "2", "title": "none", "body": "Ada", "rank": 2},
                            {"_id": "3", "title": "Ada", "body": "Ada", "rank": 3},
                        ]
                    )
                    await collection.create_index([("title", "text")], name="title_text")
                    await collection.create_index([("body", "text")], name="body_text")
                    await collection.create_index([("rank", 1)], name="rank_1")

                    with self.assertRaisesRegex(OperationFailure, "ambiguous"):
                        await collection.find({"$text": {"$search": "Ada"}}).to_list()

                    title_results = await collection.find(
                        {"$text": {"$search": "Ada"}},
                        {"_id": 1, "score": {"$meta": "textScore"}},
                        sort={"score": {"$meta": "textScore"}},
                        hint="title_text",
                    ).to_list()
                    body_results = await collection.find(
                        {"$text": {"$search": "Ada"}},
                        {"_id": 1, "score": {"$meta": "textScore"}},
                        sort={"score": {"$meta": "textScore"}},
                        hint="body_text",
                    ).to_list()
                    command_result = await client.analytics.command(
                        {
                            "find": "text_hint",
                            "filter": {"$text": {"$search": "Ada"}},
                            "projection": {"_id": 1, "score": {"$meta": "textScore"}},
                            "sort": {"score": {"$meta": "textScore"}},
                            "hint": "body_text",
                        }
                    )
                    explanation = await collection.find(
                        {"$text": {"$search": "Ada"}},
                        hint="body_text",
                    ).explain()

                    self.assertEqual(
                        [document["_id"] for document in title_results],
                        ["1", "3"],
                    )
                    self.assertEqual(
                        [document["_id"] for document in body_results],
                        ["2", "3"],
                    )
                    self.assertEqual(
                        [document["_id"] for document in command_result["cursor"]["firstBatch"]],
                        ["2", "3"],
                    )
                    self.assertEqual(explanation["hinted_index"], "body_text")
                    self.assertEqual(explanation["details"]["textQuery"]["index"], "body_text")

                    with self.assertRaisesRegex(OperationFailure, "text index"):
                        await collection.find(
                            {"$text": {"$search": "Ada"}},
                            hint="rank_1",
                        ).to_list()

    async def test_collection_supports_local_geo_queries_and_geo_near(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.places
                    await collection.insert_many(
                        [
                            {"_id": "a", "name": "Ada", "location": {"type": "Point", "coordinates": [0, 0]}, "kind": "home"},
                            {"_id": "b", "name": "Grace", "location": [2, 0], "kind": "office"},
                            {"_id": "c", "name": "Linus", "location": {"type": "Point", "coordinates": [1, 1]}, "kind": "home"},
                            {"_id": "d", "name": "Route", "location": {"type": "LineString", "coordinates": [[0, 0], [3, 0]]}, "kind": "route"},
                            {
                                "_id": "e",
                                "name": "Campus",
                                "location": {
                                    "type": "Polygon",
                                    "coordinates": [[[4, -1], [6, -1], [6, 1], [4, 1], [4, -1]]],
                                },
                                "kind": "area",
                            },
                        ]
                    )

                    within = await collection.find(
                        {
                            "location": {
                                "$geoWithin": {
                                    "$geometry": {
                                        "type": "Polygon",
                                        "coordinates": [[[-1, -1], [1.5, -1], [1.5, 1.5], [-1, 1.5], [-1, -1]]],
                                    }
                                }
                            }
                        }
                    ).to_list()
                    near = await collection.find(
                        {
                            "location": {
                                "$near": {
                                    "$geometry": {"type": "Point", "coordinates": [0, 0]},
                                    "$maxDistance": 1.5,
                                }
                            }
                        }
                    ).to_list()
                    intersects = await collection.find(
                        {
                            "location": {
                                "$geoIntersects": {
                                    "$geometry": {"type": "Point", "coordinates": [2, 0]},
                                }
                            }
                        }
                    ).to_list()
                    aggregate = await collection.aggregate(
                        [
                            {
                                "$geoNear": {
                                    "near": {"type": "Point", "coordinates": [0, 0]},
                                    "key": "location",
                                    "distanceField": "dist",
                                    "query": {"kind": "home"},
                                }
                            },
                            {"$project": {"_id": 1, "dist": 1}},
                        ]
                    ).to_list()

                    self.assertEqual({document["_id"] for document in within}, {"a", "c"})
                    self.assertEqual({document["_id"] for document in near}, {"a", "c", "d"})
                    self.assertEqual({document["_id"] for document in intersects}, {"b", "d"})
                    self.assertEqual(
                        aggregate,
                        [
                            {"_id": "a", "dist": 0.0},
                            {"_id": "c", "dist": 1.4142135623730951},
                        ],
                    )

    async def test_aggregate_supports_coll_stats_stage_via_collection_and_command(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many([{"_id": "1", "kind": "view"}, {"_id": "2", "kind": "click"}])
                    await collection.create_index([("kind", 1)], name="kind_idx")

                    collstats = await collection.aggregate(
                        [{"$collStats": {"count": {}, "storageStats": {"scale": 2}}}]
                    ).to_list()
                    command_result = await client.analytics.command(
                        {
                            "aggregate": "events",
                            "pipeline": [{"$collStats": {"count": {}, "storageStats": {"scale": 2}}}],
                            "cursor": {},
                        }
                    )

                    expected = collstats[0]
                    self.assertEqual(expected["ns"], "analytics.events")
                    self.assertEqual(expected["count"], {"count": 2})
                    self.assertEqual(expected["storageStats"]["ns"], "analytics.events")
                    self.assertEqual(expected["storageStats"]["scaleFactor"], 2)
                    self.assertEqual(command_result["cursor"]["firstBatch"], collstats)

    async def test_database_command_find_supports_advanced_projection_operators(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.schools
                    await collection.insert_many(
                        [
                            {
                                "_id": "1",
                                "students": [{"school": 101, "age": 10}, {"school": 102, "age": 11}],
                                "tags": ["python", "mongo", "sqlite"],
                                "grades": [{"subject": "math", "score": 10}, {"subject": "history", "score": 8}],
                            }
                        ]
                    )

                    positional = await client.analytics.command(
                        {
                            "find": "schools",
                            "filter": {"students.school": 102},
                            "projection": {"students.$": 1, "_id": 0},
                        }
                    )
                    operators = await client.analytics.command(
                        {
                            "find": "schools",
                            "filter": {"_id": "1"},
                            "projection": {
                                "_id": 0,
                                "tags": {"$slice": 2},
                                "grades": {"$elemMatch": {"subject": "history"}},
                            },
                        }
                    )

                    self.assertEqual(
                        positional["cursor"]["firstBatch"],
                        [{"students": [{"school": 102, "age": 11}]}],
                    )
                    self.assertEqual(
                        operators["cursor"]["firstBatch"],
                        [{"tags": ["python", "mongo"], "grades": [{"subject": "history", "score": 8}]}],
                    )

    async def test_create_indexes_support_hidden_indexes_and_hidden_hints_are_rejected(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many([{"_id": "1", "kind": "view"}, {"_id": "2", "kind": "click"}])

                    created = await client.analytics.command(
                        {
                            "createIndexes": "events",
                            "indexes": [{"key": {"kind": 1}, "name": "kind_hidden", "hidden": True, "background": True}],
                        }
                    )
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()

                    self.assertEqual(created["ok"], 1.0)
                    self.assertIn(
                        {"name": "kind_hidden", "key": {"kind": 1}, "unique": False, "hidden": True},
                        indexes,
                    )
                    self.assertTrue(info["kind_hidden"]["hidden"])
                    with self.assertRaisesRegex(OperationFailure, "hint does not correspond to a usable index"):
                        await collection.find({"kind": "view"}, hint="kind_hidden").to_list()

    async def test_create_index_supports_ttl_and_expires_documents(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    past = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(seconds=120)
                    future = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=120)
                    await collection.insert_many(
                        [
                            {"_id": "expired", "expires_at": past, "kind": "old"},
                            {"_id": "fresh", "expires_at": future, "kind": "new"},
                        ]
                    )

                    name = await collection.create_index([("expires_at", 1)], expire_after_seconds=30)
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()
                    found = await collection.find_one({"_id": "expired"})
                    documents = await collection.find({}, {"kind": 1, "_id": 0}).to_list()

                    self.assertEqual(name, "expires_at_1")
                    self.assertEqual(indexes[1]["expireAfterSeconds"], 30)
                    self.assertEqual(info["expires_at_1"]["expireAfterSeconds"], 30)
                    self.assertIsNone(found)
                    self.assertEqual(documents, [{"kind": "new"}])

    async def test_create_index_and_create_indexes_round_trip_collation_metadata(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    created_name = await collection.create_index(
                        [("name", 1)],
                        collation={"locale": "en", "strength": 2},
                    )
                    created_names = await collection.create_indexes(
                        [
                            IndexModel(
                                [("alias", 1)],
                                name="alias_1",
                                collation={"locale": "en", "strength": 1},
                            )
                        ]
                    )
                    indexes = await collection.list_indexes().to_list()
                    info = await collection.index_information()

                    self.assertEqual(created_name, "name_1")
                    self.assertEqual(created_names, ["alias_1"])
                    self.assertIn(
                        {
                            "name": "name_1",
                            "key": {"name": 1},
                            "unique": False,
                            "collation": {"locale": "en", "strength": 2},
                        },
                        indexes,
                    )
                    self.assertEqual(info["alias_1"]["collation"], {"locale": "en", "strength": 1})

    async def test_create_index_rejects_invalid_ttl_definitions(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    with self.assertRaises(TypeError):
                        await collection.create_index([("expires_at", 1)], expire_after_seconds=-1)
                    with self.assertRaises(OperationFailure):
                        await collection.create_index([("tenant", 1), ("expires_at", 1)], expire_after_seconds=30)
                    with self.assertRaises(OperationFailure):
                        await collection.create_index([("_id", 1)], expire_after_seconds=30)

    async def test_collection_can_create_and_drop_multiple_indexes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    names = await collection.create_indexes(
                        [
                            IndexModel([("email", 1)], unique=True),
                            IndexModel([("tenant", 1), ("created_at", -1)], name="tenant_created"),
                        ]
                    )
                    await collection.drop_index("tenant_created")
                    indexes_after_drop = await collection.list_indexes().to_list()
                    await collection.drop_indexes()
                    indexes_after_drop_all = await collection.list_indexes().to_list()

                    self.assertEqual(names, ["email_1", "tenant_created"])
                    self.assertEqual(
                        indexes_after_drop,
                        [
                            {"name": "_id_", "key": {"_id": 1}, "unique": True},
                            {"name": "email_1", "key": {"email": 1}, "unique": True},
                        ],
                    )
                    self.assertEqual(
                        indexes_after_drop_all,
                        [{"name": "_id_", "key": {"_id": 1}, "unique": True}],
                    )

    async def test_hint_requires_existing_index_and_is_reflected_in_explain(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.insert_many(
                        [
                            {"_id": "1", "kind": "view", "rank": 2},
                            {"_id": "2", "kind": "view", "rank": 1},
                        ]
                    )
                    await collection.create_index([("kind", 1)], name="kind_idx")

                    documents = await collection.find(
                        {"kind": "view"},
                        sort=[("rank", 1)],
                        hint="kind_idx",
                    ).to_list()
                    explanation = await collection.find(
                        {"kind": "view"},
                        hint="kind_idx",
                    ).explain()

                    self.assertEqual(
                        documents,
                        [
                            {"_id": "2", "kind": "view", "rank": 1},
                            {"_id": "1", "kind": "view", "rank": 2},
                        ],
                    )
                    self.assertEqual(explanation["hint"], "kind_idx")
                    self.assertEqual(explanation["hinted_index"], "kind_idx")

                    with self.assertRaises(OperationFailure):
                        await collection.find({"kind": "view"}, hint="missing_idx").to_list()

    async def test_create_indexes_rolls_back_batch_on_failure(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events

                    with self.assertRaises(OperationFailure):
                        await collection.create_indexes(
                            [
                                IndexModel([("email", 1)], name="idx_email"),
                                IndexModel([("email", 1)], unique=True, name="idx_email_unique"),
                            ]
                        )

                    self.assertEqual(
                        await collection.list_indexes().to_list(),
                        [{"name": "_id_", "key": {"_id": 1}, "unique": True}],
                    )

    async def test_drop_index_by_spec_supports_single_custom_named_index(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index([("email", 1)], name="idx_email")

                    await collection.drop_index([("email", 1)])

                    self.assertEqual(
                        await collection.list_indexes().to_list(),
                        [{"name": "_id_", "key": {"_id": 1}, "unique": True}],
                    )

    async def test_drop_index_by_spec_rejects_ambiguous_index_aliases(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index([("email", 1)], name="idx_email")
                    await collection.create_index([("email", 1)], name="idx_email_alias")

                    with self.assertRaisesRegex(OperationFailure, "multiple indexes found with key pattern"):
                        await collection.drop_index([("email", 1)])

                    await collection.drop_index("idx_email")

                    self.assertEqual(
                        await collection.list_indexes().to_list(),
                        [
                            {"name": "_id_", "key": {"_id": 1}, "unique": True},
                            {"name": "idx_email_alias", "key": {"email": 1}, "unique": False},
                        ],
                    )

    async def test_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["email"], unique=True)
                    await collection.insert_one({"_id": "1", "email": "a@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "2", "email": "a@example.com"})

    async def test_compound_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["tenant", "email"], unique=True)
                    await collection.insert_one({"_id": "1", "tenant": "a", "email": "x@example.com"})
                    await collection.insert_one({"_id": "2", "tenant": "b", "email": "x@example.com"})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "3", "tenant": "a", "email": "x@example.com"})

    async def test_nested_unique_index_is_enforced_via_public_api(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.analytics.events
                    await collection.create_index(["profile.email"], unique=True)
                    await collection.insert_one({"_id": "1", "profile": {"email": "a@example.com"}})

                    with self.assertRaises(DuplicateKeyError):
                        await collection.insert_one({"_id": "2", "profile": {"email": "a@example.com"}})

    async def test_delete_one_removes_first_matching_document(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_one({"_id": "user-1", "role": "admin"})

                    result = await collection.delete_one({"role": "admin"})

                    self.assertEqual(result.deleted_count, 1)
                    self.assertIsNone(await collection.find_one({"_id": "user-1"}))

    async def test_delete_one_without_match_returns_zero(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    result = await client.test.users.delete_one({"role": "admin"})

                    self.assertEqual(result.deleted_count, 0)

    async def test_update_and_delete_support_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    await collection.insert_one({"_id": doc_id, "name": "Ada"})

                    update_result = await collection.update_one(
                        {"_id": {"tenant": 1, "user": 2}},
                        {"$set": {"active": True}},
                    )
                    delete_result = await collection.delete_one({"_id": {"tenant": 1, "user": 2}})

                    self.assertEqual(update_result.matched_count, 1)
                    self.assertEqual(delete_result.deleted_count, 1)

    async def test_find_one_projection_supports_embedded_document_id(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    doc_id = {"tenant": 1, "user": 2}
                    await collection.insert_one({"_id": doc_id, "name": "Ada", "role": "admin"})

                    found = await collection.find_one({"_id": {"tenant": 1, "user": 2}}, {"name": 1, "_id": 0})

                    self.assertEqual(found, {"name": "Ada"})

    async def test_client_and_database_expose_collection_and_database_names(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    await client.alpha.users.insert_one({"_id": "1"})
                    await client.beta.events.insert_one({"_id": "2"})

                    databases = await client.list_database_names()
                    collections = await client.alpha.list_collection_names()

                    self.assertEqual(set(databases), {"alpha", "beta"})
                    self.assertEqual(collections, ["users"])

    async def test_invalid_input_types_raise_type_error(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    with self.assertRaises(TypeError):
                        await collection.insert_one([])

                    with self.assertRaises(TypeError):
                        await collection.find_one([])

                    with self.assertRaises(TypeError):
                        await collection.find_one({}, [])

                    with self.assertRaises(TypeError):
                        await collection.update_one([], {})

                    with self.assertRaises(TypeError):
                        await collection.update_one({}, [1])

                    with self.assertRaises(TypeError):
                        await collection.delete_one([])

                    with self.assertRaises(TypeError):
                        await collection.count_documents([])

    async def test_update_one_rejects_invalid_update_shapes(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, {})

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, {"name": "Ada"})

                    with self.assertRaises(ValueError):
                        await collection.update_one({}, [])

                    with self.assertRaises(TypeError):
                        await collection.update_one({}, {"$set": []})

    async def test_update_operations_support_aggregation_pipeline_updates(self):
        for engine_name in ENGINE_FACTORIES:
            with self.subTest(engine=engine_name):
                async with open_client(engine_name) as client:
                    collection = client.test.users
                    await collection.insert_many(
                        [
                            {"_id": "1", "tenant": "a", "name": "Ada", "points": 2, "bonus": 3, "legacy": "x"},
                            {"_id": "2", "tenant": "a", "name": "Bob", "points": 5, "bonus": 1, "legacy": "y"},
                        ]
                    )

                    first = await collection.update_one(
                        {"_id": "1"},
                        [
                            {"$set": {"total": {"$add": ["$points", "$bonus"]}, "label": "$$tag"}},
                            {"$unset": "legacy"},
                        ],
                        let={"tag": "single"},
                    )
                    many = await collection.update_many(
                        {"tenant": "a"},
                        [{"$set": {"tenant_label": {"$concat": ["$tenant", "-active"]}}}],
                    )
                    after = await collection.find_one_and_update(
                        {"_id": "2"},
                        [
                            {
                                "$replaceWith": {
                                    "$mergeObjects": [
                                        "$$ROOT",
                                        {"summary": {"$concat": ["$name", ":", "$$suffix"]}},
                                    ]
                                }
                            }
                        ],
                        let={"suffix": "done"},
                        return_document=ReturnDocument.AFTER,
                    )
                    bulk = await collection.bulk_write(
                        [
                            UpdateOne({"_id": "1"}, [{"$set": {"bulk_rank": {"$literal": 1}}}]),
                            UpdateMany({"tenant": "a"}, [{"$set": {"bulk_tag": {"$literal": "batch"}}}]),
                            UpdateOne(
                                {"tenant": "b", "kind": {"$eq": "new"}},
                                [{"$set": {"created_from_filter": "$tenant", "state": {"$literal": "upserted"}}}],
                                upsert=True,
                            ),
                        ]
                    )

                    documents = await collection.find({}, sort=[("_id", 1)]).to_list()

                    self.assertEqual(first.matched_count, 1)
                    self.assertEqual(first.modified_count, 1)
                    self.assertEqual(many.matched_count, 2)
                    self.assertEqual(many.modified_count, 2)
                    self.assertEqual(after["_id"], "2")
                    self.assertEqual(after["summary"], "Bob:done")
                    self.assertEqual(bulk.matched_count, 3)
                    self.assertEqual(bulk.modified_count, 3)
                    self.assertEqual(bulk.upserted_count, 1)
                    self.assertEqual(
                        documents,
                        [
                            {
                                "_id": "1",
                                "tenant": "a",
                                "name": "Ada",
                                "points": 2,
                                "bonus": 3,
                                "total": 5,
                                "label": "single",
                                "tenant_label": "a-active",
                                "bulk_rank": 1,
                                "bulk_tag": "batch",
                            },
                            {
                                "_id": "2",
                                "tenant": "a",
                                "name": "Bob",
                                "points": 5,
                                "bonus": 1,
                                "legacy": "y",
                                "tenant_label": "a-active",
                                "summary": "Bob:done",
                                "bulk_tag": "batch",
                            },
                            {
                                "_id": bulk.upserted_ids[2],
                                "tenant": "b",
                                "kind": "new",
                                "created_from_filter": "b",
                                "state": "upserted",
                            },
                        ],
                    )
