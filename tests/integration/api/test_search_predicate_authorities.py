"""Search eligibility uses a native MQL oracle, not another local evaluator."""

import asyncio

from copy import deepcopy
from functools import partial
from unittest.mock import patch

import pytest

from pymongo.operations import SearchIndexModel as DriverSearchIndexModel

from mongoeco import MongoClient, SearchIndexModel
from mongoeco.engines import _sqlite_search_runtime as runtime
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import SearchIndexDefinition

from tests.differential.semantic_query_matching_cases import (
    BSON_REPRESENTATION_CONDITIONS,
    BSON_REPRESENTATION_SEED,
)
from tests.differential.semantic_search_eligibility_cases import (
    FILTERS,
    SEED,
    collation_eligibility,
    hybrid_eligibility,
    mutations,
)
from tests.integration.api.test_semantic_group_regressions import (
    _capture,
    test_expression_matches_native_capture as replay_native_case,
)


VECTOR_DEFINITION = {
    "fields": [
        {
            "type": "vector",
            "path": "embedding",
            "numDimensions": 2,
            "similarity": "cosine",
        }
    ]
}
TEXT_DEFINITION = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "title": {"type": "string"},
            "kind": {"type": "token"},
            "a": {"type": "number"},
            "scalar": {"type": "number"},
        },
    }
}


def routes(collection, *, route, model_factory, indexes_ready=False):
    if not indexes_ready:
        collection.create_search_index(
            model_factory(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))
    output = {}
    for name, query in FILTERS.items():
        if route in {"vector", "vector-downstream"}:
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": "vectors",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "numCandidates": 100,
                        "limit": 100,
                        **({"filter": query} if route == "vector" else {}),
                    }
                }
            ]
        else:
            pipeline = [
                {
                    "$search": {
                        "index": "text",
                        "text": {"query": "ada", "path": "title"},
                    }
                },
                {"$match": query},
            ]
        if route == "vector-downstream":
            pipeline.append({"$match": query})
        if route == "compound":
            pipeline[0]["$search"] = {
                "index": "text",
                "compound": {
                    "must": [{"text": {"query": "ada", "path": "title"}}],
                    "should": [{"text": {"query": "ada", "path": "title"}}],
                },
            }
        if route == "collector":
            pipeline[0]["$search"]["count"] = {"type": "total"}
        documents = list(collection.aggregate(pipeline))
        output[name] = [doc["_id"] for doc in documents]
        assert [
            doc["_id"]
            for doc in collection.aggregate([*pipeline, {"$skip": 1}, {"$limit": 2}])
        ] == output[name][1:3]
        if route == "writeback":
            target_name = "selected_" + name
            list(collection.aggregate([*pipeline, {"$merge": {"into": target_name}}]))
            persisted = [
                doc["_id"]
                for doc in collection.database[target_name].find({}, sort=[("_id", 1)])
            ]
            assert persisted == output[name]
    return output


def search_fixture_factory(engine_factory, surface):
    indexes_ready = surface.startswith("wire-")
    if indexes_ready:
        # Index administration is outside the wire subset. Only fixture setup
        # uses the engine admin SPI; all data and Search commands use PyMongo.
        engine = engine_factory()

        async def prepare_indexes():
            await engine.connect()
            await engine.create_search_index(
                "semantic_group",
                "records",
                SearchIndexDefinition(
                    VECTOR_DEFINITION, name="vectors", index_type="vectorSearch"
                ),
            )
            await engine.create_search_index(
                "semantic_group",
                "records",
                SearchIndexDefinition(TEXT_DEFINITION, name="text"),
            )

        asyncio.run(prepare_indexes())

        def factory():
            return engine
    else:
        factory = engine_factory
    return factory, indexes_ready


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async", "wire-sync", "wire-async"])
@pytest.mark.parametrize(
    "route",
    ["vector", "vector-downstream", "text", "compound", "collector", "writeback"],
)
def test_search_preserves_native_mql_eligibility(
    engine_factory, dialect, surface, route
):
    factory, indexes_ready = search_fixture_factory(engine_factory, surface)
    replay_native_case(
        factory,
        dialect,
        "search_eligibility",
        "semantic_search_eligibility",
        partial(
            routes,
            route=route,
            indexes_ready=indexes_ready,
            model_factory=DriverSearchIndexModel
            if surface.startswith("wire-")
            else SearchIndexModel,
        ),
        surface=surface,
    )


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_search_v1_range_controls_are_independent_of_mql(engine_factory):

    with MongoClient(engine_factory()) as client:
        collection = client.search_controls.records
        collection.insert_many(SEED)
        collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))
        # The declared search-v1 range correlates its bounds on one array element.
        # These literal witnesses are independent of both the index and MQL matcher.
        assert [
            doc["_id"]
            for doc in collection.aggregate(
                [
                    {
                        "$search": {
                            "index": "text",
                            "range": {"path": "a", "gt": 5, "lt": 10},
                        }
                    }
                ]
            )
        ] == ["d", "g"]
        assert [
            doc["_id"]
            for doc in collection.aggregate(
                [{"$search": {"index": "text", "range": {"path": "a", "gt": 2**53}}}]
            )
        ] == ["b"]


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_numeric_search_collectors_preserve_integer_buckets(engine_factory):
    with MongoClient(engine_factory()) as client:
        collection = client.search_controls.records
        collection.insert_many(SEED)
        definition = {
            "mappings": {
                "dynamic": False,
                "fields": {"title": {"type": "string"}, "scalar": {"type": "number"}},
            }
        }
        collection.create_search_index(SearchIndexModel(definition, name="text"))
        result = list(
            collection.aggregate(
                [
                    {
                        "$searchMeta": {
                            "index": "text",
                            "text": {"query": "ada", "path": "title"},
                            "count": {"type": "total"},
                            "facet": {
                                "path": "scalar",
                                "type": "number",
                                "numBuckets": 10,
                            },
                        }
                    }
                ]
            )
        )
        assert result[0]["count"] == {"total": len(SEED), "exact": True}
        assert result[0]["facet"]["buckets"] == [
            {"value": 0, "count": len(SEED) - 2},
            {"value": 2**53, "count": 1},
            {"value": 2**53 + 1, "count": 1},
        ]


def mutation_routes(collection, *, route, indexes_ready):
    def select(query):
        if route == "vector":
            pipeline = [
                {
                    "$vectorSearch": {
                        "index": "vectors",
                        "path": "embedding",
                        "queryVector": [1.0, 0.0],
                        "numCandidates": 100,
                        "limit": 100,
                        "filter": query,
                    }
                }
            ]
        else:
            pipeline = [
                {
                    "$search": {
                        "index": "text",
                        "text": {"query": "ada", "path": "title"},
                    }
                },
                {"$match": query},
            ]
        return [row["_id"] for row in collection.aggregate(pipeline)]

    if not indexes_ready:
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))
    return mutations(collection, select=select)


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async", "wire-sync", "wire-async"])
@pytest.mark.parametrize("route", ["vector", "text"])
def test_search_candidate_domains_are_rebuilt_after_writes(
    engine_factory, dialect, surface, route
):
    factory, ready = search_fixture_factory(engine_factory, surface)
    replay_native_case(
        factory,
        dialect,
        "search_eligibility",
        "semantic_search_candidate_mutations",
        partial(mutation_routes, route=route, indexes_ready=ready),
        surface=surface,
    )


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_vector_filter_is_prepared_before_empty_input(engine_factory):
    with MongoClient(engine_factory()) as client:
        collection = client.search_controls.records
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        with pytest.raises(OperationFailure, match="query operator"):
            list(
                collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "vectors",
                                "path": "embedding",
                                "queryVector": [1.0, 0.0],
                                "numCandidates": 10,
                                "limit": 10,
                                "filter": {"$invalid": 1},
                            }
                        }
                    ]
                )
            )


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_search_v1_exists_and_negation_use_declared_field_presence(engine_factory):
    with MongoClient(engine_factory()) as client:
        collection = client.search_controls.records
        collection.insert_many(SEED)
        collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))
        exists = {"index": "text", "exists": {"path": "kind"}}
        expected = [doc["_id"] for doc in SEED if "kind" in doc]
        assert [
            doc["_id"] for doc in collection.aggregate([{"$search": exists}])
        ] == expected
        assert list(
            collection.aggregate(
                [{"$searchMeta": {**exists, "count": {"type": "total"}}}]
            )
        ) == [{"count": {"total": len(expected), "exact": True}}]
        excluded = {
            "index": "text",
            "compound": {
                "must": [{"text": {"query": "ada", "path": "title"}}],
                "mustNot": [{"exists": {"path": "kind"}}],
            },
        }
        assert [
            doc["_id"] for doc in collection.aggregate([{"$search": excluded}])
        ] == ["e"]


def collation_routes(collection, *, route, indexes_ready):
    if not indexes_ready:
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))

    def select(collation):
        if route.startswith("vector"):
            spec = {
                "index": "vectors",
                "path": "embedding",
                "queryVector": [1.0, 0.0],
                "numCandidates": 100,
                "limit": 100,
            }
            if route == "vector":
                spec["filter"] = {"kind": "a"}
            pipeline = [{"$vectorSearch": spec}]
            if route == "vector-downstream":
                pipeline.append({"$match": {"kind": "a"}})
        else:
            pipeline = [
                {
                    "$search": {
                        "index": "text",
                        "text": {"query": "ada", "path": "title"},
                    }
                },
                {"$match": {"kind": "a"}},
            ]
        options = {"collation": collation} if collation else {}
        result = [row["_id"] for row in collection.aggregate(pipeline, **options)]
        explanation = collection.database.command(
            {
                "explain": {
                    "aggregate": collection.name,
                    "pipeline": pipeline,
                    "cursor": {},
                    **options,
                },
                "verbosity": "executionStats",
            }
        )
        details = explanation["engine_plan"]["details"]
        # returnedHitCount describes the Search stage. A following match can
        # remain outside that domain when no safe pushdown was selected.
        filtered_stage = (
            route == "vector" or details.get("downstreamFilterPrefilter") is not None
        )
        expected_count = len(result) if filtered_stage else 3
        assert details["returnedHitCount"] == expected_count
        if collation and route.startswith("vector"):
            for name in ("vectorFilterPrefilter", "downstreamFilterCandidatePrefilter"):
                assert not (details.get(name) or {}).get("candidateable")
        return result

    return collation_eligibility(collection, select=select)


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize(
    ["engine_factory", "exact"],
    [(MemoryEngine, False), (SQLiteEngine, False), (SQLiteEngine, True)],
)
@pytest.mark.parametrize("surface", ["sync", "async", "wire-sync", "wire-async"])
@pytest.mark.parametrize("route", ["vector", "vector-downstream", "text"])
def test_search_filter_context_survives_binary_cache_and_exact_fallback(
    engine_factory, exact, dialect, surface, route
):
    factory, indexes_ready = search_fixture_factory(engine_factory, surface)
    decide = runtime.decide_sqlite_search_backend

    def without_ann(*args, **kwargs):
        if exact:
            kwargs["ann_available"] = False
        return decide(*args, **kwargs)

    with patch.object(runtime, "decide_sqlite_search_backend", side_effect=without_ann):
        replay_native_case(
            factory,
            dialect,
            "search_eligibility",
            "semantic_search_collation",
            partial(collation_routes, route=route, indexes_ready=indexes_ready),
            surface=surface,
        )


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_empty_search_collectors_have_zero_count_and_no_buckets(engine_factory):
    with MongoClient(engine_factory()) as client:
        collection = client.collectors.records
        collection.insert_one({"_id": 1, "title": "Ada", "scalar": "one"})
        collection.create_search_index(
            SearchIndexModel(
                {
                    "mappings": {
                        "dynamic": False,
                        "fields": {
                            "title": {"type": "string"},
                            "scalar": {"type": "token"},
                        },
                    }
                },
                name="text",
            )
        )
        result = list(
            collection.aggregate(
                [
                    {
                        "$searchMeta": {
                            "index": "text",
                            "text": {"query": "absent", "path": "title"},
                            "count": {"type": "total"},
                            "facet": {
                                "path": "scalar",
                                "type": "token",
                                "numBuckets": 10,
                            },
                        }
                    }
                ]
            )
        )
        assert result[0]["count"] == {"total": 0, "exact": True}
        assert result[0]["facet"]["buckets"] == []


def test_sqlite_collector_failure_cleans_temporary_state_and_recovers():
    import sqlite3  # noqa: PLC0415 - fault injection belongs to this SQLite control

    engine = SQLiteEngine()
    with MongoClient(engine) as client:
        collection = client.collector_fault.records
        collection.insert_one({"_id": 1, "title": "Ada", "scalar": "one"})
        collection.create_search_index(
            SearchIndexModel(
                {
                    "mappings": {
                        "dynamic": False,
                        "fields": {
                            "title": {"type": "string"},
                            "scalar": {"type": "token"},
                        },
                    },
                },
                name="text",
            )
        )
        pipeline = [
            {
                "$searchMeta": {
                    "index": "text",
                    "text": {"query": "ada", "path": "title"},
                    "count": {"type": "total"},
                    "facet": {"path": "scalar", "type": "token", "numBuckets": 10},
                }
            }
        ]
        denied = []
        states = []
        collect = runtime._collect_sqlite_search_facets

        def with_sql_failure(engine, connection, **kwargs):
            before = connection.in_transaction

            def fail_candidate_read(action, table, _column, _database, _trigger):
                if action == sqlite3.SQLITE_READ and table.startswith(
                    "mongoeco_search_candidates_"
                ):
                    denied.append(table)
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            connection.set_authorizer(fail_candidate_read)
            try:
                return collect(engine, connection, **kwargs)
            finally:
                connection.set_authorizer(None)
                leftovers = [
                    row[0]
                    for row in connection.execute("SELECT name FROM sqlite_temp_master")
                    if row[0].startswith("mongoeco_search_candidates_")
                ]
                states.append((before, connection.in_transaction, leftovers))

        with (
            patch.object(
                runtime, "_collect_sqlite_search_facets", side_effect=with_sql_failure
            ),
            pytest.raises(sqlite3.DatabaseError),
        ):
            list(collection.aggregate(pipeline))
        assert denied
        assert states
        assert all(before == after and not tables for before, after, tables in states)
        result = list(collection.aggregate(pipeline))
        assert result[0]["count"] == {"total": 1, "exact": True}
        assert result[0]["facet"]["buckets"] == [{"value": "one", "count": 1}]


def hybrid_routes(collection, *, indexes_ready):
    if not indexes_ready:
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )

    def select(stage_filter, downstream_filter):
        pipeline = [
            {
                "$vectorSearch": {
                    "index": "vectors",
                    "path": "embedding",
                    "queryVector": [1.0, 0.0],
                    "numCandidates": 100,
                    "limit": 100,
                    "filter": stage_filter,
                }
            },
            {"$match": downstream_filter},
            {"$limit": 100},
        ]
        return [row["_id"] for row in collection.aggregate(pipeline)]

    return hybrid_eligibility(collection, select=select)


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async", "wire-sync", "wire-async"])
def test_vector_query_and_downstream_filters_share_eligible_universe(
    engine_factory, dialect, surface
):
    factory, indexes_ready = search_fixture_factory(engine_factory, surface)
    replay_native_case(
        factory,
        dialect,
        "search_eligibility",
        "semantic_search_hybrid_filters",
        partial(hybrid_routes, indexes_ready=indexes_ready),
        surface=surface,
    )


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_vector_universe_rebuilds_for_invalid_documents_and_dimension_changes(
    engine_factory,
):
    native = _capture("9.0", "search_eligibility")["cases"][
        "semantic_search_eligibility"
    ]
    documents = deepcopy(SEED)
    invalid = {"a", "b", "c", "d", "e", "f"}
    values = {
        "a": None,
        "b": [1.0],
        "c": [1.0, 0.0, 0.0],
        "d": ["bad", 0.0],
        "f": [float("nan"), 0.0],
    }
    for document in documents:
        if document["_id"] == "e":
            document.pop("embedding")
        elif document["_id"] in values:
            document["embedding"] = values[document["_id"]]
    with MongoClient(engine_factory()) as client:
        collection = client.vector_universe.records
        collection.insert_many(documents)
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )

        def select(query, vector):
            return [
                row["_id"]
                for row in collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "vectors",
                                "path": "embedding",
                                "queryVector": vector,
                                "numCandidates": 100,
                                "limit": 100,
                                "filter": query,
                            }
                        }
                    ]
                )
            ]

        for label, query in FILTERS.items():
            assert select(query, [1.0, 0.0]) == [
                identifier for identifier in native[label] if identifier not in invalid
            ]
        collection.update_one({"_id": "b"}, {"$set": {"embedding": [1.0, 0.0]}})
        invalid.remove("b")
        label = "exists"
        query = FILTERS[label]
        expected = [
            identifier for identifier in native[label] if identifier not in invalid
        ]
        assert select(query, [1.0, 0.0]) == expected
        collection.drop_search_index("vectors")
        collection.create_search_index(
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
                name="vectors",
                type="vectorSearch",
            )
        )
        assert select(query, [1.0, 0.0, 0.0]) == ["c"]
        collection.drop_search_index("vectors")
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        assert select(query, [1.0, 0.0]) == expected


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("coordinate", [float("nan"), float("inf"), float("-inf")])
def test_nonfinite_query_vectors_fail_before_empty_search(engine_factory, coordinate):
    with MongoClient(engine_factory()) as client:
        collection = client.vector_inputs.records
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        with pytest.raises(OperationFailure, match="finite numeric"):
            list(
                collection.aggregate(
                    [
                        {
                            "$vectorSearch": {
                                "index": "vectors",
                                "path": "embedding",
                                "queryVector": [coordinate, 0.0],
                                "numCandidates": 10,
                                "limit": 1,
                            }
                        }
                    ]
                )
            )
        assert collection.count_documents({}) == 0


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("route", ["vector", "text"])
def test_search_bson_classification_uses_native_eligibility(
    engine_factory, dialect, route
):
    native = _capture(dialect, "query_matching")["cases"][
        "semantic_query_matching_bson_representations"
    ]
    documents = deepcopy(BSON_REPRESENTATION_SEED)
    for document in documents:
        document.update(title="Ada", embedding=[1.0, 0.0])
    with MongoClient(engine_factory(), mongodb_dialect=dialect) as client:
        collection = client.bson_search.records
        collection.insert_many(documents)
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))
        for label, condition in BSON_REPRESENTATION_CONDITIONS.items():
            query = {"a": condition}
            pipeline = (
                [
                    {
                        "$vectorSearch": {
                            "index": "vectors",
                            "path": "embedding",
                            "queryVector": [1.0, 0.0],
                            "limit": 100,
                            "numCandidates": 100,
                            "filter": query,
                        }
                    }
                ]
                if route == "vector"
                else [
                    {
                        "$search": {
                            "index": "text",
                            "text": {"query": "ada", "path": "title"},
                        }
                    },
                    {"$match": query},
                ]
            )
            assert [
                row["_id"]
                for row in collection.aggregate([*pipeline, {"$sort": {"_id": 1}}])
            ] == native[label]["find"]
