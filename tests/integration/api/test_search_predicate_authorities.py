"""Search eligibility uses a native MQL oracle, not another local evaluator."""

import asyncio

from functools import partial

import pytest

from pymongo.operations import SearchIndexModel as DriverSearchIndexModel

from mongoeco import MongoClient, SearchIndexModel
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import SearchIndexDefinition

from tests.differential.semantic_search_eligibility_cases import (
    FILTERS,
    SEED,
    mutations,
)
from tests.integration.api.test_semantic_group_regressions import (
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
