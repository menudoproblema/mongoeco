"""Native MQL eligibility through Search candidates, writes and SQLite reopen."""

import asyncio
import math

from functools import partial
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from bson import Binary

from mongoeco import MongoClient, SearchIndexModel
from mongoeco.core import filtering as filtering_module
from mongoeco.engines import (
    MemoryEngine,
    SQLiteEngine,
    _sqlite_search_runtime as sqlite_runtime,
    _sqlite_vector_backend as vector_backend,
)
from mongoeco.types import SearchIndexDefinition

from tests.differential.semantic_search_bson_cases import (
    FILTERS,
    PHASES,
    advance,
    reset,
    selections_after_writes,
)
from tests.integration.api.test_search_predicate_authorities import VECTOR_DEFINITION
from tests.integration.api.test_semantic_group_regressions import (
    _capture,
    test_expression_matches_native_capture as replay_native_case,
)


TEXT_DEFINITION = {
    "mappings": {
        "dynamic": False,
        "fields": {
            "title": {"type": "string"},
            "kind": {"type": "token"},
            "v": {"type": "token"},
        },
    }
}
ROUTES = ("vector", "vector-downstream", "text", "compound")
_TOP_K = 2
_SUBTYPE_BOUNDARY = 32
_EXHAUSTIVE_CANDIDATE_COUNT = 100


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("route", ["vector", "vector-downstream"])
@pytest.mark.parametrize(
    ["candidate_count", "limit"], [(16, _TOP_K), (100, _TOP_K), (100, 100)]
)
def test_exhaustive_vector_budget_survives_incomplete_ann(
    dialect, route, candidate_count, limit
):
    search = vector_backend.Index.search
    modes = []

    def incomplete(index, query, *, count, exact=False):
        modes.append(exact)
        matches = search(index, query, count=count, exact=True)
        if exact:
            return matches
        # A permitted approximate result can omit even a tied neighbour. A
        # request for the entire population must not depend on ANN recall.
        return SimpleNamespace(keys=matches.keys[:-1], distances=matches.distances[:-1])

    expected = _capture(dialect, "search_bson")["cases"]["semantic_search_bson_vector"][
        "seed"
    ]["all"]
    with MongoClient(SQLiteEngine(), mongodb_dialect=dialect) as client:
        collection = client.exhaustive.rows
        reset(collection)
        indexes(collection)
        stages = pipeline({}, route, limit=limit)
        stages[0]["$vectorSearch"]["numCandidates"] = candidate_count
        with (
            patch.object(vector_backend.Index, "search", new=incomplete),
            patch.object(
                sqlite_runtime,
                "exact_vector_hits_sync",
                wraps=sqlite_runtime.exact_vector_hits_sync,
            ) as fallback,
        ):
            actual = ids(collection, stages)
            if candidate_count == _EXHAUSTIVE_CANDIDATE_COUNT:
                explanation = collection.database.command(
                    {
                        "explain": {
                            "aggregate": collection.name,
                            "pipeline": stages,
                            "cursor": {},
                        },
                        "verbosity": "executionStats",
                    }
                )
                assert (
                    explanation["engine_plan"]["details"]["exactFallbackReason"]
                    == "candidate-underflow"
                )
                assert fallback.called
            else:
                fallback.assert_not_called()
    assert modes
    if candidate_count == _EXHAUSTIVE_CANDIDATE_COUNT:
        assert actual == expected[:limit]
    else:
        assert len(actual) == _TOP_K
    assert not any(modes)


def indexes(collection):
    collection.create_search_index(
        SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
    )
    collection.create_search_index(SearchIndexModel(TEXT_DEFINITION, name="text"))


def pipeline(query, route, *, limit=100):
    if route.startswith("vector"):
        spec = {
            "index": "vectors",
            "path": "embedding",
            "queryVector": [1.0, 0.0],
            "numCandidates": 100,
            "limit": limit,
        }
        if route == "vector":
            spec["filter"] = query
            return [{"$vectorSearch": spec}]
        return [{"$vectorSearch": spec}, {"$match": query}]
    spec = {"index": "text", "text": {"query": "ada", "path": "title"}}
    if route == "compound":
        spec = {
            "index": "text",
            "compound": {"must": [{"text": {"query": "ada", "path": "title"}}]},
        }
    return [{"$search": spec}, {"$match": query}]


def ids(collection, stages):
    cursor = collection.aggregate(stages)
    try:
        return [row["_id"] for row in cursor]
    finally:
        cursor.close()


def select_search(  # noqa: PLR0913 - route plus native snapshot identity
    collection, query, *, route, universe, domain, phase, name
):
    del domain
    stages = pipeline(query, route)
    result = ids(collection, stages)
    # The final action output is checked against native MQL, independently of
    # these metamorphic warm/window checks.
    assert ids(collection, stages) == result
    assert ids(collection, [*stages, {"$skip": 1}, {"$limit": _TOP_K}]) == result[1:3]
    if name == "all":
        universe[phase] = result
    if route.startswith("vector"):
        limited = ids(collection, pipeline(query, route, limit=_TOP_K))
        expected = (
            result[:_TOP_K]
            if route == "vector"
            else [key for key in universe[phase][:_TOP_K] if key in result]
        )
        assert limited == expected
    return result


def search_action(collection, *, route, ready):
    if not ready:
        indexes(collection)
    return selections_after_writes(
        collection,
        domain="vector" if route.startswith("vector") else "document",
        select=partial(select_search, route=route, universe={}),
    )


def factory_for_wire(engine_factory, surface):
    if not surface.startswith("wire-"):
        return engine_factory, False
    engine = engine_factory()

    async def prepare():
        await engine.connect()
        for spec, name, kind in (
            (VECTOR_DEFINITION, "vectors", "vectorSearch"),
            (TEXT_DEFINITION, "text", "search"),
        ):
            await engine.create_search_index(
                "semantic_group",
                "records",
                SearchIndexDefinition(spec, name=name, index_type=kind),
            )

    asyncio.run(prepare())
    return lambda: engine, True


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async", "wire-sync", "wire-async"])
@pytest.mark.parametrize("route", ROUTES)
def test_search_bson_filters_preserve_native_eligibility_and_phase_windows(
    dialect, engine_factory, surface, route
):
    factory, ready = factory_for_wire(engine_factory, surface)
    domain = "vector" if route.startswith("vector") else "document"
    replay_native_case(
        factory,
        dialect,
        "search_bson",
        "semantic_search_bson_" + domain,
        partial(search_action, route=route, ready=ready),
        surface=surface,
    )


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("route", ROUTES)
def test_sqlite_bson_filters_survive_fresh_engines_and_warm_reopen(
    tmp_path, dialect, route
):
    path = str(tmp_path / "search.sqlite")
    domain = "vector" if route.startswith("vector") else "document"
    expected = _capture(dialect, "search_bson")["cases"][
        "semantic_search_bson_" + domain
    ]
    with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect) as client:
        collection = client.persistence.rows
        reset(collection)
        indexes(collection)
    for phase in PHASES:
        # Mutate on another engine, after the previous snapshot has been warmed.
        with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect) as client:
            advance(client.persistence.rows, phase)
        for _ in range(2):
            with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect) as client:
                collection = client.persistence.rows
                universe = {}
                for name, query in FILTERS.items():
                    actual = select_search(
                        collection,
                        query,
                        route=route,
                        universe=universe,
                        domain=domain,
                        phase=phase,
                        name=name,
                    )
                    assert actual == expected[phase][name]
                explanation = collection.database.command(
                    {
                        "explain": {
                            "aggregate": collection.name,
                            "pipeline": pipeline({}, route),
                            "cursor": {},
                        },
                        "verbosity": "executionStats",
                    }
                )
                assert (
                    explanation["engine_plan"]["details"]["backend"]
                    == {
                        "vector": "usearch",
                        "vector-downstream": "usearch",
                        "text": "fts5",
                        "compound": "fts5-prefilter",
                    }[route]
                )


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("route", ROUTES)
def test_sqlite_bson_filters_keep_native_truth_in_python_fallback(dialect, route):
    decide = sqlite_runtime.decide_sqlite_search_backend
    backends = []

    def portable(*args, **kwargs):
        kwargs.update(ann_available=False, fts5_available=False)
        decision = decide(*args, **kwargs)
        backends.append(decision.backend)
        return decision

    domain = "vector" if route.startswith("vector") else "document"
    with patch.object(
        sqlite_runtime, "decide_sqlite_search_backend", side_effect=portable
    ):
        replay_native_case(
            SQLiteEngine,
            dialect,
            "search_bson",
            "semantic_search_bson_" + domain,
            partial(search_action, route=route, ready=False),
            surface="sync",
        )
    assert backends
    assert set(backends) == {"python"}


@pytest.mark.parametrize("tied", [False, True])
def test_sqlite_ranks_all_received_ann_candidates_before_truncating(tied):
    engine = SQLiteEngine()
    with MongoClient(engine) as client:
        collection = client.batch.rows
        collection.insert_many(
            [
                {"_id": "00", "title": "Ada", "embedding": [1.0, 0.0]},
                {
                    "_id": "01",
                    "title": "Ada",
                    "embedding": [1.0, 0.0] if tied else [0.8, 0.6],
                },
                {
                    "_id": "02",
                    "title": "Ada",
                    "embedding": [1.0, 0.0] if tied else [0.0, 1.0],
                },
            ]
        )
        indexes(collection)
        scores = []
        score = sqlite_runtime.score_vector_document

        def observe(document, **options):
            scores.append(document["_id"])
            return score(document, **options)

        def reversed_batch(state, **_options):
            # All candidates fit the requested budget. ANN order need not be
            # the exact public score order, especially on a quantized tie.
            return [(key, 0.0) for key in reversed(state.storage_keys_by_slot)]

        with (
            patch.object(
                sqlite_runtime,
                "search_sqlite_vector_backend",
                side_effect=reversed_batch,
            ),
            patch.object(sqlite_runtime, "score_vector_document", side_effect=observe),
        ):
            assert ids(collection, pipeline({}, "vector", limit=_TOP_K)) == ["00", "01"]
        assert set(scores) == {"00", "01", "02"}


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("subtype", [0, 5])
@pytest.mark.parametrize("limit", [2, 64])
def test_vector_residual_preparation_is_shared_with_exact_fallback(
    dialect, subtype, limit
):
    expected_ids = _capture(dialect, "p2_read_cost")["cases"][
        "semantic_p2_vector_filter_eligibility"
    ]["late"][str(subtype)]
    expected = sorted(
        expected_ids, key=lambda n: (-1 / math.sqrt(1 + (int(n) / 64) ** 2), n)
    )[:limit]
    with MongoClient(SQLiteEngine(), mongodb_dialect=dialect) as client:
        collection = client.p2.records
        collection.insert_many(
            [
                {
                    "_id": f"{n:03}",
                    "embedding": [1.0, n / 64],
                    "v": Binary(b"x", 5 if n >= _SUBTYPE_BOUNDARY else 0),
                }
                for n in range(64)
            ]
        )
        collection.create_search_index(
            SearchIndexModel(VECTOR_DEFINITION, name="vectors", type="vectorSearch")
        )
        with (
            patch.object(
                filtering_module,
                "compile_filter",
                wraps=filtering_module.compile_filter,
            ) as repeated,
            patch.object(
                sqlite_runtime,
                "exact_vector_hits_sync",
                wraps=sqlite_runtime.exact_vector_hits_sync,
            ) as exact,
        ):
            actual = ids(
                collection,
                pipeline({"v": Binary(b"x", subtype)}, "vector", limit=limit),
            )
        assert actual == expected
        assert exact.call_count == int(limit > len(expected_ids))
        residual_calls = [
            call for call in repeated.call_args_list
            if call.args and set(call.args[0]) == {"v"}
        ]
        assert len(residual_calls) == 1
