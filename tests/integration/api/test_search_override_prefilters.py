"""Unproven dialect equality must retain the canonical residual in every plan."""

import asyncio

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.compat.base import MongoDialect70, MongoDialect80, MongoDialect90
from mongoeco.engines import (
    MemoryEngine,
    SQLiteEngine,
    _memory_search_runtime as memory_runtime,
    _sqlite_search_runtime as sqlite_runtime,
)

from tests.integration.api.test_search_bson_persistence import (
    ROUTES,
    ids,
    indexes,
    pipeline,
)
from tests.integration.api.test_semantic_group_regressions import _AsyncView


class _CaseFold:
    def values_equal(self, left, right):
        if type(left) is str and type(right) is str:
            return left.casefold() == right.casefold()
        return super().values_equal(left, right)


class _Fold70(_CaseFold, MongoDialect70):
    pass


class _Fold80(_CaseFold, MongoDialect80):
    pass


class _Fold90(_CaseFold, MongoDialect90):
    pass


DIALECTS = (_Fold70, _Fold80, _Fold90)
SEED = [
    {"_id": str(i), "title": "Ada", "kind": "keep", "v": value, "embedding": [1.0, 0.0]}
    for i, value in enumerate(("KEEP", "keep", "other"))
]


def assert_selection(collection, route):
    query = {"v": "keep"}
    assert [row["_id"] for row in collection.find(query, sort=[("_id", 1)])] == [
        "0",
        "1",
    ]
    # These scalar keys have no proof under this user override. Fail if the
    # runtime attempts them, including when explain reconstructs the plan.
    with (
        patch.object(
            memory_runtime,
            "candidate_rows_for_vector_filter",
            side_effect=AssertionError("unproven MQL prefilter"),
        ),
        patch.object(
            sqlite_runtime,
            "vector_filter_candidate_storage_keys",
            side_effect=AssertionError("unproven MQL prefilter"),
        ),
        patch.object(
            sqlite_runtime,
            "_sqlite_candidate_storage_keys_for_downstream_filter",
            side_effect=AssertionError("unproven MQL prefilter"),
        ),
    ):
        for _ in range(2):
            assert ids(collection, pipeline(query, route)) == ["0", "1"]
        explanation = collection.database.command(
            {
                "explain": {
                    "aggregate": collection.name,
                    "pipeline": pipeline(query, route),
                    "cursor": {},
                },
                "verbosity": "executionStats",
            }
        )
        details = explanation["engine_plan"]["details"]
        for name in ("vectorFilterPrefilter", "downstreamFilterCandidatePrefilter"):
            assert not (details.get(name) or {}).get("candidateable")


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("engine", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("route", ROUTES)
def test_custom_equality_uses_canonical_filter_before_ranking(
    dialect, engine, surface, route
):
    if surface == "sync":
        with MongoClient(engine(), mongodb_dialect=dialect()) as client:
            c = client.overrides.rows
            c.insert_many(SEED)
            indexes(c)
            assert_selection(c, route)
    else:

        async def run():
            async with AsyncMongoClient(engine(), mongodb_dialect=dialect()) as client:
                c = _AsyncView(client.overrides.rows, asyncio.get_running_loop())
                await asyncio.to_thread(c.insert_many, SEED)
                await asyncio.to_thread(indexes, c)
                await asyncio.to_thread(assert_selection, c, route)

        asyncio.run(run())


@pytest.mark.parametrize("dialect", DIALECTS)
@pytest.mark.parametrize("route", ROUTES)
def test_custom_equality_survives_sqlite_reopen(tmp_path, dialect, route):
    path = str(tmp_path / "overrides.sqlite")
    with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect()) as client:
        c = client.overrides.rows
        c.insert_many(SEED)
        indexes(c)
        assert_selection(c, route)
    with MongoClient(SQLiteEngine(path), mongodb_dialect=dialect()) as client:
        assert_selection(client.overrides.rows, route)
