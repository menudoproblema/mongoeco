"""Malformed write queries fail before a valid prefix changes the collection."""

import asyncio
import hashlib
import json

from functools import cache
from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.write_query_cases import RAW_QUERY_CASES
from tests.support import ENGINE_FACTORIES
from tests.support_write_parity import observe_async_raw_write, observe_raw_write


TARGETS = ("7.0", "8.0", "9.0")
CELLS = list(product(TARGETS, ("4.9", "4.19"), ENGINE_FACTORIES))


@cache
def references(target):
    root = Path(__file__).parents[2]
    data = json.loads(
        (
            root / f"fixtures/write_queries_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for key, relative in (
        ("query_catalog_sha256", "differential/write_query_cases.py"),
        ("observer_sha256", "support_write_parity.py"),
    ):
        assert data[key] == hashlib.sha256((root / relative).read_bytes()).hexdigest()
    assert set(data["observations"]) == {case.id for case in RAW_QUERY_CASES}
    return data["observations"]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", RAW_QUERY_CASES, ids=lambda c: c.id)
def test_sync_query_shape_matches_native(target, profile, engine, case):
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        assert (
            observe_raw_write(client.queries, case.id, case)
            == references(target)[case.id]
        )


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", RAW_QUERY_CASES, ids=lambda c: c.id)
def test_async_query_shape_matches_native(target, profile, engine, case):
    async def exercise():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            assert (
                await observe_async_raw_write(client.queries, case.id, case)
                == references(target)[case.id]
            )

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ("sync", "async"))),
)
def test_wire_query_shape_matches_native(target, engine, surface):
    async def exercise():
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:
            options = {"directConnection": True, "serverSelectionTimeoutMS": 3000}
            if surface == "sync":

                def calls():
                    with pymongo.MongoClient(proxy.address.uri, **options) as client:
                        for case in RAW_QUERY_CASES:
                            assert (
                                observe_raw_write(client.queries, case.id, case)
                                == references(target)[case.id]
                            )

                await asyncio.to_thread(calls)
            else:
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri, **options
                ) as client:
                    for case in RAW_QUERY_CASES:
                        assert (
                            await observe_async_raw_write(client.queries, case.id, case)
                            == references(target)[case.id]
                        )

    asyncio.run(exercise())
