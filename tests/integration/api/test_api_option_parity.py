"""Projections, insert comments and iterables match native sync/async references."""

import asyncio
import hashlib
import json

from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.api_option_cases import API_OPTION_CASES
from tests.support import ENGINE_FACTORIES
from tests.support_api_options import (
    assert_api_observation,
    observe_api_case,
    observe_async_api_case,
)


TARGETS = ("7.0", "8.0", "9.0")
CELLS = list(product(TARGETS, ("4.9", "4.19"), ENGINE_FACTORIES))


def references(target):
    root = Path(__file__).parents[3]
    capture = json.loads(
        (
            root / f"tests/fixtures/api_options_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for relative, digest in capture["source_sha256"].items():
        assert hashlib.sha256((root / relative).read_bytes()).hexdigest() == digest
    assert set(capture["observations"]) == {case.id for case in API_OPTION_CASES}
    return capture["observations"]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
def test_sync_options_match_native(target, profile, engine, subtests):
    expected = references(target)
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        database = client.api_options
        database.command({"profile": 2, "slowms": 0})
        for case in API_OPTION_CASES:
            with subtests.test(case=case.id):
                assert_api_observation(
                    observe_api_case(database, case.id, case), expected[case.id]
                )


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
def test_async_options_match_native(target, profile, engine, subtests):
    async def exercise():
        expected = references(target)
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            database = client.api_options
            await database.command({"profile": 2, "slowms": 0})
            for case in API_OPTION_CASES:
                with subtests.test(case=case.id):
                    actual = await observe_async_api_case(database, case.id, case)
                    assert_api_observation(actual, expected[case.id])

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ("sync", "async"))),
)
def test_wire_options_match_native(target, engine, surface, subtests):
    async def exercise():
        expected = references(target)
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:
            options = {"directConnection": True, "serverSelectionTimeoutMS": 3000}

            def sync_calls():
                with pymongo.MongoClient(proxy.address.uri, **options) as client:
                    database = client.api_options
                    database.command({"profile": 2, "slowms": 0})
                    for case in API_OPTION_CASES:
                        with subtests.test(case=case.id):
                            actual = observe_api_case(database, case.id, case)
                            assert_api_observation(actual, expected[case.id])

            if surface == "sync":
                await asyncio.to_thread(sync_calls)
            else:
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri, **options
                ) as client:
                    database = client.api_options
                    await database.command({"profile": 2, "slowms": 0})
                    for case in API_OPTION_CASES:
                        with subtests.test(case=case.id):
                            actual = await observe_async_api_case(
                                database, case.id, case
                            )
                            assert_api_observation(actual, expected[case.id])

    asyncio.run(exercise())
