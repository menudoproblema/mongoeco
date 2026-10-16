"""Identity/validator failures preserve native errors, partial writes and recovery."""

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

from tests.differential.write_identity_cases import (
    IDENTITY_CASES,
    RAW_IDENTITY_CASE_IDS,
)
from tests.support import ENGINE_FACTORIES
from tests.support_write_parity import (
    assert_write_observations,
    observe_async_raw_write,
    observe_async_write_case,
    observe_raw_write,
    observe_write_case,
)


TARGETS = ("7.0", "8.0", "9.0")
CELLS = list(product(TARGETS, ("4.9", "4.19"), ENGINE_FACTORIES))
RAW_CASES = [case for case in IDENTITY_CASES if case.id in RAW_IDENTITY_CASE_IDS]


@cache
def identity_references(target):
    root = Path(__file__).parents[2]
    fixture = root / f"fixtures/write_identity_mongodb_{target.replace('.', '_')}.json"
    capture = json.loads(fixture.read_text())
    for key, relative in (
        ("catalog_sha256", "differential/write_cases.py"),
        ("identity_catalog_sha256", "differential/write_identity_cases.py"),
        ("observer_sha256", "support_write_parity.py"),
    ):
        assert (
            capture[key] == hashlib.sha256((root / relative).read_bytes()).hexdigest()
        )
    assert set(capture["observations"]) == {case.id for case in IDENTITY_CASES}
    return capture["observations"]


@cache
def raw_identity_references(target):
    identity_references(target)
    path = (
        Path(__file__).parents[2]
        / f"fixtures/write_identity_mongodb_{target.replace('.', '_')}.json"
    )
    references = json.loads(path.read_text())["raw_observations"]
    assert set(references) == set(RAW_IDENTITY_CASE_IDS)
    return references


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", RAW_CASES, ids=lambda case: case.id)
def test_sync_identity_commands_match_native(target, profile, engine, case):
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        actual = observe_raw_write(client.identity, case.id, case)
    assert actual == raw_identity_references(target)[case.id]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", RAW_CASES, ids=lambda case: case.id)
def test_async_identity_commands_match_native(target, profile, engine, case):
    async def exercise():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            return await observe_async_raw_write(client.identity, case.id, case)

    assert asyncio.run(exercise()) == raw_identity_references(target)[case.id]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", IDENTITY_CASES, ids=lambda case: case.id)
def test_sync_identity_errors_match_native(target, profile, engine, case):
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        actual = observe_write_case(client.identity, case.id, case)
    assert_write_observations(actual, identity_references(target)[case.id], case)


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", IDENTITY_CASES, ids=lambda case: case.id)
def test_async_identity_errors_match_native(target, profile, engine, case):
    async def exercise():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            return await observe_async_write_case(client.identity, case.id, case)

    assert_write_observations(
        asyncio.run(exercise()), identity_references(target)[case.id], case
    )


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ("sync", "async"))),
)
def test_wire_identity_errors_match_native(target, engine, surface):
    async def exercise():
        references = identity_references(target)
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:
            options = {"directConnection": True, "serverSelectionTimeoutMS": 3000}

            def sync_calls():
                with pymongo.MongoClient(proxy.address.uri, **options) as client:
                    for case in IDENTITY_CASES:
                        actual = observe_write_case(client.identity, case.id, case)
                        assert_write_observations(actual, references[case.id], case)
                    for case in RAW_CASES:
                        assert (
                            observe_raw_write(client.identity_raw, case.id, case)
                            == raw_identity_references(target)[case.id]
                        )

            if surface == "sync":
                await asyncio.to_thread(sync_calls)
            else:
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri, **options
                ) as client:
                    for case in IDENTITY_CASES:
                        actual = await observe_async_write_case(
                            client.identity, case.id, case
                        )
                        assert_write_observations(actual, references[case.id], case)
                    for case in RAW_CASES:
                        assert (
                            await observe_async_raw_write(
                                client.identity_raw, case.id, case
                            )
                            == raw_identity_references(target)[case.id]
                        )

    asyncio.run(exercise())
