"""Failed operation payloads preserve native BSON, indices and partial writes."""

import asyncio
import hashlib
import json

from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.write_operation_cases import WRITE_OPERATION_CASES
from tests.support import ENGINE_FACTORIES
from tests.support_write_parity import (
    assert_write_observations,
    observe_async_write_case,
    observe_write_case,
)


TARGETS = ("7.0", "8.0", "9.0")
CELLS = list(product(TARGETS, ("4.19",), ENGINE_FACTORIES))


def operation_references(target):
    root = Path(__file__).parents[2]
    fixture = (
        root / f"fixtures/write_operations_mongodb_{target.replace('.', '_')}.json"
    )
    capture = json.loads(fixture.read_text())
    for key, relative in (
        ("catalog_sha256", "differential/write_cases.py"),
        ("operation_catalog_sha256", "differential/write_operation_cases.py"),
        ("observer_sha256", "support_write_parity.py"),
    ):
        assert (
            capture[key] == hashlib.sha256((root / relative).read_bytes()).hexdigest()
        )
    assert set(capture["observations"]) == {case.id for case in WRITE_OPERATION_CASES}
    return capture["observations"]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", WRITE_OPERATION_CASES, ids=lambda case: case.id)
def test_sync_operation_errors_match_native(target, profile, engine, case):
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        actual = observe_write_case(client.write_operations, case.id, case)
    assert_write_observations(actual, operation_references(target)[case.id], case)


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", WRITE_OPERATION_CASES, ids=lambda case: case.id)
def test_async_operation_errors_match_native(target, profile, engine, case):
    async def exercise():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            return await observe_async_write_case(
                client.write_operations, case.id, case
            )

    assert_write_observations(
        asyncio.run(exercise()), operation_references(target)[case.id], case
    )


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ("sync", "async"))),
)
def test_wire_operation_errors_match_native(target, engine, surface):
    async def exercise():
        references = operation_references(target)
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:
            options = {"directConnection": True, "serverSelectionTimeoutMS": 3000}

            def sync_calls():
                with pymongo.MongoClient(proxy.address.uri, **options) as client:
                    for case in WRITE_OPERATION_CASES:
                        actual = observe_write_case(
                            client.write_operations, case.id, case
                        )
                        assert_write_observations(actual, references[case.id], case)

            if surface == "sync":
                await asyncio.to_thread(sync_calls)
            else:
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri, **options
                ) as client:
                    for case in WRITE_OPERATION_CASES:
                        actual = await observe_async_write_case(
                            client.write_operations, case.id, case
                        )
                        assert_write_observations(actual, references[case.id], case)

    asyncio.run(exercise())
