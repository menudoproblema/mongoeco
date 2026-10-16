"""Shared command/item errors preserve native contracts and connection reuse."""

import asyncio
import hashlib
import json

from dataclasses import replace
from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.shared_error_cases import SHARED_ERROR_CASES
from tests.differential.write_cases import WriteRequest
from tests.support import ENGINE_FACTORIES
from tests.support_unacknowledged import PoolObservation, client_options
from tests.support_write_parity import (
    assert_write_observations,
    observe_async_write_case,
    observe_write_case,
)


TARGETS = ("7.0", "8.0", "9.0")
SQRT_DOMAIN_CODE = 28714
CELLS = list(product(TARGETS, ("4.9", "4.19"), ENGINE_FACTORIES))


def references(target):
    root = Path(__file__).parents[2]
    capture = json.loads(
        (
            root / f"fixtures/shared_errors_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for key, path in (
        ("catalog_sha256", "differential/write_cases.py"),
        ("shared_error_catalog_sha256", "differential/shared_error_cases.py"),
        ("observer_sha256", "support_write_parity.py"),
        (
            "shared_error_mirror_catalog_sha256",
            "differential/shared_error_mirror_cases.py",
        ),
    ):
        assert capture[key] == hashlib.sha256((root / path).read_bytes()).hexdigest()
    assert capture["native_physical_connection_reuse"] is True
    assert set(capture["observations"]) == {case.id for case in SHARED_ERROR_CASES}
    return capture["observations"]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", SHARED_ERROR_CASES, ids=lambda case: case.id)
def test_sync_shared_error_contract(target, profile, engine, case):
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        actual = observe_write_case(
            client.shared_errors, case.id, case, error_contract=True
        )
    assert_write_observations(actual, references(target)[case.id], case)


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", SHARED_ERROR_CASES, ids=lambda case: case.id)
def test_async_shared_error_contract(target, profile, engine, case):
    async def run():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            return await observe_async_write_case(
                client.shared_errors, case.id, case, error_contract=True
            )

    assert_write_observations(asyncio.run(run()), references(target)[case.id], case)


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ("sync", "async"))),
)
def test_wire_shared_errors_reuse_physical_connection(target, engine, surface):
    async def run():
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:
            expected = references(target)

            def sync_calls():
                pool = PoolObservation()
                with pymongo.MongoClient(
                    proxy.address.uri, **client_options(pool)
                ) as client:
                    for case in SHARED_ERROR_CASES:
                        actual = observe_write_case(
                            client.shared_errors, case.id, case, error_contract=True
                        )
                        assert_write_observations(actual, expected[case.id], case)
                    assert pool.observation() == {
                        "physical_connections": 1,
                        "reused": True,
                    }

            async def async_calls():
                pool = PoolObservation()
                async with pymongo.AsyncMongoClient(
                    proxy.address.uri, **client_options(pool)
                ) as client:
                    for case in SHARED_ERROR_CASES:
                        actual = await observe_async_write_case(
                            client.shared_errors, case.id, case, error_contract=True
                        )
                        assert_write_observations(actual, expected[case.id], case)
                    assert pool.observation() == {
                        "physical_connections": 1,
                        "reused": True,
                    }

            await asyncio.to_thread(
                sync_calls if surface == "sync" else lambda: asyncio.run(async_calls())
            )

    asyncio.run(run())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("method", ["update_one", "replace_one"])
def test_mongodb7_classic_sort_rejection_has_complete_command_details(
    engine, surface, method
):
    raw_case = next(case for case in SHARED_ERROR_CASES if case.id == "command_sort")
    change = {"$set": {"value": 1}} if method == "update_one" else {"value": 1}
    case = replace(
        raw_case,
        requests=[
            WriteRequest(method, [{}, change], {"sort": {"_id": 1}}),
            raw_case.requests[1],
        ],
    )

    async def run():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect="7.0", pymongo_profile="4.19"
        ) as client:
            return await observe_async_write_case(
                client.shared_errors, case.id, case, error_contract=True
            )

    if surface == "async":
        actual = asyncio.run(run())
    else:
        with MongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect="7.0", pymongo_profile="4.19"
        ) as client:
            actual = observe_write_case(
                client.shared_errors, case.id, case, error_contract=True
            )
    # Both public methods issue the same classic update command as the raw
    # reference: MongoDB 7 rejects sort at command parsing, before any item.
    assert_write_observations(actual, references("7.0")[case.id], case)


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("consume", ["iteration", "first"])
def test_streamed_aggregation_error_keeps_complete_command_document(
    target, engine, surface, consume
):
    pipeline = [{"$set": {"x": {"$sqrt": "$x"}}}]

    def check(error):
        details = error.details
        assert error.code == SQRT_DOMAIN_CODE
        assert details["code"] == SQRT_DOMAIN_CODE
        assert details["codeName"] == "Location28714"
        assert details["ok"] == 0.0
        assert isinstance(details["errmsg"], str)
        assert details["errmsg"]
        assert "full error:" not in details["errmsg"]

    if surface == "sync":
        with MongoClient(ENGINE_FACTORIES[engine](), mongodb_dialect=target) as client:
            collection = client.stream_errors.items
            collection.insert_one({"_id": "a", "x": -1})
            cursor = collection.aggregate(pipeline, batch_size=1)
            invoke = cursor.first if consume == "first" else lambda: next(iter(cursor))
            with pytest.raises(pymongo.errors.OperationFailure) as caught:
                invoke()
            check(caught.value)
            assert (
                collection.update_one({}, {"$set": {"recovered": True}}).modified_count
                == 1
            )
    else:

        async def exercise():
            async with AsyncMongoClient(
                ENGINE_FACTORIES[engine](), mongodb_dialect=target
            ) as client:
                collection = client.stream_errors.items
                await collection.insert_one({"_id": "a", "x": -1})
                cursor = collection.aggregate(pipeline, batch_size=1)
                invoke = cursor.first if consume == "first" else cursor.__anext__
                with pytest.raises(pymongo.errors.OperationFailure) as caught:
                    await invoke()
                check(caught.value)
                result = await collection.update_one({}, {"$set": {"recovered": True}})
                assert result.modified_count == 1

        asyncio.run(exercise())
