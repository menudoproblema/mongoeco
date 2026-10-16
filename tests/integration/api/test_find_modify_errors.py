"""FindAndModify error documents retain native metadata and permit recovery."""

import asyncio
import hashlib
import json
import re

from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.find_modify_cases import FIND_MODIFY_CASES
from tests.support import ENGINE_FACTORIES
from tests.support_write_parity import (
    assert_write_observations,
    observe_async_write_case,
    observe_write_case,
)


DUPLICATE_KEY_CODE = 11000
TARGETS = ("7.0", "8.0", "9.0")
CELLS = list(product(TARGETS, ("4.9", "4.19"), ENGINE_FACTORIES))


def find_modify_references(target):
    root = Path(__file__).parents[2]
    fixture = root / f"fixtures/find_modify_mongodb_{target.replace('.', '_')}.json"
    capture = json.loads(fixture.read_text())
    for key, relative in (
        ("catalog_sha256", "differential/write_cases.py"),
        ("find_modify_catalog_sha256", "differential/find_modify_cases.py"),
        ("observer_sha256", "support_write_parity.py"),
    ):
        assert (
            capture[key] == hashlib.sha256((root / relative).read_bytes()).hexdigest()
        )
    assert set(capture["observations"]) == {case.id for case in FIND_MODIFY_CASES}
    return capture["observations"]


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", FIND_MODIFY_CASES, ids=lambda case: case.id)
def test_sync_command_errors_match_native(target, profile, engine, case):
    with MongoClient(
        ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
    ) as client:
        actual = observe_write_case(client.find_modify, case.id, case)
    assert_write_observations(actual, find_modify_references(target)[case.id], case)


@pytest.mark.parametrize(["target", "profile", "engine"], CELLS)
@pytest.mark.parametrize("case", FIND_MODIFY_CASES, ids=lambda case: case.id)
def test_async_command_errors_match_native(target, profile, engine, case):
    async def exercise():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile=profile
        ) as client:
            return await observe_async_write_case(client.find_modify, case.id, case)

    assert_write_observations(
        asyncio.run(exercise()), find_modify_references(target)[case.id], case
    )


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ("sync", "async"))),
)
def test_wire_command_errors_match_native(target, engine, surface):
    async def exercise():
        references = find_modify_references(target)
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:
            options = {"directConnection": True, "serverSelectionTimeoutMS": 3000}

            def sync_calls():
                with pymongo.MongoClient(proxy.address.uri, **options) as client:
                    for case in FIND_MODIFY_CASES:
                        actual = observe_write_case(client.find_modify, case.id, case)
                        assert_write_observations(actual, references[case.id], case)

            if surface == "sync":
                await asyncio.to_thread(sync_calls)
            else:

                async def async_calls():
                    async with pymongo.AsyncMongoClient(
                        proxy.address.uri, **options
                    ) as client:
                        for case in FIND_MODIFY_CASES:
                            actual = await observe_async_write_case(
                                client.find_modify, case.id, case
                            )
                            assert_write_observations(actual, references[case.id], case)

                # The experimental 4.9 async driver polls sockets synchronously.
                # Give the server an independent loop, as the managed kit does.
                await asyncio.to_thread(lambda: asyncio.run(async_calls()))

    asyncio.run(exercise())


def _duplicate_index(error):
    # A client must be able to consume this server error as a complete document.
    assert error.code == DUPLICATE_KEY_CODE
    message = error.details["errmsg"]
    match = re.fullmatch(
        r"Plan executor error during findAndModify :: caused by :: "
        r"E11000 duplicate key error collection: .* index: (.*?) dup key: (.*)",
        message,
    )
    assert match is not None
    assert match[1] == "key_1"
    assert error.details["keyPattern"] == {"key": 1}
    assert error.details["keyValue"] == {"key": "taken"}
    return match[1]


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("method", ["find_one_and_update", "find_one_and_replace"])
def test_sync_duplicate_error_can_be_translated_and_recovered(engine, method):
    with MongoClient(ENGINE_FACTORIES[engine]()) as client:
        collection = client.recovery.items
        collection.create_index("key", unique=True)
        collection.insert_many(
            [{"_id": "a", "key": "old"}, {"_id": "b", "key": "taken"}]
        )
        change = (
            {"$set": {"key": "taken"}}
            if method.endswith("update")
            else {"key": "taken"}
        )
        with pytest.raises(pymongo.errors.DuplicateKeyError) as caught:
            getattr(collection, method)({"_id": "a"}, change)
        assert _duplicate_index(caught.value) == "key_1"
        assert collection.find_one({"key": "taken"})["_id"] == "b"
        assert collection.find_one({"_id": "a"})["key"] == "old"


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("method", ["find_one_and_update", "find_one_and_replace"])
def test_async_duplicate_error_can_be_translated_and_recovered(engine, method):
    async def exercise():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            collection = client.recovery.items
            await collection.create_index("key", unique=True)
            await collection.insert_many(
                [{"_id": "a", "key": "old"}, {"_id": "b", "key": "taken"}]
            )
            change = (
                {"$set": {"key": "taken"}}
                if method.endswith("update")
                else {"key": "taken"}
            )
            with pytest.raises(pymongo.errors.DuplicateKeyError) as caught:
                await getattr(collection, method)({"_id": "a"}, change)
            assert _duplicate_index(caught.value) == "key_1"
            assert (await collection.find_one({"key": "taken"}))["_id"] == "b"
            assert (await collection.find_one({"_id": "a"}))["key"] == "old"

    asyncio.run(exercise())
