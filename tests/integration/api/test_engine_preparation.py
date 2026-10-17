"""End-to-end preparation stays within the existing engine contract."""

import asyncio
import hashlib
import json

from functools import lru_cache
from pathlib import Path
from unittest.mock import patch

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine, semantic_core
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.engine_preparation_cases import PREPARATION_CASES
from tests.support_error_phases import (
    comparable,
    observe_async_error_phases,
    observe_error_phases,
)
from tests.support_unacknowledged import PoolObservation, client_options


@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("size", [1, 3])
def test_update_many_prepares_each_original_selector_once(engine_type, surface, size):
    selector = {"group": "a"}
    documents = [{"_id": index, "group": "a"} for index in range(size)]

    def verify(spy, result):
        calls = [
            call
            for call in spy.call_args_list
            if call.args[0] == selector and call.args[1] is None
        ]
        assert len(calls) == size
        assert result.matched_count == size
        assert result.modified_count == size

    async def run_async():
        async with AsyncMongoClient(engine_type()) as client:
            collection = client.preparation.items
            await collection.insert_many(documents)
            with patch.object(
                semantic_core,
                "ensure_query_plan",
                wraps=semantic_core.ensure_query_plan,
            ) as spy:
                result = await collection.update_many(selector, {"$set": {"value": 1}})
            verify(spy, result)
            assert await collection.count_documents({"value": 1}) == size

    if surface == "async":
        asyncio.run(run_async())
    else:
        with MongoClient(engine_type()) as client:
            collection = client.preparation.items
            collection.insert_many(documents)
            with patch.object(
                semantic_core,
                "ensure_query_plan",
                wraps=semantic_core.ensure_query_plan,
            ) as spy:
                result = collection.update_many(selector, {"$set": {"value": 1}})
            verify(spy, result)
            assert collection.count_documents({"value": 1}) == size


@lru_cache(maxsize=3)
def preparation_reference(target):
    root = Path(__file__).parents[3]
    reference = json.loads(
        (
            root
            / "tests/fixtures"
            / f"engine_preparation_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for path, digest in reference["inputs_sha256"].items():
        assert hashlib.sha256((root / path).read_bytes()).hexdigest() == digest
    assert reference["native_sync_async_equal"]
    assert reference["native_physical_connection_reuse"]
    assert set(reference["observations"]) == {case.id for case in PREPARATION_CASES}
    return reference["observations"]


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", PREPARATION_CASES, ids=lambda case: case.id)
def test_preparation_matches_native(target, engine_type, surface, case):
    async def run():
        async with AsyncMongoClient(
            engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            return await observe_async_error_phases(client, "preparation", [case])

    if surface == "async":
        actual = asyncio.run(run())
    else:
        with MongoClient(
            engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            actual = observe_error_phases(client, "preparation", [case])
    assert comparable(actual[case.id]) == comparable(
        preparation_reference(target)[case.id]
    )


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_preparation_wire_preserves_native_results_and_connection(
    target, engine_type, surface
):
    async def run():
        async with AsyncMongoEcoProxyServer(
            engine=engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as proxy:

            def calls():
                pool = PoolObservation()
                if surface == "sync":
                    with pymongo.MongoClient(
                        proxy.address.uri, **client_options(pool)
                    ) as client:
                        rows = observe_error_phases(
                            client, "preparation", PREPARATION_CASES
                        )
                else:

                    async def ac():
                        async with pymongo.AsyncMongoClient(
                            proxy.address.uri, **client_options(pool)
                        ) as client:
                            return await observe_async_error_phases(
                                client, "preparation", PREPARATION_CASES
                            )

                    rows = asyncio.run(ac())
                assert pool.observation() == {"physical_connections": 1, "reused": True}
                return rows

            return await asyncio.to_thread(calls)

    assert comparable(asyncio.run(run())) == comparable(preparation_reference(target))
