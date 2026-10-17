"""The existing v2 boundary agrees with native commands through both facades."""

import asyncio
import hashlib
import json

from functools import lru_cache
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.engine_boundary_cases import BOUNDARY_CASES
from tests.support_error_phases import (
    comparable,
    observe_async_error_phases,
    observe_error_phases,
)
from tests.support_unacknowledged import PoolObservation, client_options


@lru_cache(maxsize=3)
def boundary_reference(target):
    root = Path(__file__).parents[3]
    reference = json.loads(
        (
            root
            / "tests/fixtures"
            / f"engine_boundary_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for path, digest in reference["inputs_sha256"].items():
        assert hashlib.sha256((root / path).read_bytes()).hexdigest() == digest
    assert reference["native_sync_async_equal"]
    assert reference["native_physical_connection_reuse"]
    assert set(reference["observations"]) == {case.id for case in BOUNDARY_CASES}
    return reference["observations"]


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", BOUNDARY_CASES, ids=lambda case: case.id)
def test_boundary_matches_native(target, engine_type, surface, case):
    async def run():
        async with AsyncMongoClient(
            engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            return await observe_async_error_phases(client, "boundary", [case])

    if surface == "async":
        actual = asyncio.run(run())
    else:
        with MongoClient(
            engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            actual = observe_error_phases(client, "boundary", [case])
    assert comparable(actual[case.id]) == comparable(
        boundary_reference(target)[case.id]
    )


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_boundary_wire_preserves_native_results_and_connection(
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
                        rows = observe_error_phases(client, "boundary", BOUNDARY_CASES)
                else:

                    async def ac():
                        async with pymongo.AsyncMongoClient(
                            proxy.address.uri, **client_options(pool)
                        ) as client:
                            return await observe_async_error_phases(
                                client, "boundary", BOUNDARY_CASES
                            )

                    rows = asyncio.run(ac())
                assert pool.observation() == {"physical_connections": 1, "reused": True}
                return rows

            return await asyncio.to_thread(calls)

    assert comparable(asyncio.run(run())) == comparable(boundary_reference(target))
