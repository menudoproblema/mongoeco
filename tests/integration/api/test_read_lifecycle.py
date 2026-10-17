"""Read lifecycle agrees with native clients through both unchanged facades."""

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

from tests.support_error_phases import comparable
from tests.support_read_lifecycle import observe_async_reads, observe_reads
from tests.support_unacknowledged import PoolObservation, client_options


@lru_cache(maxsize=3)
def read_reference(target):
    root = Path(__file__).parents[3]
    reference = json.loads(
        (
            root
            / "tests/fixtures"
            / f"read_lifecycle_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for path, digest in reference["inputs_sha256"].items():
        assert hashlib.sha256((root / path).read_bytes()).hexdigest() == digest
    assert reference["native_sync_async_equal"]
    assert reference["native_physical_connection_reuse"]
    return reference["observations"]


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_read_lifecycle_matches_native(target, engine_type, surface):
    async def run():
        async with AsyncMongoClient(
            engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            return await observe_async_reads(client, "reads")

    if surface == "async":
        actual = asyncio.run(run())
    else:
        with MongoClient(
            engine_type(), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            actual = observe_reads(client, "reads")
    assert comparable(actual) == comparable(read_reference(target))


@pytest.mark.parametrize("target", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("engine_type", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_read_lifecycle_wire_preserves_results_errors_and_connection(
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
                        rows = observe_reads(client, "reads")
                else:

                    async def ac():
                        async with pymongo.AsyncMongoClient(
                            proxy.address.uri, **client_options(pool)
                        ) as client:
                            return await observe_async_reads(client, "reads")

                    rows = asyncio.run(ac())
                assert pool.observation() == {"physical_connections": 1, "reused": True}
                return rows

            return await asyncio.to_thread(calls)

    assert comparable(asyncio.run(run())) == comparable(read_reference(target))
