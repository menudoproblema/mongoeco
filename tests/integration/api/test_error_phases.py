"""Native command-phase regressions cover errors, effects, absence and reuse."""

import asyncio
import hashlib
import json
import warnings

from functools import lru_cache
from itertools import product
from pathlib import Path

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api._async.cursor import _AsyncCursorIterator
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.error_absence_cases import EXTRA_CASES
from tests.differential.error_composition_cases import COMPOSITION_CASES
from tests.differential.error_phase_cases import CASES
from tests.support import ENGINE_FACTORIES
from tests.support_error_phases import (
    comparable,
    observe_async_error_phases,
    observe_error_phases,
)
from tests.support_unacknowledged import PoolObservation, client_options


ALL_CASES = CASES + EXTRA_CASES + COMPOSITION_CASES
TARGETS = ["7.0", "8.0", "9.0"]


@lru_cache(maxsize=6)
def references(target, driver="4.19"):
    capture = json.loads(
        (
            Path(__file__).parents[2]
            / f"fixtures/error_phases_mongodb_{target.replace('.', '_')}.json"
        ).read_text()
    )
    for name, digest in capture["inputs_sha256"].items():
        assert (
            hashlib.sha256((Path(__file__).parents[3] / name).read_bytes()).hexdigest()
            == digest
        )
    assert capture["native_sync_async_equal"]
    assert capture["native_physical_connection_reuse"]
    assert set(capture["observations"]) == {case.id for case in ALL_CASES}
    if driver == "4.9":
        delta = json.loads(
            (
                Path(__file__).parents[2]
                / "fixtures/error_phases_pymongo49_deltas.json"
            ).read_text()
        )["targets"][target]
        baseline_path = (
            Path(__file__).parents[2]
            / f"fixtures/error_phases_mongodb_{target.replace('.', '_')}.json"
        )
        assert (
            delta["baseline_fixture_sha256"]
            == hashlib.sha256(baseline_path.read_bytes()).hexdigest()
        )
        assert delta["inputs_sha256"] == capture["inputs_sha256"]
        assert delta["compared_scenarios"] == len(ALL_CASES)
        assert delta["native_sync_async_equal"]
        assert delta["native_physical_connection_reuse"]
        capture["observations"].update(delta["overrides"])
    return capture["observations"]


@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("case", ALL_CASES, ids=lambda case: case.id)
def test_direct_error_phases(target, engine, surface, case):
    async def run():
        async with AsyncMongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            return await observe_async_error_phases(client, "phase_errors", [case])

    if surface == "async":
        actual = asyncio.run(run())
    else:
        with MongoClient(
            ENGINE_FACTORIES[engine](), mongodb_dialect=target, pymongo_profile="4.19"
        ) as client:
            actual = observe_error_phases(client, "phase_errors", [case])
    assert comparable(actual[case.id]) == comparable(references(target)[case.id])


@pytest.mark.parametrize(
    ["target", "engine", "surface"],
    list(product(TARGETS, ENGINE_FACTORIES, ["sync", "async"])),
)
def test_wire_error_phases_reuse_connection(target, engine, surface):
    async def run():
        async with AsyncMongoEcoProxyServer(
            engine=ENGINE_FACTORIES[engine](),
            mongodb_dialect=target,
            pymongo_profile="4.19",
        ) as proxy:

            def calls():
                pool = PoolObservation()
                if surface == "sync":
                    with pymongo.MongoClient(
                        proxy.address.uri, **client_options(pool)
                    ) as client:
                        actual = observe_error_phases(client, "phase_errors", ALL_CASES)
                else:

                    async def ac():
                        async with pymongo.AsyncMongoClient(
                            proxy.address.uri, **client_options(pool)
                        ) as client:
                            return await observe_async_error_phases(
                                client, "phase_errors", ALL_CASES
                            )

                    actual = asyncio.run(ac())
                assert pool.observation() == {"physical_connections": 1, "reused": True}
                for case in ALL_CASES:
                    assert comparable(actual[case.id]) == comparable(
                        references(
                            target,
                            "4.9" if pymongo.version.startswith("4.9.") else "4.19",
                        )[case.id]
                    ), case.id

            await asyncio.to_thread(calls)

    asyncio.run(run())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_failed_delete_many_closes_iterator_without_requiring_a_running_loop(
    engine, surface, monkeypatch
):

    retained = []
    original = _AsyncCursorIterator._close_after_failure

    async def observe(self):
        retained.append(self)
        await original(self)

    monkeypatch.setattr(_AsyncCursorIterator, "_close_after_failure", observe)
    query = {"$expr": {"$divide": ["$n", 0]}}

    async def ac():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            collection = client.cleanup.items
            await collection.insert_one({"_id": "a", "n": 1})
            with pytest.raises(pymongo.errors.WriteError):
                await collection.delete_many(query)
            assert (
                await collection.update_one({}, {"$set": {"recovered": True}})
            ).modified_count == 1

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always", RuntimeWarning)
        if surface == "async":
            asyncio.run(ac())
        else:
            with MongoClient(ENGINE_FACTORIES[engine]()) as client:
                collection = client.cleanup.items
                collection.insert_one({"_id": "a", "n": 1})
                with pytest.raises(pymongo.errors.WriteError):
                    collection.delete_many(query)
                assert (
                    collection.update_one(
                        {}, {"$set": {"recovered": True}}
                    ).modified_count
                    == 1
                )
        assert retained
        assert all(item._closed and item._source is None for item in retained)
        assert not [
            warning
            for warning in caught
            if issubclass(warning.category, RuntimeWarning)
        ]
