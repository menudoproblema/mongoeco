"""Direct classic commands preserve the native wire oracle's parsing and effects."""

import asyncio

from itertools import product

import pymongo
import pytest

from mongoeco import AsyncMongoClient, MongoClient

from tests.differential.write_preflight_cases import COMMAND_PREFLIGHT_CASES
from tests.integration.wire.test_unacknowledged import (
    BACKENDS,
    TARGETS,
    engine,
    reference,
)
from tests.support_unacknowledged import (
    PoolObservation,
    assert_observation,
    observe,
    observe_async,
)


@pytest.mark.parametrize(
    ["target", "backend", "surface"],
    list(product(TARGETS, BACKENDS, ("sync", "async"))),
)
@pytest.mark.parametrize("case", COMMAND_PREFLIGHT_CASES, ids=lambda case: case.id)
def test_direct_write_preflight_matches_native(
    target, backend, surface, case, tmp_path
):
    profile = ".".join(str(v) for v in pymongo.version_tuple[:2])
    options = {"mongodb_dialect": target, "pymongo_profile": profile}
    if surface == "sync":
        with MongoClient(engine(backend, tmp_path), **options) as client:
            actual = observe(client, "preflight", case.id, case, PoolObservation())
    else:

        async def exercise():
            async with AsyncMongoClient(engine(backend, tmp_path), **options) as client:
                return await observe_async(
                    client, "preflight", case.id, case, PoolObservation()
                )

        actual = asyncio.run(exercise())
    # Direct engine calls have no wire pool; compare all semantic observations.
    actual.pop("connection")
    expected = {
        k: v
        for k, v in reference(target)["observations"][case.id].items()
        if k != "connection"
    }
    assert_observation(actual, expected)
