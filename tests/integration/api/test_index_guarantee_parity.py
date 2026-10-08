"""Native names/errors and invalid-batch cleanup, with explicit metadata limits."""

import asyncio

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, IndexModel, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.version_delta_cases import capture_outcome
from tests.integration.api.test_review_improvement_parity import (
    assert_native_outcome,
    failure_outcome,
)


FIXTURES = {
    d: loads(
        (
            Path(__file__).parents[2]
            / "fixtures"
            / f"mongodb_index_guarantees_{d.replace('.', '_')}.json"
        ).read_text()
    )["cases"]
    for d in ("7.0", "8.0", "9.0")
}


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize(
    ["label", "name"], [("custom", "custom"), ("integer", 123), ("empty", "")]
)
def test_builtin_names_and_errors_match_native(dialect, backend, surface, label, name):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            coll = client.test.records
            before = list(coll.list_indexes())
            actual = capture_outcome(lambda: coll.create_index("_id", name=name))
            assert list(coll.list_indexes()) == before
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                coll = client.test.records
                before = await coll.list_indexes().to_list()
                try:
                    actual = {
                        "ok": True,
                        "result": await coll.create_index("_id", name=name),
                    }
                except OperationFailure as error:
                    actual = failure_outcome(error)
                assert await coll.list_indexes().to_list() == before
                return actual

        actual = asyncio.run(exercise())
    assert_native_outcome(
        actual, FIXTURES[dialect]["index_id_name_" + label]["calls"][0]
    )


@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("route", ["facade", "command"])
def test_invalid_batch_preserves_catalog_and_native_error(
    dialect, backend, surface, route
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    models = [
        IndexModel("p", name="first"),
        IndexModel("n", name="existing", unique=True),
    ]
    command = {
        "createIndexes": "records",
        "indexes": [model.document for model in models],
    }
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            coll = client.test.records
            coll.create_index("n", name="existing")
            before = list(coll.list_indexes())
            actual = capture_outcome(
                lambda: (
                    coll.create_indexes(models)
                    if route == "facade"
                    else client.test.command(command)
                )
            )
            assert list(coll.list_indexes()) == before
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                coll = client.test.records
                await coll.create_index("n", name="existing")
                before = await coll.list_indexes().to_list()
                try:
                    await coll.create_indexes(
                        models
                    ) if route == "facade" else await client.test.command(command)
                except OperationFailure as error:
                    actual = failure_outcome(error)
                else:
                    actual = {"ok": True, "result": None}
                assert await coll.list_indexes().to_list() == before
                return actual

        actual = asyncio.run(exercise())
    assert_native_outcome(
        actual, FIXTURES[dialect]["index_invalid_batch_conflict"]["call"]
    )
