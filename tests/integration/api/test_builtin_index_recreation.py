"""Builtin index API adaptation preserves the strict immutable Engine SPI."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient, IndexModel, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine

from tests.integration.api.test_review_index_characterization import FIXTURES


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("explicit_name", [False, True])
def test_builtin_default_and_listed_metadata_recreate(
    backend, surface, dialect, explicit_name
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    options = {"name": "_id_"} if explicit_name else {}
    expected = FIXTURES[dialect][
        f"index_id_{'named' if explicit_name else 'omitted'}_fresh"
    ]["calls"][0]["result"]
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            collection = client.test.records
            assert collection.create_index("_id", **options) == expected
            assert collection.create_indexes([IndexModel("_id")]) == ["_id_1"]
            listed = next(iter(collection.list_indexes()))
            assert (
                collection.create_index(
                    list(listed["key"].items()),
                    name=listed["name"],
                    unique=listed["unique"],
                    collation=listed.get("collation"),
                )
                == "_id_"
            )
            assert list(collection.list_indexes()) == [listed]
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                collection = client.test.records
                assert await collection.create_index("_id", **options) == expected
                assert await collection.create_indexes([IndexModel("_id")]) == ["_id_1"]
                listed = (await collection.list_indexes().to_list())[0]
                assert (
                    await collection.create_index(
                        list(listed["key"].items()),
                        name=listed["name"],
                        unique=listed["unique"],
                        collation=listed.get("collation"),
                    )
                    == "_id_"
                )
                assert await collection.list_indexes().to_list() == [listed]

        asyncio.run(exercise())


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("invalid_unique", [None, 1, "true"])
def test_builtin_adaptation_does_not_hide_invalid_uniqueness(
    backend, surface, dialect, invalid_unique
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            collection = client.test.records
            before = list(collection.list_indexes())
            with pytest.raises(TypeError, match="unique must be a bool"):
                collection.create_index("_id", unique=invalid_unique)
            assert list(collection.list_indexes()) == before
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                collection = client.test.records
                before = await collection.list_indexes().to_list()
                with pytest.raises(TypeError, match="unique must be a bool"):
                    await collection.create_index("_id", unique=invalid_unique)
                assert await collection.list_indexes().to_list() == before

        asyncio.run(exercise())
