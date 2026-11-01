"""Builtin index API adaptation preserves the strict immutable Engine SPI."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient, IndexModel, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.semantic_followup_cases import index_id_models
from tests.integration.api.test_review_index_characterization import FIXTURES
from tests.integration.api.test_semantic_group_regressions import _capture


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_owned_index_model_preserves_explicit_false_options(backend, dialect):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        actual = index_id_models(client.test.records, model_factory=IndexModel)
    assert (
        actual
        == _capture(dialect, "followup")["cases"]["semantic_followup_f06_id_models"]
    )


def test_index_model_request_presence_is_private_and_retains_false():
    omitted = IndexModel("_id")
    explicit = IndexModel("_id", unique=False, hidden=False, sparse=False)
    assert omitted.document == {"key": {"_id": 1}, "name": "_id_1"}
    assert explicit.document == omitted.document | {
        "unique": False,
        "hidden": False,
        "sparse": False,
    }
    assert "_explicit_options" not in explicit.document


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_hidden_recreation_uses_dialect_through_model_and_command(backend, dialect):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    with MongoClient(engine, mongodb_dialect=dialect) as client:
        collection = client.test.records
        collection.create_index("a", name="first")
        expected = _capture(dialect, "followup")["cases"][
            "semantic_followup_f06_catalog_matrix"
        ]["same_name_hidden"]
        for invoke in [
            lambda: collection.create_indexes(
                [IndexModel("a", name="first", hidden=True)]
            ),
            lambda: collection.database.command(
                {
                    "createIndexes": collection.name,
                    "indexes": [{"key": {"a": 1}, "name": "first", "hidden": True}],
                }
            ),
        ]:
            if expected["ok"]:
                invoke()
            else:
                with pytest.raises(OperationFailure) as error:
                    invoke()
                assert error.value.code == expected["code"]
            assert not collection.index_information()["first"].get("hidden", False)


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
                        collation=listed.get("collation"),
                    )
                    == "_id_"
                )
                assert await collection.list_indexes().to_list() == [listed]

        asyncio.run(exercise())


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize(
    ["invalid_unique", "code"], [(None, 14), (1, 197), ("true", 14)]
)
def test_builtin_adaptation_does_not_hide_invalid_uniqueness(
    backend, surface, dialect, invalid_unique, code
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=dialect) as client:
            collection = client.test.records
            before = list(collection.list_indexes())
            with pytest.raises(OperationFailure) as error:
                collection.create_index("_id", unique=invalid_unique)
            assert error.value.code == code
            assert list(collection.list_indexes()) == before
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=dialect) as client:
                collection = client.test.records
                before = await collection.list_indexes().to_list()
                with pytest.raises(OperationFailure) as error:
                    await collection.create_index("_id", unique=invalid_unique)
                assert error.value.code == code
                assert await collection.list_indexes().to_list() == before

        asyncio.run(exercise())
