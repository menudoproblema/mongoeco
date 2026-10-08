"""Index presentation and deferred options retain shared storage semantics."""

import asyncio

from pathlib import Path

import pytest

from bson.json_util import loads

from mongoeco import AsyncMongoClient, IndexModel, MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.integration.api.test_mongodb9_aggregation_validation import DIALECT9


GOLDEN = loads(
    (
        Path(__file__).parents[2] / "fixtures" / "mongodb_version_deltas_9_0.json"
    ).read_text()
)["cases"]["list_indexes_collation"]["result"]


def assert_presentation(documents):
    expected = {item["name"]: item for item in GOLDEN}
    assert set(expected) == {item["name"] for item in documents}
    for document in documents:
        reference = expected[document["name"]]
        assert document["key"] == reference["key"]
        if reference["collation"]["locale"] == "simple":
            assert document["collation"] == reference["collation"]
        else:
            assert document["collation"]["locale"] == reference["collation"]["locale"]
            assert (
                document["collation"]["strength"] == reference["collation"]["strength"]
            )


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_simple_collation_is_presented_without_storage_or_identity_changes(
    backend, surface
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            collection = client.test.records
            collection.insert_one({"_id": "seed"})
            collection.create_index("a", name="implicit_simple")
            collection.create_index(
                "b", name="explicit_simple", collation={"locale": "simple"}
            )
            collection.create_index(
                "c", name="unicode", collation={"locale": "en", "strength": 2}
            )
            assert_presentation(list(collection.list_indexes()))
            command = client.test.command({"listIndexes": "records"})
            assert_presentation(command["cursor"]["firstBatch"])
            assert collection.index_information()["implicit_simple"]["collation"] == {
                "locale": "simple"
            }
            # A listed simple collation is accepted as the same existing index.
            assert (
                collection.create_index(
                    "a", name="implicit_simple", collation={"locale": "simple"}
                )
                == "implicit_simple"
            )
            old = client._runner.run(engine.list_indexes("test", "records"))
            assert "collation" not in next(
                item for item in old if item["name"] == "implicit_simple"
            )
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=DIALECT9) as client:
                collection = client.test.records
                await collection.insert_one({"_id": "seed"})
                await collection.create_index("a", name="implicit_simple")
                await collection.create_index(
                    "b", name="explicit_simple", collation={"locale": "simple"}
                )
                await collection.create_index(
                    "c", name="unicode", collation={"locale": "en", "strength": 2}
                )
                assert_presentation(await collection.list_indexes().to_list())
                command = await client.test.command({"listIndexes": "records"})
                assert_presentation(command["cursor"]["firstBatch"])
                assert (await collection.index_information())["implicit_simple"][
                    "collation"
                ] == {"locale": "simple"}
                assert (
                    await collection.create_index(
                        "a", name="implicit_simple", collation={"locale": "simple"}
                    )
                    == "implicit_simple"
                )
                old = await engine.list_indexes("test", "records")
                assert "collation" not in next(
                    item for item in old if item["name"] == "implicit_simple"
                )

        asyncio.run(exercise())


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("route", ["single", "models", "command"])
def test_wildcard_projection_rejected_before_index_batch_side_effects(
    backend, surface, route
):
    engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
    models = [
        IndexModel("a", name="would_be_created"),
        IndexModel("$**", wildcardProjection={"a": 1}),
    ]
    if surface == "sync":
        with MongoClient(engine, mongodb_dialect=DIALECT9) as client:
            collection = client.test.records
            before = list(collection.list_indexes())
            names_before = client.test.list_collection_names()

            def action():
                if route == "single":
                    collection.create_index("$**", wildcard_projection={"a": 1})
                elif route == "models":
                    collection.create_indexes(models)
                else:
                    client.test.command(
                        {
                            "createIndexes": "records",
                            "indexes": [model.document for model in models],
                        }
                    )

            with pytest.raises(OperationFailure, match="supported index subset"):
                action()
            assert list(collection.list_indexes()) == before
            assert client.test.list_collection_names() == names_before
    else:

        async def exercise():
            async with AsyncMongoClient(engine, mongodb_dialect=DIALECT9) as client:
                collection = client.test.records
                before = await collection.list_indexes().to_list()
                names_before = await client.test.list_collection_names()

                async def action():
                    if route == "single":
                        await collection.create_index(
                            "$**", wildcard_projection={"a": 1}
                        )
                    elif route == "models":
                        await collection.create_indexes(models)
                    else:
                        await client.test.command(
                            {
                                "createIndexes": "records",
                                "indexes": [model.document for model in models],
                            }
                        )

                with pytest.raises(OperationFailure, match="supported index subset"):
                    await action()
                assert await collection.list_indexes().to_list() == before
                assert await client.test.list_collection_names() == names_before

        asyncio.run(exercise())


@pytest.mark.parametrize("dialect", ["7.0", "8.0"])
def test_older_index_contract_remains_unchanged(dialect):
    with MongoClient(MemoryEngine(), mongodb_dialect=dialect) as client:
        collection = client.test.records
        collection.create_index("a", name="implicit_simple")
        assert "collation" not in collection.index_information()["implicit_simple"]
        collection.create_index("$**", wildcard_projection={"a": 1})
