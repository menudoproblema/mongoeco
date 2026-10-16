"""Failed payloads are public snapshots independent of caller and storage state."""

import asyncio

from copy import deepcopy

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.errors import BulkWriteError, OperationFailure
from mongoeco.types import InsertOne, WriteErrorEntry

from tests.differential.write_operation_cases import bson_payload
from tests.integration.api.test_insert_many_write_contract import BATCH_FACTORIES


BAD_VALUE = 2


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
@pytest.mark.parametrize("method", ["insert_many", "bulk_write"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_failed_insert_payload_is_detached_and_preserves_public_bson(
    factory, method, surface
):
    original = {"_id": "a", **bson_payload()}
    before = deepcopy(original)
    payload = [original] if method == "insert_many" else [InsertOne(original)]

    async def asynchronous():
        async with AsyncMongoClient(factory()) as client:
            collection = client.conservation.items
            await collection.insert_one({"_id": "a"})
            with pytest.raises(BulkWriteError) as failure:
                await getattr(collection, method)(payload)
            return failure.value, await collection.find_one({"_id": "a"})

    if surface == "async":
        error, stored = asyncio.run(asynchronous())
    else:
        with MongoClient(factory()) as client:
            collection = client.conservation.items
            collection.insert_one({"_id": "a"})
            with pytest.raises(BulkWriteError) as failure:
                getattr(collection, method)(payload)
            error, stored = failure.value, collection.find_one({"_id": "a"})
    operation = error.details["writeErrors"][0]["op"]
    assert operation == before
    assert operation["date"].microsecond == before["date"].microsecond
    assert type(operation["nested"]["second"]) is type(before["nested"]["second"])
    original["nested"]["first"] = "caller changed"
    assert operation == before
    operation["items"].append("error changed")
    assert original["items"] == before["items"]
    assert stored == {"_id": "a"}


def test_write_error_entry_materializes_independent_payloads_and_keeps_legacy_string():
    source = {"q": {"nested": [1]}}
    entry = WriteErrorEntry(index=3, errmsg="failed", code=2, operation=source)
    first, second = entry.to_document(), entry.to_document()
    first["op"]["q"]["nested"].append(2)
    assert second["op"] == source == {"q": {"nested": [1]}}
    assert (
        WriteErrorEntry(index=0, errmsg="legacy", operation="UpdateOne").to_document()[
            "op"
        ]
        == "UpdateOne"
    )


@pytest.mark.parametrize("factory", BATCH_FACTORIES[:2])
def test_missing_hint_fails_even_when_a_collated_scan_matches_no_documents(factory):
    async def exercise():
        async with AsyncMongoClient(factory(), pymongo_profile="4.19") as client:
            collection = client.conservation.items
            await collection.insert_one({"_id": "a", "name": "Ada"})
            cursor = collection.find(
                {"name": "missing"}, hint="missing_index", collation={"locale": "en"}
            )
            try:
                with pytest.raises(OperationFailure) as failure:
                    await cursor.to_list()
                assert failure.value.code == BAD_VALUE
            finally:
                await cursor.close()
            with pytest.raises(OperationFailure) as failure:
                await collection.delete_many(
                    {"name": "missing"},
                    hint="missing_index",
                    collation={"locale": "en"},
                )
            assert failure.value.code == BAD_VALUE
            assert await collection.find_one({}) == {"_id": "a", "name": "Ada"}

    asyncio.run(exercise())


@pytest.mark.parametrize("factory", BATCH_FACTORIES)
@pytest.mark.parametrize("method", ["insert_many", "bulk_write"])
def test_failed_generated_id_is_the_id_assigned_to_the_original_input(factory, method):
    async def exercise():
        documents = [{"name": "valid"}, {"nested": [1]}]
        payload = (
            documents
            if method == "insert_many"
            else [InsertOne(doc) for doc in documents]
        )
        async with AsyncMongoClient(factory()) as client:
            collection = await client.conservation.create_collection(
                "validated", validator={"$jsonSchema": {"required": ["name"]}}
            )
            with pytest.raises(BulkWriteError) as failure:
                await getattr(collection, method)(payload)
            error = failure.value.details
            assert error["nInserted"] == 1
            assert error["writeErrors"][0]["op"] == documents[1]
            assert "_id" in documents[1]
            documents[1]["nested"].append(2)
            assert error["writeErrors"][0]["op"]["nested"] == [1]

    asyncio.run(exercise())
