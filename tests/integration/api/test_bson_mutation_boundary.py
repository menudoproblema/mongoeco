"""Mutation admission precedes state, index and change-delivery effects."""

import asyncio

from decimal import Decimal
from unittest.mock import patch
from uuid import UUID

import pytest

from bson import BSON
from bson.errors import InvalidDocument
from pymongo.errors import InvalidOperation

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api import bson_input
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.types import CodecOptions


ROUTES = (
    "update",
    "update_many",
    "replace",
    "delete",
    "delete_many",
    "find_update",
    "find_replace",
    "find_delete",
)


async def _mutate(collection, route, query, value=3, **options):
    calls = {
        "update": lambda: collection.update_one(
            query, {"$set": {"v": value}}, **options
        ),
        "update_many": lambda: collection.update_many(
            query, {"$set": {"v": value}}, **options
        ),
        "replace": lambda: collection.replace_one(
            query, {"_id": 0, "v": value}, **options
        ),
        "delete": lambda: collection.delete_one(query, **options),
        "delete_many": lambda: collection.delete_many(query, **options),
        "find_update": lambda: collection.find_one_and_update(
            query, {"$set": {"v": value}}, **options
        ),
        "find_replace": lambda: collection.find_one_and_replace(
            query, {"_id": 0, "v": value}, **options
        ),
        "find_delete": lambda: collection.find_one_and_delete(query, **options),
    }
    return await calls[route]()


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("route", ROUTES)
@pytest.mark.parametrize("placement", ["filter", "comment"])
def test_rejected_mutation_keeps_state_indexes_and_events(
    backend, route, placement, tmp_path
):
    path = str(tmp_path / "mutations.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    async def exercise():
        async with AsyncMongoClient(engine()) as client:
            collection = client.test.rows
            await collection.insert_many([{"_id": 0, "v": 1}, {"_id": 1, "v": 2}])
            await collection.create_index("v", unique=True)
            stream = collection.watch(max_await_time_ms=1)
            try:
                query = {"_id": {"$in": [0, 1]}}
                options = {}
                if placement == "filter":
                    query = {"$or": [{"_id": 0}, {"v": Decimal(1)}]}
                else:
                    options["comment"] = Decimal(1)
                with pytest.raises(InvalidDocument):
                    await _mutate(collection, route, query, **options)
                # A committed update barrier detects events from any false prefix.
                result = await collection.update_one({"_id": 0}, {"$set": {"v": 3}})
                assert result.modified_count == 1
                event = await stream.try_next()
                assert event["documentKey"] == {"_id": 0}
                assert event["updateDescription"]["updatedFields"] == {"v": 3}
                assert await stream.try_next() is None
                assert await collection.find().sort("_id").to_list() == [
                    {"_id": 0, "v": 3},
                    {"_id": 1, "v": 2},
                ]
            finally:
                await stream.close()

    asyncio.run(exercise())
    if backend == "sqlite-file":
        with MongoClient(engine()) as client:
            assert list(client.test.rows.find().sort("_id")) == [
                {"_id": 0, "v": 3},
                {"_id": 1, "v": 2},
            ]


@pytest.mark.parametrize("route", ROUTES)
def test_one_client_admission_for_single_and_multi_mutation(route):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            collection = client.test.rows
            await collection.insert_many([{"_id": 0, "v": 1}, {"_id": 1, "v": 2}])
            with patch.object(
                bson_input, "encode_bson_request", wraps=bson_input.encode_bson_request
            ) as encode:
                await _mutate(collection, route, {})
            # Internal multi-write selectors and per-ID plans must not encode again.
            assert encode.call_count == 1

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ["profile", "expected"], [("4.9", TypeError), ("4.19", InvalidOperation)]
)
def test_modern_sort_capability_precedes_or_follows_the_ended_session(
    profile, expected
):
    # Native SDK 4.19 / MongoDB witness is retained in native round 5. The
    # default 4.9 profile rejects this keyword before session dispatch instead.
    with MongoClient(MemoryEngine(), pymongo_profile=profile) as client:
        session = client.start_session()
        session.close()
        with pytest.raises(expected):
            client.test.rows.update_one({}, {"$set": {"v": 2}}, sort=5, session=session)


@pytest.mark.parametrize("method", ["update_one", "replace_one"])
def test_upsert_identifier_uses_builtin_uuid_options_without_user_codecs(method):
    class ForbiddenDocument(dict):
        def __init__(self, *args, **kwargs):
            message = "write results must not use the user's document factory"
            raise AssertionError(message)

    callbacks = []
    identifier = UUID("00112233-4455-6677-8899-aabbccddeeff")
    with MongoClient(MemoryEngine()) as client:
        collection = client.test.rows.with_options(
            codec_options=CodecOptions(
                document_class=ForbiddenDocument,
                uuid_representation="standard",
                type_registry={UUID: callbacks.append},
            )
        )
        change = {"$set": {"v": 2}} if method == "update_one" else {"_id": identifier}
        result = getattr(collection, method)({"_id": identifier}, change, upsert=True)
        assert result.upserted_id == identifier
        assert callbacks == []


@pytest.mark.parametrize(
    "route", ["update", "update_many", "replace", "find_update", "find_replace"]
)
def test_unsupported_objects_reach_the_bson_encoder_before_internal_copies(route):
    class Unsupported:
        def __deepcopy__(self, memo):
            message = "client BSON admission must precede deepcopy"
            raise AssertionError(message)

    value = Unsupported()
    with pytest.raises(InvalidDocument):
        BSON.encode({"v": value})

    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            with pytest.raises(InvalidDocument):
                await _mutate(client.test.rows, route, {}, value=value)
            assert await client.test.rows.count_documents({}) == 0

    asyncio.run(exercise())
