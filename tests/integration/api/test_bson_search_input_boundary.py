"""Search admission precedes local catalog validation and mutation."""

# ruff: noqa: PLR2004 - explicit cumulative request counts

import asyncio

from decimal import Decimal
from unittest.mock import patch

import pytest

from bson.errors import InvalidDocument
from pymongo.operations import SearchIndexModel as DriverSearchIndexModel

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.api._async import _collection_search_indexes as search_api
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure
from mongoeco.types import CodecOptions, UuidRepresentation


DEFINITION = {"mappings": {"dynamic": True}}
ROUTES = ("create", "create-many", "list", "update", "drop")


@pytest.mark.parametrize("backend", ["memory", "sqlite-file"])
@pytest.mark.parametrize("route", ROUTES)
def test_invalid_complete_search_frame_cannot_reach_engine(tmp_path, backend, route):
    path = str(tmp_path / "search.sqlite")

    def engine():
        return MemoryEngine() if backend == "memory" else SQLiteEngine(path)

    class Opaque:
        def __deepcopy__(self, memo):
            # Admission must inspect the original, before model normalization.
            return 1

    async def exercise():
        implementation = engine()
        async with AsyncMongoClient(implementation) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 0})
            await collection.create_search_index(
                SearchIndexModel(DEFINITION, name="kept")
            )
            before = await collection.list_search_indexes().to_list()
            with (
                patch.object(
                    implementation,
                    "create_search_index",
                    wraps=implementation.create_search_index,
                ) as create,
                patch.object(
                    implementation,
                    "list_search_indexes",
                    wraps=implementation.list_search_indexes,
                ) as listing,
                patch.object(
                    implementation,
                    "update_search_index",
                    wraps=implementation.update_search_index,
                ) as update,
                patch.object(
                    implementation,
                    "drop_search_index",
                    wraps=implementation.drop_search_index,
                ) as drop,
            ):

                async def invoke():
                    if route == "create":
                        return await collection.create_search_index(
                            {
                                "definition": {
                                    "mappings": {"dynamic": True},
                                    "original": Opaque(),
                                }
                            }
                        )
                    if route == "create-many":
                        return await collection.create_search_indexes(
                            [
                                SearchIndexModel(DEFINITION, name="new"),
                                DriverSearchIndexModel(
                                    {"original": Opaque()}, name="bad"
                                ),
                            ]
                        )
                    if route == "list":
                        return collection.list_search_indexes(5, comment=Decimal(1))
                    if route == "update":
                        return await collection.update_search_index(
                            5, {"original": Opaque()}
                        )
                    return await collection.drop_search_index(5, comment=Decimal(1))

                with pytest.raises(InvalidDocument):
                    await invoke()
                assert (
                    create.call_count
                    == listing.call_count
                    == update.call_count
                    == drop.call_count
                    == 0
                )
            assert await collection.list_search_indexes().to_list() == before
            assert await collection.find_one({"_id": 0}) == {"_id": 0}
        if backend == "sqlite-file":
            async with AsyncMongoClient(engine()) as client:
                assert (
                    await client.test.records.list_search_indexes().to_list() == before
                )

    asyncio.run(exercise())


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("model_kind", ["local", "sdk", "envelope", "shorthand"])
def test_search_lifecycle_admits_once_and_ignores_collection_codecs(
    factory, model_kind
):
    class Factory(dict):
        def __init__(self, *args, **kwargs):
            pytest.fail("Search must ignore the collection document factory")

    def decoder(value):
        pytest.fail("Search must ignore collection decoders")

    with MongoClient(factory()) as client:
        collection = client.test.records.with_options(
            codec_options=CodecOptions(
                document_class=Factory,
                type_registry={int: decoder},
                uuid_representation=UuidRepresentation.STANDARD,
            )
        )
        model = {
            "local": lambda: SearchIndexModel(DEFINITION),
            "sdk": lambda: DriverSearchIndexModel(DEFINITION),
            "envelope": lambda: {"definition": DEFINITION},
            "shorthand": lambda: DEFINITION,
        }[model_kind]()
        with patch.object(
            search_api, "encode_bson_request", wraps=search_api.encode_bson_request
        ) as admit:
            assert collection.create_search_index(model) == "default"
            assert admit.call_count == 1
            assert collection.create_search_indexes(
                [DriverSearchIndexModel(DEFINITION, name="other")]
            ) == ["other"]
            assert admit.call_count == 2
            cursor = collection.list_search_indexes()
            assert admit.call_count == 3
            assert [row["name"] for row in cursor] == ["default", "other"]
            assert admit.call_count == 3
            collection.update_search_index("default", {"mappings": {"dynamic": False}})
            assert admit.call_count == 4
            collection.drop_search_index("other")
            assert admit.call_count == 5
            assert all(
                call.kwargs["codec_options"].uuid_representation
                == UuidRepresentation.UNSPECIFIED
                for call in admit.call_args_list
            )
        assert [row["name"] for row in collection.list_search_indexes()] == ["default"]


@pytest.mark.parametrize("factory", [MemoryEngine, SQLiteEngine])
def test_search_multi_create_preserves_rollback_on_engine_failure(factory):
    with MongoClient(factory()) as client:
        collection = client.test.records
        collection.create_search_index(SearchIndexModel(DEFINITION, name="kept"))
        with pytest.raises(
            OperationFailure, match="Conflicting search index definition"
        ):
            collection.create_search_indexes(
                [
                    SearchIndexModel(DEFINITION, name="new"),
                    SearchIndexModel({"mappings": {"dynamic": False}}, name="kept"),
                ]
            )
        assert [row["name"] for row in collection.list_search_indexes()] == ["kept"]


@pytest.mark.parametrize("route", ["create", "create-many", "update", "drop"])
def test_search_max_time_encoding_error_precedes_comment(route):
    with MongoClient(MemoryEngine()) as client:
        collection = client.test.records
        kwargs = {"comment": Decimal(1), "max_time_ms": "\ud800"}

        def invoke():
            if route == "create":
                collection.create_search_index(
                    DriverSearchIndexModel(DEFINITION), **kwargs
                )
            elif route == "create-many":
                collection.create_search_indexes(
                    [DriverSearchIndexModel(DEFINITION)], **kwargs
                )
            elif route == "update":
                collection.update_search_index("subject", DEFINITION, **kwargs)
            else:
                collection.drop_search_index("subject", **kwargs)

        with pytest.raises(UnicodeEncodeError):
            invoke()
