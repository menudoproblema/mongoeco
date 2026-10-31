"""Image configuration commands share the native contract over wire."""

import asyncio

import pymongo
import pytest

from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer

from tests.differential.semantic_watch_image_cases import option_validation
from tests.integration.api.test_semantic_group_regressions import (
    _AsyncView,
    _capture,
    _normalize,
)


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
@pytest.mark.parametrize("surface", ["sync", "async"])
def test_image_option_validation_matches_native_through_wire(
    engine_factory, dialect, surface
):
    expected = _capture(dialect, "watch_image")["cases"][
        "semantic_watch_image_option_validation"
    ]

    async def exercise():
        async with AsyncMongoEcoProxyServer(
            engine=engine_factory(), mongodb_dialect=dialect
        ) as proxy:
            uri = f"mongodb://{proxy.address.host}:{proxy.address.port}"
            if surface == "sync":

                def run():
                    with pymongo.MongoClient(
                        uri, serverSelectionTimeoutMS=3000
                    ) as client:
                        return option_validation(client.wire_images.records)

                actual = await asyncio.to_thread(run)
            else:
                async with pymongo.AsyncMongoClient(
                    uri, serverSelectionTimeoutMS=3000
                ) as client:
                    view = _AsyncView(
                        client.wire_images.records, asyncio.get_running_loop(), client
                    )
                    actual = await asyncio.to_thread(option_validation, view)
            assert _normalize(actual) == _normalize(expected)

    asyncio.run(exercise())
