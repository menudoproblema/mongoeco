"""The proxy's existing subset excludes the changeStream aggregation stage."""

import asyncio

import pymongo
import pytest

from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_wire_watch_stage_is_explicitly_unsupported(engine_factory, dialect):
    async def exercise():
        async with AsyncMongoEcoProxyServer(
            engine=engine_factory(), mongodb_dialect=dialect
        ) as proxy:

            def run():
                with (
                    pymongo.MongoClient(
                        proxy.address.uri, serverSelectionTimeoutMS=2000
                    ) as client,
                    pytest.raises(
                        pymongo.errors.OperationFailure,
                        match=r"Unsupported aggregation stage: \$changeStream",
                    ),
                ):
                    client.test.records.watch()

            await asyncio.to_thread(run)

    asyncio.run(exercise())
