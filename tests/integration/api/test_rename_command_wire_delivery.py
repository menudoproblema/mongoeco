"""Wire namespace writes deliver through the supplied local client's public stream."""

import asyncio

import pymongo
import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_wire_rename_keeps_errors_and_committed_effects_on_the_shared_client(
    engine_factory, dialect
):
    async def exercise():
        async with AsyncMongoClient(
            engine_factory(), mongodb_dialect=dialect
        ) as client:
            source, target = client.wire_rename.source, client.wire_rename.target
            await source.insert_one({"_id": "source"})
            await target.insert_one({"_id": "target"})
            streams = [
                owner.watch(max_await_time_ms=1) for owner in (source, target, client)
            ]
            try:
                async with AsyncMongoEcoProxyServer(client=client) as proxy:

                    def write():
                        with pymongo.MongoClient(
                            proxy.address.uri, serverSelectionTimeoutMS=3000
                        ) as wire:
                            command = {
                                "renameCollection": "wire_rename.source",
                                "to": "wire_rename.target",
                                "dropTarget": True,
                            }
                            with pytest.raises(
                                pymongo.errors.OperationFailure
                            ) as error:
                                wire.wire_rename.command(command)
                            assert error.value.code == 13  # noqa: PLR2004 - native admin entrypoint error
                            assert wire.wire_rename.target.find_one({}) == {
                                "_id": "target"
                            }
                            wire.admin.command(command)

                    await asyncio.to_thread(write)
                    for stream in streams:
                        assert (await stream.try_next())["operationType"] == "rename"
                    for stream in streams[:2]:
                        event = await stream.try_next()
                        assert event["operationType"] == "invalidate"
                        assert "ns" not in event
                        assert "documentKey" not in event
                        assert not stream.alive
                    assert streams[-1].alive
                # A borrowed client and its stream still work after proxy teardown.
                await target.insert_one({"_id": "after_proxy"})
                assert (await streams[-1].try_next())["documentKey"] == {
                    "_id": "after_proxy"
                }
            finally:
                for stream in streams:
                    stream.close()

    asyncio.run(exercise())
