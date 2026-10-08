"""Large and small RawBSONDocument replies survive shared immutable buffers."""

import asyncio

import pymongo
import pytest

from bson.raw_bson import RawBSONDocument

from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.wire import AsyncMongoEcoProxyServer


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
def test_raw_bson_documents_and_nested_buffers_remain_stable(backend):
    expected_max_wire_version = 20

    async def exercise():
        engine = MemoryEngine() if backend == "memory" else SQLiteEngine()
        async with AsyncMongoEcoProxyServer(
            engine=engine, pymongo_profile="4.18"
        ) as proxy:

            def run_client():
                sizes = [4095, 4096, 4097, 8193]
                with pymongo.MongoClient(
                    proxy.address.uri,
                    directConnection=True,
                    serverSelectionTimeoutMS=3000,
                ) as client:
                    assert (
                        client.admin.command("hello")["maxWireVersion"]
                        == expected_max_wire_version
                    )
                    documents = [
                        {"_id": index, "nested": {"text": "x" * size}}
                        for index, size in enumerate(sizes)
                    ]
                    client.test.buffers.insert_many(documents)
                    raw = client.test.get_collection(
                        "buffers",
                        codec_options=client.codec_options.with_options(
                            document_class=RawBSONDocument
                        ),
                    )
                    results = list(raw.find({}, sort=[("_id", 1)], batch_size=2))
                    saved = [bytes(document.raw) for document in results]
                    for document, size in zip(results, sizes, strict=True):
                        assert document["nested"]["text"] == "x" * size
                        if isinstance(document.raw, memoryview):
                            assert document.raw.readonly
                            with pytest.raises(TypeError):
                                document.raw[0] = 0
                    client.test.buffers.delete_many({})
                    assert [bytes(document.raw) for document in results] == saved
                    assert [document["nested"]["text"] for document in results] == [
                        "x" * size for size in sizes
                    ]

            await asyncio.to_thread(run_client)

    asyncio.run(exercise())
