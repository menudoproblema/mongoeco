"""The common public result path does not rebuild every container twice."""

import asyncio

from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.core.codec import DocumentCodec
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine


@pytest.mark.parametrize("backend", ["memory", "sqlite", "sqlite-file"])
def test_default_public_materialization_avoids_recursive_codec_copy(tmp_path, backend):
    async def exercise():
        engine = (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(
                str(tmp_path / "reads.db") if backend == "sqlite-file" else ":memory:"
            )
        )
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            expected = [
                {"_id": value, "nested": {"items": [value, {"value": value}]}}
                for value in range(25)
            ]
            await collection.insert_many(expected)
            with patch.object(
                DocumentCodec,
                "_apply_codec_options_recursive",
                wraps=DocumentCodec._apply_codec_options_recursive,
            ) as recursive:
                result = await collection.find({}, sort=[("_id", 1)]).to_list()
                assert result == expected
                recursive.assert_not_called()
            result[0]["nested"]["items"][1]["value"] = "changed"
            assert await collection.find_one({"_id": 0}) == expected[0]

    asyncio.run(exercise())
