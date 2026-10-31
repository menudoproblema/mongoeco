"""Atomic destination replacement, including failure and resource preservation."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient, MongoClient, SearchIndexModel
from mongoeco.engines import MemoryEngine, SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.integration.api.test_semantic_group_regressions import _AsyncView


def test_spi_v2_provider_keeps_plain_rename_without_claiming_atomic_overwrite():
    class SpiOnlyEngine:
        def __init__(self):
            self.delegate = MemoryEngine()

        def __getattr__(self, name):
            if name == "_rename_collection":
                raise AttributeError(name)
            return getattr(self.delegate, name)

    with MongoClient(SpiOnlyEngine()) as client:
        client.spi.source.insert_one({"_id": 1})
        client.admin.command({"renameCollection": "spi.source", "to": "spi.target"})
        assert client.spi.target.find_one({}) == {"_id": 1}

        client.spi.source.insert_one({"_id": 2})
        with pytest.raises(OperationFailure) as error:
            client.admin.command(
                {
                    "renameCollection": "spi.source",
                    "to": "spi.target",
                    "dropTarget": True,
                }
            )
        assert error.value.code == 20  # noqa: PLR2004 - explicit unsupported operation code
        assert client.spi.source.find_one({}) == {"_id": 2}
        assert client.spi.target.find_one({}) == {"_id": 1}


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_plain_rename_command_respects_the_providers_public_spi_override(
    engine_factory,
):
    class PolicyEngine(engine_factory):
        async def rename_collection(self, *args, **kwargs):
            message = "provider rename policy"
            raise RuntimeError(message)

    with MongoClient(PolicyEngine()) as client:
        client.spi.source.insert_one({"_id": 1})
        with pytest.raises(RuntimeError, match="provider rename policy"):
            client.admin.command({"renameCollection": "spi.source", "to": "spi.target"})
        assert client.spi.source.find_one({}) == {"_id": 1}
        assert client.spi.target.find_one({}) is None


def _exercise(client, engine, failure, monkeypatch):
    database = client["atomic_rename"]
    source = database.create_collection(
        "source", changeStreamPreAndPostImages={"enabled": True}
    )
    target = database.create_collection(
        "target", changeStreamPreAndPostImages={"enabled": False}
    )
    source.insert_one({"_id": "source", "v": 1})
    target.insert_one({"_id": "target", "v": 2})
    source.create_index("v", name="source_index")
    target.create_index("v", name="target_index")
    stream = client.watch(max_await_time_ms=1)
    method = (
        "_record_namespace_changes"
        if failure == "effects"
        else "_drop_collection_contents_locked"
        if isinstance(engine, MemoryEngine)
        else "_drop_collection_contents"
    )
    original = getattr(engine, method)

    def fail_after(*args, **kwargs):
        original(*args, **kwargs)
        message = "injected rename failure"
        raise RuntimeError(message)

    command = {
        "renameCollection": "atomic_rename.source",
        "to": "atomic_rename.target",
        "dropTarget": True,
    }
    try:
        with monkeypatch.context() as patch:
            patch.setattr(engine, method, fail_after)
            with pytest.raises(RuntimeError, match="injected rename failure"):
                client["admin"].command(command)
        assert source.find_one({"v": 1}, hint="source_index") == {
            "_id": "source",
            "v": 1,
        }
        assert target.find_one({"v": 2}, hint="target_index") == {
            "_id": "target",
            "v": 2,
        }
        assert source.options()["changeStreamPreAndPostImages"] == {"enabled": True}
        assert target.options()["changeStreamPreAndPostImages"] == {"enabled": False}
        assert stream.try_next() is None
        client["admin"].command(command)
        assert stream.try_next()["operationType"] == "rename"
        target.insert_one({"_id": "barrier", "v": 3})
        assert stream.try_next()["documentKey"] == {"_id": "barrier"}
        assert target.find_one({"v": 1}, hint="source_index") == {
            "_id": "source",
            "v": 1,
        }
        assert target.find_one({"_id": "target"}) is None
    finally:
        stream.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("surface", ["sync", "async"])
@pytest.mark.parametrize("failure", ["destination", "effects"])
def test_destination_replacement_and_rename_effects_roll_back_together(
    engine_factory, surface, failure, monkeypatch
):
    engine = engine_factory()
    if surface == "sync":
        with MongoClient(engine) as client:
            _exercise(client, engine, failure, monkeypatch)
        return

    async def exercise():
        async with AsyncMongoClient(engine) as client:
            view = _AsyncView(client, asyncio.get_running_loop(), client)
            await asyncio.to_thread(_exercise, view, engine, failure, monkeypatch)

    asyncio.run(exercise())


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
def test_rename_replaces_destination_search_resources_and_preserves_them_on_failure(
    engine_factory, monkeypatch
):
    engine = engine_factory()
    with MongoClient(engine) as client:
        source, target = client.rename_search.source, client.rename_search.target
        source.insert_one({"_id": "source", "title": "retained"})
        target.insert_one({"_id": "target", "title": "retained"})
        index = SearchIndexModel({"mappings": {"dynamic": True}}, name="text")
        source.create_search_index(index)
        target.create_search_index(index)
        pipeline = [
            {
                "$search": {
                    "index": "text",
                    "text": {"query": "retained", "path": "title"},
                }
            }
        ]

        def found(collection):
            return [row["_id"] for row in collection.aggregate(pipeline)]

        assert found(source) == ["source"]
        assert found(target) == ["target"]
        record = engine._record_namespace_changes

        def fail_after(*args, **kwargs):
            record(*args, **kwargs)
            message = "injected rename failure"
            raise RuntimeError(message)

        command = {
            "renameCollection": "rename_search.source",
            "to": "rename_search.target",
            "dropTarget": True,
        }
        with monkeypatch.context() as patch:
            patch.setattr(engine, "_record_namespace_changes", fail_after)
            with pytest.raises(RuntimeError, match="injected rename failure"):
                client.admin.command(command)
        assert found(source) == ["source"]
        assert found(target) == ["target"]
        client.admin.command(command)
        assert found(target) == ["source"]
