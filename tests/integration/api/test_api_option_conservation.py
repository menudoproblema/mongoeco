"""API inputs remain detached and insertion comments reach observable runtime state."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient, MongoClient

from tests.support import ENGINE_FACTORIES


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
@pytest.mark.parametrize("method", ["insert_one", "insert_many"])
def test_insert_comment_is_visible_while_active_and_detached_afterwards(
    engine, method, monkeypatch
):
    async def exercise():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            database = client.conservation
            await database.command({"profile": 2})
            collection = database.items
            session = client.start_session()
            comment = {"request": "42", "tags": ["before"]}
            expected = {"request": "42", "tags": ["before"]}
            runtime = collection._runtime
            name = (
                "engine_insert_document"
                if method == "insert_one"
                else "engine_insert_documents"
            )
            original = getattr(runtime, name)

            async def observed_insert(*args, **kwargs):
                active = await client.admin.command({"currentOp": 1, "$all": True})
                matching = [
                    entry
                    for entry in active["inprog"]
                    if entry.get("ns") == "conservation.items"
                ]
                assert len(matching) == 1
                assert matching[0]["comment"] == expected
                comment["tags"].append("changed during insert")
                return await original(*args, **kwargs)

            monkeypatch.setattr(runtime, name, observed_insert)
            payload = {"_id": "a"} if method == "insert_one" else [{"_id": "a"}]
            await getattr(collection, method)(payload, comment=comment, session=session)
            state = next(iter(session.engine_state.values()))
            assert state["last_operation"]["comment"] == expected
            profiles = (
                await database["system.profile"]
                .find({"command.insert": "items", "command.comment": {"$exists": True}})
                .to_list()
            )
            assert len(profiles) == 1
            assert profiles[0]["command"]["comment"] == expected
            assert await collection.find_one({"_id": "a"}) == {"_id": "a"}
            active = await client.admin.command({"currentOp": 1, "$all": True})
            assert not any(
                e.get("ns") == "conservation.items" for e in active["inprog"]
            )

    asyncio.run(exercise())


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
def test_projection_list_is_captured_before_lazy_cursor_execution(engine):
    async def exercise():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            collection = client.conservation.items
            await collection.insert_one({"_id": "a", "name": "Ada", "hidden": 1})
            projection = ["name"]
            cursor = collection.find({}, projection)
            clone = cursor.clone()
            projection.append("hidden")
            expected = [{"_id": "a", "name": "Ada"}]
            assert await cursor.to_list() == expected
            assert await clone.to_list() == expected

    asyncio.run(exercise())
    with MongoClient(ENGINE_FACTORIES[engine]()) as client:
        collection = client.conservation.items
        collection.insert_one({"_id": "a", "name": "Ada", "hidden": 1})
        projection = ["name"]
        cursor = collection.find({}, projection)
        clone = cursor.clone()
        projection.append("hidden")
        expected = [{"_id": "a", "name": "Ada"}]
        assert cursor.to_list() == expected
        assert clone.to_list() == expected


@pytest.mark.parametrize("engine", ENGINE_FACTORIES)
def test_insert_many_accepts_an_iterable_that_can_only_be_consumed_once(engine):
    class SinglePassDocuments:
        def __init__(self):
            self.iterations = 0
            self.documents = [{"name": "Ada"}, {"name": "Bea"}]

        def __iter__(self):
            self.iterations += 1
            assert self.iterations == 1
            yield from self.documents

    async def exercise():
        async with AsyncMongoClient(ENGINE_FACTORIES[engine]()) as client:
            documents = SinglePassDocuments()
            result = await client.conservation.items.insert_many(documents)
            assert result.inserted_ids == [d["_id"] for d in documents.documents]
            assert documents.iterations == 1

    asyncio.run(exercise())
    with MongoClient(ENGINE_FACTORIES[engine]()) as client:
        documents = SinglePassDocuments()
        result = client.conservation.items.insert_many(documents)
        assert result.inserted_ids == [d["_id"] for d in documents.documents]
        assert documents.iterations == 1
