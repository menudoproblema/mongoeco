"""DDL state and committed effects fail, commit and replay as one unit."""

import asyncio

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.engines import MemoryEngine, SQLiteEngine


def _mutate(client, operation):
    if operation == "drop_database":
        client.drop_database("namespaces")
    elif operation == "rename":
        client.namespaces.source.rename("target")
    else:
        client.namespaces.source.drop()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("operation", ["drop", "rename", "drop_database"])
def test_ddl_recording_failure_restores_catalog_data_indexes_and_publication(
    backend, operation, tmp_path, monkeypatch
):
    engine = (
        MemoryEngine()
        if backend == "memory"
        else SQLiteEngine(str(tmp_path / "db.sqlite"))
    )
    with MongoClient(engine) as client:
        source = client.namespaces.create_collection(
            "source", validator={"v": {"$gte": 0}}
        )
        source.insert_one({"_id": 1, "v": 2})
        source.create_index("v")
        stream = client.watch(max_await_time_ms=1)
        method = (
            "_prepare_committed_changes_locked"
            if backend == "memory"
            else "_record_namespace_changes"
        )
        record = getattr(engine, method)

        def fail_after_prepare(*args, **kwargs):
            record(*args, **kwargs)
            message = "injected DDL effect failure"
            raise RuntimeError(message)

        try:
            with monkeypatch.context() as patch:
                patch.setattr(engine, method, fail_after_prepare)
                with pytest.raises(RuntimeError, match="injected DDL effect failure"):
                    _mutate(client, operation)
            assert client.namespaces.list_collection_names() == ["source"]
            assert source.find_one({"v": 2}, hint="v_1") == {"_id": 1, "v": 2}
            assert source.options()["validator"] == {"v": {"$gte": 0}}
            assert stream.try_next() is None
            _mutate(client, operation)
            client.namespaces.barrier.insert_one({"_id": "after"})
            expected = (
                ["drop", "dropDatabase"]
                if operation == "drop_database"
                else [operation]
            )
            expected.append("insert")
            assert [stream.try_next()["operationType"] for _ in expected] == expected
        finally:
            stream.close()


@pytest.mark.parametrize("backend", ["memory", "sqlite"])
@pytest.mark.parametrize("operation", ["drop", "rename", "drop_database"])
def test_ddl_rename_target_and_scope_survive_journal_restart(
    backend, operation, tmp_path
):
    def engine():
        return (
            MemoryEngine()
            if backend == "memory"
            else SQLiteEngine(str(tmp_path / "db.sqlite"))
        )

    options = {"change_stream_journal_path": str(tmp_path / "journal.json")}
    with MongoClient(engine(), **options) as client:
        source = client.namespaces.source
        source.insert_one({"_id": 0})
        stream = client.watch(max_await_time_ms=1)
        source.insert_one({"_id": 1})
        token = stream.try_next()["_id"]
        _mutate(client, operation)
        stream.close()
    with MongoClient(engine(), **options) as client:
        stream = client.watch(resume_after=token, max_await_time_ms=1)
        try:
            event = stream.try_next()
            assert event["operationType"] == (
                "rename" if operation == "rename" else "drop"
            )
            assert "documentKey" not in event
            if operation == "rename":
                assert event["to"] == {"db": "namespaces", "coll": "target"}
            if operation == "drop_database":
                assert stream.try_next()["operationType"] == "dropDatabase"
            client.namespaces.barrier.insert_one({"_id": "after_restart"})
            assert stream.try_next()["documentKey"] == {"_id": "after_restart"}
        finally:
            stream.close()


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("operation", ["drop", "rename", "drop_database"])
@pytest.mark.parametrize("commit", [False, True])
def test_local_transaction_delivers_namespace_effects_only_after_commit(
    engine_factory, operation, commit
):
    # Local transaction capability, not a claim that native DDL is allowed in
    # every transaction/topology. The replica-set corpus owns native behavior.
    async def exercise():
        async with AsyncMongoClient(engine_factory()) as client:
            source = client.namespaces.source
            await source.insert_one({"_id": 1})
            stream = client.watch(max_await_time_ms=1)
            session = client.start_session()
            session.start_transaction()
            try:
                if operation == "drop_database":
                    await client.drop_database("namespaces", session=session)
                elif operation == "rename":
                    await source.rename("target", session=session)
                else:
                    await source.drop(session=session)
                assert await stream.try_next() is None
                if commit:
                    session.commit_transaction()
                    event = await stream.try_next()
                    assert event["operationType"] == (
                        "rename" if operation == "rename" else "drop"
                    )
                    if operation == "drop_database":
                        assert (await stream.try_next())[
                            "operationType"
                        ] == "dropDatabase"
                else:
                    session.abort_transaction()
                    assert await source.find_one({}) == {"_id": 1}
                await client.namespaces.barrier.insert_one({"_id": "after"})
                assert (await stream.try_next())["documentKey"] == {"_id": "after"}
            finally:
                session.close()
                stream.close()

    asyncio.run(exercise())
