"""Hot catalog reuse never exposes shared mutable metadata to public callers."""

import asyncio
import subprocess
import sys

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.engines.sqlite import SQLiteEngine


@pytest.mark.parametrize("file_backed", [False, True])
def test_hot_catalog_reuses_read_only_generation(tmp_path, file_backed):
    async def exercise():
        engine = SQLiteEngine(
            str(tmp_path / "catalog.db") if file_backed else ":memory:"
        )
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.create_index(
                "key", partial_filter_expression={"active": True}
            )
            with engine._lock:
                first = engine._load_indexes("test", "records")
                second = engine._load_indexes("test", "records")
            assert first is second
            with pytest.raises(TypeError, match="immutable"):
                first.clear()
            with pytest.raises(TypeError, match="immutable"):
                first[0].fields.append("other")
            with pytest.raises(TypeError, match="immutable"):
                first[0].partial_filter_expression["active"] = False
            info = await collection.index_information()
            info["key_1"]["partialFilterExpression"]["active"] = False
            indexes = await collection.list_indexes().to_list()
            selected = next(index for index in indexes if index["name"] == "key_1")
            selected["partialFilterExpression"]["active"] = False
            info = await collection.index_information()
            assert info["key_1"]["partialFilterExpression"] == {"active": True}
            await collection.insert_many(
                [
                    {"_id": 1, "key": "yes", "active": True},
                    {"_id": 2, "key": "yes", "active": False},
                ]
            )
            assert await collection.count_documents({"key": "yes", "active": True}) == 1

    asyncio.run(exercise())


@pytest.mark.parametrize("action", ["create", "drop"])
def test_catalog_observes_external_ddl_without_reconnecting(tmp_path, action):
    async def exercise():
        path = str(tmp_path / "external.db")
        async with AsyncMongoClient(SQLiteEngine(path)) as first:
            collection = first.test.records
            await collection.insert_one({"_id": 1, "key": 7})
            if action == "drop":
                await collection.create_index("key")
            async with AsyncMongoClient(SQLiteEngine(path)) as second:
                await collection.index_information()
                if action == "create":
                    await second.test.records.create_index("key")
                else:
                    await second.test.records.drop_index("key_1")
                expected = await second.test.records.index_information()
                assert await collection.index_information() == expected
                assert await collection.find({"key": 7}).to_list() == [
                    {"_id": 1, "key": 7}
                ]

    asyncio.run(exercise())


@pytest.mark.parametrize("read", ["scan", "first", "count"])
def test_read_replans_after_external_index_drop(tmp_path, read):
    async def exercise():
        path = str(tmp_path / "external-read.db")
        async with AsyncMongoClient(SQLiteEngine(path)) as first:
            collection = first.test.records
            await collection.insert_one({"_id": 1, "key": 7})
            await collection.create_index("key")
            async with AsyncMongoClient(SQLiteEngine(path)) as second:
                await collection.index_information()
                await second.test.records.drop_index("key_1")
                if read == "count":
                    assert await collection.count_documents({"key": 7}) == 1
                else:
                    cursor = collection.find({"key": 7})
                    if read == "first":
                        cursor = cursor.limit(1)
                    assert await cursor.to_list() == [{"_id": 1, "key": 7}]

    asyncio.run(exercise())


def test_catalog_is_specific_to_connection_snapshot(tmp_path):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "views.db"))
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.create_index("key")
            old_connection = engine._create_sqlite_connection()
            try:
                old_connection.execute("BEGIN")
                old_connection.execute("SELECT name FROM indexes").fetchall()
                await collection.drop_index("key_1")
                await collection.create_index("other")
                with engine._lock:
                    current = engine._load_indexes("test", "records")
                    with engine._bind_connection(old_connection):
                        old = engine._load_indexes("test", "records")
                    assert [index.name for index in current] == ["other_1"]
                    assert [index.name for index in old] == ["key_1"]
                    assert engine._load_indexes("test", "records") is current
                    old_connection.rollback()
                    with engine._bind_connection(old_connection):
                        refreshed = engine._load_indexes("test", "records")
                    assert [index.name for index in refreshed] == ["other_1"]
            finally:
                old_connection.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("read", ["scan", "first", "count"])
def test_catalog_and_read_share_view_during_external_ddl(tmp_path, monkeypatch, read):
    async def exercise():
        path = str(tmp_path / "ddl-during-read.db")
        engine = SQLiteEngine(path)
        writer = SQLiteEngine(path)
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1, "key": 7})
            await collection.create_index("key")
            async with AsyncMongoClient(writer):
                original = engine._load_indexes
                dropped = False

                def drop_after_catalog(db_name, coll_name):
                    nonlocal dropped
                    catalog = original(db_name, coll_name)
                    if not dropped and engine._require_connection().in_transaction:
                        dropped = True
                        writer._drop_index_sync("test", "records", "key_1", None)
                    return catalog

                monkeypatch.setattr(engine, "_load_indexes", drop_after_catalog)
                if read == "count":
                    assert await collection.count_documents({"key": 7}) == 1
                else:
                    cursor = collection.find({"key": 7})
                    if read == "first":
                        cursor = cursor.limit(1)
                    assert await cursor.to_list() == [{"_id": 1, "key": 7}]
                assert dropped
                assert "key_1" not in await collection.index_information()

    asyncio.run(exercise())


@pytest.mark.parametrize("action", ["create", "drop"])
def test_catalog_observes_ddl_from_another_process(tmp_path, action):
    async def exercise():
        path = str(tmp_path / "process.db")
        async with AsyncMongoClient(SQLiteEngine(path)) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1, "key": 7})
            if action == "drop":
                await collection.create_index("key")
            await collection.index_information()
            script = (
                "import sys\n"
                "from mongoeco import MongoClient\n"
                "from mongoeco.engines.sqlite import SQLiteEngine\n"
                "with MongoClient(SQLiteEngine(sys.argv[1])) as client:\n"
                "    if sys.argv[2] == 'create':\n"
                "        client.test.records.create_index('key')\n"
                "    else:\n"
                "        client.test.records.drop_index('key_1')\n"
            )
            await asyncio.to_thread(
                subprocess.run,
                [sys.executable, "-c", script, path, action],
                check=True,
                capture_output=True,
                timeout=20,
            )
            assert ("key_1" in await collection.index_information()) == (
                action == "create"
            )
            assert await collection.find({"key": 7}).to_list() == [{"_id": 1, "key": 7}]

    asyncio.run(exercise())


def test_recreated_namespace_does_not_reuse_old_index_identity(tmp_path):
    async def exercise():
        path = str(tmp_path / "recreated.db")
        async with AsyncMongoClient(SQLiteEngine(path)) as first:
            collection = first.test.records
            await collection.insert_one({"_id": 1, "key": 7})
            await collection.create_index("key")
            assert await collection.find({"key": 7}).limit(1).to_list() == [
                {"_id": 1, "key": 7}
            ]
            async with AsyncMongoClient(SQLiteEngine(path)) as second:
                await second.test.records.drop()
                await second.test.records.insert_one({"_id": 2, "key": 7})
                await second.test.records.create_index("key")
                assert await collection.find({"key": 7}).limit(1).to_list() == [
                    {"_id": 2, "key": 7}
                ]
                assert await collection.count_documents({"key": 7}) == 1

    asyncio.run(exercise())


def test_equivalent_connection_views_share_catalog_representation(tmp_path):
    async def exercise():
        engine = SQLiteEngine(str(tmp_path / "equal-views.db"))
        async with AsyncMongoClient(engine) as client:
            await client.test.records.create_index("key")
            with engine._lock:
                original = engine._load_indexes("test", "records")
            other = engine._create_sqlite_connection()
            try:
                with engine._bind_connection(other):
                    current = engine._load_indexes("test", "records")
                assert current is original
                other.close()
                assert other.index_catalogs == {}
                assert [index.name for index in original] == ["key_1"]
            finally:
                other.close()
        assert engine._cache_state.index_catalog_pool._entries == {}

    asyncio.run(exercise())


@pytest.mark.parametrize("file_backed", [False, True])
@pytest.mark.parametrize("commit", [False, True])
def test_catalog_generations_survive_ddl_and_transaction_finish(
    tmp_path, file_backed, commit
):
    async def exercise():
        engine = SQLiteEngine(
            str(tmp_path / "catalog.db") if file_backed else ":memory:"
        )
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.create_index("key")
            with engine._lock:
                original = engine._load_indexes("test", "records")
            session = client.start_session()
            session.start_transaction()
            try:
                await collection.drop_index("key_1", session=session)
                await collection.create_index("other", session=session)
                with (
                    engine._lock,
                    engine._bind_connection(engine._require_connection(session)),
                ):
                    changed = engine._load_indexes("test", "records")
                assert [index.name for index in original] == ["key_1"]
                assert [index.name for index in changed] == ["other_1"]
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                with engine._lock:
                    final = engine._load_indexes("test", "records")
                assert [index.name for index in final] == (
                    ["other_1"] if commit else ["key_1"]
                )
                assert [index.name for index in original] == ["key_1"]
                assert [index.name for index in changed] == ["other_1"]
            finally:
                if session.in_transaction:
                    session.abort_transaction()
                session.close()

    asyncio.run(exercise())
