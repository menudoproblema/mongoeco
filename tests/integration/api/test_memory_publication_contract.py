"""Histories mixing transaction publication with acknowledged ordinary writes."""

import asyncio

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import ChangePublicationPolicy, OperationContext
from mongoeco.engines.memory import MemoryEngine
from mongoeco.errors import InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import SearchIndexDefinition


_SECOND_PREPARED_ROOT = 2
_WRITE_SET_PREPARATION_FAILURE = "write-set preparation failed"


@pytest.mark.parametrize("action", ["insert", "update", "delete"])
@pytest.mark.parametrize("finish", ["commit", "abort"])
@pytest.mark.parametrize("transaction_writes", [False, True])
@pytest.mark.parametrize("namespace", ["same", "collection", "database"])
def test_acknowledged_write_survives_transaction_finish(
    action,
    finish,
    transaction_writes,
    namespace,
):
    async def exercise():
        async with AsyncMongoClient(MemoryEngine()) as client:
            local = client.first.records
            external = {
                "same": local,
                "collection": client.first.other,
                "database": client.second.other,
            }[namespace]
            await local.insert_one({"_id": "local", "value": 0})
            await external.insert_one({"_id": "external", "value": 0})
            session = client.start_session()
            session.start_transaction()
            try:
                if transaction_writes:
                    await local.update_one(
                        {"_id": "local"},
                        {"$set": {"value": 1}},
                        session=session,
                    )
                if action == "insert":
                    key = "inserted"
                    expected = {"_id": key, "value": 2}
                    result = await external.insert_one(expected)
                    assert result.inserted_id == key
                elif action == "update":
                    key = "external"
                    expected = {"_id": key, "value": 2}
                    result = await external.update_one(
                        {"_id": key},
                        {"$set": {"value": 2}},
                    )
                    assert result.modified_count == 1
                else:
                    key = "external"
                    expected = None
                    result = await external.delete_one({"_id": key})
                    assert result.deleted_count == 1
                assert await external.find_one({"_id": key}) == expected
                if finish == "abort":
                    session.abort_transaction()
                elif transaction_writes:
                    with pytest.raises(OperationFailure, match="Write conflict"):
                        session.commit_transaction()
                    session.abort_transaction()
                else:
                    session.commit_transaction()
                assert await external.find_one({"_id": key}) == expected
            finally:
                session.close()

    asyncio.run(exercise())


async def _mutate_catalog(engine, action):
    if action == "create_collection":
        await engine.create_collection("other", "new", options={"capped": False})
    elif action == "drop_collection":
        await engine.drop_collection("other", "records")
    elif action == "rename_collection":
        await engine.rename_collection("other", "records", "renamed")
    elif action == "drop_database":
        await engine.drop_database("other")
    elif action == "create_index":
        await engine.create_index("other", "records", ["another"], name="new_idx")
    elif action == "drop_index":
        await engine.drop_index("other", "records", "value_idx")
    elif action == "drop_indexes":
        await engine.drop_indexes("other", "records")
    elif action == "create_search_index":
        await engine.create_search_index(
            "other",
            "records",
            SearchIndexDefinition({"mappings": {"dynamic": True}}, name="new"),
        )
    elif action == "update_search_index":
        await engine.update_search_index(
            "other",
            "records",
            "search",
            {"mappings": {"dynamic": False}},
        )
    else:
        await engine.drop_search_index("other", "records", "search")


@pytest.mark.parametrize(
    "action",
    [
        "create_collection",
        "drop_collection",
        "rename_collection",
        "drop_database",
        "create_index",
        "drop_index",
        "drop_indexes",
        "create_search_index",
        "update_search_index",
        "drop_search_index",
    ],
)
@pytest.mark.parametrize("transaction_writes", [False, True])
def test_external_catalog_mutations_participate_in_publication(
    action, transaction_writes
):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            await client.first.local.insert_one({"_id": "local"})
            await client.other.records.insert_one({"_id": 1, "value": 1})
            await engine.create_index("other", "records", ["value"], name="value_idx")
            await engine.create_search_index(
                "other",
                "records",
                SearchIndexDefinition({"mappings": {"dynamic": True}}, name="search"),
            )
            session = client.start_session()
            session.start_transaction()
            try:
                if transaction_writes:
                    await client.first.local.insert_one(
                        {"_id": "transaction"}, session=session
                    )
                await _mutate_catalog(engine, action)

                async def observe():
                    result = {}
                    for name in await engine.list_collections("other"):
                        result[name] = (
                            await client.other[name].find({}).to_list(),
                            await engine.list_indexes("other", name),
                            await engine.list_search_indexes("other", name),
                            await engine.collection_options("other", name),
                        )
                    return result

                expected = await observe()
                if transaction_writes:
                    with pytest.raises(OperationFailure, match="Write conflict"):
                        session.commit_transaction()
                    session.abort_transaction()
                else:
                    session.commit_transaction()
                assert await observe() == expected
            finally:
                session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("transaction_writes", [False, True])
def test_ttl_publication_does_not_resurrect_expired_documents(transaction_writes):
    async def exercise():
        clock = [datetime(2026, 1, 1, tzinfo=UTC)]
        engine = MemoryEngine()
        async with AsyncMongoClient(engine, now_factory=lambda: clock[0]) as client:
            await client.test.local.insert_one({"_id": "local"})
            await client.other.records.create_index("expires", expire_after_seconds=0)
            await client.other.records.insert_one(
                {"_id": 1, "expires": clock[0] + timedelta(seconds=10)}
            )
            session = client.start_session()
            session.start_transaction()
            try:
                if transaction_writes:
                    await client.test.local.insert_one(
                        {"_id": "transaction"}, session=session
                    )
                clock[0] += timedelta(seconds=20)
                assert await client.other.records.find_one({"_id": 1}) is None
                # Rewinding the deterministic clock prevents a second purge from
                # concealing resurrection by the transaction commit.
                clock[0] -= timedelta(seconds=20)
                if transaction_writes:
                    with pytest.raises(OperationFailure, match="Write conflict"):
                        session.commit_transaction()
                    session.abort_transaction()
                else:
                    session.commit_transaction()
                assert await client.other.records.find_one({"_id": 1}) is None
            finally:
                session.close()

    asyncio.run(exercise())


def test_failed_mutation_does_not_publish_a_generation():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            await client.test.records.insert_one({"_id": 1, "value": 0})
            session = client.start_session()
            session.start_transaction()
            try:
                await client.test.records.insert_one(
                    {"_id": "transaction"}, session=session
                )
                version = engine._mvcc_version
                with (
                    patch.object(
                        engine,
                        "_update_indexes_locked",
                        side_effect=RuntimeError("index failure"),
                    ),
                    pytest.raises(RuntimeError, match="index failure"),
                ):
                    await client.other.records.insert_one({"_id": "failed"})
                assert engine._mvcc_version == version
                session.commit_transaction()
                assert (
                    await client.test.records.find_one({"_id": "transaction"})
                    is not None
                )
                assert await client.other.records.find_one({"_id": "failed"}) is None
            finally:
                session.close()

    asyncio.run(exercise())


def test_transaction_hooks_reject_reentrant_partial_publication():
    engine = MemoryEngine()
    session = ClientSession()
    engine.create_session_state(session)
    with (
        engine._publication_scope(engine._storage),
        pytest.raises(OperationFailure, match="publication is in progress"),
    ):
        engine._start_session_transaction(session)
    assert not engine._publication_stack


def test_publication_rejects_detached_and_mixed_storage_roots():
    engine = MemoryEngine()
    session = ClientSession()
    engine.create_session_state(session)
    session.start_transaction()
    snapshot = engine._mvcc_states[session.session_id]
    try:
        with (
            pytest.raises(OperationFailure, match="detached storage view"),
            engine._publication_scope({}),
        ):
            pytest.fail("a detached view entered publication")
        with (
            pytest.raises(OperationFailure, match="inconsistent publication views"),
            engine._publication_scope(
                engine._storage,
                views=(
                    snapshot.indexes,
                    engine._index_data,
                    engine._search_indexes,
                    engine._collections,
                    engine._collection_options,
                ),
            ),
        ):
            pytest.fail("mixed roots entered publication")
        with (
            engine._publication_scope(engine._storage),
            pytest.raises(
                OperationFailure, match="nested publication changes storage view"
            ),
            engine._publication_scope(snapshot.storage),
        ):
            pytest.fail("a nested publication switched ownership")
        with (
            pytest.raises(OperationFailure, match="requires an ownership scope"),
            engine._publication_scope(snapshot.storage),
        ):
            pytest.fail("a transaction publication entered without ownership")
        assert not engine._publication_stack
    finally:
        session.close()


def test_changes_cannot_bypass_publication_or_target_another_session():
    engine = MemoryEngine()
    session = ClientSession()
    engine.create_session_state(session)
    session.start_transaction()
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_70,
        session=session,
        publication=ChangePublicationPolicy.EMIT,
    )
    try:
        with pytest.raises(InvalidOperation, match="requires an active publication"):
            engine._record_committed_change(context, {"value": 1})
        with (
            engine._publication_scope(engine._storage),
            pytest.raises(InvalidOperation, match="another storage view"),
        ):
            engine._record_committed_change(context, {"value": 1})
        assert engine._commit_sequence == 0
        assert engine._mvcc_states[session.session_id].pending_changes == []
    finally:
        session.close()


@pytest.mark.parametrize("action", ["create", "drop", "rename", "index"])
@pytest.mark.parametrize("finish", ["commit", "abort"])
def test_catalog_only_transaction_tracks_writes_without_outbox(action, finish):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            await client.other.records.insert_one({"_id": 1, "value": 1})
            session = client.start_session()
            session.start_transaction()
            try:
                if action == "create":
                    await engine.create_collection("other", "new", context=session)
                elif action == "drop":
                    await engine.drop_database("other", context=session)
                elif action == "rename":
                    await engine.rename_collection(
                        "other", "records", "new", context=session
                    )
                else:
                    await engine.create_index(
                        "other", "records", ["value"], name="value_idx", context=session
                    )
                state = engine._mvcc_states[session.session_id]
                assert state.has_writes
                assert state.pending_changes == []
                if finish == "commit":
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                names = set(await engine.list_collections("other"))
                if finish == "abort":
                    assert names == {"records"}
                elif action == "drop":
                    assert names == set()
                elif action == "create":
                    assert names == {"records", "new"}
                elif action == "rename":
                    assert names == {"new"}
                if action == "index":
                    indexes = await engine.list_indexes("other", "records")
                    assert ("value_idx" in {index["name"] for index in indexes}) == (
                        finish == "commit"
                    )
            finally:
                session.close()

    asyncio.run(exercise())


def test_discarded_merge_does_not_publish_an_empty_namespace():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine):
            version = engine._mvcc_version
            outcome = await engine.merge_document(
                "missing",
                "records",
                {"_id": 1},
                when_matched="merge",
                when_not_matched="discard",
            )
            assert not outcome.applied
            assert await engine.list_databases() == []
            assert engine._mvcc_version == version

    asyncio.run(exercise())


@pytest.mark.parametrize(
    "action", ["insert", "batch", "update", "upsert", "delete", "merge"]
)
@pytest.mark.parametrize("transactional", [False, True])
def test_failed_outbox_preparation_preserves_data_indexes_and_history(
    action, transactional
):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1, "value": "before"})
            await collection.create_index("value", unique=True)
            engine.register_change_consumer("audit")
            session = client.start_session() if transactional else None
            if session is not None:
                session.start_transaction()
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                session=session,
                publication=ChangePublicationPolicy.EMIT,
            )

            async def mutate():
                if action == "insert":
                    await engine.insert_document(
                        "test",
                        "records",
                        {"_id": 2, "value": "after"},
                        overwrite=False,
                        operation_context=context,
                    )
                elif action == "batch":
                    await engine.insert_documents(
                        "test",
                        "records",
                        [
                            {"_id": 2, "value": "after"},
                            {"_id": 3, "value": "third"},
                        ],
                        operation_context=context,
                    )
                elif action in {"update", "upsert"}:
                    await engine.update_with_operation(
                        "test",
                        "records",
                        compile_update_operation(
                            {"_id": 2 if action == "upsert" else 1},
                            update_spec={"$set": {"value": "after"}},
                        ),
                        upsert=action == "upsert",
                        operation_context=context,
                    )
                elif action == "delete":
                    await engine.delete_with_operation(
                        "test",
                        "records",
                        compile_update_operation({"_id": 1}),
                        operation_context=context,
                    )
                else:
                    await engine.merge_document(
                        "test",
                        "records",
                        {"_id": 1, "value": "after"},
                        when_matched="replace",
                        when_not_matched="insert",
                        operation_context=context,
                    )

            version = engine._mvcc_version
            sequence = engine._commit_sequence
            try:
                with (
                    patch.object(
                        engine,
                        "_prepare_committed_changes_locked",
                        side_effect=RuntimeError("outbox preparation failed"),
                    ),
                ):
                    if transactional:
                        await mutate()
                        with pytest.raises(
                            RuntimeError, match="outbox preparation failed"
                        ):
                            session.commit_transaction()
                    else:
                        with pytest.raises(
                            RuntimeError, match="outbox preparation failed"
                        ):
                            await mutate()
                if session is not None:
                    session.abort_transaction()
                assert engine._mvcc_version == version
                assert engine._commit_sequence == sequence
                assert await collection.find({}).to_list() == [
                    {"_id": 1, "value": "before"}
                ]
                assert await collection.find({"value": "before"}).hint(
                    "value_1"
                ).to_list() == [
                    {"_id": 1, "value": "before"},
                ]
                changes = []
                engine.dispatch_committed_changes("audit", changes.append)
                assert changes == []
                assert not engine._publication_stack
            finally:
                if session is not None:
                    session.close()
                engine.unregister_change_consumer("audit")

    asyncio.run(exercise())


def test_committed_callback_runs_outside_publication_and_can_start_a_snapshot():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            context = OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                publication=ChangePublicationPolicy.EMIT,
            )
            observed = []

            def on_commit(outcome):
                assert not engine._publication_stack
                session = client.start_session()
                session.start_transaction()
                try:
                    assert (
                        engine._mvcc_states[session.session_id].snapshot_version
                        == engine._mvcc_version
                    )
                    observed.append(outcome.commit_sequence)
                    session.commit_transaction()
                finally:
                    session.close()

            outcome = await engine.insert_document(
                "test",
                "records",
                {"_id": 1},
                overwrite=False,
                operation_context=context,
                on_commit=on_commit,
            )
            assert observed == [outcome.commit_sequence]

    asyncio.run(exercise())


def test_sync_read_only_transaction_does_not_reinstall_its_snapshot():
    with MongoClient(MemoryEngine()) as client:
        collection = client.test.records
        collection.insert_one({"_id": 1, "value": 0})
        session = client.start_session()
        session.start_transaction()
        try:
            collection.update_one({"_id": 1}, {"$set": {"value": 2}})
            session.commit_transaction()
            assert collection.find_one({"_id": 1}) == {"_id": 1, "value": 2}
        finally:
            session.close()


def test_transaction_commit_publishes_only_its_touched_namespace():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            untouched = client.shared.untouched
            changed = client.shared.changed
            await untouched.insert_one({"_id": 1, "value": "stable"})
            await untouched.create_index("value")
            await engine.create_collection(
                "shared",
                "untouched_options",
                options={"validator": {"value": {"$type": "string"}}},
            )
            await changed.insert_one({"_id": 1, "value": "before"})

            storage_root = engine._storage["shared"]["untouched"]
            index_root = engine._indexes["shared"]["untouched"]
            index_data_root = engine._index_data["shared"]["untouched"]
            options_root = engine._collection_options["shared"]["untouched_options"]

            session = client.start_session()
            session.start_transaction()
            try:
                await changed.update_one(
                    {"_id": 1},
                    {"$set": {"value": "after"}},
                    session=session,
                )
                state = engine._mvcc_states[session.session_id]
                assert state.touched_namespaces == {("shared", "changed")}
                assert state.touched_databases == set()
                session.commit_transaction()
            finally:
                session.close()

            assert await changed.find_one({"_id": 1}) == {
                "_id": 1,
                "value": "after",
            }
            assert engine._storage["shared"]["untouched"] is storage_root
            assert engine._indexes["shared"]["untouched"] is index_root
            assert engine._index_data["shared"]["untouched"] is index_data_root
            assert (
                engine._collection_options["shared"]["untouched_options"]
                is options_root
            )

    asyncio.run(exercise())


def test_transaction_rename_tracks_both_namespaces_and_preserves_siblings():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            await client.shared.records.insert_one({"_id": 1})
            await client.shared.sibling.insert_one({"_id": 2})
            sibling_root = engine._storage["shared"]["sibling"]

            session = client.start_session()
            session.start_transaction()
            try:
                await engine.rename_collection(
                    "shared",
                    "records",
                    "renamed",
                    context=session,
                )
                state = engine._mvcc_states[session.session_id]
                assert state.touched_namespaces == {
                    ("shared", "records"),
                    ("shared", "renamed"),
                }
                session.commit_transaction()
            finally:
                session.close()

            assert set(await engine.list_collections("shared")) == {
                "renamed",
                "sibling",
            }
            assert engine._storage["shared"]["sibling"] is sibling_root

    asyncio.run(exercise())


def test_transaction_database_drop_preserves_unrelated_database_roots():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            await client.dropped.records.insert_one({"_id": 1})
            await client.stable.records.insert_one({"_id": 2})
            stable_storage = engine._storage["stable"]
            stable_collections = engine._collections["stable"]

            session = client.start_session()
            session.start_transaction()
            try:
                await engine.drop_database("dropped", context=session)
                state = engine._mvcc_states[session.session_id]
                assert state.touched_databases == {"dropped"}
                session.commit_transaction()
            finally:
                session.close()

            assert "dropped" not in await engine.list_databases()
            assert engine._storage["stable"] is stable_storage
            assert engine._collections["stable"] is stable_collections

    asyncio.run(exercise())


def test_transaction_write_set_is_prepared_before_any_live_root_is_replaced():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_one({"_id": 1, "value": "before"})
            session = client.start_session()
            session.start_transaction()
            try:
                await collection.update_one(
                    {"_id": 1},
                    {"$set": {"value": "after"}},
                    session=session,
                )
                live_roots = (
                    engine._storage,
                    engine._indexes,
                    engine._index_data,
                    engine._search_indexes,
                    engine._collections,
                    engine._collection_options,
                )
                prepare_root = engine._prepare_transaction_nested_root
                calls = 0

                def fail_during_preparation(*args, **kwargs):
                    nonlocal calls
                    calls += 1
                    if calls == _SECOND_PREPARED_ROOT:
                        raise RuntimeError(_WRITE_SET_PREPARATION_FAILURE)
                    return prepare_root(*args, **kwargs)

                with (
                    patch.object(
                        engine,
                        "_prepare_transaction_nested_root",
                        side_effect=fail_during_preparation,
                    ),
                    pytest.raises(RuntimeError, match="preparation failed"),
                ):
                    session.commit_transaction()

                current_roots = (
                    engine._storage,
                    engine._indexes,
                    engine._index_data,
                    engine._search_indexes,
                    engine._collections,
                    engine._collection_options,
                )
                assert all(
                    current is original
                    for current, original in zip(
                        current_roots,
                        live_roots,
                        strict=True,
                    )
                )
                assert await collection.find_one({"_id": 1}) == {
                    "_id": 1,
                    "value": "before",
                }
                assert session.session_id in engine._mvcc_states
                session.abort_transaction()
            finally:
                session.close()

    asyncio.run(exercise())


@pytest.mark.parametrize("publication", list(ChangePublicationPolicy))
def test_storage_conflicts_do_not_depend_on_change_publication(publication):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            await client.test.records.insert_one({"_id": "original"})
            session = client.start_session()
            session.start_transaction()
            try:
                context = OperationContext.create(
                    dialect=MONGODB_DIALECT_70,
                    session=session,
                    publication=publication,
                )
                await engine.insert_document(
                    "test",
                    "records",
                    {"_id": "transaction"},
                    overwrite=False,
                    operation_context=context,
                )
                await engine.insert_document(
                    "other",
                    "records",
                    {"_id": "external"},
                    overwrite=False,
                    operation_context=OperationContext.create(
                        dialect=MONGODB_DIALECT_70,
                        publication=publication,
                    ),
                )
                with pytest.raises(OperationFailure, match="Write conflict"):
                    session.commit_transaction()
                session.abort_transaction()
                assert (
                    await client.other.records.find_one({"_id": "external"}) is not None
                )
                assert (
                    await client.test.records.find_one({"_id": "transaction"}) is None
                )
            finally:
                session.close()

    asyncio.run(exercise())
