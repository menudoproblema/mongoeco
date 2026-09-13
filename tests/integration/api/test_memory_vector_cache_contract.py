"""Vector cache capacity must not weaken exact results or view isolation."""

import asyncio
import gc

import pytest

from mongoeco import AsyncMongoClient, SearchIndexModel
from mongoeco.engines._vector_index_cache import VectorIndexCache
from mongoeco.engines._vector_query_cache import VectorQueryCacheBudget
from mongoeco.engines.memory import MemoryEngine


_INDEX_CACHE_CAPACITY = 20
_INDEX_CACHE_ENTRY_BYTES = 10
_INDEX_CACHE_ENTRY_COUNT = _INDEX_CACHE_CAPACITY // _INDEX_CACHE_ENTRY_BYTES


def vector_index_model():
    return SearchIndexModel(
        {
            "fields": [
                {
                    "type": "vector",
                    "path": "embedding",
                    "numDimensions": 2,
                    "similarity": "dotProduct",
                }
            ]
        },
        name="vectors",
        type="vectorSearch",
    )


async def query(collection, vector, *, filter_spec=None, session=None):
    spec = {
        "index": "vectors",
        "path": "embedding",
        "queryVector": vector,
        "numCandidates": 10,
        "limit": 2,
    }
    if filter_spec is not None:
        spec["filter"] = filter_spec
    return await collection.aggregate(
        [{"$vectorSearch": spec}], session=session
    ).to_list()


def test_warm_vector_query_does_not_decode_collection_again(monkeypatch):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            await collection.insert_many(
                [
                    {"_id": index, "embedding": [1.0, index / 100], "payload": index}
                    for index in range(100)
                ]
            )
            await collection.create_search_index(vector_index_model())
            expected = await query(collection, [1.0, 1.0])
            decoded = []
            decode = engine._decode_storage_document

            def observe(*args, **kwargs):
                decoded.append(1)
                return decode(*args, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(engine, "_decode_storage_document", observe)
                assert await query(collection, [1.0, 1.0]) == expected
            assert not decoded

    asyncio.run(exercise())


def test_unique_vector_queries_do_not_keep_every_complete_ranking():
    async def exercise():
        engine = MemoryEngine()
        # The representation may become more compact. Drive eviction by its
        # byte budget, not by an arbitrary number of query entries that fit.
        engine._vector_query_cache_budget = VectorQueryCacheBudget(
            256 * 1024, 64 * 1024
        )
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            size = 1000
            queries = 100
            await collection.insert_many(
                [
                    {"_id": index, "embedding": [1.0, index / size]}
                    for index in range(size)
                ]
            )
            await collection.create_search_index(vector_index_model())
            for number in range(queries):
                result = await query(collection, [1.0, (number + 1) / queries])
                assert [document["_id"] for document in result] == [size - 1, size - 2]
            state = engine._vector_document_cache[("db", "records", "vectors")][1]
            assert (
                sum(len(rows) for rows in state.vector_score_cache.values())
                < size * queries
            )
            stats = engine._vector_query_cache_budget.stats()
            assert stats["estimatedBytes"] <= stats["capacityBytes"]
            assert stats["evictions"] > 0

    asyncio.run(exercise())


def test_materialized_vector_indexes_share_a_bounded_engine_lru():
    async def exercise():
        engine = MemoryEngine()
        engine._vector_document_cache = VectorIndexCache(
            capacity_bytes=_INDEX_CACHE_CAPACITY,
            entry_bytes=_INDEX_CACHE_ENTRY_BYTES,
            size_of=lambda _value, _limit: _INDEX_CACHE_ENTRY_BYTES,
        )
        async with AsyncMongoClient(engine) as client:
            collections = [client.db[f"records_{index}"] for index in range(3)]
            for index, collection in enumerate(collections):
                await collection.insert_one(
                    {"_id": index, "embedding": [1.0, float(index)]}
                )
                await collection.create_search_index(vector_index_model())
                assert [row["_id"] for row in await query(collection, [1.0, 1.0])] == [
                    index
                ]

            stats = engine._vector_document_cache.stats()
            assert stats["entries"] == _INDEX_CACHE_ENTRY_COUNT
            assert stats["estimatedBytes"] <= stats["capacityBytes"]
            assert stats["evictions"] == 1
            assert [row["_id"] for row in await query(collections[0], [1.0, 1.0])] == [
                0
            ]
            assert (
                engine._vector_document_cache.stats()["evictions"]
                == _INDEX_CACHE_ENTRY_COUNT
            )

    asyncio.run(exercise())


def test_all_vector_indexes_and_query_cache_kinds_share_one_budget():
    async def exercise():
        engine = MemoryEngine()
        budget = VectorQueryCacheBudget(64 * 1024, 32 * 1024)
        engine._vector_query_cache_budget = budget
        async with AsyncMongoClient(engine) as client:
            collections = [client.db[f"records_{number}"] for number in range(3)]
            for collection in collections:
                await collection.insert_many(
                    [
                        {
                            "_id": index,
                            "embedding": [1.0, index / 100],
                            "group": index % 2,
                        }
                        for index in range(100)
                    ]
                )
                await collection.create_search_index(vector_index_model())
            for number in range(50):
                for collection in collections:
                    result = await query(
                        collection,
                        [1.0, (number + 1) / 50],
                        filter_spec={"_id": {"$gte": number}, "group": 1},
                    )
                    assert [row["_id"] for row in result] == [99, 97]
                    assert budget.stats()["estimatedBytes"] <= budget.capacity_bytes
            assert budget.stats()["evictions"] > 0
            for _definition, index in engine._vector_document_cache.values():
                assert index.vector_score_cache._budget is budget
                assert index.vector_row_filter_cache._budget is budget
                assert index.vector_ranked_row_cache._budget is budget

    asyncio.run(exercise())


@pytest.mark.parametrize("commit", [False, True])
def test_transaction_vector_views_do_not_contaminate_cached_global_scores(commit):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            await collection.insert_many(
                [
                    {"_id": index, "embedding": [1.0, float(index)]}
                    for index in range(10)
                ]
            )
            await collection.create_search_index(vector_index_model())
            before = await query(collection, [0.0, 1.0])
            assert [row["_id"] for row in before] == [9, 8]
            session = client.start_session()
            try:
                session.start_transaction()
                await collection.update_one(
                    {"_id": 0}, {"$set": {"embedding": [1.0, 100.0]}}, session=session
                )
                transactional = await query(collection, [0.0, 1.0], session=session)
                assert [row["_id"] for row in transactional] == [0, 9]
                assert await query(collection, [0.0, 1.0]) == before
                if commit:
                    session.commit_transaction()
                else:
                    session.abort_transaction()
                after = await query(collection, [0.0, 1.0])
                assert after == (transactional if commit else before)
            finally:
                session.close()

    asyncio.run(exercise())


def test_payload_filter_and_index_recreation_use_current_documents():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            await collection.insert_many(
                [
                    {"_id": index, "embedding": [1.0, float(index)], "active": True}
                    for index in range(10)
                ]
            )
            await collection.create_search_index(vector_index_model())
            await query(collection, [0.0, 1.0], filter_spec={"active": True})
            await collection.update_one({"_id": 9}, {"$set": {"payload": "updated"}})
            assert (await query(collection, [0.0, 1.0]))[0]["payload"] == "updated"
            await collection.update_one({"_id": 9}, {"$set": {"active": False}})
            filtered = await query(collection, [0.0, 1.0], filter_spec={"active": True})
            assert [row["_id"] for row in filtered] == [8, 7]
            await collection.delete_one({"_id": 8})
            await collection.insert_one(
                {"_id": 20, "embedding": [1.0, 200.0], "active": True}
            )
            filtered = await query(collection, [0.0, 1.0], filter_spec={"active": True})
            assert [row["_id"] for row in filtered] == [20, 7]
            await collection.drop()
            await collection.insert_one({"_id": "new", "embedding": [1.0, 0.0]})
            await collection.create_search_index(vector_index_model())
            assert [row["_id"] for row in await query(collection, [0.0, 1.0])] == [
                "new"
            ]

    asyncio.run(exercise())


def test_old_pinned_index_remains_accounted_until_its_last_owner_releases_it():
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            collection = client.db.records
            await collection.insert_one({"_id": 0, "embedding": [1.0, 1.0]})
            await collection.create_search_index(vector_index_model())
            await query(collection, [1.0, 1.0])
            old_index = engine._vector_document_cache[("db", "records", "vectors")][1]
            budget = engine._vector_query_cache_budget
            await collection.drop()
            assert budget.stats()["estimatedBytes"] > 0
            del old_index
            gc.collect()
            assert budget.stats()["estimatedBytes"] == 0

    asyncio.run(exercise())


def test_transaction_commit_invalidates_only_touched_vector_namespaces(monkeypatch):
    async def exercise():
        engine = MemoryEngine()
        async with AsyncMongoClient(engine) as client:
            changed = client.db.changed
            stable = client.db.stable
            for collection, prefix in ((changed, "changed"), (stable, "stable")):
                await collection.insert_one(
                    {
                        "_id": prefix,
                        "embedding": [1.0, 1.0],
                        "payload": "before",
                    }
                )
                await collection.create_search_index(vector_index_model())
                await query(collection, [1.0, 1.0])

            stable_index = engine._vector_document_cache[("db", "stable", "vectors")][1]
            session = client.start_session()
            session.start_transaction()
            try:
                await changed.update_one(
                    {"_id": "changed"},
                    {"$set": {"payload": "after"}},
                    session=session,
                )
                session.commit_transaction()
            finally:
                session.close()

            assert (
                engine._vector_document_cache[("db", "stable", "vectors")][1]
                is stable_index
            )
            assert ("db", "changed", "vectors") not in engine._vector_document_cache
            decoded = []
            decode = engine._decode_storage_document

            def observe(*args, **kwargs):
                decoded.append(1)
                return decode(*args, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(engine, "_decode_storage_document", observe)
                assert [row["_id"] for row in await query(stable, [1.0, 1.0])] == [
                    "stable"
                ]
            assert not decoded
            assert await query(changed, [1.0, 1.0]) == [
                {
                    "_id": "changed",
                    "embedding": [1.0, 1.0],
                    "payload": "after",
                }
            ]

    asyncio.run(exercise())
