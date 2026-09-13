"""A read execution retains its preparation instead of repeating it per layer/row."""

import asyncio

from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

import mongoeco.api._async.collection as collection_module

from mongoeco import AsyncMongoClient, MongoClient
from mongoeco.api import operations
from mongoeco.core import projections
from mongoeco.engines import semantic_core
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure


@pytest.fixture(params=["memory", "sqlite", "sqlite-file"])
def engine(request, tmp_path):
    if request.param == "memory":
        return MemoryEngine()
    return SQLiteEngine(
        str(tmp_path / "reads.db") if request.param == "sqlite-file" else ":memory:"
    )


@contextmanager
def preparation_counts():
    with (
        patch.object(
            operations,
            "compile_find_operation",
            wraps=operations.compile_find_operation,
        ) as compile_operation,
        patch.object(collection_module, "compile_find_operation", compile_operation),
        patch.object(
            operations.FindOperation,
            "bind",
            autospec=True,
            side_effect=operations.FindOperation.bind,
        ) as bind,
        patch.object(
            semantic_core,
            "compile_find_semantics_from_operation",
            wraps=semantic_core.compile_find_semantics_from_operation,
        ) as compile_semantics,
        patch.object(
            projections,
            "_parse_projection_spec",
            wraps=projections._parse_projection_spec,
        ) as parse,
    ):
        yield compile_operation, bind, compile_semantics, parse


def documents(count):
    return [
        {"_id": value, "nested": {"value": value, "ignored": "large"}}
        for value in range(count)
    ]


@pytest.mark.parametrize("count", [1, 25, 257])
@pytest.mark.parametrize("ordered", [False, True])
def test_find_prepares_once_and_projection_parse_does_not_scale_with_rows(
    engine, count, ordered
):
    async def exercise():
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(documents(count))
            sort = [("nested.value", -1)] if ordered else None
            expected_order = [
                document["_id"]
                for document in await collection.find({}, sort=sort).to_list()
            ]
            with preparation_counts() as counts:
                cursor = collection.find(
                    {}, {"nested.value": 1}, batch_size=3, sort=sort
                )
                result = await cursor.to_list()
                assert result == [
                    {"_id": value, "nested": {"value": value}}
                    for value in expected_order
                ]
                assert tuple(counter.call_count for counter in counts) == (1, 1, 1, 1)
                context = cursor._as_operation().context
                assert context is cursor._operation_context

    asyncio.run(exercise())


@pytest.mark.parametrize("bounded", [False, True])
def test_sync_consumption_reuses_preparation_across_batches(engine, bounded):
    with MongoClient(engine) as client:
        collection = client.test.records
        collection.insert_many(documents(25))
        with preparation_counts() as counts:
            cursor = collection.find(
                {}, {"nested.value": 1}, batch_size=3, sort=[("_id", 1)]
            )
            if bounded:
                result = cursor.to_list(length=7) + cursor.to_list()
            else:
                result = list(cursor)
            assert result == [
                {"_id": value, "nested": {"value": value}} for value in range(25)
            ]
            assert tuple(counter.call_count for counter in counts) == (1, 1, 1, 1)


@pytest.mark.parametrize("skip", [0, 5])
def test_invalid_projection_is_not_evaluated_without_a_projected_row(engine, skip):
    async def exercise():
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            if skip:
                await collection.insert_many(documents(3))
            with preparation_counts() as counts:
                cursor = collection.find({}, {"nested": 1, "other": 0}, skip=skip)
                assert await cursor.to_list() == []
                assert counts[-1].call_count == 0
            await collection.insert_one({"_id": "visible", "nested": 1})
            with pytest.raises(OperationFailure):
                await collection.find({}, {"nested": 1, "other": 0}).to_list()

    asyncio.run(exercise())


@pytest.mark.parametrize(
    ["method", "argument", "field"],
    [
        ("sort", [("_id", -1)], "sort"),
        ("skip", 2, "skip"),
        ("limit", 2, "limit"),
        ("batch_size", 2, "batch_size"),
        ("hint", "_id_", "hint"),
        ("comment", "prepared", "comment"),
        ("max_time_ms", 60_000, "max_time_ms"),
    ],
)
def test_cursor_mutation_invalidates_only_its_preparation(
    engine, method, argument, field
):
    async def exercise():
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(documents(5))
            cursor = collection.find({}, {"nested.value": 1})
            original = cursor._as_operation()
            original_semantics = cursor._base_semantics()
            getattr(cursor, method)(argument)
            with preparation_counts() as counts:
                operation = cursor._as_operation()
                assert operation is not original
                assert operation.context is original.context
                assert getattr(operation, field) == argument
                assert cursor._base_semantics() is not original_semantics
                result = await cursor.to_list()
                # explain/base-semantics and a physical scan are distinct
                # requests; both must use the same bound operation context.
                assert counts[0].call_count == 1
                assert counts[1].call_count == 1
                assert counts[-1].call_count == 1
                for call in counts[2].call_args_list:
                    assert call.args[0].context is original.context
                assert result

    asyncio.run(exercise())


def test_clone_has_its_own_context_and_rewind_retains_existing_clock(engine):
    async def exercise():
        async with AsyncMongoClient(engine) as client:
            collection = client.test.records
            await collection.insert_many(documents(5))
            cursor = collection.find({}, {"nested.value": 1}, batch_size=2)
            original = cursor._as_operation()
            clone_limit = 2
            clone = cursor.clone().limit(clone_limit)
            cloned = clone._as_operation()
            assert cloned.context is not original.context
            assert cloned.context.operation_id != original.context.operation_id
            assert cloned.context.expressions is not original.context.expressions
            with preparation_counts() as counts:
                result = await cursor.to_list()
                assert len(await clone.to_list()) == clone_limit
                cursor.rewind()
                assert cursor._as_operation() is original
                assert await cursor.to_list() == result
                assert tuple(counter.call_count for counter in counts) == (0, 0, 3, 3)

    asyncio.run(exercise())


def test_bound_execution_freezes_input_bindings_clock_and_session(engine):
    async def exercise():
        initial = datetime(2100, 1, 1, tzinfo=UTC)
        now = [initial]
        async with AsyncMongoClient(engine, now_factory=lambda: now[0]) as client:
            collection = client.test.records
            await collection.insert_many(
                [
                    {"_id": 1, "created": initial - timedelta(days=1)},
                    {"_id": 2, "created": initial + timedelta(days=1)},
                ]
            )
            session = client.start_session()
            other_session = client.start_session()
            try:
                selector = {"$expr": {"$lt": ["$created", "$$NOW"]}}
                projection = {"_id": 1}
                bindings = {"threshold": {"value": 1}}
                cursor = collection.find(
                    selector, projection, let=bindings, session=session
                )
                operation = cursor._as_operation()
                now[0] += timedelta(days=2)
                selector["$expr"]["$lt"][1] = initial - timedelta(days=10)
                projection["created"] = 1
                bindings["threshold"]["value"] = 0
                # A clone starts a new operation context; rewind retains the
                # original cursor's bound clock as required by its contract.
                clone = cursor.clone()
                assert await cursor.to_list() == [{"_id": 1}]
                assert await clone.to_list() == [{"_id": 1}, {"_id": 2}]
                cursor.rewind()
                assert await cursor.to_list() == [{"_id": 1}]
                assert operation.context.session is session
                assert operation.context.expressions.now == initial.replace(tzinfo=None)
                assert operation.context.expressions.bindings == {
                    "threshold": {"value": 1}
                }
                independent = collection.find({}, session=other_session)._as_operation()
                assert independent.context.session is other_session
                assert independent.context is not operation.context
            finally:
                session.close()
                other_session.close()

    asyncio.run(exercise())
