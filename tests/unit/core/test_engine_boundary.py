"""Explicit adapter inputs preserve validation, ownership and local callbacks."""

import asyncio
import inspect

from types import SimpleNamespace

import pytest

from mongoeco.api._async._collection_runtime import CollectionRuntimeCoordinator
from mongoeco.api.operations import compile_update_operation
from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines.adapter import EngineSpiAdapter
from mongoeco.engines.semantic_core import compile_find_semantics

from tests.unit.core.test_engine_spi_v2 import _NativeEngine


BOUNDARIES = (
    "update_outcome",
    "delete_outcome",
    "insert_outcome",
    "insert_many_outcomes",
    "merge_outcome",
    "open_read_snapshot",
    "get_document",
    "count_documents",
)


def arguments(name, context):
    if name in {"update_outcome", "delete_outcome"}:
        value = compile_update_operation(
            {"_id": "b"},
            update_spec={"$set": {"n": 1}},
            dialect=context.dialect,
        ).bind(context)
    elif name in {"open_read_snapshot", "count_documents"}:
        value = compile_find_semantics({}, operation_context=context)
    elif name == "insert_many_outcomes":
        value = [{"_id": "b"}]
    elif name == "get_document":
        value = "b"
    else:
        value = {"_id": "b"}
    kwargs = (
        {"when_matched": "merge", "when_not_matched": "insert"}
        if name == "merge_outcome"
        else {}
    )
    return ("db", "items", value), kwargs


@pytest.mark.parametrize("name", BOUNDARIES)
@pytest.mark.parametrize("invalid", ["missing", "none", "type", "unknown"])
def test_invalid_context_or_keyword_never_reaches_engine(name, invalid):
    engine = _NativeEngine()
    adapter = EngineSpiAdapter(engine)
    context = OperationContext.create(dialect=MONGODB_DIALECT_70)
    args, kwargs = arguments(name, context)
    if invalid != "missing":
        kwargs["operation_context"] = {
            "none": None,
            "type": "invalid",
            "unknown": context,
        }[invalid]
    if invalid == "unknown":
        kwargs["unknown"] = True

    async def invoke():
        result = getattr(adapter, name)(*args, **kwargs)
        if inspect.isawaitable(result):
            await result

    with pytest.raises(TypeError):
        asyncio.run(invoke())
    assert engine.calls == []


@pytest.mark.parametrize("container", [list, tuple])
def test_batch_preserves_documents_and_reuses_existing_list(container):
    async def run():
        engine = _NativeEngine()
        adapter = EngineSpiAdapter(engine)
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)
        documents = container([{"_id": "a"}, {"_id": "b"}])
        callbacks = []
        outcomes = await adapter.insert_many_outcomes(
            "db",
            "items",
            documents,
            operation_context=context,
            bypass_document_validation=True,
            on_commit=callbacks.append,
        )
        _, args, kwargs = engine.calls[0]
        assert isinstance(args[2], list)
        assert (args[2] is documents) == (container is list)
        assert all(
            actual is original
            for actual, original in zip(args[2], documents, strict=True)
        )
        assert kwargs == {
            "operation_context": context,
            "bypass_document_validation": True,
        }
        assert callbacks == list(outcomes)

    asyncio.run(run())


def test_coordinator_reuses_validated_adapter():
    adapter = EngineSpiAdapter(_NativeEngine())
    collection = SimpleNamespace(_validated_engine_spi=adapter, _change_hub=None)
    coordinator = CollectionRuntimeCoordinator(collection)
    assert coordinator._engine_spi is adapter
