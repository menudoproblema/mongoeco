"""Preparation preserves SPI v2 context, input ownership and cursor lifetimes."""

from dataclasses import replace
from unittest.mock import AsyncMock, patch

import pytest

from mongoeco import AsyncMongoClient
from mongoeco.api.operations import (
    compile_aggregate_operation,
    compile_find_operation,
    compile_find_selection_from_update_operation,
    compile_update_operation,
)
from mongoeco.compat import (
    MONGODB_DIALECT_70,
    MONGODB_DIALECT_80,
    MONGODB_DIALECT_90,
)
from mongoeco.core.operation_context import OperationContext
from mongoeco.engines import MemoryEngine, semantic_core
from mongoeco.engines.adapter import EngineSpiAdapter
from mongoeco.engines.semantic_core import (
    compile_find_semantics_from_operation,
    compile_update_semantics,
)
from mongoeco.errors import InvalidOperation
from mongoeco.session import ClientSession

from tests.consumer.engine_canary import ExternalCanaryEngine


class _FalseBindings(dict):
    def __bool__(self):
        return False


@pytest.mark.parametrize("kind", ["find", "update", "aggregate"])
@pytest.mark.parametrize("valid", [False, True])
def test_bound_context_guard_preserves_nonempty_false_bindings(kind, valid):
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_70, bindings={"tenant": "A"} if valid else {}
    )
    if kind == "find":
        operation = compile_find_operation({})
    elif kind == "update":
        operation = compile_update_operation({}, update_spec={"$set": {"x": 1}})
    else:
        operation = compile_aggregate_operation([])
    values = _FalseBindings(tenant="A")
    bound = operation.bind(context)
    if valid:
        assert replace(bound, let=values).context is context
    else:
        with pytest.raises(ValueError, match="variables diverge"):
            replace(bound, let=values)


@pytest.mark.parametrize(
    "dialect", [MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90]
)
@pytest.mark.parametrize("kind", ["find", "update"])
def test_unbound_conversion_preserves_operation_dialect_collation_and_variables(
    dialect, kind
):
    options = {"dialect": dialect, "collation": {"locale": "en", "strength": 2}}
    if kind == "find":
        operation = compile_find_operation({}, variables={"tenant": "A"}, **options)
        compile_semantics = compile_find_semantics_from_operation
    else:
        operation = compile_update_operation(
            {}, update_spec={"$set": {"x": 1}}, let={"tenant": "A"}, **options
        )
        compile_semantics = compile_update_semantics
    semantics = compile_semantics(operation)
    assert semantics.dialect is dialect
    assert semantics.collation.locale == "en"
    assert semantics.variables.bindings == {"tenant": "A"}
    assert (
        compile_semantics(operation, dialect=MONGODB_DIALECT_70).dialect
        is MONGODB_DIALECT_70
    )


def test_explicit_empty_update_bindings_apply_to_the_selector_too():
    operation = compile_update_operation(
        {}, update_spec={"$set": {"x": 1}}, let={"tenant": 1}
    )
    with patch.object(
        semantic_core, "ensure_query_plan", wraps=semantic_core.ensure_query_plan
    ) as spy:
        semantics = compile_update_semantics(
            operation, selector_filter={"x": 1}, variables={}
        )
    assert semantics.variables.bindings == {}
    assert spy.call_args_list[-1].kwargs["variables"] == {}


@pytest.mark.parametrize("kind", ["find", "update", "aggregate"])
def test_prepared_inputs_are_owned_and_binding_keeps_context_authority(kind):
    values = [1, {"nested": 2}]
    specification = {"x": {"$in": values}}
    update = {"$set": {"payload": {"values": [1]}}}
    if kind == "find":
        operation = compile_find_operation(specification)
    elif kind == "update":
        operation = compile_update_operation(specification, update_spec=update)
    else:
        operation = compile_aggregate_operation([{"$match": specification}])
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_80, bindings={"tenant": "A"}
    )
    bound = operation.bind(context)
    values.clear()
    update["$set"]["payload"]["values"].append(2)
    assert bound.context is context
    assert bound.bind(context) is bound
    assert bound.let is context.expressions
    if kind == "aggregate":
        assert bound.pipeline[0]["$match"]["x"]["$in"] == [1, {"nested": 2}]
    else:
        assert bound.filter_spec["x"]["$in"] == [1, {"nested": 2}]
        if kind == "update":
            assert bound.update_spec["$set"]["payload"]["values"] == [1]
    with pytest.raises(ValueError, match="dialect diverges"):
        replace(bound, dialect=MONGODB_DIALECT_70)


def test_selection_from_update_reuses_the_corresponding_plan_and_context():
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_80, bindings={"tenant": 7}
    )
    operation = compile_update_operation(
        {"x": {"$gte": 1}}, update_spec={"$set": {"x": 2}}, dialect=MONGODB_DIALECT_80
    ).bind(context)
    selection = compile_find_selection_from_update_operation(operation, limit=1)
    assert selection.plan is operation.plan
    assert selection.context is context
    assert selection.filter_spec == operation.filter_spec
    assert selection.limit == 1


@pytest.mark.asyncio
async def test_cursor_execution_and_rewind_capture_deadlines_lazily():
    async with AsyncMongoClient(MemoryEngine()) as client:
        collection = client.preparation.items
        await collection.insert_many([{"_id": 1}, {"_id": 2}])
        with patch.object(
            semantic_core, "operation_deadline", return_value=float("inf")
        ) as clock:
            cursor = collection.find({}).max_time_ms(1000).batch_size(1)
            context = cursor._operation_context
            assert clock.call_count == 0
            assert await cursor.to_list() == [{"_id": 1}, {"_id": 2}]
            assert clock.call_count == 1
            cursor.rewind()
            assert cursor._operation_context is context
            assert await cursor.to_list() == [{"_id": 1}, {"_id": 2}]
            expected_executions = 2
            assert clock.call_count == expected_executions
            clone = cursor.clone()
            assert clone._operation_context is not context
            await clone.close()
            await cursor.close()


@pytest.mark.parametrize(
    "method_name",
    ["plan_find_execution", "plan_find_semantics", "explain_find_semantics"],
)
@pytest.mark.parametrize("bound", [False, True])
@pytest.mark.asyncio
async def test_planning_boundary_preserves_payload_result_and_session(
    method_name, bound
):
    session = ClientSession()
    context = OperationContext.create(dialect=MONGODB_DIALECT_80, session=session)
    operation = compile_find_operation({}, dialect=MONGODB_DIALECT_80)
    if bound:
        operation = operation.bind(context)
    payload = (
        operation
        if method_name == "plan_find_execution"
        else compile_find_semantics_from_operation(operation)
    )
    engine = MemoryEngine()
    result = object()
    method = AsyncMock(return_value=result)
    with patch.object(engine, method_name, method):
        adapter = EngineSpiAdapter(engine)
        options = (
            {"dialect": MONGODB_DIALECT_80}
            if method_name == "plan_find_execution"
            else {}
        )
        assert (
            await getattr(adapter, method_name)(
                "db", "items", payload, context=None if bound else session, **options
            )
            is result
        )
    method.assert_awaited_once_with("db", "items", payload, context=session, **options)


@pytest.mark.parametrize(
    "method_name",
    ["plan_find_execution", "plan_find_semantics", "explain_find_semantics"],
)
@pytest.mark.asyncio
async def test_planning_rejects_a_session_that_diverges_from_bound_context(method_name):
    context = OperationContext.create(
        dialect=MONGODB_DIALECT_80, session=ClientSession()
    )
    operation = compile_find_operation({}, dialect=MONGODB_DIALECT_80).bind(context)
    payload = (
        operation
        if method_name == "plan_find_execution"
        else compile_find_semantics_from_operation(operation)
    )
    engine = MemoryEngine()
    with (
        patch.object(engine, method_name, AsyncMock()) as method,
        pytest.raises(InvalidOperation, match="session diverges"),
    ):
        await getattr(EngineSpiAdapter(engine), method_name)(
            "db", "items", payload, context=ClientSession()
        )
    method.assert_not_awaited()


@pytest.mark.asyncio
async def test_minimal_engine_stays_valid_without_optional_planning():
    adapter = EngineSpiAdapter(ExternalCanaryEngine())
    with pytest.raises(AttributeError, match="plan_find_execution"):
        await adapter.plan_find_execution("db", "items", compile_find_operation({}))
