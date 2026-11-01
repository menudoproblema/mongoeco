import asyncio
import time
import unittest


# Literal binding and collation values are part of the assertions.
# ruff: noqa: PLR2004
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime
from unittest.mock import AsyncMock, patch

import pytest

from mongoeco.api import operations as operations_module
from mongoeco.api._async.collection import AsyncCollection
from mongoeco.api.operations import (
    compile_aggregate_operation,
    compile_find_operation,
    compile_update_operation,
)
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.collation import normalize_collation
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
    resolve_operation_session,
)
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.semantic_core import compile_find_semantics
from mongoeco.errors import CollectionInvalid, InvalidOperation, OperationFailure
from mongoeco.session import ClientSession
from mongoeco.types import CodecOptions


class OperationContextTests(unittest.TestCase):
    def test_collection_default_is_resolved_once_without_losing_context(self):
        async def exercise():
            engine = MemoryEngine()
            collection = AsyncCollection(engine, "db", "rows")
            context = collection._new_operation_context(bindings={"tenant": 1})
            with patch.object(
                engine,
                "collection_options",
                AsyncMock(return_value={"collation": {"locale": "en", "strength": 2}}),
            ) as options:
                resolved = await collection._resolve_operation_context(context)
                assert resolved.collation == normalize_collation(
                    {"locale": "en", "strength": 2}
                )
                assert resolved.collation_resolved
                assert resolved.operation_id == context.operation_id
                assert resolved.expressions.now == context.expressions.now
                assert resolved.expressions.bindings == context.expressions.bindings
                assert await collection._resolve_operation_context(resolved) is resolved
                options.assert_awaited_once()
            for error in [
                CollectionInvalid("absent"),
                OperationFailure("absent", code=26),
                NotImplementedError(),
            ]:
                with patch.object(
                    engine, "collection_options", AsyncMock(side_effect=error)
                ):
                    resolved = await collection._resolve_operation_context(context)
                    assert resolved.collation is None
                    assert resolved.collation_resolved
            with patch.object(
                engine,
                "collection_options",
                AsyncMock(side_effect=OperationFailure("failed", code=99)),
            ), pytest.raises(OperationFailure, match="failed"):
                await collection._resolve_operation_context(context)
            with patch.object(engine, "collection_options", None):
                assert (
                    await collection._resolve_operation_context(context)
                ).collation_resolved
            explicit = replace(
                context, collation=normalize_collation({"locale": "simple"})
            )
            assert (
                await collection._resolve_operation_context(explicit)
            ).collation_resolved

        asyncio.run(exercise())

    def test_collation_resolution_preserves_the_simple_context_in_suboperations(self):
        pending = OperationContext.create(dialect=MONGODB_DIALECT_70)
        assert not pending.collation_resolved
        simple = pending.derive(collation=None)
        assert simple.collation is None
        assert simple.collation_resolved
        assert simple.operation_id == pending.operation_id
        assert simple.expressions.now == pending.expressions.now
        assert simple.derive(bindings={"child": 1}).collation_resolved
        assert not pending.derive(bindings={"child": 1}).collation_resolved
        assert OperationContext.create(
            dialect=MONGODB_DIALECT_70, collation={"locale": "simple"}
        ).collation_resolved
        with pytest.raises(TypeError, match="collation_resolved"):
            replace(pending, collation_resolved=1)

    def test_bound_context_is_the_single_session_authority(self):
        bound_session = ClientSession()
        other_session = ClientSession()
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            session=bound_session,
        )

        assert resolve_operation_session(context, None) is bound_session
        assert (
            resolve_operation_session(context, bound_session)
            is bound_session
        )
        with pytest.raises(InvalidOperation, match='session diverges'):
            resolve_operation_session(context, other_session)

    def test_context_copies_bindings_and_is_frozen(self):
        bindings = {'tenant': {'id': 7}}
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            bindings=bindings,
            collation={'locale': 'en', 'strength': 2},
            publication=ChangePublicationPolicy.EMIT,
        )
        bindings['tenant'] = {'id': 9}

        assert context.expressions.bindings['tenant'] == {'id': 7}
        assert context.collation.locale == 'en'
        assert context.collation.strength == 2
        with pytest.raises(FrozenInstanceError):
            context.operation_id = 'changed'

    def test_derived_context_preserves_operation_identity_and_time(self):
        captured_now = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC).replace(
            tzinfo=None,
        )
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            expressions=ExpressionExecutionContext(now=captured_now),
            bindings=None,
        )

        derived = context.derive(bindings={'tenant': 7})

        assert derived.operation_id == context.operation_id
        assert derived.expressions.now == context.expressions.now
        assert derived.expressions.bindings['tenant'] == 7

    def test_derive_assigns_an_event_ordinal_without_changing_identity(self):
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)

        derived = context.derive(change_event_index=3)

        assert derived.operation_id == context.operation_id
        assert context.change_event_index == 0
        assert derived.change_event_index == 3

    def test_derive_does_not_mutate_or_drop_existing_bindings(self):
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            bindings={'tenant': {'id': 7}},
        )

        derived = context.derive(bindings={'request': 'one'})

        assert context.expressions.bindings == {'tenant': {'id': 7}}
        assert derived.expressions.bindings == {
            'tenant': {'id': 7},
            'request': 'one',
        }

    def test_independent_operations_receive_distinct_identities(self):
        first = OperationContext.create(dialect=MONGODB_DIALECT_70)
        second = OperationContext.create(dialect=MONGODB_DIALECT_70)

        assert first.operation_id != second.operation_id

    def test_derive_can_explicitly_clear_optional_values(self):
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            collation={'locale': 'en'},
            change_operation_type='update',
        )

        derived = context.derive(
            collation=None,
            change_operation_type=None,
        )

        assert derived.collation is None
        assert derived.change_operation_type is None

    def test_direct_context_rejects_invalid_publication_and_identity(self):
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)

        with pytest.raises(TypeError, match='dialect'):
            replace(context, dialect=object())
        with pytest.raises(TypeError, match='expressions'):
            replace(context, expressions=object())
        with pytest.raises(TypeError, match='codec_options'):
            replace(context, codec_options=object())
        with pytest.raises(TypeError, match='collation'):
            replace(context, collation=object())
        with pytest.raises(ValueError, match='operation_id'):
            replace(context, operation_id='')
        with pytest.raises(TypeError, match='publication'):
            replace(context, publication='emit')
        with pytest.raises(ValueError, match='change_operation_type'):
            replace(context, change_operation_type='future')
        with pytest.raises(ValueError, match='change_event_index'):
            replace(context, change_event_index=-1)
        with pytest.raises(ValueError, match='change_event_index'):
            replace(context, change_event_index=True)
        with pytest.raises(TypeError, match='session'):
            replace(context, session=object())

        assert isinstance(context.codec_options, CodecOptions)

    def test_unpublishable_change_preserves_sequence_as_a_gap(self):
        emitting = OperationContext.create(
            dialect=MONGODB_DIALECT_70,
            publication=ChangePublicationPolicy.EMIT,
        )
        disabled = OperationContext.create(dialect=MONGODB_DIALECT_70)

        degraded = emitting.for_unpublishable_change()

        assert degraded.operation_id == emitting.operation_id
        assert degraded.publication is ChangePublicationPolicy.RECORD_GAP
        assert disabled.for_unpublishable_change() is disabled

    def test_create_rejects_two_binding_sources(self):
        with pytest.raises(ValueError, match='mutually exclusive'):
            OperationContext.create(
                dialect=MONGODB_DIALECT_70,
                bindings={'tenant': 1},
                expressions=ExpressionExecutionContext(),
            )

    def test_find_deadline_is_compiled_once(self):
        semantics = compile_find_semantics({}, max_time_ms=1_000)
        first = semantics.deadline
        time.sleep(0.001)

        assert first is not None
        assert semantics.deadline == first

    def test_compiled_operations_bind_once_and_idempotently(self):
        context = OperationContext.create(dialect=MONGODB_DIALECT_70)
        operations = (
            compile_find_operation({'tenant': 1}),
            compile_update_operation(
                {'tenant': 1},
                update_spec={'$set': {'value': 2}},
            ),
            compile_aggregate_operation([{'$match': {'tenant': 1}}]),
        )

        for operation in operations:
            with self.subTest(operation=type(operation).__name__):
                bound = operation.bind(context)
                assert bound.bind(context) is bound
                with pytest.raises(RuntimeError, match='another context'):
                    bound.bind(
                        OperationContext.create(
                            dialect=MONGODB_DIALECT_70,
                        ),
                    )

    def test_binding_recompiles_all_semantics_from_context_authority(self):
        context = OperationContext.create(
            dialect=MONGODB_DIALECT_80,
            collation={'locale': 'en', 'strength': 2},
            bindings={'tenant': 'second'},
        )
        operations = (
            compile_find_operation(
                {'tenant': '$$tenant'},
                collation={'locale': 'simple'},
                variables={'tenant': 'first'},
            ),
            compile_update_operation(
                {'tenant': '$$tenant'},
                update_spec={'$set': {'value': '$$tenant'}},
                collation={'locale': 'simple'},
                let={'tenant': 'first'},
            ),
            compile_aggregate_operation(
                [{'$match': {'tenant': '$$tenant'}}],
                collation={'locale': 'simple'},
                let={'tenant': 'first'},
            ),
        )

        for operation in operations:
            with self.subTest(operation=type(operation).__name__):
                bound = operation.bind(context)
                assert bound.context is context
                assert bound.dialect is MONGODB_DIALECT_80
                normalized_collation = normalize_collation(bound.collation)
                assert normalized_collation == context.collation
                assert bound.let == context.expressions

        with pytest.raises(ValueError, match='dialect diverges'):
            operations[0].with_overrides(context=context)

    def test_update_plan_cache_fails_closed_for_unsafe_shapes(self):
        assert operations_module._field_only_update_path(None) is None
        nested_path = operations_module._field_only_update_path(
            'nested..value',
        )
        assert nested_path is None
        assert operations_module._build_update_plan_template_cache_key(
            {},
            dialect=MONGODB_DIALECT_70,
            collation=None,
            array_filters=None,
            variables=None,
            planning_mode=operations_module.PlanningMode.STRICT,
        ) is None
        assert operations_module._build_update_plan_template_cache_key(
            {'$set': {1: 'value'}},
            dialect=MONGODB_DIALECT_70,
            collation=None,
            array_filters=None,
            variables=None,
            planning_mode=operations_module.PlanningMode.STRICT,
        ) is None
        assert operations_module._build_update_plan_template_cache_key(
            {'$set': {'value': 1}},
            dialect=MONGODB_DIALECT_70,
            collation=None,
            array_filters=None,
            variables={'tenant': object()},
            planning_mode=operations_module.PlanningMode.STRICT,
        ) is None

        with pytest.raises(AssertionError, match='classic plans'):
            operations_module._UpdatePlanTemplate.from_plan(
                object(),
                slot_map={},
                is_upsert_insert=False,
            )
        with pytest.raises(OperationFailure):
            compile_find_operation({}, variables={'Tenant': 1})

        operations_module._clear_update_plan_template_cache()
        with patch.object(
            operations_module,
            '_UPDATE_PLAN_TEMPLATE_CACHE_MAX_SIZE',
            0,
        ):
            compile_update_operation(
                {'_id': 1},
                update_spec={'$set': {'value': 1}},
            )
        assert not operations_module._UPDATE_PLAN_TEMPLATE_CACHE


if __name__ == '__main__':
    unittest.main()
