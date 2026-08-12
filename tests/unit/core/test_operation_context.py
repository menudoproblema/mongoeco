import time
import unittest


# Literal binding and collation values are part of the assertions.
# ruff: noqa: PLR2004
from dataclasses import FrozenInstanceError, replace
from datetime import UTC, datetime

import pytest

from mongoeco.compat import MONGODB_DIALECT_70
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import (
    ChangePublicationPolicy,
    OperationContext,
)
from mongoeco.engines.semantic_core import compile_find_semantics


class OperationContextTests(unittest.TestCase):
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

        with pytest.raises(ValueError, match='operation_id'):
            replace(context, operation_id='')
        with pytest.raises(TypeError, match='publication'):
            replace(context, publication='emit')
        with pytest.raises(ValueError, match='change_operation_type'):
            replace(context, change_operation_type='future')

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


if __name__ == '__main__':
    unittest.main()
