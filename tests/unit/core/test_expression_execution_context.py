import copy

from datetime import datetime
from unittest.mock import patch

import pytest

import mongoeco.core.aggregation.compiled_pipeline as compiled_pipeline_module
import mongoeco.core.aggregation.stages as aggregation_stages_module
import mongoeco.core.filtering as filtering_module
import mongoeco.core.operators as operators_module

from mongoeco.core.aggregation import apply_pipeline, compile_pipeline
from mongoeco.core.expression_context import (
    ExpressionExecutionContext,
    ensure_expression_context,
)
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.operators import UpdateEngine
from mongoeco.core.query_plan import compile_filter


def test_expression_execution_context_is_an_immutable_mapping_with_a_stable_now():
    now = datetime(2026, 7, 26, 12, 0, 0, 123000)
    context = ExpressionExecutionContext({'limit': 3}, now=now)

    assert context.get('limit') == 3
    assert dict(context) == {'limit': 3, 'NOW': now}
    assert context.with_bindings({'tenant': 'a'}).get('NOW') is now
    assert ensure_expression_context(dict(context)).get('NOW') is now
    with pytest.raises(TypeError):
        context.bindings['limit'] = 4


def test_expression_execution_context_is_deeply_immutable():
    source = {'scope': {'values': [1, {'enabled': True}]}}
    context = ExpressionExecutionContext(source)

    source['scope']['values'].append(2)

    assert context.bindings['scope'] == {
        'values': [1, {'enabled': True}],
    }
    with pytest.raises(TypeError, match='immutable'):
        context.bindings['scope']['new'] = True
    with pytest.raises(TypeError, match='immutable'):
        context.bindings['scope']['values'].append(3)
    with pytest.raises(TypeError, match='immutable'):
        context.bindings['scope']['values'][1]['enabled'] = False
    with pytest.raises(TypeError, match='immutable'):
        context.bindings['scope'] |= {'new': True}
    frozen_scope = context.bindings['scope']
    assert copy.deepcopy(frozen_scope) is frozen_scope
    assert (
        copy.deepcopy(context.bindings['scope']['values'])
        is context.bindings['scope']['values']
    )


def test_core_execution_boundaries_capture_one_context_and_propagate_it():
    context = ExpressionExecutionContext(
        {},
        now=datetime(2026, 7, 26, 12, 0, 0, 123000),
    )
    expr_plan = compile_filter(
        {
            "$and": [
                {"$expr": {"$eq": ["$$NOW", "$$NOW"]}},
                {"$expr": {"$ne": ["$$NOW", None]}},
            ]
        }
    )

    with patch.object(filtering_module, "ensure_expression_context", return_value=context) as ensure:
        assert QueryEngine.match_plan({}, expr_plan)
    assert ensure.call_args_list[0].args == (None,)
    assert all(call.args == (context,) for call in ensure.call_args_list[1:])

    with patch.object(
        aggregation_stages_module,
        "ensure_expression_context",
        return_value=context,
    ) as ensure:
        transformed = apply_pipeline(
            [{"_id": 1}, {"_id": 2}],
            [{"$set": {"at": "$$NOW"}}],
        )
    ensure.assert_called_once_with(None)
    assert [document["at"] for document in transformed] == [context.now, context.now]

    compiled = compile_pipeline([{"$set": {"at": "$$NOW"}}])
    assert compiled is not None
    with patch.object(
        compiled_pipeline_module,
        "ensure_expression_context",
        return_value=context,
    ) as ensure:
        transformed = compiled.execute([{"_id": 1}, {"_id": 2}])
    ensure.assert_called_once_with(None)
    assert [document["at"] for document in transformed] == [context.now, context.now]

    document = {"_id": 1}
    with patch.object(operators_module, "ensure_expression_context", return_value=context) as ensure:
        UpdateEngine.apply_update(document, [{"$set": {"first": "$$NOW"}}, {"$set": {"second": "$$NOW"}}])
    ensure.assert_called_once_with(None)
    assert document["first"] == document["second"] == context.now


def test_nested_expression_scopes_preserve_root_and_current_explicitly():
    [result] = apply_pipeline(
        [{'_id': 'root', 'values': [1, 2]}],
        [
            {
                '$set': {
                    'mapped': {
                        '$map': {
                            'input': '$values',
                            'as': 'item',
                            'in': {
                                'root': '$$ROOT._id',
                                'current': '$$CURRENT._id',
                                'item': '$$item',
                            },
                        },
                    },
                },
            },
        ],
    )

    assert result['mapped'] == [
        {'root': 'root', 'current': 'root', 'item': 1},
        {'root': 'root', 'current': 'root', 'item': 2},
    ]
