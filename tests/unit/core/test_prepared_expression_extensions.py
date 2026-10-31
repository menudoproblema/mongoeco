"""Prepared expressions preserve extension ownership and registry changes."""

import json

from pathlib import Path
from unittest.mock import Mock

import pytest

from mongoeco.compat import MONGODB_DIALECT_80
from mongoeco.core.aggregation.extensions import (
    get_registered_aggregation_expression_operator,
    register_aggregation_expression_operator,
    unregister_aggregation_expression_operator,
)
from mongoeco.core.aggregation.preparation import prepare_pipeline
from mongoeco.core.aggregation.stages import apply_pipeline


@pytest.mark.parametrize("operator", ["$getField", "$setField", "$unsetField"])
def test_registered_field_expression_owns_its_opaque_arguments(operator):
    previous = get_registered_aggregation_expression_operator(operator)
    handler = Mock(return_value="extension-result")
    operand = {"opaque": "$$not_a_builtin_binding"}
    register_aggregation_expression_operator(operator, handler)
    try:
        prepared = prepare_pipeline([{"$project": {"v": {operator: operand}}}])
        handler.assert_not_called()
        assert apply_pipeline([{"_id": 1}], prepared) == [
            {"_id": 1, "v": "extension-result"}
        ]
        handler.assert_called_once()
        assert handler.call_args.args[1] == operand
    finally:
        if previous is None:
            unregister_aggregation_expression_operator(operator)
        else:
            register_aggregation_expression_operator(operator, previous)


def test_expression_registry_changes_refresh_native_average_finalization():
    previous = get_registered_aggregation_expression_operator("$not")
    handler = Mock(return_value=False)
    register_aggregation_expression_operator("$not", handler)
    try:
        prepared = prepare_pipeline(
            [
                {"$sort": {"_id": 1}},
                {"$group": {"_id": {"$not": False}, "average": {"$avg": "$x"}}},
            ],
            dialect=MONGODB_DIALECT_80,
        )
        handler.assert_not_called()
        unregister_aggregation_expression_operator("$not")
        # Use the independent captured plain-MongoDB request as the oracle after
        # removing the local override, rather than comparing two local planners.
        capture = json.loads(
            (
                Path(__file__).parents[2]
                / "fixtures"
                / "semantic_expression_grammar_8_0.json"
            ).read_text()
        )
        native = capture["cases"]["semantic_expression_average_contexts"]["$not"][0]
        maximum = 2**63 - 1
        rows = [
            {"_id": i, "x": value} for i, value in enumerate((maximum, 1, -maximum))
        ]
        result = apply_pipeline(rows, prepared, dialect=MONGODB_DIALECT_80)
        assert result == [
            {"_id": True, "average": float(native["average"]["$numberDouble"])}
        ]
    finally:
        if previous is None:
            unregister_aggregation_expression_operator("$not")
        else:
            register_aggregation_expression_operator("$not", previous)
