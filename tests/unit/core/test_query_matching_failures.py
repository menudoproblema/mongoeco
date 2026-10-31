"""A matcher failure must remain a failure through generated composition."""

from unittest.mock import patch

import pytest

from mongoeco.core.compiled_query import CompiledQuery
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import compile_filter


@pytest.mark.parametrize("error", [KeyError, TypeError, AttributeError])
def test_leaf_failures_propagate_from_both_matchers(error):
    plan = compile_filter({"o.a": {"$gt": 5}})
    compiled = CompiledQuery(plan)
    with patch.object(
        QueryEngine, "_evaluate_comparison", side_effect=error("injected")
    ):
        for match in (lambda doc: QueryEngine.match_plan(doc, plan), compiled.match):
            with pytest.raises(error, match="injected"):
                match({"o": {"a": 6}})


def test_template_cache_does_not_retain_a_temporary_matcher():
    CompiledQuery._compile_expression.cache_clear()
    try:
        with patch.object(QueryEngine, "match_plan", return_value=True):
            first = CompiledQuery(compile_filter({"a": 5}))
            assert first.match({"a": 6})
        # Compiling under temporary instrumentation must not pin that handler
        # into a shared template after the owner has been restored.
        second = CompiledQuery(compile_filter({"a": 7}))
        assert not first.match({"a": 6})
        assert not second.match({"a": 6})
    finally:
        CompiledQuery._compile_expression.cache_clear()
