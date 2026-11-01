"""Candidate keys have an explicit, conservative semantic-context domain."""

from dataclasses import replace

import pytest

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search_filter_prefilter import supports_mql_candidate_prefilter


@pytest.mark.parametrize(
    "dialect", [MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90]
)
def test_candidate_context_keeps_builtins_and_declines_unproven_clones(dialect):
    assert supports_mql_candidate_prefilter(OperationContext.create(dialect=dialect))
    clone = replace(dialect, label="unproven")
    assert not supports_mql_candidate_prefilter(OperationContext.create(dialect=clone))
    assert not supports_mql_candidate_prefilter(
        OperationContext.create(
            dialect=dialect, collation={"locale": "en", "strength": 2}
        )
    )


def test_legacy_candidate_context_retains_builtin_seven_semantics():
    assert supports_mql_candidate_prefilter(None)
