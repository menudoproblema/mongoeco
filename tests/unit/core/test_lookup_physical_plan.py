from __future__ import annotations

from dataclasses import replace
from unittest.mock import patch

import pytest

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.aggregation.lookup_physical import (
    build_bounded_lookup_hash_plan,
    explain_lookup_physical_plans,
)
from mongoeco.core.aggregation.runtime import _lookup_matches
from mongoeco.core.aggregation.runtime_state import apply_pipeline_states
from mongoeco.core.aggregation.stages import apply_pipeline
from mongoeco.core.collation import CollationSpec
from mongoeco.core.runtime_metadata import RuntimeDocumentState
from mongoeco.errors import ExecutionTimeout
from mongoeco.types import UNDEFINED


_LOOKUP = {
    "$lookup": {
        "from": "foreign",
        "localField": "key",
        "foreignField": "key",
        "as": "matches",
    },
}
_UNIQUE_DOCUMENT_COUNT = 100
_STATE_DOCUMENT_COUNT = 25
_SMALL_ASSOCIATION_BUDGET = 10
_EXPECTED_DEADLINE_CHECKS = 2


def _resolver(foreign):
    return lambda name: foreign if name == "foreign" else None


def test_bounded_hash_reduces_equality_work_to_candidates() -> None:
    local = [{"_id": index, "key": index} for index in range(_UNIQUE_DOCUMENT_COUNT)]
    foreign = [
        {"_id": f"f-{index}", "key": index} for index in range(_UNIQUE_DOCUMENT_COUNT)
    ]

    with patch(
        "mongoeco.core.aggregation.join_stages._lookup_matches",
        wraps=_lookup_matches,
    ) as matches:
        result = apply_pipeline(
            local,
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            lookup_hash_max_associations=1_000,
        )

    assert matches.call_count == _UNIQUE_DOCUMENT_COUNT
    assert [document["matches"][0]["_id"] for document in result] == [
        f"f-{index}" for index in range(_UNIQUE_DOCUMENT_COUNT)
    ]


def test_bounded_hash_preserves_arrays_null_missing_undefined_and_order() -> None:
    local = [
        {"_id": "missing"},
        {"_id": "null", "key": None},
        {"_id": "undefined", "key": UNDEFINED},
        {"_id": "numeric", "key": 1},
        {"_id": "array", "key": [1, 2]},
        {"_id": "document", "key": {"nested": 1}},
    ]
    foreign = [
        {"_id": "missing"},
        {"_id": "null", "key": None},
        {"_id": "undefined", "key": UNDEFINED},
        {"_id": "numeric-float", "key": 1.0},
        {"_id": "array", "key": [1, 2]},
        {"_id": "array-duplicate", "key": [1, 1]},
        {"_id": "document", "key": {"nested": 1}},
    ]

    for dialect in (MONGODB_DIALECT_70, MONGODB_DIALECT_80):
        expected = apply_pipeline(
            local,
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            dialect=dialect,
        )
        actual = apply_pipeline(
            local,
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            dialect=dialect,
            lookup_hash_max_associations=100,
        )

        assert actual == expected
        assert [item["_id"] for item in actual[3]["matches"]] == [
            "numeric-float",
            "array",
            "array-duplicate",
        ]


def test_bounded_hash_falls_back_for_collation() -> None:
    local = [{"_id": "one", "key": "ADA"}, {"_id": "two", "key": "LINUS"}]
    foreign = [{"_id": "ada", "key": "ada"}, {"_id": "linus", "key": "linus"}]

    with patch(
        "mongoeco.core.aggregation.join_stages._lookup_matches",
        wraps=_lookup_matches,
    ) as matches:
        result = apply_pipeline(
            local,
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            collation=CollationSpec(locale="en", strength=2),
            lookup_hash_max_associations=100,
        )

    assert matches.call_count == len(local) * len(foreign)
    assert [[item["_id"] for item in row["matches"]] for row in result] == [
        ["ada"],
        ["linus"],
    ]


def test_bounded_hash_falls_back_when_association_budget_is_exceeded() -> None:
    local = [{"_id": "local", "key": 19}]
    foreign = [{"_id": "foreign", "key": list(range(20))}]

    with patch(
        "mongoeco.core.aggregation.join_stages._lookup_matches",
        wraps=_lookup_matches,
    ) as matches:
        result = apply_pipeline(
            local,
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            lookup_hash_max_associations=5,
        )

    assert matches.call_count == 1
    assert result[0]["matches"] == foreign


def test_bounded_hash_falls_back_for_correlated_pipeline() -> None:
    pipeline_lookup = {
        "$lookup": {
            "from": "foreign",
            "localField": "key",
            "foreignField": "key",
            "let": {"wanted": "$key"},
            "pipeline": [{"$match": {"$expr": {"$eq": ["$key", "$$wanted"]}}}],
            "as": "matches",
        },
    }
    local = [{"_id": "one", "key": 1}, {"_id": "two", "key": 2}]
    foreign = [{"_id": "one", "key": 1}, {"_id": "two", "key": 2}]

    with patch(
        "mongoeco.core.aggregation.join_stages._lookup_matches",
        wraps=_lookup_matches,
    ) as matches:
        result = apply_pipeline(
            local,
            [pipeline_lookup],
            collection_resolver=_resolver(foreign),
            lookup_hash_max_associations=100,
        )

    assert matches.call_count == len(local) * len(foreign)
    assert [[item["_id"] for item in row["matches"]] for row in result] == [
        ["one"],
        ["two"],
    ]


def test_bounded_hash_checks_deadline_while_indexing_foreign_documents() -> None:
    foreign = [{"_id": index, "key": index} for index in range(300)]

    with (
        patch(
            "mongoeco.core.work_control.enforce_deadline",
            side_effect=[None, ExecutionTimeout("operation exceeded time limit")],
        ) as deadline_check,
        pytest.raises(ExecutionTimeout),
    ):
        apply_pipeline(
            [{"_id": "local", "key": 1}],
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            lookup_hash_max_associations=1_000,
            deadline=float("inf"),
        )
    assert deadline_check.call_count == _EXPECTED_DEADLINE_CHECKS


def test_runtime_state_lookup_uses_the_same_bounded_hash_plan() -> None:
    local = [
        RuntimeDocumentState({"_id": index, "key": index})
        for index in range(_STATE_DOCUMENT_COUNT)
    ]
    foreign = [
        {"_id": f"f-{index}", "key": index} for index in range(_STATE_DOCUMENT_COUNT)
    ]

    with patch(
        "mongoeco.core.aggregation.runtime_state._lookup_matches",
        wraps=_lookup_matches,
    ) as matches:
        result = apply_pipeline_states(
            local,
            [_LOOKUP],
            collection_resolver=_resolver(foreign),
            lookup_hash_max_associations=100,
        )

    assert matches.call_count == _STATE_DOCUMENT_COUNT
    assert [state.persistence_document()["matches"][0]["_id"] for state in result] == [
        f"f-{index}" for index in range(_STATE_DOCUMENT_COUNT)
    ]


def test_unsafe_values_remain_residual_and_force_full_local_scan() -> None:
    foreign = [{"_id": "tuple", "key": (1, 2)}]
    plan = build_bounded_lookup_hash_plan(
        foreign,
        "key",
        document_getter=lambda document: document,
        dialect=MONGODB_DIALECT_70,
        collation=None,
        max_associations=_SMALL_ASSOCIATION_BUDGET,
    )

    assert plan is not None
    assert plan.residual_indices == (0,)
    assert plan.candidate_indices([1]) == [0]
    assert plan.candidate_indices([(1, 2)]) is None


def test_lookup_explain_covers_nested_fallback_reasons() -> None:
    simple = _LOOKUP["$lookup"]
    pipeline = [
        {},
        {"$lookup": {**simple, "pipeline": []}},
        {"$facet": {"branch": [{"$lookup": simple}]}},
        {"$unionWith": {"coll": "archive", "pipeline": [{"$lookup": simple}]}},
    ]

    plans = explain_lookup_physical_plans(
        pipeline,
        dialect=MONGODB_DIALECT_70,
        collation=None,
        max_associations=None,
    )

    assert [plan["reason"] for plan in plans] == [
        "pipeline-form",
        "no-association-budget",
        "no-association-budget",
    ]
    assert plans[1]["stagePath"] == [2, "$facet", "branch", 0]
    assert plans[2]["stagePath"] == [3, "$unionWith.pipeline", 0]

    custom = replace(MONGODB_DIALECT_70, label="custom")
    custom_plan = explain_lookup_physical_plans(
        [{"$lookup": simple}],
        dialect=custom,
        collation=None,
        max_associations=_SMALL_ASSOCIATION_BUDGET,
    )
    assert custom_plan[0]["reason"] == "custom-dialect"

    collated_plan = explain_lookup_physical_plans(
        [{"$lookup": simple}],
        dialect=MONGODB_DIALECT_70,
        collation=CollationSpec(locale="en"),
        max_associations=_SMALL_ASSOCIATION_BUDGET,
    )
    assert collated_plan[0]["reason"] == "collation"

    eligible_plan = explain_lookup_physical_plans(
        [{"$lookup": simple}],
        dialect=MONGODB_DIALECT_70,
        collation=None,
        max_associations=_SMALL_ASSOCIATION_BUDGET,
    )
    assert eligible_plan[0]["strategy"] == "bounded-hash-candidate"
    assert eligible_plan[0]["maxAssociations"] == _SMALL_ASSOCIATION_BUDGET
