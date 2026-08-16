from __future__ import annotations

import pytest

from mongoeco.core.search_planning import (
    CardinalityEffect,
    OrderEffect,
    SearchPhaseOwner,
    SearchPhaseOwnership,
    SearchPipelinePlan,
    SearchPipelineStrategy,
    SearchPlanningMode,
    SearchPlanPhase,
    SearchPlanRejection,
    SearchPlanRule,
    SearchStageDomain,
    SearchStageEffect,
    SearchWindow,
    compile_search_pipeline_plan,
    search_stage_effect,
)


def _search_spec(**extra: object) -> dict[str, object]:
    return {
        "index": "by_text",
        "text": {"query": "ada", "path": "title"},
        **extra,
    }


@pytest.mark.parametrize(
    ["stage", "domain", "expected"],
    [
        (
            {"$project": {"_id": 1}},
            SearchStageDomain.DOCUMENT,
            (CardinalityEffect.PRESERVE, OrderEffect.PRESERVE, True, True),
        ),
        (
            {"$match": {"kind": "note"}},
            SearchStageDomain.DOCUMENT,
            (CardinalityEffect.FILTER, OrderEffect.PRESERVE, True, False),
        ),
        (
            {"$limit": 2},
            SearchStageDomain.STREAM,
            (CardinalityEffect.REDUCE, OrderEffect.PRESERVE, True, True),
        ),
        (
            {"$unwind": "$items"},
            SearchStageDomain.DOCUMENT,
            (CardinalityEffect.EXPAND, OrderEffect.PRESERVE, False, False),
        ),
        (
            {"$group": {"_id": "$kind"}},
            SearchStageDomain.FULL_SET,
            (CardinalityEffect.UNKNOWN, OrderEffect.UNKNOWN, False, False),
        ),
    ],
)
def test_stage_effects_are_explicit(
    stage: dict[str, object],
    domain: SearchStageDomain,
    expected: tuple[CardinalityEffect, OrderEffect, bool, bool],
) -> None:
    effect = search_stage_effect(stage)
    assert (
        effect.domain,
        effect.cardinality,
        effect.order,
        effect.monotonic,
        effect.topk_safe,
    ) == (domain, *expected)


def test_direct_window_plan_carries_rule_effects_and_ownership() -> None:
    skip = 2
    limit = 3
    plan = compile_search_pipeline_plan(
        "$search",
        _search_spec(),
        [{"$project": {"_id": 1}}, {"$skip": skip}, {"$limit": limit}],
    )

    assert plan.strategy is SearchPipelineStrategy.DIRECT_WINDOW
    assert plan.result_limit_hint == skip + limit
    assert plan.window is not None
    assert plan.window.skip == skip
    assert plan.applied_rules == (SearchPlanRule.DIRECT_WINDOW,)
    assert [effect.operator for effect in plan.effects] == [
        "$project",
        "$skip",
        "$limit",
    ]
    assert plan.ownership[0].owner.value == "engine"


def test_prefix_plan_pushes_only_leading_expression_free_match() -> None:
    plan = compile_search_pipeline_plan(
        "$search",
        _search_spec(),
        [{"$match": {"kind": "note"}}, {"$limit": 2}],
    )
    expression_plan = compile_search_pipeline_plan(
        "$search",
        _search_spec(),
        [{"$match": {"$expr": {"$eq": ["$kind", "$$kind"]}}}, {"$limit": 2}],
    )

    assert plan.strategy is SearchPipelineStrategy.PREFIX_ITERATIVE
    assert plan.downstream_filter_spec == {"kind": "note"}
    assert SearchPlanRule.DOWNSTREAM_FILTER in plan.applied_rules
    assert expression_plan.strategy is SearchPipelineStrategy.PREFIX_ITERATIVE
    assert expression_plan.downstream_filter_spec is None


def test_vector_match_remains_post_topk() -> None:
    plan = compile_search_pipeline_plan(
        "$vectorSearch",
        {"index": "vector", "path": "embedding", "queryVector": [1.0], "limit": 2},
        [{"$match": {"keep": True}}, {"$limit": 1}],
    )

    assert plan.strategy is SearchPipelineStrategy.PREFIX_ITERATIVE
    assert plan.downstream_filter_spec is None
    assert SearchPlanRejection.VECTOR_POST_TOPK in plan.rejection_reasons


@pytest.mark.parametrize(
    ["case", "rejection"],
    [
        (
            ("$searchMeta", _search_spec(count={"type": "total"}), False),
            SearchPlanRejection.METADATA_MODE,
        ),
        (
            ("$search", _search_spec(facet={"path": "kind"}), False),
            SearchPlanRejection.COLLECTOR_FULL_SET,
        ),
        (
            ("$search", _search_spec(), True),
            SearchPlanRejection.WRITEBACK,
        ),
    ],
)
def test_full_execution_rejections_are_stable(
    case: tuple[str, dict[str, object], bool],
    rejection: SearchPlanRejection,
) -> None:
    operator, specification, writeback = case
    plan = compile_search_pipeline_plan(
        operator,
        specification,
        [{"$limit": 1}],
        writeback=writeback,
    )
    assert plan.strategy is SearchPipelineStrategy.FULL
    assert rejection in plan.rejection_reasons


def test_reference_mode_disables_every_shortcut() -> None:
    plan = compile_search_pipeline_plan(
        "$search",
        _search_spec(),
        [{"$match": {"kind": "note"}}, {"$limit": 1}],
        mode=SearchPlanningMode.REFERENCE,
    )

    assert plan.strategy is SearchPipelineStrategy.FULL
    assert plan.result_limit_hint is None
    assert plan.downstream_filter_spec is None
    assert plan.applied_rules == (SearchPlanRule.REFERENCE_ORACLE,)
    assert plan.rejection_reasons == (SearchPlanRejection.REFERENCE_MODE,)


def test_plan_rejects_contradictory_reference_state() -> None:
    with pytest.raises(ValueError, match="reference Search plan"):
        SearchPipelinePlan(
            strategy=SearchPipelineStrategy.DIRECT_WINDOW,
            mode=SearchPlanningMode.REFERENCE,
            result_limit_hint=1,
        )


def test_plan_value_objects_reject_invalid_windows_and_duplicate_decisions() -> None:
    with pytest.raises(ValueError, match="non-negative"):
        SearchWindow(skip=-1)
    with pytest.raises(ValueError, match="rules must be unique"):
        SearchPipelinePlan(
            applied_rules=(
                SearchPlanRule.FULL_EXECUTION,
                SearchPlanRule.FULL_EXECUTION,
            ),
        )
    with pytest.raises(ValueError, match="rejections must be unique"):
        SearchPipelinePlan(
            rejection_reasons=(
                SearchPlanRejection.UNSAFE_STAGE,
                SearchPlanRejection.UNSAFE_STAGE,
            ),
        )


def test_merge_effect_and_non_trailing_zero_limit_are_explicit() -> None:
    effect = search_stage_effect({"$merge": {"into": "target"}})
    plan = compile_search_pipeline_plan(
        "$search",
        _search_spec(),
        [
            {"$match": {"kind": "note"}},
            {"$limit": 0},
            {"$project": {"_id": 1}},
        ],
    )

    assert effect.writeback is True
    assert effect.domain is SearchStageDomain.WRITEBACK
    assert effect.requires_full_set is True
    assert plan.strategy is SearchPipelineStrategy.EMPTY


def test_full_set_domains_and_metadata_dependencies_are_semantic() -> None:
    sort = search_stage_effect({"$sort": {"score": -1}})
    exact = search_stage_effect(
        {"$project": {"matches": "$searchHighlights", "score": {"$meta": "textScore"}}},
    )
    similar_name = search_stage_effect(
        {
            "$project": {
                "searchHighlightsPreview": "$ordinary",
                "ordinary": "$prefix-searchHighlights-suffix",
            },
        },
    )

    assert sort.domain is SearchStageDomain.FULL_SET
    assert sort.requires_full_set is True
    assert exact.metadata_dependencies == ("searchHighlights", "textScore")
    assert similar_name.metadata_dependencies == ()


@pytest.mark.parametrize(
    "factory",
    [
        lambda: SearchWindow(skip=True),
        lambda: SearchWindow(limit=1.5),
        lambda: SearchPipelinePlan(strategy="full"),
        lambda: SearchPipelinePlan(mode="optimized"),
        lambda: SearchPipelinePlan(result_limit_hint=True),
        lambda: SearchPipelinePlan(residual_pipeline=[]),
        lambda: SearchPipelinePlan(ownership=("engine",)),
    ],
)
def test_plan_value_objects_reject_untyped_impossible_states(factory) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()


def _valid_stage_effect(**overrides: object) -> SearchStageEffect:
    values = {
        "operator": "$match",
        "domain": SearchStageDomain.DOCUMENT,
        "cardinality": CardinalityEffect.FILTER,
        "order": OrderEffect.PRESERVE,
        "monotonic": True,
        "topk_safe": False,
        "downstream_filter_safe": True,
        **overrides,
    }
    return SearchStageEffect(**values)


@pytest.mark.parametrize(
    "factory",
    [
        lambda: _valid_stage_effect(operator=""),
        lambda: _valid_stage_effect(domain="document"),
        lambda: _valid_stage_effect(monotonic=1),
        lambda: _valid_stage_effect(metadata_dependencies=["textScore"]),
        lambda: _valid_stage_effect(
            metadata_dependencies=("textScore", "textScore"),
        ),
        lambda: _valid_stage_effect(writeback=True),
        lambda: _valid_stage_effect(requires_full_set=True),
        lambda: SearchPhaseOwnership("search", SearchPhaseOwner.ENGINE),
        lambda: SearchPhaseOwnership(SearchPlanPhase.SEARCH, "engine"),
        lambda: SearchPipelinePlan(window=object()),
        lambda: SearchPipelinePlan(downstream_filter_spec=[]),
        lambda: SearchPipelinePlan(
            ownership=(
                SearchPhaseOwnership(
                    SearchPlanPhase.SEARCH,
                    SearchPhaseOwner.ENGINE,
                ),
                SearchPhaseOwnership(
                    SearchPlanPhase.SEARCH,
                    SearchPhaseOwner.CURSOR,
                ),
            ),
        ),
    ],
)
def test_search_plan_value_objects_reject_every_cross_field_contradiction(
    factory,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        factory()
