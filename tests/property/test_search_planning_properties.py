from __future__ import annotations

from copy import deepcopy

from hypothesis import given, seed, strategies as st

from mongoeco.core.search_planning import (
    SearchPipelineStrategy,
    SearchPlanningMode,
    SearchPlanRejection,
    SearchPlanRule,
    compile_search_pipeline_plan,
)

from tests.property._config import PROPERTY_SEED


def _stage_strategy() -> st.SearchStrategy[dict[str, object]]:
    window = st.one_of(
        st.integers(min_value=0, max_value=8).map(lambda value: {"$skip": value}),
        st.integers(min_value=0, max_value=8).map(lambda value: {"$limit": value}),
    )
    return st.one_of(
        st.sampled_from(
            [
                {"$match": {"kind": "keep"}},
                {"$match": {"$expr": {"$gt": ["$score", 0]}}},
                {"$project": {"_id": 1, "kind": 1}},
                {"$project": {"matches": {"$meta": "searchHighlights"}}},
                {"$set": {"copy": "$searchHighlights"}},
                {"$addFields": {"ordinary": True}},
                {"$unset": "searchHighlights"},
                {"$replaceRoot": {"newRoot": "$$ROOT"}},
                {"$replaceWith": "$$ROOT"},
                {"$unwind": "$items"},
                {"$group": {"_id": "$kind", "count": {"$sum": 1}}},
                {"$facet": {"items": [{"$limit": 1}]}},
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": "kind",
                        "foreignField": "kind",
                        "as": "joined",
                    },
                },
                {"$unionWith": "foreign"},
                {"$sort": {"score": -1}},
            ],
        ).map(deepcopy),
        window,
    )


@st.composite
def _planning_case(
    draw: st.DrawFn,
) -> tuple[str, dict[str, object], list[dict[str, object]], bool]:
    operator = draw(st.sampled_from(("$search", "$searchMeta", "$vectorSearch")))
    specification: dict[str, object] = {
        "text": {"query": "ada", "path": "title"},
    }
    if operator == "$searchMeta":
        specification["count"] = {"type": "total"}
        if draw(st.booleans()):
            specification["facet"] = {"path": "kind", "type": "token"}
    elif operator == "$search" and draw(st.booleans()):
        specification["highlight"] = {"path": "title"}
    elif operator == "$vectorSearch":
        specification = {
            "path": "embedding",
            "queryVector": [1.0, 0.0],
            "numCandidates": 8,
            "limit": 4,
        }
    pipeline = draw(st.lists(_stage_strategy(), min_size=0, max_size=7))
    return operator, specification, pipeline, draw(st.booleans())


@seed(PROPERTY_SEED)
@given(case=_planning_case())
def test_optimized_and_reference_plans_preserve_contractual_invariants(
    case: tuple[str, dict[str, object], list[dict[str, object]], bool],
) -> None:
    operator, specification, pipeline, writeback = case
    original_specification = deepcopy(specification)
    original_pipeline = deepcopy(pipeline)

    optimized = compile_search_pipeline_plan(
        operator,
        specification,
        pipeline,
        writeback=writeback,
    )
    reference = compile_search_pipeline_plan(
        operator,
        specification,
        pipeline,
        writeback=writeback,
        mode=SearchPlanningMode.REFERENCE,
    )

    message = (
        f"seed={PROPERTY_SEED} operator={operator!r} specification={specification!r} "
        f"pipeline={pipeline!r} writeback={writeback!r} "
        f"optimized={optimized.to_document()!r} reference={reference.to_document()!r}"
    )
    assert specification == original_specification, message
    assert pipeline == original_pipeline, message
    assert reference.strategy is SearchPipelineStrategy.FULL, message
    assert reference.applied_rules == (SearchPlanRule.REFERENCE_ORACLE,), message
    assert reference.rejection_reasons == (SearchPlanRejection.REFERENCE_MODE,), message
    assert reference.residual_pipeline == optimized.residual_pipeline, message
    assert reference.effects == optimized.effects, message
    assert reference.collectors == optimized.collectors, message
    assert reference.metadata_dependencies == optimized.metadata_dependencies, message
    assert reference.ownership == optimized.ownership, message
    assert len({item.phase for item in optimized.ownership}) == len(
        optimized.ownership,
    ), message
    if writeback:
        assert optimized.strategy is SearchPipelineStrategy.FULL, message
        assert SearchPlanRejection.WRITEBACK in optimized.rejection_reasons, message
    if operator == "$searchMeta":
        assert optimized.strategy is SearchPipelineStrategy.FULL, message
        assert SearchPlanRejection.METADATA_MODE in optimized.rejection_reasons, message
    if operator == "$vectorSearch":
        assert optimized.downstream_filter_spec is None, message

    rendered = optimized.to_document()
    pipeline.append({"$limit": 99})
    specification["mutated"] = True
    assert optimized.to_document() == rendered, message
