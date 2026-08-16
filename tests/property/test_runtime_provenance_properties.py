from __future__ import annotations

from copy import deepcopy
from itertools import pairwise

from hypothesis import given, seed, strategies as st

from mongoeco.core.aggregation.runtime_state import apply_pipeline_states
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    RuntimeMetadata,
    RuntimeMetadataKey,
    RuntimeVirtualField,
)

from tests.property._config import PROPERTY_SEED


_METADATA_VALUES = st.recursive(
    st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-10, max_value=10),
        st.text(max_size=12),
    ),
    lambda children: st.one_of(
        st.lists(children, max_size=4),
        st.dictionaries(st.text(min_size=1, max_size=8), children, max_size=4),
    ),
    max_leaves=12,
)


def _pipeline_strategy() -> st.SearchStrategy[list[dict[str, object]]]:
    return st.sampled_from(
        [
            [
                {"$match": {"kind": "kept"}},
                {"$sort": {"_id": 1}},
                {"$skip": 0},
                {"$limit": 2},
            ],
            [{"$project": {"_id": 1, "copied": "$searchHighlights"}}],
            [{"$set": {"copied": "$searchHighlights"}}],
            [{"$addFields": {"copied": "$searchHighlights"}}],
            [{"$unset": "searchHighlights"}],
            [{"$replaceRoot": {"newRoot": "$$ROOT"}}],
            [{"$replaceWith": "$$ROOT"}],
            [{"$unwind": "$items"}],
            [
                {
                    "$group": {
                        "_id": "$kind",
                        "captured": {"$push": "$searchHighlights"},
                    },
                },
            ],
            [
                {
                    "$facet": {
                        "kept": [
                            {
                                "$project": {
                                    "_id": 1,
                                    "copied": "$searchHighlights",
                                },
                            },
                        ],
                    },
                },
            ],
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": "foreign_id",
                        "foreignField": "_id",
                        "as": "joined",
                    },
                },
            ],
            [{"$unionWith": "foreign"}],
        ],
    ).map(deepcopy)


def _state(identifier: object, value: object) -> RuntimeDocumentState:
    return (
        RuntimeDocumentState(
            {
                "_id": identifier,
                "kind": "kept",
                "foreign_id": "foreign",
                "items": [{"value": 1}, {"value": 2}],
                "__mongoeco_user_data": {"kept": True},
                "searchHighlights": {"caller": identifier},
            },
        )
        .with_metadata_value(RuntimeMetadataKey.SEARCH_HIGHLIGHTS, value)
        .with_virtual_field(
            "searchHighlights",
            value,
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
    )


def _contains_runtime_value(value: object) -> bool:
    if isinstance(value, (RuntimeDocumentState, RuntimeMetadata, RuntimeVirtualField)):
        return True
    if isinstance(value, dict):
        return any(
            key.startswith("\x00mongoeco") or _contains_runtime_value(item)
            for key, item in value.items()
        )
    if isinstance(value, list | tuple):
        return any(_contains_runtime_value(item) for item in value)
    return False


@seed(PROPERTY_SEED)
@given(value=_METADATA_VALUES, pipeline=_pipeline_strategy())
def test_runtime_provenance_never_aliases_or_leaks_internal_state(
    value: object,
    pipeline: list[dict[str, object]],
) -> None:
    local = _state("local", value)
    foreign = _state("foreign", value)
    original_public = local.public_document()
    original_persistence = local.persistence_document()
    original_pipeline = deepcopy(pipeline)

    result = apply_pipeline_states(
        [local],
        pipeline,
        collection_resolver=lambda name: [foreign] if name == "foreign" else [],
    )
    message = (
        f"seed={PROPERTY_SEED} value={value!r} pipeline={pipeline!r} result={result!r}"
    )

    assert pipeline == original_pipeline, message
    assert local.public_document() == original_public, message
    assert local.persistence_document() == original_persistence, message
    assert original_persistence["searchHighlights"] == {"caller": "local"}, message
    assert all(isinstance(state, RuntimeDocumentState) for state in result), message
    for state in result:
        public = state.public_document()
        persisted = state.persistence_document()
        assert not _contains_runtime_value(public), message
        assert not _contains_runtime_value(persisted), message
        public["mutatedByCaller"] = True
        assert "mutatedByCaller" not in state.public_document(), message
    for first, second in pairwise(result):
        assert first.metadata is not second.metadata, message
