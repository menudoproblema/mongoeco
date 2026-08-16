from __future__ import annotations

import pytest

from mongoeco.core.aggregation.runtime_state import (
    _detach_virtual_fields,
    apply_pipeline_states,
)
from mongoeco.core.runtime_metadata import (
    RuntimeDocumentState,
    RuntimeMetadataKey,
)
from mongoeco.errors import OperationFailure


def _state(document: dict | None = None) -> RuntimeDocumentState:
    highlights = [
        {"path": "title", "texts": [{"type": "hit", "value": "one"}]},
        {"path": "body", "texts": [{"type": "hit", "value": "two"}]},
    ]
    return (
        RuntimeDocumentState(document or {"_id": 1, "kind": "kept"})
        .with_metadata_value(
            RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
            highlights,
        )
        .with_virtual_field(
            "searchHighlights",
            highlights,
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
    )


def _state_with_virtual_value(value: object) -> RuntimeDocumentState:
    return (
        RuntimeDocumentState({"_id": 1, "__mongoeco_user_data": {"kept": True}})
        .with_metadata_value(RuntimeMetadataKey.SEARCH_HIGHLIGHTS, value)
        .with_virtual_field(
            "searchHighlights",
            value,
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
    )


def test_selection_order_and_window_preserve_runtime_state() -> None:
    states = [_state({"_id": 2, "kind": "kept"}), _state({"_id": 1, "kind": "kept"})]

    result = apply_pipeline_states(
        states,
        [
            {"$match": {"searchHighlights.0.path": "title"}},
            {"$sort": {"_id": 1}},
            {"$skip": 0},
            {"$limit": 1},
        ],
    )

    assert [item.public_document()["_id"] for item in result] == [1]
    assert result[0].public_document()["searchHighlights"][0]["path"] == "title"
    assert "searchHighlights" not in result[0].persistence_document()


def test_projection_preserves_implicit_alias_but_materializes_explicit_copy() -> None:
    result = apply_pipeline_states(
        [_state()],
        [
            {
                "$project": {
                    "_id": 1,
                    "searchHighlights": 1,
                    "copied": "$searchHighlights",
                },
            },
        ],
    )[0]

    assert result.public_document()["searchHighlights"][0]["path"] == "title"
    assert result.persistence_document()["copied"][1]["path"] == "body"
    assert "searchHighlights" not in result.persistence_document()


def test_set_overwrite_and_unset_remove_virtual_provenance() -> None:
    overwritten = apply_pipeline_states(
        [_state()],
        [{"$set": {"searchHighlights": "caller"}}],
    )[0]
    assert overwritten.public_document()["searchHighlights"] == "caller"
    assert overwritten.persistence_document()["searchHighlights"] == "caller"

    removed = apply_pipeline_states(
        [_state()],
        [{"$unset": "searchHighlights"}],
    )[0]
    assert "searchHighlights" not in removed.public_document()
    assert not removed.metadata.virtual_fields


def test_unwind_fans_out_virtual_value_without_making_it_persistable() -> None:
    result = apply_pipeline_states(
        [_state()],
        [{"$unwind": "$searchHighlights"}],
    )

    assert [item.public_document()["searchHighlights"]["path"] for item in result] == [
        "title",
        "body",
    ]
    assert all("searchHighlights" not in item.persistence_document() for item in result)


def test_group_materializes_explicit_metadata_reference() -> None:
    result = apply_pipeline_states(
        [_state()],
        [
            {
                "$group": {
                    "_id": None,
                    "captured": {"$push": "$searchHighlights"},
                },
            },
        ],
    )[0]

    assert result.persistence_document()["captured"][0][0]["path"] == "title"
    assert not result.metadata.virtual_fields


def test_facet_keeps_branch_provenance_scoped_and_non_persistable() -> None:
    result = apply_pipeline_states(
        [_state()],
        [{"$facet": {"kept": [{"$project": {"_id": 1, "searchHighlights": 1}}]}}],
    )[0]

    assert result.public_document()["kept"][0]["searchHighlights"][0]["path"] == "title"
    assert "searchHighlights" not in result.persistence_document()["kept"][0]


def test_lookup_does_not_mix_local_and_foreign_provenance() -> None:
    local = _state({"_id": 1, "foreign_id": "f1"})
    foreign = _state({"_id": "f1", "kind": "foreign"})
    result = apply_pipeline_states(
        [local],
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
        collection_resolver=lambda name: [foreign] if name == "foreign" else [],
    )[0]

    public = result.public_document()
    persisted = result.persistence_document()
    assert public["searchHighlights"][0]["path"] == "title"
    assert public["joined"][0]["searchHighlights"][1]["path"] == "body"
    assert "searchHighlights" not in persisted
    assert "searchHighlights" not in persisted["joined"][0]


def test_union_with_keeps_each_stream_state_independent() -> None:
    local = _state({"_id": "local"})
    foreign = _state({"_id": "foreign"})
    result = apply_pipeline_states(
        [local],
        [{"$unionWith": "foreign"}],
        collection_resolver=lambda name: [foreign] if name == "foreign" else [],
    )

    assert [item.public_document()["_id"] for item in result] == ["local", "foreign"]
    assert all(item.metadata.virtual_fields for item in result)
    assert result[0].metadata is not result[1].metadata


def test_replace_root_rebases_nested_virtual_provenance() -> None:
    nested = RuntimeDocumentState(
        {"_id": 1, "nested": {"value": 2}}
    ).with_virtual_field(
        "nested.searchHighlights",
        [{"path": "nested"}],
        source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    )
    result = apply_pipeline_states(
        [nested],
        [{"$replaceRoot": {"newRoot": "$nested"}}],
    )[0]

    assert result.public_document()["searchHighlights"] == [{"path": "nested"}]
    assert result.persistence_document() == {"value": 2}


def test_projection_and_add_fields_cover_remove_exclusion_and_shadowing() -> None:
    excluded = apply_pipeline_states(
        [_state()],
        [{"$project": {"searchHighlights": 0}}],
    )[0]
    removed = apply_pipeline_states(
        [_state()],
        [{"$set": {"searchHighlights": "$$REMOVE", "copy": "$kind"}}],
    )[0]
    real = _state({"_id": 1, "searchHighlights": "real"})
    shadowed = apply_pipeline_states(
        [real],
        [{"$project": {"searchHighlights": 1}}],
    )[0]
    computed_remove = apply_pipeline_states(
        [_state()],
        [{"$project": {"kind": 1, "removed": "$$REMOVE"}}],
    )[0]

    assert "searchHighlights" not in excluded.public_document()
    assert removed.persistence_document() == {"_id": 1, "kind": "kept", "copy": "kept"}
    assert shadowed.persistence_document()["searchHighlights"] == "real"
    assert "removed" not in computed_remove.persistence_document()
    with pytest.raises(OperationFailure, match="addFields requires"):
        apply_pipeline_states([_state()], [{"$addFields": []}])


def test_replace_root_supports_root_and_structured_expression_provenance() -> None:
    root = apply_pipeline_states(
        [_state()],
        [{"$replaceRoot": {"newRoot": "$$ROOT"}}],
    )[0]
    structured = apply_pipeline_states(
        [_state()],
        [
            {
                "$replaceRoot": {
                    "newRoot": {
                        "wrapped": ["$$ROOT"],
                    },
                },
            },
        ],
    )[0]

    assert root.public_document()["searchHighlights"][0]["path"] == "title"
    assert (
        structured.public_document()["wrapped"][0]["searchHighlights"][1]["path"]
        == "body"
    )
    assert "searchHighlights" not in structured.persistence_document()["wrapped"][0]

    nested = (
        RuntimeDocumentState({"nested": {"value": 1}, "other": {}})
        .with_virtual_field(
            "nested.searchHighlights",
            [{"path": "nested"}],
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
        .with_virtual_field(
            "other.searchHighlights",
            [{"path": "other"}],
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
    )
    copied = apply_pipeline_states(
        [nested],
        [{"$replaceRoot": {"newRoot": {"copied": "$nested"}}}],
    )[0]
    literal = apply_pipeline_states(
        [nested],
        [{"$replaceRoot": {"newRoot": {"$literal": {"value": 1}}}}],
    )[0]
    assert copied.public_document()["copied"]["searchHighlights"]
    assert literal.persistence_document() == {"value": 1}

    rebased = apply_pipeline_states(
        [nested],
        [{"$replaceRoot": {"newRoot": "$nested"}}],
    )[0]
    assert rebased.public_document()["searchHighlights"] == [{"path": "nested"}]


def test_detach_ignores_virtual_mapping_absent_from_stage_output() -> None:
    state = _state()
    detached = _detach_virtual_fields(
        state,
        {"_id": 1},
        [("missing", state.metadata.virtual_fields[0])],
    )

    assert detached.persistence_document() == {"_id": 1}
    assert not detached.metadata.virtual_fields


def test_unwind_rebases_virtual_array_indices() -> None:
    state = RuntimeDocumentState({"_id": 1, "items": [{}, {}]}).with_virtual_field(
        "items.1.searchHighlights",
        [{"path": "second"}],
        source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    )

    result = apply_pipeline_states([state], [{"$unwind": "$items"}])

    assert "searchHighlights" not in result[0].public_document()["items"]
    assert result[1].public_document()["items"]["searchHighlights"] == [
        {"path": "second"},
    ]

    shadowed = RuntimeDocumentState({"items": [1, 2]}).with_virtual_field(
        "items",
        ["virtual"],
        source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    )
    assert [
        item.persistence_document()["items"]
        for item in apply_pipeline_states([shadowed], [{"$unwind": "$items"}])
    ] == [1, 2]


def test_structural_stages_require_resolvers_and_valid_facet_shapes() -> None:
    with pytest.raises(OperationFailure, match="facet requires"):
        apply_pipeline_states([_state()], [{"$facet": []}])
    with pytest.raises(OperationFailure, match="field names"):
        apply_pipeline_states([_state()], [{"$facet": {1: []}}])
    with pytest.raises(OperationFailure, match="lookup requires"):
        apply_pipeline_states(
            [_state()],
            [
                {
                    "$lookup": {
                        "from": "foreign",
                        "localField": "_id",
                        "foreignField": "_id",
                        "as": "joined",
                    },
                },
            ],
        )
    with pytest.raises(OperationFailure, match="unionWith requires"):
        apply_pipeline_states([_state()], [{"$unionWith": "foreign"}])


def test_lookup_pipeline_and_union_pipeline_keep_scopes_independent() -> None:
    foreign = _state({"_id": "foreign", "owner": 1})
    lookup = apply_pipeline_states(
        [_state({"_id": 1})],
        [
            {
                "$lookup": {
                    "from": "foreign",
                    "let": {"owner": "$_id"},
                    "pipeline": [{"$match": {"$expr": {"$eq": ["$owner", "$$owner"]}}}],
                    "as": "joined",
                },
            },
        ],
        collection_resolver=lambda _name: [foreign],
    )[0]
    union = apply_pipeline_states(
        [_state({"_id": "local", "kind": "keep"})],
        [{"$unionWith": {"coll": "foreign", "pipeline": [{"$limit": 1}]}}],
        collection_resolver=lambda _name: [foreign],
    )

    assert lookup.public_document()["joined"][0]["searchHighlights"]
    assert [state.public_document()["_id"] for state in union] == [
        "local",
        "foreign",
    ]


@pytest.mark.parametrize(
    ["stage", "expected_key"],
    [
        ({"$count": "total"}, "total"),
        ({"$sortByCount": "$kind"}, "count"),
        (
            {
                "$bucket": {
                    "groupBy": "$_id",
                    "boundaries": [0, 2, 4],
                    "default": "other",
                },
            },
            "count",
        ),
        ({"$bucketAuto": {"groupBy": "$_id", "buckets": 2}}, "count"),
        (
            {
                "$setWindowFields": {
                    "sortBy": {"_id": 1},
                    "output": {"row": {"$documentNumber": {}}},
                },
            },
            "row",
        ),
    ],
)
def test_materializing_group_family_drops_implicit_provenance(
    stage,
    expected_key,
) -> None:
    result = apply_pipeline_states(
        [_state({"_id": 1, "kind": "a"}), _state({"_id": 2, "kind": "b"})],
        [stage],
    )

    assert result
    assert expected_key in result[0].persistence_document()
    assert all(not state.metadata.virtual_fields for state in result)


def test_documents_and_sample_zero_replace_the_runtime_stream() -> None:
    replaced = apply_pipeline_states(
        [_state()],
        [{"$documents": [{"_id": "replacement"}]}],
    )
    sampled = apply_pipeline_states(replaced, [{"$sample": {"size": 0}}])

    assert replaced[0].persistence_document() == {"_id": "replacement"}
    assert sampled == []


def test_generic_stage_fallback_uses_only_persistable_documents() -> None:
    result = apply_pipeline_states([_state()], [{"$redact": "$$KEEP"}])

    assert result[0].persistence_document() == {"_id": 1, "kind": "kept"}
    assert not result[0].metadata.virtual_fields


@pytest.mark.parametrize(
    "value",
    [
        7,
        {"path": "title", "detail": {"offset": 1}},
        ["first", "second"],
        [[{"path": "nested"}], [{"path": "other"}]],
    ],
)
@pytest.mark.parametrize(
    "stage",
    [
        {
            "$project": {
                "_id": 1,
                "__mongoeco_user_data": 1,
                "copied": "$searchHighlights",
            },
        },
        {"$addFields": {"copied": "$searchHighlights"}},
        {
            "$replaceWith": {
                "_id": "$_id",
                "__mongoeco_user_data": "$__mongoeco_user_data",
                "copied": "$searchHighlights",
            },
        },
    ],
)
def test_explicit_metadata_materialization_is_shape_independent(
    value: object,
    stage: dict,
) -> None:
    result = apply_pipeline_states([_state_with_virtual_value(value)], [stage])[0]

    persisted = result.persistence_document()
    assert persisted["copied"] == value
    assert persisted["__mongoeco_user_data"] == {"kept": True}
    assert "searchHighlights" not in persisted


@pytest.mark.parametrize(
    "stage",
    [
        {"$match": {"_id": 1}},
        {"$sort": {"_id": 1}},
        {"$skip": 0},
        {"$limit": 1},
        {"$project": {"_id": 1, "searchHighlights": 1}},
        {"$set": {"ordinary": True}},
        {"$addFields": {"ordinary": True}},
    ],
)
def test_implicit_metadata_preservation_is_shape_independent(stage: dict) -> None:
    value = [[{"path": "nested", "texts": [1, 2]}]]
    result = apply_pipeline_states([_state_with_virtual_value(value)], [stage])[0]

    assert result.public_document()["searchHighlights"] == value
    assert "searchHighlights" not in result.persistence_document()


def test_runtime_state_and_stage_outputs_are_deeply_isolated() -> None:
    source_document = {"_id": 1, "nested": {"values": [1, 2]}}
    virtual_value = [{"texts": [{"value": "Ada"}]}]
    source = (
        RuntimeDocumentState(source_document)
        .with_metadata_value(RuntimeMetadataKey.SEARCH_HIGHLIGHTS, virtual_value)
        .with_virtual_field(
            "searchHighlights",
            virtual_value,
            source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
        )
    )

    first, second = apply_pipeline_states(
        [source, source.with_document({"_id": 2, "nested": {"values": [1, 2]}})],
        [{"$set": {"copy": "$searchHighlights"}}],
    )
    first_public = first.public_document()
    first_public["nested"]["values"].append(3)
    first_public["copy"][0]["texts"][0]["value"] = "mutated"

    assert source_document == {"_id": 1, "nested": {"values": [1, 2]}}
    assert virtual_value == [{"texts": [{"value": "Ada"}]}]
    assert first.persistence_document()["copy"][0]["texts"][0]["value"] == "Ada"
    assert second.persistence_document()["copy"][0]["texts"][0]["value"] == "Ada"
    assert first.metadata is not second.metadata


def test_legal_internal_looking_names_are_always_ordinary_data() -> None:
    source = RuntimeDocumentState(
        {
            "_id": 1,
            "__mongoeco_textScore__": {"nested": [1]},
            "__mongoeco_runtime": "caller",
        },
    )

    result = apply_pipeline_states(
        [source],
        [
            {"$addFields": {"copy": "$__mongoeco_textScore__"}},
            {"$replaceWith": "$$ROOT"},
        ],
    )[0]

    assert result.persistence_document() == {
        "_id": 1,
        "__mongoeco_textScore__": {"nested": [1]},
        "__mongoeco_runtime": "caller",
        "copy": {"nested": [1]},
    }
    assert not result.metadata.entries
    assert not result.metadata.virtual_fields
