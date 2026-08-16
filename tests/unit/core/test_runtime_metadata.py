from __future__ import annotations

from copy import deepcopy

import pytest

from mongoeco.core.runtime_metadata import (
    RUNTIME_METADATA_FIELD,
    VIRTUAL_FIELDS_KEY,
    RuntimeDocumentState,
    RuntimeMaterializationPolicy,
    RuntimeMetadata,
    RuntimeMetadataEntry,
    RuntimeMetadataKey,
    RuntimeVirtualField,
    ensure_runtime_state,
    legacy_document_from_runtime_state,
    prepare_persistence_document,
    prepare_public_document,
    runtime_state_from_legacy_document,
)


def _highlighted_state() -> RuntimeDocumentState:
    highlights = [{"path": "title", "texts": [{"value": "match"}]}]
    return (
        RuntimeDocumentState({"_id": 1, "title": "match"})
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


def test_runtime_state_keeps_virtual_metadata_outside_bson_document() -> None:
    state = _highlighted_state()

    assert "searchHighlights" not in state.document
    assert state.resolve("searchHighlights.0.path") == (True, "title")
    assert prepare_public_document(state)["searchHighlights"][0]["path"] == "title"
    assert "searchHighlights" not in prepare_persistence_document(state)


def test_real_field_shadows_virtual_alias_without_losing_metadata() -> None:
    state = _highlighted_state().with_document(
        {"_id": 1, "title": "match", "searchHighlights": "caller"},
    )

    assert state.resolve("searchHighlights") == (True, "caller")
    assert prepare_public_document(state)["searchHighlights"] == "caller"
    found, metadata = state.metadata_value(RuntimeMetadataKey.SEARCH_HIGHLIGHTS)
    assert found is True
    assert isinstance(metadata, list)


def test_explicit_materialization_creates_owned_persistable_data() -> None:
    state = _highlighted_state()
    materialized = state.materialize_virtual(
        "searchHighlights",
        destination_path="audit.matches",
    )

    persisted = prepare_persistence_document(materialized)
    assert persisted["audit"]["matches"][0]["path"] == "title"
    assert "searchHighlights" not in persisted

    public = prepare_public_document(materialized)
    public["audit"]["matches"][0]["path"] = "mutated"
    assert (
        prepare_public_document(materialized)["audit"]["matches"][0]["path"] == "title"
    )


def test_state_owns_inputs_and_outputs() -> None:
    document = {"_id": 1, "nested": {"value": 1}}
    metadata_value = [{"path": "title"}]
    state = RuntimeDocumentState(document).with_virtual_field(
        "searchHighlights",
        metadata_value,
        source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    )
    document["nested"]["value"] = 2
    metadata_value[0]["path"] = "changed"

    assert state.document["nested"]["value"] == 1
    assert state.resolve("searchHighlights.0.path") == (True, "title")
    copied = deepcopy(state)
    assert copied is not state
    assert copied.document is not state.document


def test_runtime_metadata_rejects_duplicate_keys_and_paths() -> None:
    entry = RuntimeMetadataEntry(RuntimeMetadataKey.TEXT_SCORE, 1.0)
    virtual = RuntimeVirtualField(
        "score",
        1.0,
        RuntimeMetadataKey.TEXT_SCORE,
        RuntimeMaterializationPolicy.VIRTUAL,
    )

    with pytest.raises(ValueError, match="keys must be unique"):
        RuntimeMetadata(entries=(entry, entry))
    with pytest.raises(ValueError, match="paths must be unique"):
        RuntimeMetadata(virtual_fields=(virtual, virtual))


def test_ensure_runtime_state_does_not_share_caller_document() -> None:
    document = {"_id": 1}
    state = ensure_runtime_state(document)
    document["value"] = 2
    assert state.document == {"_id": 1}


@pytest.mark.parametrize(
    ["factory", "error", "message"],
    [
        (
            lambda: RuntimeMetadataEntry("score", 1),
            TypeError,
            "metadata key",
        ),
        (
            lambda: RuntimeVirtualField("", 1, RuntimeMetadataKey.TEXT_SCORE),
            ValueError,
            "field path",
        ),
        (
            lambda: RuntimeVirtualField(
                "score",
                1,
                "textScore",
            ),
            TypeError,
            "field source",
        ),
        (
            lambda: RuntimeVirtualField(
                "score",
                1,
                RuntimeMetadataKey.TEXT_SCORE,
                "virtual",
            ),
            TypeError,
            "policy",
        ),
        (
            lambda: RuntimeMetadata(entries=[]),
            TypeError,
            "entries",
        ),
        (
            lambda: RuntimeMetadata(virtual_fields=[]),
            TypeError,
            "virtual fields",
        ),
        (
            lambda: RuntimeDocumentState([]),
            TypeError,
            "requires a document",
        ),
        (
            lambda: RuntimeDocumentState({}, metadata={}),
            TypeError,
            "requires RuntimeMetadata",
        ),
    ],
)
def test_runtime_contract_rejects_invalid_shapes(factory, error, message) -> None:
    with pytest.raises(error, match=message):
        factory()


def test_runtime_state_mapping_and_path_helpers_cover_nested_arrays() -> None:
    expected_field_count = 2
    state = RuntimeDocumentState({"_id": 1, "items": [{"name": "Ada"}]})
    virtual = state.with_virtual_field(
        "items.1.matches.0",
        {"value": "Ada"},
        source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    )

    assert list(state) == ["_id", "items"]
    assert len(state) == expected_field_count
    assert state["_id"] == 1
    assert virtual.resolve("items.1.matches.0.value") == (True, "Ada")
    assert virtual.resolve("items.8") == (False, None)
    assert virtual.resolve("items.invalid") == (False, None)
    assert virtual.materialize_virtual("missing") is virtual
    assert prepare_public_document({"_id": 2}) == {"_id": 2}
    assert prepare_persistence_document({"_id": 2}) == {"_id": 2}
    assert isinstance(ensure_runtime_state(state), RuntimeDocumentState)
    assert state.resolve("") == (True, state.public_document())
    assert not state.without_virtual_path("missing").metadata.virtual_fields

    invalid_list_path = RuntimeDocumentState({"items": []}).with_virtual_field(
        "items.invalid",
        "ignored",
        source=RuntimeMetadataKey.SEARCH_HIGHLIGHTS,
    )
    assert invalid_list_path.public_document() == {"items": []}


def test_legacy_sidecar_round_trip_is_confined_to_compatibility_boundary() -> None:
    legacy = {
        "_id": 1,
        RUNTIME_METADATA_FIELD: {
            "textScore": 2.0,
            "vectorSearchScore": 0.5,
            "highlights": [{"path": "title"}],
            VIRTUAL_FIELDS_KEY: {
                "searchHighlights": [{"path": "title"}],
                1: "ignored",
                "": "ignored",
            },
        },
    }
    state = runtime_state_from_legacy_document(legacy)

    assert state.resolve(f"{RUNTIME_METADATA_FIELD}.textScore") == (True, 2.0)
    assert state.resolve(f"{RUNTIME_METADATA_FIELD}.vectorSearchScore") == (
        True,
        0.5,
    )
    assert state.resolve(f"{RUNTIME_METADATA_FIELD}.highlights") == (
        True,
        [{"path": "title"}],
    )
    assert legacy_document_from_runtime_state(state) == {
        "_id": 1,
        RUNTIME_METADATA_FIELD: {
            "textScore": 2.0,
            "vectorSearchScore": 0.5,
            "highlights": [{"path": "title"}],
            VIRTUAL_FIELDS_KEY: {
                "searchHighlights": [{"path": "title"}],
            },
        },
    }
    assert legacy_document_from_runtime_state(RuntimeDocumentState({"_id": 2})) == {
        "_id": 2,
    }
    assert runtime_state_from_legacy_document(
        {"_id": 3, RUNTIME_METADATA_FIELD: "invalid"},
    ).document == {"_id": 3}
