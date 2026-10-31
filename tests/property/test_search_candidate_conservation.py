"""Candidate conservation against native MQL captures over arbitrary subuniverses."""

from copy import deepcopy
from pathlib import Path

from hypothesis import example, given, seed, settings, strategies as st

from mongoeco.engines._memory_vector_runtime import (
    build_materialized_vector_index,
    candidate_positions_for_vector_filter,
    candidate_rows_for_vector_filter,
)
from mongoeco.types import SearchIndexDefinition

from tests.differential._real_parity_base import _decode_capture
from tests.differential.semantic_search_eligibility_cases import FILTERS, SEED
from tests.property._config import PROPERTY_SEED


IDS = tuple(doc["_id"] for doc in SEED)
DEFINITION = SearchIndexDefinition(
    {"fields": [{"type": "vector", "path": "embedding", "numDimensions": 2}]},
    name="vectors",
    index_type="vectorSearch",
)
NATIVE = _decode_capture(
    (
        Path(__file__).parents[1] / "fixtures/semantic_search_eligibility_9_0.json"
    ).read_text()
)["cases"]["semantic_search_eligibility"]


def assert_conservation(expected, universe, matches, metadata):
    candidates = universe if matches is None else set(matches)
    assert expected <= candidates <= universe
    if metadata["exact"]:
        assert candidates == expected


@seed(PROPERTY_SEED)
@settings(max_examples=60, deadline=None)
@given(
    selected=st.sets(st.sampled_from(IDS)),
    invalid_vectors=st.sets(st.sampled_from(IDS)),
)
@example(selected=set(IDS), invalid_vectors=set())
def test_candidates_conserve_native_matches_in_the_same_phase_domain(
    selected, invalid_vectors
):
    documents = [deepcopy(doc) for doc in SEED if doc["_id"] in selected]
    for doc in documents:
        if doc["_id"] in invalid_vectors:
            doc.pop("embedding")
    index = build_materialized_vector_index(documents, DEFINITION)
    vector_positions = index.vector_row_positions.get("embedding", ())
    vector_universe = selected - invalid_vectors
    for name, query in FILTERS.items():
        truth = set(NATIVE[name])
        positions, metadata = candidate_positions_for_vector_filter(
            index, filter_spec=query
        )
        position_ids = (
            None if positions is None else [documents[p]["_id"] for p in positions]
        )
        assert_conservation(truth & selected, selected, position_ids, metadata)
        rows, metadata = candidate_rows_for_vector_filter(
            index, query_path="embedding", filter_spec=query
        )
        row_ids = (
            None
            if rows is None
            else [documents[vector_positions[row]]["_id"] for row in rows]
        )
        assert_conservation(truth & vector_universe, vector_universe, row_ids, metadata)
