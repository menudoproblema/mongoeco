"""Unsupported BSON domains cannot prune candidates in materialized indexes."""

from bson import Binary, Code, DBRef
from hypothesis import given, settings, strategies as st

from mongoeco.engines._memory_vector_runtime import (
    build_materialized_vector_index,
    candidate_positions_for_vector_filter,
    candidate_rows_for_vector_filter,
)
from mongoeco.engines._sqlite_vector_backend import (
    build_sqlite_vector_backend,
    vector_filter_candidate_storage_keys,
)

from tests.property.test_search_candidate_conservation import DEFINITION


@settings(max_examples=40, deadline=None)
@given(
    payload=st.binary(max_size=12),
    variant=st.sampled_from(("bytes", "binary", "code", "dbref", "document")),
    clause=st.sampled_from(("x", {"$in": ["x"]}, {"$gt": 1}, {"$exists": False})),
)
def test_unrepresented_bson_domains_retain_the_full_eligible_universe(
    payload, variant, clause
):
    values = {
        "bytes": payload,
        "binary": Binary(payload, 5),
        "code": Code("x"),
        "dbref": DBRef("items", 1),
        "document": {"$ref": "items", "$id": 1},
    }
    documents = [
        {"_id": 0, "v": "x", "embedding": [1.0, 0.0]},
        {"_id": 1, "v": values[variant], "embedding": [0.0, 1.0]},
    ]
    memory = build_materialized_vector_index(documents, DEFINITION)
    sqlite = build_sqlite_vector_backend(
        db_name="property",
        coll_name="rows",
        definition=DEFINITION,
        physical_name="vectors",
        path="embedding",
        collection_version=1,
        documents=[(doc["_id"], doc) for doc in documents],
    )
    for resolve, index in (
        (candidate_positions_for_vector_filter, memory),
        (
            lambda index, **options: candidate_rows_for_vector_filter(
                index, query_path="embedding", **options
            ),
            memory,
        ),
        (vector_filter_candidate_storage_keys, sqlite),
    ):
        candidates, metadata = resolve(index, filter_spec={"v": clause})
        assert candidates is None or set(candidates) == {0, 1}
        assert metadata["exact"] is False
