"""Image guards may prune only events rejected by the original pipeline."""

from copy import deepcopy

from bson import Code
from hypothesis import given, settings, strategies as st

from mongoeco._change_streams.pipeline import pre_image_match
from mongoeco.compat import MONGODB_DIALECT_90
from mongoeco.core.aggregation import apply_pipeline
from mongoeco.core.filtering import QueryEngine


_STAGES = [
    {"$set": {"kind": "$operationType"}},
    {"$set": {"kind": Code("$operationType")}},
    {"$set": {"kind": "$fullDocument.kind"}},
    {"$set": {"kind": "insert"}},
    {"$set": {"operationType": "insert"}},
    {"$set": {"documentKey._id": 2}},
    {"$set": {"alias": "$kind"}},
    {"$unset": "kind"},
    {"$unset": "fullDocument"},
    {"$project": {"_id": 1, "kind": 1, "operationType": 1}},
    {"$project": {"_id": 1, "kind": "$operationType", "fullDocument": 1}},
    {"$project": {"fullDocument": 0}},
]


@settings(max_examples=250, deadline=None)
@given(
    stages=st.lists(st.sampled_from(_STAGES), max_size=4),
    event=st.fixed_dictionaries(
        {
            "operationType": st.sampled_from(["insert", "update", "delete"]),
            "capturedImage": st.sampled_from(
                [None, {"kind": "insert"}, {"kind": "update"}]
            ),
            "resolvedImage": st.sampled_from(
                [None, {"kind": "update"}, {"kind": "delete"}]
            ),
        }
    ),
    field=st.sampled_from(
        ["kind", "alias", "operationType", "documentKey._id", "fullDocument.kind"]
    ),
    operand=st.sampled_from([None, "insert", "update", 1, 2]),
    logical=st.sampled_from([None, "$and", "$or", "$nor"]),
)
def test_guard_never_loses_a_matching_event(stages, event, field, operand, logical):
    document = {
        "_id": {"_data": "opaque"},
        "operationType": event["operationType"],
        "documentKey": {"_id": 1},
        "fullDocument": deepcopy(event["capturedImage"]),
    }
    predicate = {field: operand}
    if logical is not None:
        predicate = {logical: [predicate, {"operationType": "update"}]}
    pipeline = [*deepcopy(stages), {"$match": predicate}]
    resolved = deepcopy(document)
    resolved["fullDocument"] = deepcopy(event["resolvedImage"])
    result = apply_pipeline([resolved], pipeline, dialect=MONGODB_DIALECT_90)
    if result:
        guard = pre_image_match(pipeline, MONGODB_DIALECT_90)
        assert guard is None or QueryEngine.match_plan(
            document, guard, dialect=MONGODB_DIALECT_90
        )
