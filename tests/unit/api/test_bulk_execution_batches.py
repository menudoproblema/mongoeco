from unittest.mock import patch

import mongoeco.api._async._collection_bulk as bulk_module
from mongoeco.api._async._collection_bulk import PreparedBulkWriteRequest
from mongoeco.types import DeleteOne, InsertOne, UpdateOne


def _prepared(index, request):
    return PreparedBulkWriteRequest(index=index, request=request)


def test_classic_ordered_bulk_batches_keep_contiguous_family_runs():
    requests = [
        _prepared(0, InsertOne({"_id": 0})),
        _prepared(1, UpdateOne({"_id": 1}, {"$set": {"seen": True}})),
        _prepared(2, InsertOne({"_id": 2})),
    ]

    batches = bulk_module._classic_bulk_batches(requests, ordered=True)

    assert [[request.index for request in batch] for batch in batches] == [[0], [1], [2]]


def test_classic_unordered_bulk_batches_group_families_and_obey_batch_limit():
    assert bulk_module._CLASSIC_BULK_BATCH_LIMIT == 100_000
    requests = [
        _prepared(0, InsertOne({"_id": 0})),
        _prepared(1, UpdateOne({"_id": 1}, {"$set": {"seen": True}})),
        _prepared(2, InsertOne({"_id": 2})),
        _prepared(3, DeleteOne({"_id": 3})),
        _prepared(4, InsertOne({"_id": 4})),
    ]

    with patch.object(bulk_module, "_CLASSIC_BULK_BATCH_LIMIT", 2):
        batches = bulk_module._classic_bulk_batches(requests, ordered=False)

    assert [[request.index for request in batch] for batch in batches] == [
        [0, 2],
        [4],
        [1],
        [3],
    ]
