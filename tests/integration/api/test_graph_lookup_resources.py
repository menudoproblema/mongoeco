"""Observable failure and recovery when a graph exceeds its local budget."""

from unittest.mock import patch

import pytest

from mongoeco import MongoClient
from mongoeco.engines.memory import MemoryEngine
from mongoeco.engines.sqlite import SQLiteEngine
from mongoeco.errors import OperationFailure

from tests.differential.semantic_graph_lookup_cases import GRAPH_SPEC


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("allow_disk_use", [False, True])
@pytest.mark.parametrize(
    ["dialect", "expected_code"], [("7.0", 40099), ("8.0", 40099), ("9.0", 292)]
)
def test_graph_lookup_budget_failure_does_not_change_collections(
    engine_factory, allow_disk_use, dialect, expected_code
):
    with MongoClient(engine_factory(), mongodb_dialect=dialect) as client:
        database = client.graph_lookup
        roots = database.roots
        roots.insert_one({"_id": 1})
        foreign = [
            {"_id": index, "parent_id": 1, "payload": "x" * 1024}
            for index in range(2, 8)
        ]
        database.graph_foreign.insert_many(foreign)
        pipeline = [{"$graphLookup": GRAPH_SPEC | {"maxDepth": 0}}]
        with (
            patch("mongoeco.core.aggregation.graph_lookup._MAX_MEMORY_BYTES", 3000),
            pytest.raises(OperationFailure) as failure,
        ):
            list(roots.aggregate(pipeline, allowDiskUse=allow_disk_use))
        assert failure.value.code == expected_code
        assert roots.find_one() == {"_id": 1}
        assert list(database.graph_foreign.find({}, sort=[("_id", 1)])) == foreign
        (result,) = roots.aggregate(pipeline, allowDiskUse=allow_disk_use)
        assert sorted(child["_id"] for child in result["children"]) == list(range(2, 8))
