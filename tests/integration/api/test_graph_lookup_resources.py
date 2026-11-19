"""Observable failure and recovery when a graph exceeds its local budget."""

from unittest.mock import patch

import pytest

from mongoeco import MongoClient
from mongoeco.core.aggregation.lookup_physical import BoundedLookupHashPlan
from mongoeco.core.aggregation.stages import apply_pipeline
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


@pytest.mark.parametrize("engine_factory", [MemoryEngine, SQLiteEngine])
@pytest.mark.parametrize("dialect", ["7.0", "8.0", "9.0"])
def test_graph_lookup_candidate_budget_falls_back_without_changing_results(
    engine_factory, dialect
):
    chain_length = 8
    original = BoundedLookupHashPlan.candidate_indices

    def observe(limit):
        index_calls = []

        def candidates(plan, frontier):
            indices = original(plan, frontier)
            index_calls.append(indices)
            return indices

        with (
            MongoClient(
                engine_factory(aggregation_materialization_limit=limit),
                mongodb_dialect=dialect,
            ) as client,
            patch.object(BoundedLookupHashPlan, "candidate_indices", candidates),
        ):
            database = client.graph_candidates
            database.roots.insert_one({"_id": 0})
            database.graph_foreign.insert_many(
                [
                    {"_id": k, "parent_id": [k - 1, 99]}
                    for k in range(1, chain_length + 1)
                ]
            )
            (result,) = database.roots.aggregate([{"$graphLookup": GRAPH_SPEC}])
            return sorted(
                (doc["_id"], doc["depth"]) for doc in result["children"]
            ), index_calls

    indexed, calls = observe(100)
    fallback, fallback_calls = observe(1)
    assert indexed == fallback == [(k, k - 1) for k in range(1, chain_length + 1)]
    assert calls == [[k] for k in range(chain_length)] + [[]]
    assert fallback_calls == []


@pytest.mark.parametrize(
    ["override", "code"],
    [
        ({"as": "a..b"}, 15998),
        ({"connectFromField": "$reserved"}, 16410),
        ({"depthField": "bad\x00field"}, 16411),
        ({"from": 1}, 9),
        ({"restrictSearchWithMatch": {"$or": [{"$where": "true"}]}}, 2),
    ],
)
def test_graph_lookup_checks_grammar_on_empty_input(override, code):
    with (
        MongoClient(MemoryEngine(), mongodb_dialect="9.0") as client,
        pytest.raises(OperationFailure) as failure,
    ):
        list(
            client.graph_validation.roots.aggregate(
                [{"$graphLookup": GRAPH_SPEC | override}]
            )
        )
    assert failure.value.code == code


@pytest.mark.parametrize("operand", [[], {"from": "graph_foreign"}])
def test_graph_lookup_rejects_non_documents_and_missing_options(operand):
    with (
        MongoClient(MemoryEngine(), mongodb_dialect="9.0") as client,
        pytest.raises(OperationFailure) as failure,
    ):
        list(client.graph_validation.roots.aggregate([{"$graphLookup": operand}]))
    expected_code = 9
    assert failure.value.code == expected_code


def test_graph_lookup_checks_resource_resolution_and_foreign_identity():
    pipeline = [{"$graphLookup": GRAPH_SPEC}]
    root = {"_id": 1}
    assert apply_pipeline([], pipeline) == []
    with pytest.raises(
        OperationFailure, match="requires collection resolver support"
    ) as unsupported:
        apply_pipeline([root], pipeline)
    unsupported_code = 115
    assert unsupported.value.code == unsupported_code
    with pytest.raises(OperationFailure) as failure:
        apply_pipeline(
            [root],
            pipeline,
            collection_resolver=lambda _name: [{"parent_id": 1}],
        )
    expected_code = 40271
    assert failure.value.code == expected_code


def test_graph_lookup_deduplicates_seeds_and_rejects_depth_overwriting_identity():
    with MongoClient(MemoryEngine(), mongodb_dialect="9.0") as client:
        database = client.graph_identity
        database.roots.insert_one({"_id": 0})
        database.graph_foreign.insert_many(
            [
                {"_id": 1, "parent_id": 0},
                {"_id": 2, "parent_id": 0},
            ]
        )
        spec = GRAPH_SPEC | {"startWith": [0, 0], "maxDepth": 0}
        (result,) = database.roots.aggregate([{"$graphLookup": spec}])
        assert sorted(doc["_id"] for doc in result["children"]) == [1, 2]
        with pytest.raises(OperationFailure) as failure:
            list(
                database.roots.aggregate(
                    [{"$graphLookup": spec | {"depthField": "_id"}}]
                )
            )
        expected_code = 2398003
        assert failure.value.code == expected_code
