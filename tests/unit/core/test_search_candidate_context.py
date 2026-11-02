"""Candidate keys have an explicit, conservative semantic-context domain."""

import inspect

from dataclasses import replace
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from mongoeco import MongoClient, SearchIndexModel
from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90
from mongoeco.core import filtering
from mongoeco.core.expression_context import ExpressionExecutionContext
from mongoeco.core.operation_context import OperationContext
from mongoeco.core.search_filter_prefilter import supports_mql_candidate_prefilter
from mongoeco.engines import MemoryEngine, SQLiteEngine


@pytest.mark.parametrize(
    "dialect", [MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90]
)
def test_candidate_context_keeps_builtins_and_declines_unproven_clones(dialect):
    assert supports_mql_candidate_prefilter(OperationContext.create(dialect=dialect))
    clone = replace(dialect, label="unproven")
    assert not supports_mql_candidate_prefilter(OperationContext.create(dialect=clone))
    assert not supports_mql_candidate_prefilter(
        OperationContext.create(
            dialect=dialect, collation={"locale": "en", "strength": 2}
        )
    )


def test_legacy_candidate_context_retains_builtin_seven_semantics():
    assert supports_mql_candidate_prefilter(None)


@pytest.mark.parametrize("engine_name", ["memory", "sqlite"])
def test_search_residual_prepares_once_per_operation_in_productive_routes(engine_name):
    engine = MemoryEngine() if engine_name == "memory" else SQLiteEngine()
    with MongoClient(engine) as client:
        collection = client.residual.rows
        collection.insert_many(
            [{"_id": i, "title": "hello", "v": i} for i in range(10)]
        )
        collection.create_search_index(
            SearchIndexModel({"mappings": {"dynamic": True}}, name="text")
        )
        preparations = []
        decisions = []
        compile_original = filtering.compile_filter
        match_original = filtering.QueryEngine.match_plan

        def compile_counted(*args, **kwargs):
            if inspect.currentframe().f_back.f_code.co_qualname.endswith(
                "_lazy_filter_predicate.<locals>.matches"
            ):
                preparations.append(args[0])
            return compile_original(*args, **kwargs)

        def match_counted(*args, **kwargs):
            if inspect.currentframe().f_back.f_code.co_qualname.endswith(
                "_lazy_filter_predicate.<locals>.matches"
            ):
                decisions.append(args[0]["_id"])
            return match_original(*args, **kwargs)

        pipeline = [
            {"$search": {"index": "text", "text": {"path": "title", "query": "hello"}}},
            {"$match": {"$expr": {"$gte": ["$v", "$$needle"]}}},
            {"$sort": {"_id": 1}},
        ]
        needles = (0, 1)
        with (
            patch.object(filtering, "compile_filter", compile_counted),
            patch.object(filtering.QueryEngine, "match_plan", match_counted),
        ):
            for needle in needles:
                pipeline[1] = {"$match": {"v": {"$gte": needle}}}
                assert [
                    row["_id"]
                    for row in collection.aggregate(
                        pipeline,
                        let={"needle": needle},
                        collation={"locale": "en", "strength": 2},
                    )
                ] == list(range(needle, 10))
        assert len(preparations) == len(needles)
        assert decisions == list(range(10)) * len(needles)


def test_residual_execution_clocks_are_distinct_and_stable():
    first = ExpressionExecutionContext(now=datetime(2020, 1, 1, tzinfo=UTC))
    second = ExpressionExecutionContext(now=datetime(2021, 1, 1, tzinfo=UTC))
    query = {"$expr": {"$eq": ["$at", "$$NOW"]}}
    one = filtering._lazy_filter_predicate(query, variables=first)
    two = filtering._lazy_filter_predicate(query, variables=second)
    assert one({"at": first.now})
    assert not one({"at": second.now})
    assert two({"at": second.now})
    assert not two({"at": first.now})
    assert one({"at": first.now})
