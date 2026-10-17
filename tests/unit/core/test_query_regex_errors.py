"""Malformed query regex fails during preparation and defensive evaluation."""

import pytest

from mongoeco.api.operations import compile_find_operation
from mongoeco.core._filtering_support import compile_bson_regex, compile_regex
from mongoeco.core.filtering import QueryEngine
from mongoeco.core.query_plan import (
    InCondition,
    RegexCondition,
    compile_filter,
)
from mongoeco.errors import OperationFailure
from mongoeco.types import PlanningMode, Regex


REGEX_ERROR_CODE = 51091


QUERIES = [
    {"name": {"$regex": "["}},
    {"name": Regex("[")},
    {"name": {"$not": {"$regex": "["}}},
    {"name": {"$in": [Regex("[")]}},
    {"name": {"$nin": [Regex("[")]}},
    {"name": {"$all": [Regex("[")]}},
    {"$or": [{"match": False}, {"name": {"$regex": "["}}]},
]


@pytest.mark.parametrize("query", QUERIES)
def test_invalid_query_regex_rejects_without_a_document(query):
    with pytest.raises(OperationFailure) as raised:
        compile_filter(query)
    assert raised.value.code == REGEX_ERROR_CODE
    assert raised.value.details["codeName"] == "Location51091"
    assert raised.value.details["errmsg"]
    assert raised.value.details["ok"] == 0


@pytest.mark.parametrize(
    "plan", [RegexCondition("name", "["), InCondition("name", (Regex("["),))]
)
def test_explicit_plan_evaluation_uses_the_same_regex_diagnostic(plan):
    with pytest.raises(OperationFailure) as raised:
        QueryEngine.match_plan({"name": "Ada"}, plan)
    assert raised.value.code == REGEX_ERROR_CODE


def test_regex_cache_and_bson_flags_keep_valid_behavior():
    assert compile_regex("^a", "i") is compile_regex("^a", "i")
    assert compile_bson_regex(Regex("^a", "ilu")).search("Ada")


def test_relaxed_regex_reports_a_deferred_query_issue():
    operation = compile_find_operation(
        {"name": {"$regex": "["}}, planning_mode=PlanningMode.RELAXED
    )
    assert operation.planning_issues[0].scope == "query"
