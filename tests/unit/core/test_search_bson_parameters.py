"""Search's declared numeric parameters accept equivalent BSON representations."""

from decimal import Decimal

import pytest

from mongoeco.core.bson_scalars import BsonDecimal128, BsonDouble, BsonInt64
from mongoeco.core.search import compile_search_stage
from mongoeco.errors import OperationFailure


SPECS = (
    ("$search", {"range": {"path": "a", "gt": 2**53}}),
    ("$search", {"equals": {"path": "a", "value": 2**53}}),
    ("$search", {"in": {"path": "a", "value": [2**53]}}),
    ("$search", {"near": {"path": "a", "origin": 2**53, "pivot": 3}}),
    ("$search", {"phrase": {"path": "title", "query": "red fox", "slop": 1}}),
    (
        "$search",
        {
            "autocomplete": {
                "path": "title",
                "query": "red",
                "fuzzy": {"maxEdits": 1, "prefixLength": 0, "maxExpansions": 3},
            }
        },
    ),
    (
        "$search",
        {
            "compound": {
                "should": [{"range": {"path": "a", "gt": 3}}],
                "minimumShouldMatch": 1,
            }
        },
    ),
    (
        "$search",
        {
            "text": {"path": "title", "query": "red"},
            "count": {"type": "lowerBound", "threshold": 2**53},
            "facet": {"path": "a", "type": "number", "numBuckets": 4},
            "highlight": {"path": "title", "maxChars": 33, "maxNumPassages": 2},
        },
    ),
    (
        "$vectorSearch",
        {
            "path": "v",
            "queryVector": [1, 0.25],
            "limit": 1,
            "numCandidates": 2,
            "minScore": 0.1,
        },
    ),
)


def _represented(value):
    if type(value) is int:
        return BsonInt64(value)
    if type(value) is float:
        return BsonDouble(value)
    if isinstance(value, dict):
        return {key: _represented(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_represented(item) for item in value]
    return value


@pytest.mark.parametrize(["operator", "spec"], SPECS)
def test_numeric_parameter_representation_does_not_change_the_search_query(
    operator, spec
):
    assert compile_search_stage(operator, _represented(spec)) == compile_search_stage(
        operator, spec
    )


@pytest.mark.parametrize("value", [True, BsonDecimal128(Decimal(1))])
def test_vector_domain_does_not_expand_to_boolean_or_decimal(value):
    with pytest.raises(OperationFailure, match="finite numeric"):
        compile_search_stage(
            "$vectorSearch", {"path": "v", "queryVector": [value], "limit": 1}
        )


def test_mql_vector_filter_retains_its_bson_types():
    marker = BsonInt64(1)
    query = compile_search_stage(
        "$vectorSearch",
        {
            "path": "v",
            "queryVector": [BsonInt64(1)],
            "limit": BsonInt64(1),
            "filter": {"a": marker},
        },
    )
    assert query.filter_spec["a"] == marker
