"""Document comparison preserves dialect overrides and transparent ownership."""

from types import MappingProxyType

import pytest

from mongoeco.compat import MongoDialect70
from mongoeco.core.bson_ordering import bson_type_rank
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import CollationSpec, compare_with_collation


class _DescendingNumbers(MongoDialect70):
    def compare_values(self, left, right):
        if type(left) is int and type(right) is int:
            return (right > left) - (right < left)
        return super().compare_values(left, right)


@pytest.mark.parametrize("collation", [None, CollationSpec(locale="en", strength=2)])
def test_nested_documents_keep_the_dialects_value_comparator(collation):
    left, right = {"a": [{"x": 1}]}, {"a": [{"x": 2}]}
    assert compare_with_collation(
        left, right, dialect=MongoDialect70(), collation=collation
    ) < 0
    assert compare_with_collation(
        left, right, dialect=_DescendingNumbers(), collation=collation
    ) > 0


def test_container_ownership_does_not_change_bson_type_rank():
    marked = DocumentCodec.to_internal({"a": 1})
    dialect = MongoDialect70()
    assert bson_type_rank(marked, dialect.bson_type_order) == bson_type_rank(
        {"a": 1}, dialect.bson_type_order
    )
    assert dialect.policy.compare_values(marked, {"a": 1}) == 0


def test_explicit_subclass_type_rank_still_overrides_container_fallback():
    class CustomDocument(dict):
        pass

    ranks = dict(MongoDialect70().bson_type_order)
    custom_rank = 42
    ranks[CustomDocument] = custom_rank
    assert bson_type_rank(CustomDocument(a=1), MappingProxyType(ranks)) == custom_rank
