"""Consumer keys retain representation, collation and partition coherence."""

from functools import partial
from uuid import UUID

import pytest

from bson import BSON, Binary, Code, DBRef as SDKDBRef
from hypothesis import given, settings, strategies as st

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80
from mongoeco.core.aggregation.array_string_expressions import (
    _build_set_lookup,
    _set_contains,
)
from mongoeco.core.aggregation.runtime import aggregation_equality_key
from mongoeco.core.aggregation.spill import AggregationGroupSpool
from mongoeco.core.codec import DocumentCodec
from mongoeco.core.collation import CollationSpec
from mongoeco.types import DBRef


@settings(max_examples=80, deadline=None)
@given(
    name=st.text(alphabet="abcdefgh", min_size=1, max_size=8),
    identifier=st.one_of(st.integers(-(2**31), 2**31 - 1), st.text(max_size=8)),
    tenant=st.integers(-(2**31), 2**31 - 1),
    collated=st.booleans(),
)
def test_group_document_aliases_have_equal_serialized_partition_keys(
    name, identifier, tenant, collated
):
    sdk = SDKDBRef(name, identifier, tag=tenant)
    document = dict(sdk.as_doc())
    local = DBRef(name, identifier, extras={"tag": tenant})
    assert BSON.encode({"v": sdk}) == BSON.encode({"v": document})
    collation = CollationSpec(locale="en", strength=2) if collated else None
    key = partial(aggregation_equality_key, collation=collation)
    spool = AggregationGroupSpool(
        threshold=1,
        codec=DocumentCodec,
        group_id_for_document=lambda doc: doc["v"],
        group_key_for_id=key,
        deadline=None,
    )
    variants = [
        sdk,
        document,
        local,
        DocumentCodec.to_internal(sdk),
        DocumentCodec.decode(DocumentCodec.encode(local)),
    ]
    try:
        for representation in variants:
            assert key(representation) == key(document)
            assert DocumentCodec.encode(key(representation)) == DocumentCodec.encode(
                key(document)
            )
            assert spool._group_digest(representation) == spool._group_digest(document)
        assert key(SDKDBRef(name, identifier, tag=tenant + 1)) != key(document)
        assert key(dict(reversed(list(document.items())))) != key(document)
    finally:
        spool.close()


@settings(max_examples=80, deadline=None)
@given(
    payload=st.binary(max_size=16),
    subtype=st.sampled_from((0, 5, 128)),
    reverse=st.booleans(),
)
def test_bounded_set_lookup_matches_independent_binary_equivalence(
    payload, subtype, reverse
):
    left, right = payload, Binary(payload, subtype)
    if reverse:
        left, right = right, left
    expected = BSON.encode({"v": left}) == BSON.encode({"v": right})
    lookup, residual = _build_set_lookup([right], dialect=MONGODB_DIALECT_70)
    assert not residual
    assert (
        _set_contains(
            left, lookup=lookup, residual=residual, dialect=MONGODB_DIALECT_70
        )
        == expected
    )


@pytest.mark.parametrize(
    "value", [Code("x"), Code("x", {}), Code("x", {"n": 1}), 1, 1.0, {"a": 1}]
)
def test_set_residual_retains_noneligible_values_and_collation(value):
    for dialect, collation in (
        (MONGODB_DIALECT_70, None),
        (MONGODB_DIALECT_80, None),
        (MONGODB_DIALECT_70, CollationSpec(locale="en", strength=2)),
    ):
        lookup, residual = _build_set_lookup(
            [value], dialect=dialect, collation=collation
        )
        assert lookup == set()
        assert residual == [value]
        assert _set_contains(
            value,
            lookup=lookup,
            residual=residual,
            dialect=dialect,
            collation=collation,
        )


def test_set_uuid_uses_the_same_standard_binary_atom():
    value = UUID("12345678-1234-5678-1234-567812345678")
    lookup, residual = _build_set_lookup([value], dialect=MONGODB_DIALECT_70)
    assert _set_contains(
        Binary(value.bytes, 4),
        lookup=lookup,
        residual=residual,
        dialect=MONGODB_DIALECT_70,
    )
