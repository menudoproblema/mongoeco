"""Coherence across real codec representations, independent of BSON truth keys."""

from bson import Binary as SDKBinary
from hypothesis import given, settings, strategies as st

from mongoeco.compat import MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90
from mongoeco.core.bson_ordering import bson_equality_key, bson_type_rank
from mongoeco.core.codec import DocumentCodec
from mongoeco.types import Binary


FORMS = st.sampled_from(("local", "sdk", "internal", "decoded", "public", "bytes"))
DIALECTS = st.sampled_from((MONGODB_DIALECT_70, MONGODB_DIALECT_80, MONGODB_DIALECT_90))


def represented(payload, subtype, form):
    if form == "bytes" and subtype == 0:
        return payload
    if form == "local":
        return Binary(payload, subtype)
    value = SDKBinary(payload, subtype)
    if form in {"sdk", "bytes"}:
        return value
    if form == "decoded":
        return DocumentCodec.decode(DocumentCodec.encode(value))
    value = DocumentCodec.to_internal({"v": value})["v"]
    return DocumentCodec.to_pymongo(value) if form == "public" else value


@settings(max_examples=100, deadline=None)
@given(
    payload=st.binary(max_size=32),
    subtype=st.sampled_from((0, 5, 128)),
    left_form=FORMS,
    right_form=FORMS,
    dialect=DIALECTS,
)
def test_same_binary_has_same_rank_equality_and_key(
    payload, subtype, left_form, right_form, dialect
):
    left = represented(payload, subtype, left_form)
    right = represented(payload, subtype, right_form)
    assert bson_type_rank(left, dialect.bson_type_order) == bson_type_rank(
        right, dialect.bson_type_order
    )
    assert dialect.compare_values(left, right) == 0
    assert dialect.values_equal(left, right)
    assert bson_equality_key(left) == bson_equality_key(right)


@settings(max_examples=100, deadline=None)
@given(
    payload=st.binary(max_size=32), left_form=FORMS, right_form=FORMS, dialect=DIALECTS
)
def test_different_subtypes_never_collapse(payload, left_form, right_form, dialect):
    left = represented(payload, 0, left_form)
    right = represented(payload, 5, right_form)
    assert dialect.compare_values(left, right) < 0
    assert not dialect.values_equal(left, right)
    assert bson_equality_key(left) != bson_equality_key(right)


@settings(max_examples=100, deadline=None)
@given(
    payload=st.binary(max_size=32),
    a=st.sampled_from((0, 5, 128)),
    b=st.sampled_from((0, 5, 128)),
    left_form=FORMS,
    right_form=FORMS,
    dialect=DIALECTS,
)
def test_binary_equality_implies_same_semantic_key(  # noqa: PLR0913 - independent generated axes
    payload, a, b, *, left_form, right_form, dialect
):
    left = represented(payload, a, left_form)
    right = represented(payload, b, right_form)
    if dialect.values_equal(left, right):
        assert bson_equality_key(left) == bson_equality_key(right)
