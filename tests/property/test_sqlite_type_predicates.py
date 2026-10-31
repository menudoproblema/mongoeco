"""SQL type predicates agree with independent SDK BSON physical type bytes."""

import datetime
import sqlite3

from bson import BSON, Binary, Code, DBRef, Decimal128, Int64, ObjectId
from hypothesis import example, given, settings, strategies as st

from mongoeco.core.codec import DocumentCodec
from mongoeco.core.json_compat import json_dumps_compact
from mongoeco.core.query_plan import compile_filter
from mongoeco.engines.sqlite_query import translate_query_plan


SCALARS = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**31), max_value=2**31 - 1),
    st.integers(min_value=-(2**63), max_value=2**63 - 1).map(Int64),
    st.floats(),
    st.text().filter(lambda s: "\0" not in s),
    st.binary(max_size=16),
    st.binary(min_size=16, max_size=16).map(lambda b: Binary(b, 5)),
    st.sampled_from(
        [
            ObjectId("0123456789abcdef01234567"),
            datetime.datetime(2038, 4, 22, tzinfo=datetime.UTC),
            Decimal128("1.0"),
            {},
            {"v": 1},
            {"$mongoeco": {"type": "bytes", "value": "not a tag"}},
            DBRef("c", 1),
            Code("x"),
            Code("x", {}),
        ]
    ),
)
VALUES = st.one_of(
    SCALARS,
    st.lists(SCALARS, max_size=4),
    st.lists(st.lists(SCALARS, max_size=2), max_size=2),
)
TYPES = (
    ("double", (1,)),
    ("string", (2,)),
    ("object", (3,)),
    ("array", (4,)),
    ("binData", (5,)),
    ("objectId", (7,)),
    ("bool", (8,)),
    ("date", (9,)),
    ("null", (10,)),
    ("int", (16,)),
    ("long", (18,)),
    ("decimal", (19,)),
    ("number", (1, 16, 18, 19)),
)


def _physical_type(value):
    return BSON.encode({"v": value})[4]


@settings(max_examples=250, deadline=None)
@given(value=VALUES, family=st.sampled_from(TYPES), numeric=st.booleans())
@example(value=[Binary(bytes(16), 0)], family=("binData", (5,)), numeric=False)
@example(
    value=[ObjectId("0123456789abcdef01234567")],
    family=("objectId", (7,)),
    numeric=True,
)
@example(
    value=[datetime.datetime(2038, 4, 22, tzinfo=datetime.UTC)],
    family=("date", (9,)),
    numeric=False,
)
def test_type_sql_matches_bson_type_and_one_level_array_membership(
    value, family, numeric
):
    alias, codes = family
    spec = list(codes) if numeric else alias
    expected = _physical_type(value) in codes or (
        isinstance(value, list)
        and any(_physical_type(child) in codes for child in value)
    )
    sql, parameters = translate_query_plan(compile_filter({"v": {"$type": spec}}))
    document = json_dumps_compact(DocumentCodec.encode({"v": value}))
    with sqlite3.connect(":memory:") as connection:
        connection.execute("CREATE TABLE documents(document TEXT)")
        connection.execute("INSERT INTO documents VALUES (?)", (document,))
        # Only compiler SQL is interpolated; the document is a bound value.
        statement = f"SELECT 1 FROM documents WHERE ({sql})"  # noqa: S608
        result = connection.execute(statement, parameters).fetchone()
    assert (result is not None) == expected
