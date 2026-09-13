"""Catalog views are immutable; explicit public value copies remain mutable."""

from copy import deepcopy

import pytest

from mongoeco.engines._sqlite_index_catalog import SQLiteCatalogPool, SQLiteIndexCatalog
from mongoeco.types import Binary, EngineIndexRecord


def record(**kwargs):
    return EngineIndexRecord(
        name="key_1", fields=["key"], key=[("key", 1)], unique=False, **kwargs
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.append(None),
        lambda value: value.clear(),
        lambda value: value.extend([]),
        lambda value: value.insert(0, None),
        lambda value: value.pop(),
        lambda value: value.remove(value[0]),
        lambda value: value.reverse(),
        lambda value: value.sort(),
        lambda value: value.__setitem__(0, None),
        lambda value: value.__delitem__(0),
        lambda value: value.__iadd__([]),
        lambda value: value.__imul__(0),
        lambda value: setattr(value, "_ttl_indexes", ()),
    ],
)
def test_catalog_rejects_mutators(mutation):
    catalog = SQLiteIndexCatalog([record(expire_after_seconds=0)])
    with pytest.raises(TypeError, match="immutable"):
        mutation(catalog)
    assert catalog.ttl_indexes == tuple(catalog)
    assert deepcopy(catalog) is catalog


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.__setitem__("active", "changed"),
        lambda value: value.__delitem__("active"),
        lambda value: value.__ior__({"active": False}),
        lambda value: value.clear(),
        lambda value: value.pop("active"),
        lambda value: value.popitem(),
        lambda value: value.setdefault("other", True),
        lambda value: value.update({"active": False}),
    ],
)
def test_nested_documents_reject_mutators(mutation):
    catalog = SQLiteIndexCatalog([record(partial_filter_expression={"active": True})])
    with pytest.raises(TypeError, match="immutable"):
        mutation(catalog[0].partial_filter_expression)
    assert catalog[0].partial_filter_expression == {"active": True}


def test_catalog_owns_nested_values_and_public_copies():
    partial = {"key": {"$in": ["one", "two"]}}
    index = record(partial_filter_expression=partial, collation={"locale": "en"})
    catalog = SQLiteIndexCatalog([index])
    partial["key"]["$in"].clear()
    index.fields.append("other")
    assert catalog[0].fields == ["key"]
    assert catalog[0].partial_filter_expression["key"]["$in"] == ["one", "two"]
    with pytest.raises(TypeError, match="immutable"):
        catalog[0].partial_filter_expression["key"]["$in"].append("bad")
    public = catalog[0].to_definition().to_list_document()
    public["partialFilterExpression"]["key"]["$in"].clear()
    public["collation"]["locale"] = "simple"
    assert catalog[0].partial_filter_expression["key"]["$in"] == ["one", "two"]
    assert catalog[0].collation == {"locale": "en"}
    assert not catalog.ttl_indexes


def test_opaque_bson_values_are_not_shared_by_shallow_freezing():
    value = Binary(b"payload", subtype=0)
    assert (
        SQLiteIndexCatalog.from_records(
            [record(partial_filter_expression={"key": value})]
        )
        is None
    )
    assert SQLiteIndexCatalog.from_records([record()]) is not None


def test_pool_reuses_only_equal_content_without_retaining_generation_history():
    pool = SQLiteCatalogPool()
    namespace = "test", "records"
    original = SQLiteIndexCatalog([record()])
    pool.remember(namespace, ((0,),), original)
    assert pool.find(namespace, ((0,),)) is original
    for generation in range(1, 100):
        current = SQLiteIndexCatalog([record(expire_after_seconds=generation)])
        rows = ((generation,),)
        assert pool.find(namespace, rows) is None
        pool.remember(namespace, rows, current)
        assert pool.find(namespace, rows) is current
        assert len(pool._entries) == 1
    assert pool.find(namespace, ((0,),)) is None
    assert original[0].expire_after_seconds is None
    pool.clear(namespace)
    assert pool.find(namespace, ((99,),)) is None
    pool.remember(namespace, ((0,),), original)
    pool.clear()
    assert pool.find(namespace, ((0,),)) is None
