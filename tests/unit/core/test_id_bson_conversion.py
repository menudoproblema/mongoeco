"""Numeric BSON type identity survives conversion evaluation and persistent roots."""

from dataclasses import replace

import pytest

from mongoeco.core.bson_scalars import BsonInt32, BsonInt64
from mongoeco.core.identity import document_id_bson_type_changed
from mongoeco.core.operators import UpdateEngine
from mongoeco.engines._memory_collection import MemoryCollection


def test_id_type_observer_distinguishes_width_in_nested_arrays_without_wrapper_noise():
    assert document_id_bson_type_changed(
        {"_id": {"a": [1]}}, {"_id": {"a": [BsonInt64(1)]}}
    )
    assert not document_id_bson_type_changed(
        {"_id": {"a": [1]}}, {"_id": {"a": [BsonInt32(1)]}}
    )
    assert not document_id_bson_type_changed({"_id": False}, {"_id": False})


@pytest.mark.parametrize("compiled", [True, False])
def test_pipeline_conversion_owns_numeric_policy_without_leaking_to_caller(compiled):
    plan = UpdateEngine.compile_update_plan(
        [
            {
                "$set": {
                    "text": {"$toString": 2},
                    "nullable": {"$toLong": None},
                    "_id": {"$toLong": 1},
                }
            }
        ]
    )
    if not compiled:
        plan = replace(plan, compiled_pipeline=None)
    variables = {"user": "unchanged"}
    document = {"_id": 1}
    assert plan.apply(document, variables=variables)
    assert document == {"_id": BsonInt64(1), "text": "2", "nullable": None}
    assert variables == {"user": "unchanged"}
    assert not plan.apply(document, variables=variables)


def test_rekey_keeps_natural_order_and_shared_snapshot_and_rejects_collisions():
    rows = MemoryCollection([(0, "first"), (1, "middle"), (2, "last")])
    snapshot = rows.copy()
    rows.replace_key(1, "one")
    assert list(rows.items()) == [(0, "first"), ("one", "middle"), (2, "last")]
    assert list(snapshot.items()) == [(0, "first"), (1, "middle"), (2, "last")]
    assert rows.ordinal_for("one") == snapshot.ordinal_for(1)
    rows.replace_key("one", "one")
    with pytest.raises(KeyError):
        rows.replace_key("one", 2)
    assert list(rows.items()) == [(0, "first"), ("one", "middle"), (2, "last")]
